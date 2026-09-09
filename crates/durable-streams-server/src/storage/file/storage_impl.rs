use super::{
    Arc, CreateStreamResult, CreateWithDataResult, Error, FileStorage, ForkInfo, Offset,
    ProducerAppendResult, ProducerState, ReadResult, Result, RwLock, Storage, StreamConfig,
    StreamEntry, StreamMetadata, StreamState,
};
use crate::storage::shared::release_bytes;
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use tokio::sync::broadcast;
use tracing::warn;

impl Storage for FileStorage {
    fn append_batch(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
        close: bool,
    ) -> Result<crate::storage::AppendResult> {
        super::super::shared::validate_batch_shape(&messages, close)?;

        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");
        stream.ensure_available()?;

        let pending_seq = super::super::precheck_batch_append(
            &stream.config,
            stream.state,
            stream.closed,
            stream.last_seq.as_deref(),
            name,
            (!messages.is_empty()).then_some(content_type),
            seq,
        )?;
        let start_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        self.append_transaction(&mut stream, false, |stream| {
            self.append_records(name, stream, &messages)?;
            {
                let StreamEntry {
                    config,
                    last_seq,
                    updated_at,
                    ..
                } = &mut *stream;
                super::super::apply_append_metadata(
                    config,
                    last_seq,
                    updated_at,
                    pending_seq,
                    Utc::now(),
                )?;
            }
            stream.closed |= close;
            self.write_metadata_for(name, stream)?;
            let _ = stream.notify.send(());
            Ok(crate::storage::AppendResult::new(
                start_offset,
                Offset::new(stream.next_read_seq, stream.next_byte_offset),
                stream.closed,
            ))
        })
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        let stream = stream_arc.read().expect("stream lock poisoned");
        stream.ensure_available()?;
        super::super::fork::check_stream_access(&stream.config, stream.state, name)?;
        let pending = if stream.config.ttl_seconds.is_some() {
            drop(stream);
            let mut stream = stream_arc.write().expect("stream lock poisoned");
            stream.ensure_available()?;
            super::super::fork::check_stream_access(&stream.config, stream.state, name)?;
            let pending = Self::prepare_read(&stream, from_offset)?;
            super::super::fork::renew_ttl(&mut stream.config)?;
            self.write_metadata_for(name, &stream)?;
            pending
        } else {
            let pending = Self::prepare_read(&stream, from_offset)?;
            drop(stream);
            pending
        };
        self.finish_read(from_offset, pending)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let stream_arc = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?
            .clone();

        {
            let stream = stream_arc.read().expect("stream lock poisoned");
            stream.ensure_available()?;

            match super::super::fork::evaluate_delete(name, stream.state, stream.ref_count)? {
                super::super::fork::DeleteDisposition::Tombstone => {
                    drop(stream);
                    let mut stream_w = stream_arc.write().expect("stream lock poisoned");
                    stream_w.state = StreamState::Tombstone;
                    self.write_metadata_for(name, &stream_w)?;
                    return Ok(());
                }
                super::super::fork::DeleteDisposition::HardDelete => {}
            }
        }

        let fork_info = self.hard_remove_stream(&mut streams, name)?;

        if let Some(fi) = fork_info {
            self.cascade_delete(&mut streams, &fi.source_name);
        }

        Ok(())
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");
        stream.ensure_available()?;

        super::super::fork::check_stream_access(&stream.config, stream.state, name)?;

        Ok(stream.metadata())
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &super::super::ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");
        stream.ensure_available()?;

        let pending_seq = {
            let StreamEntry {
                config,
                state,
                closed,
                last_seq,
                producers,
                next_read_seq,
                next_byte_offset,
                ..
            } = &mut *stream;

            match super::super::precheck_producer_append(
                config,
                *state,
                *closed,
                last_seq.as_deref(),
                producers,
                name,
                content_type,
                producer,
                &messages,
                seq,
            )? {
                super::super::ProducerAppendPrecheck::Accept { pending_seq } => pending_seq,
                super::super::ProducerAppendPrecheck::Duplicate { epoch, seq } => {
                    return Ok(ProducerAppendResult::Duplicate {
                        epoch,
                        seq,
                        next_offset: Offset::new(*next_read_seq, *next_byte_offset),
                        closed: *closed,
                    });
                }
            }
        };

        self.append_transaction(&mut stream, false, |stream| {
            let now = Utc::now();
            self.append_records(name, stream, &messages)?;

            if should_close {
                stream.closed = true;
            }

            {
                let StreamEntry {
                    config,
                    last_seq,
                    updated_at,
                    ..
                } = &mut *stream;
                super::super::apply_append_metadata(
                    config,
                    last_seq,
                    updated_at,
                    pending_seq,
                    now,
                )?;
            }

            stream.producers.insert(
                producer.id.clone(),
                ProducerState {
                    epoch: producer.epoch,
                    last_seq: producer.seq,
                    updated_at: now,
                },
            );

            self.write_metadata_for(name, stream)?;

            Ok(ProducerAppendResult::Accepted {
                epoch: producer.epoch,
                seq: producer.seq,
                next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                closed: stream.closed,
            })
        })
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: crate::storage::StreamOptions,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        let config = config.resolve(Utc::now())?;
        let should_close = should_close || config.created_closed;
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            stream.ensure_available()?;
            match super::super::fork::evaluate_root_create(
                name,
                &stream.config,
                stream.state,
                stream.ref_count,
                &config,
            ) {
                super::super::fork::ExistingCreateDisposition::RemoveExpired => {
                    drop(stream);
                    self.remove_for_recreate(&mut streams, name)?;
                }
                super::super::fork::ExistingCreateDisposition::AlreadyExists => {
                    return Ok(CreateWithDataResult {
                        status: CreateStreamResult::AlreadyExists,
                        next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                        closed: stream.closed,
                    });
                }
                super::super::fork::ExistingCreateDisposition::Conflict(err) => {
                    return Err(err);
                }
            }
        }

        let dir = self.stream_dir_for_name(name)?;
        super::retry_on_eintr(|| fs::create_dir_all(&dir)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "create stream directory",
                format!("failed to create stream directory {}: {e}", dir.display()),
                &e,
            )
        })?;

        self.validate_stream_dir(&dir)?;
        let file = self.open_stream_file(&dir)?;
        let mut entry = StreamEntry::new(config, file, dir.clone());

        if !messages.is_empty()
            && let Err(e) = self.append_initial_records(name, &mut entry, &messages)
        {
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned stream directory");
            }
            return Err(e);
        }
        if should_close {
            entry.closed = true;
        }

        let next_offset = Offset::new(entry.next_read_seq, entry.next_byte_offset);
        let closed = entry.closed;

        if let Err(e) = self.write_metadata_for(name, &entry) {
            release_bytes(&self.total_bytes, entry.total_bytes);
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned stream directory");
            }
            return Err(e);
        }
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(CreateWithDataResult {
            status: CreateStreamResult::Created,
            next_offset,
            closed,
        })
    }

    fn replace_stream(
        &self,
        name: &str,
        config: crate::storage::StreamOptions,
        messages: Vec<Bytes>,
        closed: bool,
    ) -> Result<crate::storage::AppendResult> {
        let config = config.resolve(Utc::now())?;
        let closed = closed || config.created_closed;
        let streams = self.streams.write().expect("streams lock poisoned");
        let current = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.into()))?;
        let mut current = current.write().expect("stream lock poisoned");
        current.ensure_available()?;
        super::super::fork::check_replace(current.ref_count, current.fork_info.as_ref())?;
        let old_bytes = current.total_bytes;
        let result = self.append_transaction(&mut current, true, |entry| {
            entry.file_len = 0;
            entry.index.clear();
            entry.next_read_seq = 0;
            entry.next_byte_offset = 0;
            entry.total_bytes = 0;
            entry
                .file
                .set_len(0)
                .map_err(|e| Error::Storage(e.to_string()))?;
            entry.config = config;
            entry.closed = closed;
            entry.producers.clear();
            entry.last_seq = None;
            entry.updated_at = None;
            self.append_records(name, entry, &messages)?;
            self.write_metadata_for(name, entry)?;
            Ok(crate::storage::AppendResult::new(
                Offset::new(0, 0),
                Offset::new(entry.next_read_seq, entry.next_byte_offset),
                closed,
            ))
        });
        if result.is_ok() {
            release_bytes(&self.total_bytes, old_bytes);
        }
        result
    }

    fn subscribe(&self, name: &str) -> Result<Option<broadcast::Receiver<()>>> {
        let Some(stream_arc) = self.get_stream(name) else {
            return Ok(None);
        };
        let stream = stream_arc.read().expect("stream lock poisoned");
        stream.ensure_available()?;

        if !super::super::is_stream_visible(&stream.config, stream.state) {
            return Ok(None);
        }

        Ok(Some(stream.notify.subscribe()))
    }

    fn cleanup_expired_streams(&self) -> usize {
        let mut streams = self.streams.write().expect("streams lock poisoned");
        let mut expired = Vec::new();

        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
            if stream.unavailable {
                continue;
            }
            if super::super::is_stream_expired(&stream.config) {
                expired.push((
                    name.clone(),
                    stream.total_bytes,
                    stream.dir.clone(),
                    stream.ref_count,
                    stream.fork_info.clone(),
                ));
            }
        }

        let count = expired.len();
        for (name, _bytes, _dir, ref_count, _fork_info) in expired {
            match super::super::fork::evaluate_expired_cleanup(ref_count) {
                super::super::fork::DeleteDisposition::Tombstone => {
                    if let Some(arc) = streams.get(&name) {
                        let mut stream = arc.write().expect("stream lock poisoned");
                        stream.state = StreamState::Tombstone;
                        if let Err(e) = self.write_metadata_for(&name, &stream) {
                            warn!(%e, stream = name.as_str(), "failed to persist tombstone for expired stream");
                        }
                    }
                }
                super::super::fork::DeleteDisposition::HardDelete => {
                    if let Err(e) = self.remove_for_recreate(&mut streams, &name) {
                        warn!(%e, stream = name.as_str(), "failed to remove expired stream during cleanup");
                    }
                }
            }
        }

        count
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        let mut result = Vec::new();
        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
            stream.ensure_available()?;
            if !super::super::is_stream_visible(&stream.config, stream.state) {
                continue;
            }
            result.push((name.clone(), stream.metadata()));
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn load_subscription_state(&self) -> Result<Option<Vec<u8>>> {
        match fs::read(self.root_dir.join("subscriptions.json")) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::classify_io_failure(
                "file",
                "read subscriptions",
                "failed to read subscription state",
                &e,
            )),
        }
    }

    fn save_subscription_state(&self, state: &[u8]) -> Result<()> {
        use std::io::Write;
        let temporary = self.root_dir.join("subscriptions.json.tmp");
        let result = (|| -> std::io::Result<()> {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            // Remove an abandoned temporary file from an interrupted prior write.
            match fs::remove_file(&temporary) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            let mut file = options.open(&temporary)?;
            file.write_all(state)?;
            file.sync_all()?;
            fs::rename(&temporary, self.root_dir.join("subscriptions.json"))?;
            fs::File::open(&self.root_dir)?.sync_all()
        })();
        result.map_err(|e| {
            Error::classify_io_failure(
                "file",
                "persist subscriptions",
                "failed to persist subscription state",
                &e,
            )
        })
    }

    fn create_fork_with_options(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: crate::storage::StreamOptions,
        options: super::super::ForkOptions,
    ) -> Result<CreateStreamResult> {
        let mut config = config.resolve(Utc::now())?;
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let source_arc = streams
            .get(source_name)
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?
            .clone();

        let (mut fork_spec, resolved_offset) = {
            let source = source_arc.read().expect("stream lock poisoned");
            source.ensure_available()?;
            if options.inherit_content_type {
                config.content_type.clone_from(&source.config.content_type);
            }
            let source_next_offset = Offset::new(source.next_read_seq, source.next_byte_offset);
            super::super::fork::prepare_fork_spec(
                source_name,
                &source.config,
                source.state,
                &source_next_offset,
                fork_offset,
                &config,
            )?
        };

        fork_spec.sub_offset = options.sub_offset;
        let initial_messages = Self::fork_initial_messages(
            &streams,
            source_name,
            &resolved_offset,
            &fork_spec.config,
            &options,
        )?;

        if let Some(existing_arc) = streams.get(name) {
            let existing = existing_arc.read().expect("stream lock poisoned");
            existing.ensure_available()?;
            match super::super::fork::evaluate_fork_create(
                name,
                &existing.config,
                existing.fork_info.as_ref(),
                existing.state,
                existing.ref_count,
                &fork_spec,
            ) {
                super::super::fork::ExistingCreateDisposition::RemoveExpired => {
                    drop(existing);
                    self.remove_for_recreate(&mut streams, name)?;
                }
                super::super::fork::ExistingCreateDisposition::AlreadyExists => {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                super::super::fork::ExistingCreateDisposition::Conflict(err) => {
                    return Err(err);
                }
            }
        }

        let (fork_read_seq, fork_byte_offset) =
            resolved_offset.parse_components().unwrap_or((0, 0));

        let dir = self.stream_dir_for_name(name)?;
        super::retry_on_eintr(|| fs::create_dir_all(&dir)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "create fork directory",
                format!("failed to create fork directory {}: {e}", dir.display()),
                &e,
            )
        })?;
        self.validate_stream_dir(&dir)?;
        let file = self.open_stream_file(&dir)?;

        let mut entry = StreamEntry::new(fork_spec.config, file, dir.clone());
        entry.closed = config.created_closed;
        entry.next_read_seq = fork_read_seq;
        entry.next_byte_offset = fork_byte_offset;
        entry.fork_info = Some(ForkInfo {
            sub_offset: options.sub_offset,
            source_name: fork_spec.source_name,
            fork_offset: resolved_offset,
        });

        if let Err(e) = self.append_initial_records(name, &mut entry, &initial_messages) {
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned fork directory");
            }
            return Err(e);
        }
        if let Err(e) = self.write_metadata_for(name, &entry) {
            release_bytes(&self.total_bytes, entry.total_bytes);
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned fork directory");
            }
            return Err(e);
        }

        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        if let Some(source_arc) = streams.get(source_name) {
            let mut source = source_arc.write().expect("stream lock poisoned");
            source.ensure_available()?;
            source.ref_count += 1;
            if let Err(e) = self.write_metadata_for(source_name, &source) {
                warn!(%e, stream = source_name, "failed to persist source ref_count after fork creation");
            }
        }

        Ok(CreateStreamResult::Created)
    }
}

impl FileStorage {
    fn fork_initial_messages(
        streams: &HashMap<String, Arc<RwLock<StreamEntry>>>,
        source_name: &str,
        resolved_offset: &Offset,
        config: &StreamConfig,
        options: &super::super::ForkOptions,
    ) -> Result<Vec<Bytes>> {
        let mut source_messages = Vec::new();
        if options.sub_offset > 0 {
            let plan = super::super::fork::build_read_plan(source_name, |n| {
                Ok(streams
                    .get(n)
                    .and_then(|arc| arc.read().expect("stream lock poisoned").fork_info.clone()))
            })?;
            for segment in plan {
                let arc = streams
                    .get(&segment.name)
                    .ok_or_else(|| Error::NotFound(segment.name.clone()))?;
                let stream = arc.read().expect("stream lock poisoned");
                stream.ensure_available()?;
                let range = super::super::fork::message_range(
                    &stream.index,
                    resolved_offset,
                    segment.read_up_to.as_ref(),
                    |m| &m.offset,
                );
                source_messages.extend(Self::read_messages(&stream.file, &stream.index[range])?);
            }
        }
        super::super::fork::initial_fork_messages(config, options, source_messages)
    }
}
