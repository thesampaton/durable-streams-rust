use super::{
    Arc, CreateStreamResult, CreateWithDataResult, Error, FileStorage, ForkInfo,
    INITIAL_INDEX_CAPACITY, INITIAL_PRODUCERS_CAPACITY, NOTIFY_CHANNEL_CAPACITY, Offset,
    ProducerAppendResult, ProducerState, ReadResult, Result, RwLock, Storage, StreamConfig,
    StreamEntry, StreamMetadata, StreamState,
};
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use tokio::sync::broadcast;
use tracing::warn;

impl Storage for FileStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
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
                    return Ok(CreateStreamResult::AlreadyExists);
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
        let entry = StreamEntry::new(config, file, dir.clone());

        if let Err(e) = self.write_metadata_for(name, &entry) {
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned stream directory");
            }
            return Err(e);
        }
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::super::precheck_append(
            &stream.config,
            stream.state,
            stream.closed,
            name,
            content_type,
        )?;

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        self.append_records(name, &mut stream, &[data])?;
        let ttl_renewed = {
            let StreamEntry {
                config,
                last_seq,
                updated_at,
                ..
            } = &mut *stream;
            super::super::apply_append_metadata(config, last_seq, updated_at, None, Utc::now())
        };
        if ttl_renewed {
            self.write_metadata_for(name, &stream)?;
        }
        Ok(offset)
    }

    fn batch_append(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset> {
        if messages.is_empty() {
            return Err(Error::InvalidHeader {
                header: "Content-Length".to_string(),
                reason: "batch cannot be empty".to_string(),
            });
        }

        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        let pending_seq = super::super::precheck_batch_append(
            &stream.config,
            stream.state,
            stream.closed,
            stream.last_seq.as_deref(),
            name,
            content_type,
            seq,
        )?;
        let seq_changed = pending_seq.is_some();

        self.append_records(name, &mut stream, &messages)?;
        let ttl_renewed = {
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
            )
        };
        if ttl_renewed || seq_changed {
            self.write_metadata_for(name, &stream)?;
        }

        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let needs_ttl_renewal = {
            let stream = stream_arc.read().expect("stream lock poisoned");
            super::super::fork::check_stream_access(&stream.config, stream.state, name)?;
            stream.config.ttl_seconds.is_some()
        };

        if !needs_ttl_renewal {
            let stream = stream_arc.read().expect("stream lock poisoned");
            let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

            if from_offset.is_now() {
                return Ok(ReadResult {
                    messages: Vec::new(),
                    next_offset,
                    at_tail: true,
                    closed: stream.closed,
                });
            }

            if stream.fork_info.is_none() {
                return Self::read_local_file_messages(&stream, from_offset, next_offset);
            }

            let fi = stream.fork_info.clone().expect("checked above");
            let closed = stream.closed;
            let fork_local_messages =
                Self::read_fork_local_messages(&stream, from_offset, &fi.fork_offset)?;
            drop(stream);

            return self.assemble_fork_read(
                from_offset,
                &fi,
                fork_local_messages,
                next_offset,
                closed,
            );
        }

        let mut stream = stream_arc.write().expect("stream lock poisoned");
        super::super::fork::check_stream_access(&stream.config, stream.state, name)?;

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        if from_offset.is_now() {
            super::super::fork::renew_ttl(&mut stream.config);
            self.write_metadata_for(name, &stream)?;
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            });
        }

        if stream.fork_info.is_none() {
            let result = Self::read_local_file_messages(&stream, from_offset, next_offset)?;
            super::super::fork::renew_ttl(&mut stream.config);
            self.write_metadata_for(name, &stream)?;
            return Ok(result);
        }

        let fi = stream.fork_info.clone().expect("checked above");
        let closed = stream.closed;
        let fork_local_messages =
            Self::read_fork_local_messages(&stream, from_offset, &fi.fork_offset)?;
        super::super::fork::renew_ttl(&mut stream.config);
        self.write_metadata_for(name, &stream)?;
        drop(stream);

        self.assemble_fork_read(from_offset, &fi, fork_local_messages, next_offset, closed)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let stream_arc = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?
            .clone();

        {
            let stream = stream_arc.read().expect("stream lock poisoned");

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

        super::super::fork::check_stream_access(&stream.config, stream.state, name)?;

        Ok(super::super::build_stream_metadata(
            stream.config.clone(),
            stream.next_read_seq,
            stream.next_byte_offset,
            stream.closed,
            stream.total_bytes,
            u64::try_from(stream.index.len()).unwrap_or(u64::MAX),
            stream.created_at,
            stream.updated_at,
        ))
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::super::fork::check_stream_access(&stream.config, stream.state, name)?;

        stream.closed = true;
        stream.updated_at = Some(Utc::now());
        super::super::fork::renew_ttl(&mut stream.config);
        self.write_metadata_for(name, &stream)?;

        let _ = stream.notify.send(());
        Ok(())
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

        let now = Utc::now();
        self.append_records(name, &mut stream, &messages)?;

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
            super::super::apply_append_metadata(config, last_seq, updated_at, pending_seq, now);
        }

        stream.producers.insert(
            producer.id.clone(),
            ProducerState {
                epoch: producer.epoch,
                last_seq: producer.seq,
                updated_at: now,
            },
        );

        self.write_metadata_for(name, &stream)?;

        Ok(ProducerAppendResult::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
            next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
            closed: stream.closed,
        })
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
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
            && let Err(e) = self.append_records(name, &mut entry, &messages)
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

    fn exists(&self, name: &str) -> bool {
        let streams = self.streams.read().expect("streams lock poisoned");
        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            super::super::is_stream_visible(&stream.config, stream.state)
        } else {
            false
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let stream_arc = self.get_stream(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");

        if !super::super::is_stream_visible(&stream.config, stream.state) {
            return None;
        }

        Some(stream.notify.subscribe())
    }

    fn cleanup_expired_streams(&self) -> usize {
        let mut streams = self.streams.write().expect("streams lock poisoned");
        let mut expired = Vec::new();

        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
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
            if !super::super::is_stream_visible(&stream.config, stream.state) {
                continue;
            }
            result.push((
                name.clone(),
                super::super::build_stream_metadata(
                    stream.config.clone(),
                    stream.next_read_seq,
                    stream.next_byte_offset,
                    stream.closed,
                    stream.total_bytes,
                    u64::try_from(stream.index.len()).unwrap_or(u64::MAX),
                    stream.created_at,
                    stream.updated_at,
                ),
            ));
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let source_arc = streams
            .get(source_name)
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?
            .clone();

        let (fork_spec, resolved_offset) = {
            let source = source_arc.read().expect("stream lock poisoned");
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

        if let Some(existing_arc) = streams.get(name) {
            let existing = existing_arc.read().expect("stream lock poisoned");
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

        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        let file_len = file.metadata().map_or(0, |m| m.len());
        let entry = StreamEntry {
            config: fork_spec.config,
            index: Vec::with_capacity(INITIAL_INDEX_CAPACITY),
            closed: config.created_closed,
            next_read_seq: fork_read_seq,
            next_byte_offset: fork_byte_offset,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
            file,
            file_len,
            dir: dir.clone(),
            fork_info: Some(ForkInfo {
                source_name: fork_spec.source_name,
                fork_offset: resolved_offset,
            }),
            ref_count: 0,
            state: StreamState::Active,
        };

        if let Err(e) = self.write_metadata_for(name, &entry) {
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned fork directory");
            }
            return Err(e);
        }

        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        if let Some(source_arc) = streams.get(source_name) {
            let mut source = source_arc.write().expect("stream lock poisoned");
            source.ref_count += 1;
            if let Err(e) = self.write_metadata_for(source_name, &source) {
                warn!(%e, stream = source_name, "failed to persist source ref_count after fork creation");
            }
        }

        Ok(CreateStreamResult::Created)
    }
}
