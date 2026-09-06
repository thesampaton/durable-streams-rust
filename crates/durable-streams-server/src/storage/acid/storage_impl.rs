use super::{
    AcidStorage, Bytes, ForkInfo, MESSAGES, Offset, ProducerState, Result, STREAMS,
    StoredStreamMeta, StreamConfig, StreamState,
};
use crate::Storage;
use crate::protocol::error::Error;
use crate::protocol::producer::ProducerHeaders;
use crate::storage::shared::{payload_bytes, release_bytes};
use crate::storage::{
    CreateStreamResult, CreateWithDataResult, ForkCreateSpec, ProducerAppendPrecheck,
    ProducerAppendResult, ReadResult, StreamMetadata, apply_append_metadata, fork,
    is_stream_expired, is_stream_visible, precheck_producer_append,
};
use chrono::Utc;
use redb::{ReadableDatabase, ReadableTable};
use tokio::sync::broadcast;
use tracing::warn;

/// Result of checking for an existing fork on a cross-shard database.
enum CrossShardForkResult {
    /// Proceed with creation; carries `(removed_expired_bytes, removed_parent)`.
    Continue(u64, Option<String>),
    /// The fork already exists; the caller should return `AlreadyExists`.
    AlreadyExists,
}

impl Storage for AcidStorage {
    fn append_batch(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
        close: bool,
    ) -> Result<crate::storage::AppendResult> {
        crate::storage::shared::validate_batch_shape(&messages, close)?;

        let batch_bytes = payload_bytes(&messages);
        self.reserve_total_bytes(batch_bytes)?;

        let result = (|| {
            let shard = &self.shards[self.existing_shard_index(name)?];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            let pending_seq = crate::storage::precheck_batch_append(
                &meta.config,
                meta.state,
                meta.closed,
                meta.last_seq.as_deref(),
                name,
                (!messages.is_empty()).then_some(content_type),
                seq,
            )?;

            let start_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            Self::write_messages(
                name,
                &messages,
                batch_bytes,
                self.max_stream_bytes,
                &mut meta,
                &mut message_table,
                "failed to append batch message",
            )?;

            apply_append_metadata(
                &mut meta.config,
                &mut meta.last_seq,
                &mut meta.updated_at,
                pending_seq,
                Utc::now(),
            )?;

            meta.closed |= close;
            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            Self::write_stream_meta(&mut streams, name, &meta)?;

            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit batch append", e))?;

            Ok(crate::storage::AppendResult::new(
                start_offset,
                next_offset,
                meta.closed,
            ))
        })();

        if result.is_err() {
            release_bytes(&self.total_bytes, batch_bytes);
            return result;
        }

        self.notify_stream(name);
        result
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let shard_idx = self.existing_shard_index(name)?;
        let txn = self.shards[shard_idx]
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        fork::check_stream_access(&meta.config, meta.state, name)?;
        if meta.config.ttl_seconds.is_some() {
            drop(streams);
            drop(txn);
            return self.read_with_ttl_renewal(name, from_offset, shard_idx);
        }
        let messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;
        Self::read_snapshot(&streams, &messages, name, from_offset, &meta)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let shard = &self.shards[self.existing_shard_index(name)?];
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        match fork::evaluate_delete(name, meta.state, meta.ref_count)? {
            fork::DeleteDisposition::Tombstone => {
                let mut updated = meta;
                updated.state = StreamState::Tombstone;
                Self::write_stream_meta(&mut streams, name, &updated)?;
                drop(streams);
                txn.commit()
                    .map_err(|e| Self::storage_err("failed to commit soft delete", e))?;
                return Ok(());
            }
            fork::DeleteDisposition::HardDelete => {}
        }

        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;
        Self::delete_stream_messages(&mut messages, name)?;
        drop(messages);

        streams
            .remove(name)
            .map_err(|e| Self::storage_err("failed to remove stream metadata", e))?;

        let fork_info = meta.fork_info.clone();
        let total_bytes = meta.total_bytes;

        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit delete", e))?;

        release_bytes(&self.total_bytes, total_bytes);
        self.drop_notifier(name);

        if let Some(fi) = fork_info {
            self.cascade_delete_acid(&fi.source_name)?;
        }

        Ok(())
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        let shard = &self.shards[self.existing_shard_index(name)?];
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;

        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        fork::check_stream_access(&meta.config, meta.state, name)?;

        Ok(meta.metadata())
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        let batch_bytes = payload_bytes(&messages);
        self.reserve_total_bytes(batch_bytes)?;

        let result = (|| {
            let shard = &self.shards[self.existing_shard_index(name)?];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            let pending_seq = match precheck_producer_append(
                &meta.config,
                meta.state,
                meta.closed,
                meta.last_seq.as_deref(),
                &mut meta.producers,
                name,
                content_type,
                producer,
                &messages,
                seq,
            )? {
                ProducerAppendPrecheck::Accept { pending_seq } => pending_seq,
                ProducerAppendPrecheck::Duplicate { epoch, seq } => {
                    return Ok(ProducerAppendResult::Duplicate {
                        epoch,
                        seq,
                        next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
                        closed: meta.closed,
                    });
                }
            };

            Self::write_messages(
                name,
                &messages,
                batch_bytes,
                self.max_stream_bytes,
                &mut meta,
                &mut message_table,
                "failed to append producer message",
            )?;

            if should_close {
                meta.closed = true;
            }

            let now = Utc::now();
            meta.producers.insert(
                producer.id.clone(),
                ProducerState {
                    epoch: producer.epoch,
                    last_seq: producer.seq,
                    updated_at: now,
                },
            );
            apply_append_metadata(
                &mut meta.config,
                &mut meta.last_seq,
                &mut meta.updated_at,
                pending_seq,
                now,
            )?;

            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            let closed = meta.closed;

            Self::write_stream_meta(&mut streams, name, &meta)?;
            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit producer append", e))?;

            Ok(ProducerAppendResult::Accepted {
                epoch: producer.epoch,
                seq: producer.seq,
                next_offset,
                closed,
            })
        })();

        if result.is_err() || matches!(result, Ok(ProducerAppendResult::Duplicate { .. })) {
            release_bytes(&self.total_bytes, batch_bytes);
        }

        if result.is_ok() && (!messages.is_empty() || should_close) {
            self.notify_stream(name);
        }

        result
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
        let batch_bytes = payload_bytes(&messages);

        let mut reserved = false;
        let mut removed_expired_bytes = 0_u64;
        let mut removed_expired_parent = None;

        let result = (|| {
            let shard_idx = self
                .find_stream_shard_index(name)?
                .unwrap_or_else(|| self.shard_index(name));
            let shard = &self.shards[shard_idx];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            if let Some(existing) = Self::read_stream_meta(&streams, name)? {
                match fork::evaluate_root_create(
                    name,
                    &existing.config,
                    existing.state,
                    existing.ref_count,
                    &config,
                ) {
                    fork::ExistingCreateDisposition::RemoveExpired => {
                        removed_expired_bytes = existing.total_bytes;
                        removed_expired_parent =
                            existing.fork_info.clone().map(|info| info.source_name);
                        Self::delete_stream_messages(&mut message_table, name)?;
                        streams
                            .remove(name)
                            .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                    }
                    fork::ExistingCreateDisposition::AlreadyExists => {
                        return Ok(CreateWithDataResult {
                            status: CreateStreamResult::AlreadyExists,
                            next_offset: Offset::new(
                                existing.next_read_seq,
                                existing.next_byte_offset,
                            ),
                            closed: existing.closed,
                        });
                    }
                    fork::ExistingCreateDisposition::Conflict(err) => {
                        return Err(err);
                    }
                }
            }

            if batch_bytes > 0 {
                self.reserve_total_bytes(batch_bytes)?;
                reserved = true;
            }

            let mut meta = Self::new_stream_meta(config);
            Self::write_messages(
                name,
                &messages,
                batch_bytes,
                self.max_stream_bytes,
                &mut meta,
                &mut message_table,
                "failed to append create-with-data message",
            )?;

            if should_close {
                meta.closed = true;
            }

            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            let closed = meta.closed;

            Self::write_stream_meta(&mut streams, name, &meta)?;
            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit create stream with data", e))?;

            Ok(CreateWithDataResult {
                status: CreateStreamResult::Created,
                next_offset,
                closed,
            })
        })();

        if result.is_err() && reserved {
            release_bytes(&self.total_bytes, batch_bytes);
        }

        if result.is_ok() {
            if removed_expired_bytes > 0 {
                release_bytes(&self.total_bytes, removed_expired_bytes);
                self.drop_notifier(name);
                if let Some(parent) = removed_expired_parent {
                    self.cascade_delete_acid(&parent)?;
                }
            }
            if should_close || !messages.is_empty() {
                self.notify_stream(name);
            }
        }

        result
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
        let bytes = payload_bytes(&messages);
        self.reserve_total_bytes(bytes)?;
        let result = (|| {
            let shard = &self.shards[self.existing_shard_index(name)?];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("replace streams table", e))?;
            let mut records = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("replace messages table", e))?;
            let previous = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.into()))?;
            fork::check_replace(previous.ref_count, previous.fork_info.as_ref())?;
            Self::delete_stream_messages(&mut records, name)?;
            let mut meta = Self::new_stream_meta(config);
            Self::write_messages(
                name,
                &messages,
                bytes,
                self.max_stream_bytes,
                &mut meta,
                &mut records,
                "failed to append create-with-data message",
            )?;
            meta.closed = closed;
            Self::write_stream_meta(&mut streams, name, &meta)?;
            drop(records);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("commit replacement", e))?;
            release_bytes(&self.total_bytes, previous.total_bytes);
            Ok(crate::storage::AppendResult::new(
                Offset::new(0, 0),
                Offset::new(meta.next_read_seq, meta.next_byte_offset),
                closed,
            ))
        })();
        if result.is_err() {
            release_bytes(&self.total_bytes, bytes);
        } else {
            self.notify_stream(name);
        }
        result
    }

    fn subscribe(&self, name: &str) -> Result<Option<broadcast::Receiver<()>>> {
        let Some(shard_idx) = self.find_stream_shard_index(name)? else {
            return Ok(None);
        };
        let txn = self.shards[shard_idx]
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("subscribe read transaction", e))?;
        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("subscribe streams table", e))?;
        let Some(meta) = Self::read_stream_meta(&streams, name)? else {
            return Ok(None);
        };
        if !is_stream_visible(&meta.config, meta.state) {
            return Ok(None);
        }
        Ok(Some(self.notifier_sender(name).subscribe()))
    }

    fn cleanup_expired_streams(&self) -> usize {
        let mut total_removed = 0;

        for shard in &self.shards {
            let Ok(read_txn) = shard.db.begin_read() else {
                continue;
            };
            let Ok(streams_table) = read_txn.open_table(STREAMS) else {
                continue;
            };
            let Ok(iter) = streams_table.iter() else {
                continue;
            };

            let mut candidates: Vec<String> = Vec::new();
            for item in iter {
                let Ok((key, value)) = item else {
                    continue;
                };
                let name = key.value().to_string();
                let Ok(meta) = serde_json::from_slice::<StoredStreamMeta>(value.value()) else {
                    continue;
                };
                if is_stream_expired(&meta.config) {
                    candidates.push(name);
                }
            }

            drop(streams_table);
            drop(read_txn);

            if candidates.is_empty() {
                continue;
            }

            let Ok(txn) = Self::begin_write_txn(&shard.db) else {
                continue;
            };
            let Ok(mut streams) = txn.open_table(STREAMS) else {
                continue;
            };
            let Ok(mut messages) = txn.open_table(MESSAGES) else {
                continue;
            };

            let mut committed = Vec::new();
            for name in &candidates {
                let meta = streams
                    .get(name.as_str())
                    .ok()
                    .flatten()
                    .and_then(|v| serde_json::from_slice::<StoredStreamMeta>(v.value()).ok());
                let Some(meta) = meta else { continue };
                if !is_stream_expired(&meta.config) {
                    continue;
                }

                match fork::evaluate_expired_cleanup(meta.ref_count) {
                    fork::DeleteDisposition::Tombstone => {
                        let mut updated = meta.clone();
                        updated.state = StreamState::Tombstone;
                        let payload = serde_json::to_vec(&updated).ok();
                        if let Some(payload) = payload {
                            let _ = streams.insert(name.as_str(), payload.as_slice());
                        }
                        committed.push((name.clone(), 0, None));
                    }
                    fork::DeleteDisposition::HardDelete => {
                        let _ = Self::delete_stream_messages(&mut messages, name);
                        let _ = streams.remove(name.as_str());
                        committed.push((
                            name.clone(),
                            meta.total_bytes,
                            meta.fork_info.map(|info| info.source_name),
                        ));
                    }
                }
            }

            drop(messages);
            drop(streams);

            match txn.commit() {
                Ok(()) => {
                    let committed_len = committed.len();
                    for (name, bytes, parent) in committed {
                        release_bytes(&self.total_bytes, bytes);
                        self.drop_notifier(&name);
                        if let Some(parent) = parent {
                            let _ = self.cascade_delete_acid(&parent);
                        }
                    }
                    total_removed += committed_len;
                }
                Err(e) => {
                    warn!(%e, "failed to commit expired stream cleanup");
                }
            }
        }

        total_removed
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        let mut result = Vec::new();

        for shard in &self.shards {
            let read_txn = shard
                .db
                .begin_read()
                .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
            let streams_table = read_txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let iter = streams_table
                .iter()
                .map_err(|e| Self::storage_err("failed to iterate streams", e))?;

            for item in iter {
                let (key, value) =
                    item.map_err(|e| Self::storage_err("failed to read stream entry", e))?;
                let name = key.value().to_string();
                let meta: StoredStreamMeta = serde_json::from_slice(value.value())
                    .map_err(|e| Self::storage_err("failed to parse stream metadata", e))?;

                if !is_stream_visible(&meta.config, meta.state) {
                    continue;
                }

                result.push((name, meta.metadata()));
            }
        }

        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn load_subscription_state(&self) -> Result<Option<Vec<u8>>> {
        let txn = self.shards[0]
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("read subscription state", e))?;
        let table = match txn.open_table(super::SUBSCRIPTIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(Self::storage_err("open subscription state", e)),
        };
        Ok(table
            .get("state")
            .map_err(|e| Self::storage_err("read subscription state", e))?
            .map(|v| v.value().to_vec()))
    }

    fn save_subscription_state(&self, state: &[u8]) -> Result<()> {
        let txn = Self::begin_write_txn(&self.shards[0].db)?;
        {
            let mut table = txn
                .open_table(super::SUBSCRIPTIONS)
                .map_err(|e| Self::storage_err("open subscription state", e))?;
            table
                .insert("state", state)
                .map_err(|e| Self::storage_err("write subscription state", e))?;
        }
        txn.commit()
            .map_err(|e| Self::storage_err("commit subscription state", e))
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
        let (source_shard_idx, fork_spec) =
            self.prepare_source_fork(source_name, fork_offset, &mut config, &options)?;

        let (mut removed_expired_bytes, mut removed_expired_parent) =
            match self.remove_cross_shard_existing_fork(name, source_shard_idx, &fork_spec)? {
                CrossShardForkResult::Continue(bytes, parent) => (bytes, parent),
                CrossShardForkResult::AlreadyExists => {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
            };

        let shard = &self.shards[source_shard_idx];
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let mut source_meta = Self::read_stream_meta(&streams, source_name)?
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?;

        let source_next_offset =
            Offset::new(source_meta.next_read_seq, source_meta.next_byte_offset);
        let (mut fork_spec, resolved_offset) = fork::prepare_fork_spec(
            source_name,
            &source_meta.config,
            source_meta.state,
            &source_next_offset,
            fork_offset,
            &config,
        )?;
        fork_spec.sub_offset = options.sub_offset;
        let initial_messages = Self::fork_initial_messages(
            &streams,
            &messages,
            source_name,
            &resolved_offset,
            &fork_spec.config,
            &options,
        )?;

        if let Some(existing) = Self::read_stream_meta(&streams, name)? {
            match fork::evaluate_fork_create(
                name,
                &existing.config,
                existing.fork_info.as_ref(),
                existing.state,
                existing.ref_count,
                &fork_spec,
            ) {
                fork::ExistingCreateDisposition::RemoveExpired => {
                    removed_expired_bytes = existing.total_bytes;
                    removed_expired_parent =
                        existing.fork_info.clone().map(|info| info.source_name);
                    Self::delete_stream_messages(&mut messages, name)?;
                    streams
                        .remove(name)
                        .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                }
                fork::ExistingCreateDisposition::AlreadyExists => {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                fork::ExistingCreateDisposition::Conflict(err) => {
                    return Err(err);
                }
            }
        }

        let batch_bytes = payload_bytes(&initial_messages);
        self.reserve_total_bytes(batch_bytes)?;
        let result = (|| {
            let mut fork_meta = Self::build_fork_stored_meta(&fork_spec, &config, &resolved_offset);
            Self::write_messages(
                name,
                &initial_messages,
                batch_bytes,
                self.max_stream_bytes,
                &mut fork_meta,
                &mut messages,
                "failed to append create-with-data message",
            )?;
            Self::write_stream_meta(&mut streams, name, &fork_meta)?;
            source_meta.ref_count += 1;
            Self::write_stream_meta(&mut streams, source_name, &source_meta)?;
            Ok(())
        })();
        drop(messages);
        drop(streams);
        let result = result.and_then(|()| {
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit create fork", e))
        });
        if result.is_err() {
            release_bytes(&self.total_bytes, batch_bytes);
        }
        result?;

        self.cleanup_expired_and_notify(name, removed_expired_bytes, removed_expired_parent)?;

        Ok(CreateStreamResult::Created)
    }
}

/// Private helpers extracted from long `Storage` trait methods.
impl AcidStorage {
    /// Read path when the stream has a TTL that needs renewal (write transaction).
    fn read_with_ttl_renewal(
        &self,
        name: &str,
        from_offset: &Offset,
        shard_idx: usize,
    ) -> Result<ReadResult> {
        let shard = &self.shards[shard_idx];
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        let messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;
        let result = Self::read_snapshot(&streams, &messages, name, from_offset, &meta)?;
        drop(messages);

        fork::renew_ttl(&mut meta.config)?;
        Self::write_stream_meta(&mut streams, name, &meta)?;
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit ttl renewal", e))?;

        Ok(result)
    }

    /// Insert records and advance metadata within the caller's transaction.
    fn write_messages(
        name: &str,
        messages: &[Bytes],
        batch_bytes: u64,
        max_stream_bytes: u64,
        meta: &mut StoredStreamMeta,
        message_table: &mut redb::Table<'_, (&str, u64, u64), &[u8]>,
        operation: &'static str,
    ) -> Result<()> {
        if meta.total_bytes + batch_bytes > max_stream_bytes {
            return Err(Error::StreamSizeLimitExceeded);
        }
        for data in messages {
            let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
            message_table
                .insert(
                    (name, meta.next_read_seq, meta.next_byte_offset),
                    data.as_ref(),
                )
                .map_err(|e| Self::storage_err(operation, e))?;
            meta.next_read_seq += 1;
            meta.next_byte_offset += len;
            meta.total_bytes += len;
        }
        Ok(())
    }

    /// Check and remove an existing fork that lives on a different shard than
    /// the source stream. Returns removed bytes/parent info, or
    /// `Ok(Some(result))` to signal the caller should return early.
    #[allow(clippy::type_complexity)]
    fn remove_cross_shard_existing_fork(
        &self,
        name: &str,
        source_shard_idx: usize,
        fork_spec: &ForkCreateSpec,
    ) -> Result<CrossShardForkResult> {
        let Some(existing_shard_idx) = self.find_stream_shard_index(name)? else {
            return Ok(CrossShardForkResult::Continue(0, None));
        };
        if existing_shard_idx == source_shard_idx {
            return Ok(CrossShardForkResult::Continue(0, None));
        }

        let existing_shard = &self.shards[existing_shard_idx];
        let existing_txn = Self::begin_write_txn(&existing_shard.db)?;
        let mut existing_streams = existing_txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut existing_messages = existing_txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let Some(existing) = Self::read_stream_meta(&existing_streams, name)? else {
            return Ok(CrossShardForkResult::Continue(0, None));
        };

        match fork::evaluate_fork_create(
            name,
            &existing.config,
            existing.fork_info.as_ref(),
            existing.state,
            existing.ref_count,
            fork_spec,
        ) {
            fork::ExistingCreateDisposition::RemoveExpired => {
                let removed_bytes = existing.total_bytes;
                let removed_parent = existing.fork_info.clone().map(|info| info.source_name);
                Self::delete_stream_messages(&mut existing_messages, name)?;
                existing_streams
                    .remove(name)
                    .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                drop(existing_messages);
                drop(existing_streams);
                existing_txn.commit().map_err(|e| {
                    Self::storage_err("failed to commit expired cross-shard fork removal", e)
                })?;
                Ok(CrossShardForkResult::Continue(
                    removed_bytes,
                    removed_parent,
                ))
            }
            fork::ExistingCreateDisposition::AlreadyExists => {
                Ok(CrossShardForkResult::AlreadyExists)
            }
            fork::ExistingCreateDisposition::Conflict(err) => Err(err),
        }
    }

    /// Construct the `StoredStreamMeta` for a new fork stream.
    fn build_fork_stored_meta(
        fork_spec: &ForkCreateSpec,
        config: &StreamConfig,
        resolved_offset: &Offset,
    ) -> StoredStreamMeta {
        let (fork_read_seq, fork_byte_offset) =
            resolved_offset.parse_components().unwrap_or((0, 0));
        let mut meta = Self::new_stream_meta(fork_spec.config.clone());
        meta.closed = config.created_closed;
        meta.next_read_seq = fork_read_seq;
        meta.next_byte_offset = fork_byte_offset;
        meta.fork_info = Some(ForkInfo::new(
            fork_spec.source_name.clone(),
            resolved_offset.clone(),
            fork_spec.sub_offset,
        ));
        meta
    }

    /// Clean up expired stream bytes and notify after a successful create/fork.
    fn cleanup_expired_and_notify(
        &self,
        name: &str,
        removed_expired_bytes: u64,
        removed_expired_parent: Option<String>,
    ) -> Result<()> {
        if removed_expired_bytes > 0 {
            release_bytes(&self.total_bytes, removed_expired_bytes);
            self.drop_notifier(name);
            if let Some(parent) = removed_expired_parent {
                self.cascade_delete_acid(&parent)?;
            }
        }
        self.notifier_sender(name);
        Ok(())
    }
}

impl AcidStorage {
    fn fork_initial_messages(
        streams: &impl ReadableTable<&'static str, &'static [u8]>,
        messages: &impl ReadableTable<(&'static str, u64, u64), &'static [u8]>,
        source_name: &str,
        resolved_offset: &Offset,
        config: &StreamConfig,
        options: &super::super::ForkOptions,
    ) -> Result<Vec<Bytes>> {
        let mut source_messages = Vec::new();
        if options.sub_offset > 0 {
            let plan = fork::build_read_plan(source_name, |name| {
                Self::read_stream_meta(streams, name)?
                    .map(|meta| meta.fork_info)
                    .ok_or_else(|| Error::NotFound(name.to_string()))
            })?;
            source_messages = Self::read_message_ranges(
                messages,
                plan,
                resolved_offset.parse_components().unwrap_or((0, 0)),
                "failed to read fork prefix",
                "failed to read fork prefix",
            )?;
        }
        fork::initial_fork_messages(config, options, source_messages)
    }
}

impl AcidStorage {
    fn prepare_source_fork(
        &self,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: &mut StreamConfig,
        options: &super::super::ForkOptions,
    ) -> Result<(usize, ForkCreateSpec)> {
        let source_shard_idx = self.existing_shard_index(source_name)?;
        let source_shard = &self.shards[source_shard_idx];
        let source_read_txn = source_shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let source_read_streams = source_read_txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let source_meta = Self::read_stream_meta(&source_read_streams, source_name)?
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?;
        if options.inherit_content_type {
            config
                .content_type
                .clone_from(&source_meta.config.content_type);
        }
        let source_next_offset =
            Offset::new(source_meta.next_read_seq, source_meta.next_byte_offset);
        let (mut fork_spec, _resolved_offset) = fork::prepare_fork_spec(
            source_name,
            &source_meta.config,
            source_meta.state,
            &source_next_offset,
            fork_offset,
            config,
        )?;

        fork_spec.sub_offset = options.sub_offset;

        Ok((source_shard_idx, fork_spec))
    }
}
