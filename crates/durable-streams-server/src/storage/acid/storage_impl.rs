use super::{
    AcidStorage, Bytes, ForkInfo, MESSAGES, Offset, ProducerState, Result, STREAMS,
    StoredStreamMeta, StreamConfig, StreamState,
};
use crate::protocol::error::Error;
use crate::protocol::producer::ProducerHeaders;
use crate::storage::{
    CreateStreamResult, CreateWithDataResult, ForkCreateSpec, ProducerAppendPrecheck,
    ProducerAppendResult, ReadResult, Storage, StreamMetadata, apply_append_metadata,
    build_stream_metadata, fork, is_stream_expired, is_stream_visible, precheck_append,
    precheck_batch_append, precheck_producer_append,
};
use chrono::Utc;
use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;
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
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let shard_idx = self
            .find_stream_shard_index(name)?
            .unwrap_or_else(|| self.shard_index(name));
        let shard = &self.shards[shard_idx];
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let mut removed_expired_bytes = 0_u64;
        let mut removed_expired_parent = None;

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
                    removed_expired_parent = existing.fork_info.map(|info| info.source_name);
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

        let meta = Self::new_stream_meta(config);
        Self::write_stream_meta(&mut streams, name, &meta)?;

        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit create stream", e))?;

        if removed_expired_bytes > 0 {
            self.saturating_sub_total_bytes(removed_expired_bytes);
            self.drop_notifier(name);
            if let Some(parent) = removed_expired_parent {
                self.cascade_delete_acid(&parent)?;
            }
        }

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let message_bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        self.reserve_total_bytes(message_bytes)?;

        let result = (|| {
            let shard = &self.shards[self.existing_shard_index(name)?];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut messages = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            precheck_append(&meta.config, meta.state, meta.closed, name, content_type)?;

            if meta.total_bytes + message_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }

            let offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            messages
                .insert(
                    (name, meta.next_read_seq, meta.next_byte_offset),
                    data.as_ref(),
                )
                .map_err(|e| Self::storage_err("failed to append message", e))?;

            meta.next_read_seq += 1;
            meta.next_byte_offset += message_bytes;
            meta.total_bytes += message_bytes;
            apply_append_metadata(
                &mut meta.config,
                &mut meta.last_seq,
                &mut meta.updated_at,
                None,
                Utc::now(),
            );

            Self::write_stream_meta(&mut streams, name, &meta)?;

            drop(messages);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit append", e))?;

            Ok(offset)
        })();

        if result.is_err() {
            self.rollback_total_bytes(message_bytes);
            return result;
        }

        self.notify_stream(name);
        result
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

        let batch_bytes = Self::batch_bytes(&messages);
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

            let pending_seq = precheck_batch_append(
                &meta.config,
                meta.state,
                meta.closed,
                meta.last_seq.as_deref(),
                name,
                content_type,
                seq,
            )?;

            if meta.total_bytes + batch_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }

            for data in &messages {
                let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                message_table
                    .insert(
                        (name, meta.next_read_seq, meta.next_byte_offset),
                        data.as_ref(),
                    )
                    .map_err(|e| Self::storage_err("failed to append batch message", e))?;
                meta.next_read_seq += 1;
                meta.next_byte_offset += len;
                meta.total_bytes += len;
            }

            apply_append_metadata(
                &mut meta.config,
                &mut meta.last_seq,
                &mut meta.updated_at,
                pending_seq,
                Utc::now(),
            );

            let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
            Self::write_stream_meta(&mut streams, name, &meta)?;

            drop(message_table);
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit batch append", e))?;

            Ok(next_offset)
        })();

        if result.is_err() {
            self.rollback_total_bytes(batch_bytes);
            return result;
        }

        self.notify_stream(name);
        result
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let shard_idx = self.existing_shard_index(name)?;
        let needs_ttl_renewal = {
            let shard = &self.shards[shard_idx];
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
            meta.config.ttl_seconds.is_some()
        };

        if !needs_ttl_renewal {
            return self.read_without_ttl_renewal(name, from_offset, shard_idx);
        }

        self.read_with_ttl_renewal(name, from_offset, shard_idx)
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

        self.saturating_sub_total_bytes(total_bytes);
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

        Ok(build_stream_metadata(
            meta.config,
            meta.next_read_seq,
            meta.next_byte_offset,
            meta.closed,
            meta.total_bytes,
            meta.next_read_seq,
            meta.created_at,
            meta.updated_at,
        ))
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let shard = &self.shards[self.existing_shard_index(name)?];
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let mut meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        fork::check_stream_access(&meta.config, meta.state, name)?;

        meta.closed = true;
        meta.updated_at = Some(Utc::now());
        fork::renew_ttl(&mut meta.config);
        Self::write_stream_meta(&mut streams, name, &meta)?;

        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit close stream", e))?;

        self.notify_stream(name);
        Ok(())
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
        let batch_bytes = Self::batch_bytes(&messages);
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

            if meta.total_bytes + batch_bytes > self.max_stream_bytes {
                return Err(Error::StreamSizeLimitExceeded);
            }

            for data in &messages {
                let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
                message_table
                    .insert(
                        (name, meta.next_read_seq, meta.next_byte_offset),
                        data.as_ref(),
                    )
                    .map_err(|e| Self::storage_err("failed to append producer message", e))?;
                meta.next_read_seq += 1;
                meta.next_byte_offset += len;
                meta.total_bytes += len;
            }

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
            );

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
            self.rollback_total_bytes(batch_bytes);
        }

        if result.is_ok() && (!messages.is_empty() || should_close) {
            self.notify_stream(name);
        }

        result
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        let batch_bytes = Self::batch_bytes(&messages);

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
            Self::write_initial_messages(
                name,
                &messages,
                batch_bytes,
                self.max_stream_bytes,
                &mut meta,
                &mut message_table,
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
            self.rollback_total_bytes(batch_bytes);
        }

        if result.is_ok() {
            if removed_expired_bytes > 0 {
                self.saturating_sub_total_bytes(removed_expired_bytes);
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

    fn exists(&self, name: &str) -> bool {
        let Ok(Some(shard_idx)) = self.find_stream_shard_index(name) else {
            return false;
        };
        let shard = &self.shards[shard_idx];
        let Ok(txn) = shard.db.begin_read() else {
            return false;
        };
        let Ok(streams) = txn.open_table(STREAMS) else {
            return false;
        };

        match Self::read_stream_meta(&streams, name) {
            Ok(Some(meta)) => is_stream_visible(&meta.config, meta.state),
            _ => false,
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let shard_idx = self.find_stream_shard_index(name).ok().flatten()?;
        let shard = &self.shards[shard_idx];
        let txn = shard.db.begin_read().ok()?;
        let streams = txn.open_table(STREAMS).ok()?;
        let meta = Self::read_stream_meta(&streams, name).ok()??;

        if !is_stream_visible(&meta.config, meta.state) {
            return None;
        }

        Some(self.notifier_sender(name).subscribe())
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
                        self.rollback_total_bytes(bytes);
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

                result.push((
                    name,
                    build_stream_metadata(
                        meta.config,
                        meta.next_read_seq,
                        meta.next_byte_offset,
                        meta.closed,
                        meta.total_bytes,
                        meta.next_read_seq,
                        meta.created_at,
                        meta.updated_at,
                    ),
                ));
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

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        self.create_fork_with_options(
            name,
            source_name,
            fork_offset,
            config,
            super::super::ForkOptions::default(),
        )
    }

    fn create_fork_with_options(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        mut config: StreamConfig,
        options: super::super::ForkOptions,
    ) -> Result<CreateStreamResult> {
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

        let batch_bytes = Self::batch_bytes(&initial_messages);
        self.reserve_total_bytes(batch_bytes)?;
        let result = (|| {
            let mut fork_meta = Self::build_fork_stored_meta(&fork_spec, &config, &resolved_offset);
            Self::write_initial_messages(
                name,
                &initial_messages,
                batch_bytes,
                self.max_stream_bytes,
                &mut fork_meta,
                &mut messages,
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
            self.rollback_total_bytes(batch_bytes);
        }
        result?;

        self.cleanup_expired_and_notify(name, removed_expired_bytes, removed_expired_parent)?;

        Ok(CreateStreamResult::Created)
    }
}

/// Private helpers extracted from long `Storage` trait methods.
impl AcidStorage {
    /// Read path when the stream has no TTL (read-only transaction).
    fn read_without_ttl_renewal(
        &self,
        name: &str,
        from_offset: &Offset,
        shard_idx: usize,
    ) -> Result<ReadResult> {
        let shard = &self.shards[shard_idx];
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;

        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);

        if from_offset.is_now() {
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: meta.closed,
            });
        }

        if meta.fork_info.is_none() {
            drop(streams);
            drop(txn);
            let messages = self.read_non_forked_table_messages(name, from_offset, shard_idx)?;

            return Ok(ReadResult {
                messages,
                next_offset,
                at_tail: true,
                closed: meta.closed,
            });
        }

        let fi = meta.fork_info.clone().expect("checked above");
        let closed = meta.closed;
        drop(streams);
        drop(txn);

        let all_messages = self.collect_fork_chain_messages(name, from_offset, &fi)?;

        Ok(ReadResult {
            messages: all_messages,
            next_offset,
            at_tail: true,
            closed,
        })
    }

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
        fork::check_stream_access(&meta.config, meta.state, name)?;

        let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);
        let result = if from_offset.is_now() {
            ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: meta.closed,
            }
        } else if meta.fork_info.is_none() {
            let messages = self.read_non_forked_table_messages(name, from_offset, shard_idx)?;

            ReadResult {
                messages,
                next_offset,
                at_tail: true,
                closed: meta.closed,
            }
        } else {
            let fi = meta.fork_info.clone().expect("checked above");
            let closed = meta.closed;
            drop(streams);
            drop(txn);

            let all_messages = self.collect_fork_chain_messages(name, from_offset, &fi)?;

            let shard = &self.shards[shard_idx];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;
            fork::renew_ttl(&mut meta.config);
            Self::write_stream_meta(&mut streams, name, &meta)?;
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit ttl renewal", e))?;

            return Ok(ReadResult {
                messages: all_messages,
                next_offset,
                at_tail: true,
                closed,
            });
        };

        fork::renew_ttl(&mut meta.config);
        Self::write_stream_meta(&mut streams, name, &meta)?;
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit ttl renewal", e))?;

        Ok(result)
    }

    /// Write initial messages into the MESSAGES table during stream creation.
    fn write_initial_messages(
        name: &str,
        messages: &[Bytes],
        batch_bytes: u64,
        max_stream_bytes: u64,
        meta: &mut StoredStreamMeta,
        message_table: &mut redb::Table<'_, (&str, u64, u64), &[u8]>,
    ) -> Result<()> {
        if batch_bytes == 0 {
            return Ok(());
        }
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
                .map_err(|e| Self::storage_err("failed to append create-with-data message", e))?;
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
        StoredStreamMeta {
            config: fork_spec.config.clone(),
            closed: config.created_closed,
            next_read_seq: fork_read_seq,
            next_byte_offset: fork_byte_offset,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            last_seq: None,
            producers: HashMap::new(),
            fork_info: Some(ForkInfo {
                sub_offset: fork_spec.sub_offset,
                source_name: fork_spec.source_name.clone(),
                fork_offset: resolved_offset.clone(),
            }),
            ref_count: 0,
            state: StreamState::Active,
        }
    }

    /// Clean up expired stream bytes and notify after a successful create/fork.
    fn cleanup_expired_and_notify(
        &self,
        name: &str,
        removed_expired_bytes: u64,
        removed_expired_parent: Option<String>,
    ) -> Result<()> {
        if removed_expired_bytes > 0 {
            self.saturating_sub_total_bytes(removed_expired_bytes);
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
            let mut lineage = Vec::new();
            let mut current = source_name.to_string();
            loop {
                let meta = Self::read_stream_meta(streams, &current)?
                    .ok_or_else(|| Error::NotFound(current.clone()))?;
                let parent = meta.fork_info.as_ref().map(|fi| fi.source_name.clone());
                lineage.push((current, meta.fork_info));
                match parent {
                    Some(parent) => current = parent,
                    None => break,
                }
            }
            let plan = fork::build_read_plan(source_name, |n| {
                lineage
                    .iter()
                    .find(|(name, _)| name == n)
                    .map(|(_, fi)| fi.clone())
            });
            let (seq, byte) = resolved_offset.parse_components().unwrap_or((0, 0));
            for segment in plan {
                for item in messages
                    .range(
                        (segment.name.as_str(), seq, byte)
                            ..=(segment.name.as_str(), u64::MAX, u64::MAX),
                    )
                    .map_err(|e| Self::storage_err("failed to read fork prefix", e))?
                {
                    let (key, value) =
                        item.map_err(|e| Self::storage_err("failed to read fork prefix", e))?;
                    let (_, seq, byte) = key.value();
                    if segment
                        .read_up_to
                        .as_ref()
                        .is_some_and(|bound| Offset::new(seq, byte) >= *bound)
                    {
                        break;
                    }
                    source_messages.push(Bytes::copy_from_slice(value.value()));
                }
            }
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
