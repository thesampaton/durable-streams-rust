//! Crash-resilient redb-backed storage with sharded databases.
//!
//! [`AcidStorage`] is the implementation behind
//! [`crate::config::StorageMode::Acid`]. It stores stream metadata and
//! messages in redb tables and uses a stable hash-based shard layout so a
//! stream always maps to the same database after restarts.
//!
//! This backend comes in two persistence variants via
//! [`crate::config::AcidBackend`]:
//!
//! - [`crate::config::AcidBackend::File`] persists redb shard files beneath
//!   the configured storage directory
//! - [`crate::config::AcidBackend::InMemory`] keeps the same transactional data
//!   model in memory only
//!
//! Compared with [`super::file::FileStorage`], the acid backend trades
//! on-disk simplicity for transactional durability across metadata, message
//! writes, and fork bookkeeping.

mod layout;
mod storage_impl;

#[cfg(test)]
mod tests;

use super::{ForkInfo, NOTIFY_CHANNEL_CAPACITY, ProducerState, StreamConfig, StreamState};
use crate::config::AcidBackend;
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use redb::backends::InMemoryBackend;
use redb::{
    CommitError, Database, DatabaseError, Durability, ReadableDatabase, ReadableTable,
    SetDurabilityError, StorageError as RedbStorageError, Table, TableDefinition, TableError,
    TransactionError,
};
use seahash::hash;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::warn;

const STREAMS: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const MESSAGES: TableDefinition<(&str, u64, u64), &[u8]> = TableDefinition::new("messages");

const LAYOUT_FORMAT_VERSION: u32 = 1;
const HASH_POLICY: &str = "seahash-v1";
/// Retry backoff for startup-only operations (shard database open).
/// Not used on the request path — transient errors there propagate as 503.
const STARTUP_RETRY_BACKOFF_MS: [u64; 3] = [10, 25, 50];

#[derive(Debug, Serialize, Deserialize)]
struct LayoutManifest {
    format_version: u32,
    shard_count: usize,
    hash_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamMeta {
    config: StreamConfig,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: DateTime<Utc>,
    #[serde(default)]
    updated_at: Option<DateTime<Utc>>,
    last_seq: Option<String>,
    producers: HashMap<String, ProducerState>,
    #[serde(default)]
    fork_info: Option<ForkInfo>,
    #[serde(default)]
    ref_count: u32,
    #[serde(default)]
    state: StreamState,
}

#[derive(Debug)]
struct AcidShard {
    db: Database,
}

#[allow(clippy::module_name_repetitions)]
pub struct AcidStorage {
    shards: Vec<AcidShard>,
    shard_count: usize,
    total_bytes: AtomicU64,
    max_total_bytes: u64,
    max_stream_bytes: u64,
    notifiers: RwLock<HashMap<String, broadcast::Sender<()>>>,
}

impl AcidStorage {
    /// Create or reopen an ACID storage root.
    ///
    /// When `backend` is [`AcidBackend::File`] the backend stores its files
    /// beneath `<root>/acid`, validates a layout manifest, and rebuilds
    /// aggregate state from disk before serving requests.
    ///
    /// When `backend` is [`AcidBackend::InMemory`] the `root_dir` is ignored
    /// and each shard uses a redb [`InMemoryBackend`]. ACID transaction
    /// semantics still apply but all data is lost on shutdown.
    ///
    /// # Errors
    ///
    /// Returns `Error::Storage` if storage layout validation fails, shard
    /// databases cannot be opened, or on-disk recovery cannot complete.
    pub fn new(
        root_dir: impl Into<PathBuf>,
        shard_count: usize,
        max_total_bytes: u64,
        max_stream_bytes: u64,
        backend: AcidBackend,
    ) -> Result<Self> {
        Self::validate_shard_count(shard_count)?;

        let shards = match backend {
            AcidBackend::File => Self::create_file_shards(&root_dir.into(), shard_count)?,
            AcidBackend::InMemory => Self::create_in_memory_shards(shard_count)?,
        };

        let storage = Self {
            shards,
            shard_count,
            total_bytes: AtomicU64::new(0),
            max_total_bytes,
            max_stream_bytes,
            notifiers: RwLock::new(HashMap::new()),
        };

        let total_bytes = storage.rebuild_state_from_disk()?;
        storage.total_bytes.store(total_bytes, Ordering::Release);

        Ok(storage)
    }

    /// Return the currently tracked total payload bytes across all streams.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    fn validate_shard_count(shard_count: usize) -> Result<()> {
        if !(1..=256).contains(&shard_count) {
            return Err(Error::Storage(format!(
                "acid shard count must be in range 1..=256, got {shard_count}"
            )));
        }
        if !shard_count.is_power_of_two() {
            return Err(Error::Storage(format!(
                "acid shard count must be a power of two, got {shard_count}"
            )));
        }
        Ok(())
    }

    fn storage_err<E: ClassifyError>(context: impl Into<String>, err: E) -> Error {
        let context = context.into();
        let detail = format!("{context}: {err}");
        err.into_storage_error(context, detail)
    }

    fn classify_redb_storage_error(
        context: String,
        err: &RedbStorageError,
        detail: String,
    ) -> Error {
        match err {
            RedbStorageError::Io(io_err) => {
                Error::classify_io_failure("acid", context, detail, io_err)
            }
            RedbStorageError::DatabaseClosed | RedbStorageError::PreviousIo => {
                Error::storage_unavailable("acid", context, detail)
            }
            RedbStorageError::ValueTooLarge(_) => {
                Error::storage_insufficient("acid", context, detail)
            }
            RedbStorageError::Corrupted(_) | RedbStorageError::LockPoisoned(_) => {
                Error::Storage(detail)
            }
            _ => {
                warn!(error = %err, "unhandled redb StorageError variant");
                Error::Storage(detail)
            }
        }
    }

    #[must_use]
    fn shard_index(&self, name: &str) -> usize {
        let hash_u64 = hash(name.as_bytes());
        let hash_usize = usize::try_from(hash_u64).unwrap_or_else(|_| {
            let masked = hash_u64 & u64::from(u32::MAX);
            usize::try_from(masked).expect("masked hash value must fit in usize")
        });
        hash_usize & (self.shard_count - 1)
    }

    fn find_stream_shard_index(&self, name: &str) -> Result<Option<usize>> {
        let hashed_idx = self.shard_index(name);
        if self.stream_exists_in_shard(hashed_idx, name)? {
            return Ok(Some(hashed_idx));
        }

        let mut found = None;

        for (idx, shard) in self.shards.iter().enumerate() {
            if idx == hashed_idx {
                continue;
            }

            let txn = shard
                .db
                .begin_read()
                .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
            let streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;

            if Self::read_stream_meta(&streams, name)?.is_some() && found.replace(idx).is_some() {
                return Err(Error::Storage(format!(
                    "stream metadata exists in multiple shards for {name}"
                )));
            }
        }

        Ok(found)
    }

    fn stream_exists_in_shard(&self, shard_idx: usize, name: &str) -> Result<bool> {
        let shard = &self.shards[shard_idx];
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        Ok(Self::read_stream_meta(&streams, name)?.is_some())
    }

    fn existing_shard_index(&self, name: &str) -> Result<usize> {
        self.find_stream_shard_index(name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))
    }

    fn reserve_total_bytes(&self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }

        if self
            .total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.max_total_bytes)
            })
            .is_err()
        {
            return Err(Error::MemoryLimitExceeded);
        }
        Ok(())
    }

    fn rollback_total_bytes(&self, bytes: u64) {
        self.saturating_sub_total_bytes(bytes);
    }

    fn saturating_sub_total_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        self.total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
    }

    fn read_stream_meta<T>(streams: &T, name: &str) -> Result<Option<StoredStreamMeta>>
    where
        T: ReadableTable<&'static str, &'static [u8]>,
    {
        let payload = streams
            .get(name)
            .map_err(|e| Self::storage_err("failed to read stream metadata", e))?;

        if let Some(payload) = payload {
            let meta = serde_json::from_slice(payload.value())
                .map_err(|e| Self::storage_err("failed to parse stream metadata", e))?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    fn write_stream_meta(
        streams: &mut Table<'_, &'static str, &'static [u8]>,
        name: &str,
        meta: &StoredStreamMeta,
    ) -> Result<()> {
        let payload = serde_json::to_vec(meta)
            .map_err(|e| Self::storage_err("failed to serialize stream metadata", e))?;
        streams
            .insert(name, payload.as_slice())
            .map_err(|e| Self::storage_err("failed to write stream metadata", e))?;
        Ok(())
    }

    fn delete_stream_messages(
        messages: &mut Table<'_, (&'static str, u64, u64), &'static [u8]>,
        name: &str,
    ) -> Result<()> {
        let mut keys = Vec::new();
        let iter = messages
            .range((name, 0_u64, 0_u64)..=(name, u64::MAX, u64::MAX))
            .map_err(|e| Self::storage_err("failed to iterate stream messages", e))?;

        for item in iter {
            let (key, _) = item.map_err(|e| Self::storage_err("failed to read message key", e))?;
            let (_, read_seq, byte_offset) = key.value();
            keys.push((read_seq, byte_offset));
        }

        for (read_seq, byte_offset) in keys {
            messages
                .remove((name, read_seq, byte_offset))
                .map_err(|e| Self::storage_err("failed to delete message", e))?;
        }

        Ok(())
    }

    fn notifier_sender(&self, name: &str) -> broadcast::Sender<()> {
        let mut guard = self.notifiers.write().expect("notifiers lock poisoned");
        guard
            .entry(name.to_string())
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
                sender
            })
            .clone()
    }

    fn notify_stream(&self, name: &str) {
        if let Some(sender) = self
            .notifiers
            .read()
            .expect("notifiers lock poisoned")
            .get(name)
        {
            let _ = sender.send(());
        }
    }

    fn drop_notifier(&self, name: &str) {
        self.notifiers
            .write()
            .expect("notifiers lock poisoned")
            .remove(name);
    }

    fn new_stream_meta(config: StreamConfig) -> StoredStreamMeta {
        StoredStreamMeta {
            config,
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            last_seq: None,
            producers: HashMap::new(),
            fork_info: None,
            ref_count: 0,
            state: StreamState::Active,
        }
    }

    fn batch_bytes(messages: &[Bytes]) -> u64 {
        messages
            .iter()
            .map(|m| u64::try_from(m.len()).unwrap_or(u64::MAX))
            .sum()
    }

    /// Read messages from a stream's shard within a given offset range.
    ///
    /// Returns messages with offsets `>= from_offset` and `< up_to` (if
    /// `up_to` is `Some`). Used by fork read stitching to pull ancestor
    /// messages without fork/tombstone validation.
    fn read_messages_from_shard(
        &self,
        name: &str,
        from_offset: &Offset,
        up_to: Option<&Offset>,
    ) -> Result<Vec<Bytes>> {
        let shard = &self.shards[self.existing_shard_index(name)?];
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let message_table = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let (start_read_seq, start_byte_offset) = if from_offset.is_start() {
            (0_u64, 0_u64)
        } else {
            from_offset.parse_components().unwrap_or((0, 0))
        };

        let iter = message_table
            .range((name, start_read_seq, start_byte_offset)..=(name, u64::MAX, u64::MAX))
            .map_err(|e| Self::storage_err("failed to read shard message range", e))?;

        let mut messages = Vec::new();
        for item in iter {
            let (key, value) =
                item.map_err(|e| Self::storage_err("failed to read shard message", e))?;
            if let Some(bound) = up_to {
                let (_, read_seq, byte_offset) = key.value();
                let msg_offset = Offset::new(read_seq, byte_offset);
                if msg_offset >= *bound {
                    break;
                }
            }
            messages.push(Bytes::copy_from_slice(value.value()));
        }

        Ok(messages)
    }

    fn cascade_delete_acid(&self, parent_name: &str) -> Result<()> {
        let mut current_parent = parent_name.to_string();
        while let Some(shard_idx) = self.find_stream_shard_index(&current_parent)? {
            let shard = &self.shards[shard_idx];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;

            let Some(mut meta) = Self::read_stream_meta(&streams, &current_parent)? else {
                break;
            };

            meta.ref_count = meta.ref_count.saturating_sub(1);

            if meta.state == StreamState::Tombstone && meta.ref_count == 0 {
                let next_parent = meta.fork_info.as_ref().map(|fi| fi.source_name.clone());
                let total_bytes = meta.total_bytes;

                let mut messages = txn
                    .open_table(MESSAGES)
                    .map_err(|e| Self::storage_err("failed to open messages table", e))?;
                Self::delete_stream_messages(&mut messages, &current_parent)?;
                drop(messages);

                streams
                    .remove(current_parent.as_str())
                    .map_err(|e| Self::storage_err("failed to remove tombstoned parent", e))?;
                drop(streams);
                txn.commit()
                    .map_err(|e| Self::storage_err("failed to commit cascade delete", e))?;

                self.saturating_sub_total_bytes(total_bytes);
                self.drop_notifier(&current_parent);

                match next_parent {
                    Some(next) => current_parent = next,
                    None => break,
                }
            } else {
                Self::write_stream_meta(&mut streams, &current_parent, &meta)?;
                drop(streams);
                txn.commit()
                    .map_err(|e| Self::storage_err("failed to commit ref_count decrement", e))?;
                break;
            }
        }
        Ok(())
    }

    /// Read messages from the MESSAGES table for a non-forked stream, starting
    /// from the given offset. Opens its own read transaction.
    fn read_non_forked_table_messages(
        &self,
        name: &str,
        from_offset: &Offset,
        shard_idx: usize,
    ) -> Result<Vec<Bytes>> {
        let (start_read_seq, start_byte_offset) = if from_offset.is_start() {
            (0_u64, 0_u64)
        } else {
            from_offset.parse_components().ok_or_else(|| {
                Error::InvalidOffset("non-concrete offset in read range".to_string())
            })?
        };

        let shard = &self.shards[shard_idx];
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let message_table = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let iter = message_table
            .range((name, start_read_seq, start_byte_offset)..=(name, u64::MAX, u64::MAX))
            .map_err(|e| Self::storage_err("failed to read stream range", e))?;

        let mut messages = Vec::new();
        for item in iter {
            let (_, value) =
                item.map_err(|e| Self::storage_err("failed to read stream message", e))?;
            messages.push(Bytes::copy_from_slice(value.value()));
        }

        Ok(messages)
    }

    /// Traverse the fork chain and collect all messages for a forked stream read.
    fn collect_fork_chain_messages(
        &self,
        name: &str,
        from_offset: &Offset,
        fi: &ForkInfo,
    ) -> Result<Vec<Bytes>> {
        let mut all_messages: Vec<Bytes> = Vec::new();
        if from_offset.is_start() || *from_offset < fi.fork_offset {
            let plan = super::fork::build_read_plan(&fi.source_name, |segment_name| {
                let shard_idx = self.find_stream_shard_index(segment_name).ok().flatten()?;
                let shard = &self.shards[shard_idx];
                let txn = shard.db.begin_read().ok()?;
                let streams = txn.open_table(STREAMS).ok()?;
                let meta = Self::read_stream_meta(&streams, segment_name).ok()??;
                Some(meta.fork_info)
            });

            for segment in &plan {
                let effective_up_to = Some(
                    segment
                        .read_up_to
                        .as_ref()
                        .map_or(&fi.fork_offset, |bound| bound.min(&fi.fork_offset)),
                );
                let effective_from = from_offset;
                let segment_msgs =
                    self.read_messages_from_shard(&segment.name, effective_from, effective_up_to)?;
                all_messages.extend(segment_msgs);
            }
        }

        let fork_msgs = if from_offset.is_start() || *from_offset <= fi.fork_offset {
            self.read_messages_from_shard(name, &fi.fork_offset, None)?
        } else {
            self.read_messages_from_shard(name, from_offset, None)?
        };
        all_messages.extend(fork_msgs);

        Ok(all_messages)
    }

    fn begin_write_txn(db: &Database) -> Result<redb::WriteTransaction> {
        let mut txn = db
            .begin_write()
            .map_err(|e| Self::storage_err("failed to begin write transaction", e))?;
        txn.set_durability(Durability::Immediate)
            .map_err(|e| Self::storage_err("failed to set write durability", e))?;
        Ok(txn)
    }
}

/// Type-safe error classification trait for redb and IO error types.
///
/// Replaces the previous `Any`-based downcasting dispatcher with compile-time
/// dispatch. Each implementation maps the concrete error type into the correct
/// [`Error`] variant so that transient failures become 503, capacity failures
/// become 507, and everything else maps to a generic 500.
trait ClassifyError: std::fmt::Display {
    fn into_storage_error(self, context: String, detail: String) -> Error;
}

impl ClassifyError for std::io::Error {
    fn into_storage_error(self, context: String, detail: String) -> Error {
        Error::classify_io_failure("acid", context, detail, &self)
    }
}

impl ClassifyError for DatabaseError {
    fn into_storage_error(self, context: String, detail: String) -> Error {
        match &self {
            DatabaseError::DatabaseAlreadyOpen => {
                Error::storage_unavailable("acid", context, detail)
            }
            DatabaseError::Storage(storage_err) => {
                AcidStorage::classify_redb_storage_error(context, storage_err, detail)
            }
            DatabaseError::RepairAborted | DatabaseError::UpgradeRequired(_) => {
                Error::Storage(detail)
            }
            _ => {
                warn!(error = %self, "unhandled redb DatabaseError variant");
                Error::Storage(detail)
            }
        }
    }
}

impl ClassifyError for TransactionError {
    fn into_storage_error(self, context: String, detail: String) -> Error {
        match &self {
            TransactionError::Storage(storage_err) => {
                AcidStorage::classify_redb_storage_error(context, storage_err, detail)
            }
            TransactionError::ReadTransactionStillInUse(_) => Error::Storage(detail),
            _ => {
                warn!(error = %self, "unhandled redb TransactionError variant");
                Error::Storage(detail)
            }
        }
    }
}

impl ClassifyError for TableError {
    fn into_storage_error(self, context: String, detail: String) -> Error {
        match &self {
            TableError::Storage(storage_err) => {
                AcidStorage::classify_redb_storage_error(context, storage_err, detail)
            }
            TableError::TableTypeMismatch { .. }
            | TableError::TableIsMultimap(_)
            | TableError::TableIsNotMultimap(_)
            | TableError::TypeDefinitionChanged { .. }
            | TableError::TableDoesNotExist(_)
            | TableError::TableExists(_)
            | TableError::TableAlreadyOpen(_, _) => Error::Storage(detail),
            _ => {
                warn!(error = %self, "unhandled redb TableError variant");
                Error::Storage(detail)
            }
        }
    }
}

impl ClassifyError for CommitError {
    fn into_storage_error(self, context: String, detail: String) -> Error {
        if let CommitError::Storage(storage_err) = &self {
            AcidStorage::classify_redb_storage_error(context, storage_err, detail)
        } else {
            warn!(error = %self, "unhandled redb CommitError variant");
            Error::Storage(detail)
        }
    }
}

impl ClassifyError for RedbStorageError {
    fn into_storage_error(self, context: String, detail: String) -> Error {
        AcidStorage::classify_redb_storage_error(context, &self, detail)
    }
}

impl ClassifyError for SetDurabilityError {
    fn into_storage_error(self, _context: String, detail: String) -> Error {
        Error::Storage(detail)
    }
}

impl ClassifyError for serde_json::Error {
    fn into_storage_error(self, _context: String, detail: String) -> Error {
        Error::Storage(detail)
    }
}
