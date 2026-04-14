//! Crash-resilient redb-backed storage with sharded databases.
//!
//! This backend stores stream metadata and messages in redb tables and uses a
//! stable hash-based shard layout so a stream always maps to the same database
//! file after restarts.

use super::{
    CreateStreamResult, CreateWithDataResult, ForkInfo, NOTIFY_CHANNEL_CAPACITY,
    ProducerAppendResult, ProducerCheck, ProducerState, ReadResult, Storage, StreamConfig,
    StreamMetadata, StreamState,
};
use crate::config::AcidBackend;
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
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

        // File backend: rebuilds aggregate state from persisted data.
        // In-memory backend: databases start empty so this returns 0.
        let total_bytes = storage.rebuild_state_from_disk()?;
        storage.total_bytes.store(total_bytes, Ordering::Release);

        Ok(storage)
    }

    fn create_file_shards(root_dir: &Path, shard_count: usize) -> Result<Vec<AcidShard>> {
        let acid_dir = Self::acid_dir(root_dir);
        fs::create_dir_all(&acid_dir).map_err(|e| {
            Self::storage_err(
                format!(
                    "failed to create acid storage directory {}",
                    acid_dir.display()
                ),
                e,
            )
        })?;

        Self::load_or_create_layout(&acid_dir, shard_count)?;

        let mut shards = Vec::with_capacity(shard_count);
        for idx in 0..shard_count {
            let shard_path = acid_dir.join(format!("shard_{idx:02x}.redb"));
            let db = Self::open_shard_database(&shard_path)?;
            Self::ensure_schema(&db)?;
            shards.push(AcidShard { db });
        }

        Ok(shards)
    }

    fn create_in_memory_shards(shard_count: usize) -> Result<Vec<AcidShard>> {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let db = Database::builder()
                .create_with_backend(InMemoryBackend::new())
                .map_err(|e| Self::storage_err("failed to create in-memory shard database", e))?;
            Self::ensure_schema(&db)?;
            shards.push(AcidShard { db });
        }
        Ok(shards)
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

    /// Open a shard database with retry. This is startup-only code — the
    /// `thread::sleep` backoff is acceptable here since it runs once per shard
    /// during initialization, never on the request path.
    fn open_shard_database(shard_path: &Path) -> Result<Database> {
        let context = format!("failed to open shard database {}", shard_path.display());
        let mut delays = STARTUP_RETRY_BACKOFF_MS.into_iter();

        loop {
            match Database::builder().create(shard_path) {
                Ok(db) => return Ok(db),
                Err(err) if Self::is_retryable_database_open(&err) => {
                    if let Some(delay_ms) = delays.next() {
                        std::thread::sleep(Duration::from_millis(delay_ms));
                        continue;
                    }
                    return Err(Self::storage_err(context, err));
                }
                Err(err) => return Err(Self::storage_err(context, err)),
            }
        }
    }

    fn is_retryable_database_open(err: &DatabaseError) -> bool {
        match err {
            DatabaseError::DatabaseAlreadyOpen => true,
            DatabaseError::Storage(RedbStorageError::Io(io_err)) => {
                Error::is_retryable_io_error(io_err)
            }
            _ => false,
        }
    }

    fn acid_dir(root_dir: &Path) -> PathBuf {
        root_dir.join("acid")
    }

    fn layout_path(acid_dir: &Path) -> PathBuf {
        acid_dir.join("layout.json")
    }

    fn load_or_create_layout(acid_dir: &Path, shard_count: usize) -> Result<()> {
        let layout_path = Self::layout_path(acid_dir);
        if layout_path.exists() {
            let payload = fs::read(&layout_path).map_err(|e| {
                Self::storage_err(
                    format!("failed to read acid layout file {}", layout_path.display()),
                    e,
                )
            })?;
            let manifest: LayoutManifest = serde_json::from_slice(&payload).map_err(|e| {
                Self::storage_err(
                    format!("failed to parse acid layout file {}", layout_path.display()),
                    e,
                )
            })?;

            if manifest.format_version != LAYOUT_FORMAT_VERSION {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: format_version={}, expected={}",
                    manifest.format_version, LAYOUT_FORMAT_VERSION
                )));
            }
            if manifest.shard_count != shard_count {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: shard_count={}, expected={shard_count}",
                    manifest.shard_count
                )));
            }
            if manifest.hash_policy != HASH_POLICY {
                return Err(Error::Storage(format!(
                    "acid layout mismatch: hash_policy='{}', expected='{}'",
                    manifest.hash_policy, HASH_POLICY
                )));
            }
            return Ok(());
        }

        let manifest = LayoutManifest {
            format_version: LAYOUT_FORMAT_VERSION,
            shard_count,
            hash_policy: HASH_POLICY.to_string(),
        };
        let payload = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| Self::storage_err("failed to serialize acid layout manifest", e))?;

        let tmp_path = acid_dir.join("layout.json.tmp");
        fs::write(&tmp_path, payload).map_err(|e| {
            Self::storage_err(
                format!("failed to write temp layout file {}", tmp_path.display()),
                e,
            )
        })?;
        fs::rename(&tmp_path, &layout_path).map_err(|e| {
            Self::storage_err(
                format!("failed to write layout file {}", layout_path.display()),
                e,
            )
        })?;

        Ok(())
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

    fn shard(&self, name: &str) -> &AcidShard {
        &self.shards[self.shard_index(name)]
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
        // redb table iterators cannot be safely mutated in-place; we must collect
        // keys first, then remove in a second pass.
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
        // Use a single write lock with entry() to avoid a TOCTOU race where
        // drop_notifier() could remove the sender between a read-lock miss
        // and the subsequent write-lock acquire.
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
        let shard = self.shard(name);
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

    /// Walk up the fork chain after a hard-delete, decrementing `ref_count`s
    /// and garbage-collecting tombstoned ancestors with zero references.
    fn build_fork_config(
        fork_content_type: &str,
        fork_ttl: Option<u64>,
        fork_expires_at: Option<DateTime<Utc>>,
        fork_closed: bool,
    ) -> StreamConfig {
        let mut config = StreamConfig::new(fork_content_type.to_string());
        if let Some(ttl) = fork_ttl {
            config = config.with_ttl(ttl);
        }
        if let Some(ea) = fork_expires_at {
            config = config.with_expires_at(ea);
        }
        if fork_closed {
            config = config.with_created_closed(true);
        }
        config
    }

    fn cascade_delete_acid(&self, parent_name: &str) -> Result<()> {
        let mut current_parent = parent_name.to_string();
        loop {
            let shard = self.shard(&current_parent);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;

            let Some(mut meta) = Self::read_stream_meta(&streams, &current_parent)? else {
                break;
            };

            meta.ref_count = meta.ref_count.saturating_sub(1);

            if meta.state == StreamState::Tombstone && meta.ref_count == 0 {
                // This parent can be garbage-collected
                let fi = meta.fork_info.clone();
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

                // Continue up the chain
                if let Some(fi) = fi {
                    current_parent = fi.source_name;
                } else {
                    break;
                }
            } else {
                // Just update the ref_count
                Self::write_stream_meta(&mut streams, &current_parent, &meta)?;
                drop(streams);
                txn.commit()
                    .map_err(|e| Self::storage_err("failed to commit ref_count decrement", e))?;
                break;
            }
        }
        Ok(())
    }

    fn begin_write_txn(db: &Database) -> Result<redb::WriteTransaction> {
        let mut txn = db
            .begin_write()
            .map_err(|e| Self::storage_err("failed to begin write transaction", e))?;
        txn.set_durability(Durability::Immediate)
            .map_err(|e| Self::storage_err("failed to set write durability", e))?;
        Ok(txn)
    }

    fn ensure_schema(db: &Database) -> Result<()> {
        let txn = Self::begin_write_txn(db)?;
        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to initialize streams table", e))?;
        let messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to initialize messages table", e))?;
        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit schema initialization", e))?;
        Ok(())
    }

    fn rebuild_state_from_disk(&self) -> Result<u64> {
        let mut total = 0_u64;
        for shard in &self.shards {
            total = total.saturating_add(self.rebuild_shard(shard)?);
        }
        Ok(total)
    }

    fn rebuild_shard(&self, shard: &AcidShard) -> Result<u64> {
        let read_txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
        let streams = read_txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let mut live_bytes = 0_u64;
        let mut expired_names = Vec::new();

        {
            let iter = streams
                .iter()
                .map_err(|e| Self::storage_err("failed to iterate stream metadata", e))?;
            for item in iter {
                let (key, value) =
                    item.map_err(|e| Self::storage_err("failed to read stream metadata", e))?;
                let stream_name = key.value().to_string();
                let meta: StoredStreamMeta = serde_json::from_slice(value.value())
                    .map_err(|e| Self::storage_err("failed to parse stream metadata", e))?;
                if super::is_stream_expired(&meta.config) {
                    expired_names.push(stream_name);
                } else {
                    live_bytes = live_bytes.saturating_add(meta.total_bytes);
                }
            }
        }

        drop(streams);
        drop(read_txn);

        if expired_names.is_empty() {
            return Ok(live_bytes);
        }

        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        for name in &expired_names {
            Self::delete_stream_messages(&mut messages, name)?;
            streams
                .remove(name.as_str())
                .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
            self.drop_notifier(name);
        }

        drop(messages);
        drop(streams);
        txn.commit()
            .map_err(|e| Self::storage_err("failed to commit startup cleanup", e))?;

        Ok(live_bytes)
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

impl Storage for AcidStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let shard = self.shard(name);
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;
        let mut messages = txn
            .open_table(MESSAGES)
            .map_err(|e| Self::storage_err("failed to open messages table", e))?;

        let mut removed_expired_bytes = 0_u64;

        if let Some(existing) = Self::read_stream_meta(&streams, name)? {
            if super::is_stream_expired(&existing.config) {
                removed_expired_bytes = existing.total_bytes;
                Self::delete_stream_messages(&mut messages, name)?;
                streams
                    .remove(name)
                    .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
            } else if existing.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else if existing.config == config {
                return Ok(CreateStreamResult::AlreadyExists);
            } else {
                return Err(Error::ConfigMismatch);
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
        }

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let message_bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        self.reserve_total_bytes(message_bytes)?;

        let result = (|| {
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut messages = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            super::fork::check_stream_access(&meta.config, meta.state, name)?;

            if meta.closed {
                return Err(Error::StreamClosed);
            }

            super::validate_content_type(&meta.config.content_type, content_type)?;

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
            meta.updated_at = Some(Utc::now());

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
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            super::fork::check_stream_access(&meta.config, meta.state, name)?;

            if meta.closed {
                return Err(Error::StreamClosed);
            }

            super::validate_content_type(&meta.config.content_type, content_type)?;
            let pending_seq = super::validate_seq(meta.last_seq.as_deref(), seq)?;

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

            if let Some(new_seq) = pending_seq {
                meta.last_seq = Some(new_seq);
            }
            meta.updated_at = Some(Utc::now());

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
        let shard = self.shard(name);
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;

        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        super::fork::check_stream_access(&meta.config, meta.state, name)?;

        let next_offset = Offset::new(meta.next_read_seq, meta.next_byte_offset);

        if from_offset.is_now() {
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: meta.closed,
            });
        }

        // If stream has no fork lineage, use the fast path (local read only)
        if meta.fork_info.is_none() {
            let message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let (start_read_seq, start_byte_offset) = if from_offset.is_start() {
                (0_u64, 0_u64)
            } else {
                from_offset.parse_components().ok_or_else(|| {
                    Error::InvalidOffset("non-concrete offset in read range".to_string())
                })?
            };

            let iter = message_table
                .range(
                    (name, start_read_seq, start_byte_offset)..=(name, u64::MAX, u64::MAX),
                )
                .map_err(|e| Self::storage_err("failed to read stream range", e))?;

            let mut messages = Vec::new();
            for item in iter {
                let (_, value) =
                    item.map_err(|e| Self::storage_err("failed to read stream message", e))?;
                messages.push(Bytes::copy_from_slice(value.value()));
            }

            return Ok(ReadResult {
                messages,
                next_offset,
                at_tail: true,
                closed: meta.closed,
            });
        }

        // Fork-aware read: stitch messages from source chain + fork's own data
        let fi = meta.fork_info.clone().expect("checked above");
        let closed = meta.closed;
        drop(streams);
        drop(txn);

        let mut all_messages: Vec<Bytes> = Vec::new();

        // Read source data if from_offset is before the fork_offset
        if from_offset.is_start() || *from_offset < fi.fork_offset {
            // Build read plan by looking up fork_info across shards
            let plan = super::fork::build_read_plan(&fi.source_name, |n| {
                let s = self.shard(n);
                let txn = s.db.begin_read().ok()?;
                let streams = txn.open_table(STREAMS).ok()?;
                let meta = Self::read_stream_meta(&streams, n).ok()??;
                Some(meta.fork_info)
            });

            for (i, segment) in plan.iter().enumerate() {
                // Determine the effective upper bound for this segment
                let effective_up_to = if i == plan.len() - 1 {
                    // Last segment (the direct source): read up to fork_offset
                    Some(&fi.fork_offset)
                } else {
                    // Intermediate segment: read up to segment's read_up_to
                    segment.read_up_to.as_ref()
                };

                // Determine start offset for this segment
                let effective_from = if i == 0 { from_offset } else { &Offset::start() };

                let segment_msgs =
                    self.read_messages_from_shard(&segment.name, effective_from, effective_up_to)?;
                all_messages.extend(segment_msgs);
            }
        }

        // Add fork's own messages
        if from_offset.is_start() || *from_offset <= fi.fork_offset {
            // All fork messages (from_offset is in ancestor range or at boundary)
            let fork_msgs = self.read_messages_from_shard(name, &fi.fork_offset, None)?;
            all_messages.extend(fork_msgs);
        } else {
            // from_offset is past fork_offset — read from fork starting at from_offset
            let fork_msgs = self.read_messages_from_shard(name, from_offset, None)?;
            all_messages.extend(fork_msgs);
        }

        Ok(ReadResult {
            messages: all_messages,
            next_offset,
            at_tail: true,
            closed,
        })
    }

    fn delete(&self, name: &str) -> Result<()> {
        let shard = self.shard(name);
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        // Already tombstoned — treat as gone
        if meta.state == StreamState::Tombstone {
            return Err(Error::StreamGone(name.to_string()));
        }

        if meta.ref_count > 0 {
            // Has child forks — soft-delete (tombstone)
            let mut updated = meta;
            updated.state = StreamState::Tombstone;
            Self::write_stream_meta(&mut streams, name, &updated)?;
            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit soft delete", e))?;
            return Ok(());
        }

        // No child forks — hard delete
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

        // Cascade: decrement parent ref_count, and if parent is tombstoned
        // with ref_count reaching 0, hard-delete it too (walk up the chain).
        if let Some(fi) = fork_info {
            self.cascade_delete_acid(&fi.source_name)?;
        }

        Ok(())
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        let shard = self.shard(name);
        let txn = shard
            .db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;

        let streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        super::fork::check_stream_access(&meta.config, meta.state, name)?;

        Ok(StreamMetadata {
            config: meta.config,
            next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
            closed: meta.closed,
            total_bytes: meta.total_bytes,
            message_count: meta.next_read_seq,
            created_at: meta.created_at,
            updated_at: meta.updated_at,
        })
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let shard = self.shard(name);
        let txn = Self::begin_write_txn(&shard.db)?;
        let mut streams = txn
            .open_table(STREAMS)
            .map_err(|e| Self::storage_err("failed to open streams table", e))?;

        let mut meta = Self::read_stream_meta(&streams, name)?
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        super::fork::check_stream_access(&meta.config, meta.state, name)?;

        meta.closed = true;
        meta.updated_at = Some(Utc::now());
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
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            super::fork::check_stream_access(&meta.config, meta.state, name)?;

            super::cleanup_stale_producers(&mut meta.producers);

            if !messages.is_empty() {
                super::validate_content_type(&meta.config.content_type, content_type)?;
            }

            match super::check_producer(
                meta.producers.get(producer.id.as_str()),
                producer,
                meta.closed,
            )? {
                ProducerCheck::Accept => {}
                ProducerCheck::Duplicate { epoch, seq } => {
                    return Ok(ProducerAppendResult::Duplicate {
                        epoch,
                        seq,
                        next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
                        closed: meta.closed,
                    });
                }
            }

            let pending_seq = super::validate_seq(meta.last_seq.as_deref(), seq)?;

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

            if let Some(new_seq) = pending_seq {
                meta.last_seq = Some(new_seq);
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
            meta.updated_at = Some(now);

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

        let result = (|| {
            let shard = self.shard(name);
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut message_table = txn
                .open_table(MESSAGES)
                .map_err(|e| Self::storage_err("failed to open messages table", e))?;

            if let Some(existing) = Self::read_stream_meta(&streams, name)? {
                if super::is_stream_expired(&existing.config) {
                    removed_expired_bytes = existing.total_bytes;
                    Self::delete_stream_messages(&mut message_table, name)?;
                    streams
                        .remove(name)
                        .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                } else if existing.state == StreamState::Tombstone {
                    return Err(Error::StreamGone(name.to_string()));
                } else if existing.config == config {
                    return Ok(CreateWithDataResult {
                        status: CreateStreamResult::AlreadyExists,
                        next_offset: Offset::new(existing.next_read_seq, existing.next_byte_offset),
                        closed: existing.closed,
                    });
                } else {
                    return Err(Error::ConfigMismatch);
                }
            }

            if batch_bytes > 0 {
                self.reserve_total_bytes(batch_bytes)?;
                reserved = true;
            }

            let mut meta = Self::new_stream_meta(config);

            if batch_bytes > 0 {
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
                        .map_err(|e| {
                            Self::storage_err("failed to append create-with-data message", e)
                        })?;
                    meta.next_read_seq += 1;
                    meta.next_byte_offset += len;
                    meta.total_bytes += len;
                }
            }

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
            }
            if should_close || !messages.is_empty() {
                self.notify_stream(name);
            }
        }

        result
    }

    fn exists(&self, name: &str) -> bool {
        let shard = self.shard(name);
        let Ok(txn) = shard.db.begin_read() else {
            return false;
        };
        let Ok(streams) = txn.open_table(STREAMS) else {
            return false;
        };

        match Self::read_stream_meta(&streams, name) {
            Ok(Some(meta)) => {
                !super::is_stream_expired(&meta.config) && meta.state == StreamState::Active
            }
            _ => false,
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let shard = self.shard(name);
        let txn = shard.db.begin_read().ok()?;
        let streams = txn.open_table(STREAMS).ok()?;
        let meta = Self::read_stream_meta(&streams, name).ok()??;

        if super::is_stream_expired(&meta.config) || meta.state == StreamState::Tombstone {
            return None;
        }

        Some(self.notifier_sender(name).subscribe())
    }

    fn cleanup_expired_streams(&self) -> usize {
        let mut total_removed = 0;

        for shard in &self.shards {
            // Read pass: find candidate expired stream names.
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
                if super::is_stream_expired(&meta.config) {
                    candidates.push(name);
                }
            }

            drop(streams_table);
            drop(read_txn);

            if candidates.is_empty() {
                continue;
            }

            // Write pass: re-verify expiration and delete confirmed streams.
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
                // Re-check expiration inside the write transaction to avoid a
                // TOCTOU race with concurrent creates.
                let meta = streams
                    .get(name.as_str())
                    .ok()
                    .flatten()
                    .and_then(|v| serde_json::from_slice::<StoredStreamMeta>(v.value()).ok());
                let Some(meta) = meta else { continue };
                if !super::is_stream_expired(&meta.config) {
                    continue;
                }

                if meta.ref_count > 0 {
                    // Has child forks — tombstone instead of hard-delete
                    let mut updated = meta.clone();
                    updated.state = StreamState::Tombstone;
                    let payload = serde_json::to_vec(&updated).ok();
                    if let Some(payload) = payload {
                        let _ = streams.insert(name.as_str(), payload.as_slice());
                    }
                    committed.push((name.clone(), 0)); // no bytes freed
                } else {
                    let _ = Self::delete_stream_messages(&mut messages, name);
                    let _ = streams.remove(name.as_str());
                    committed.push((name.clone(), meta.total_bytes));
                }
            }

            drop(messages);
            drop(streams);

            match txn.commit() {
                Ok(()) => {
                    for (name, bytes) in &committed {
                        self.rollback_total_bytes(*bytes);
                        self.drop_notifier(name);
                    }
                    total_removed += committed.len();
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

                if super::is_stream_expired(&meta.config)
                    || meta.state == StreamState::Tombstone
                {
                    continue;
                }

                result.push((
                    name,
                    StreamMetadata {
                        config: meta.config,
                        next_offset: Offset::new(meta.next_read_seq, meta.next_byte_offset),
                        closed: meta.closed,
                        total_bytes: meta.total_bytes,
                        message_count: meta.next_read_seq,
                        created_at: meta.created_at,
                        updated_at: meta.updated_at,
                    },
                ));
            }
        }

        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    #[allow(clippy::too_many_lines)]
    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        // --- Phase 1: Read source meta (read txn on source shard) ---
        let source_shard = self.shard(source_name);
        let source_meta = {
            let txn = source_shard
                .db
                .begin_read()
                .map_err(|e| Self::storage_err("failed to begin read transaction", e))?;
            let streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            Self::read_stream_meta(&streams, source_name)?
                .ok_or_else(|| Error::NotFound(source_name.to_string()))?
        };

        // Validate source access (not expired, not tombstoned)
        super::fork::check_stream_access(&source_meta.config, source_meta.state, source_name)?;

        // Resolve fork offset (defaults to source tail)
        let source_next_offset =
            Offset::new(source_meta.next_read_seq, source_meta.next_byte_offset);
        let resolved_offset =
            super::fork::resolve_fork_offset(fork_offset, &source_next_offset)?;

        // Validate content type: fork must match source
        if !config
            .content_type
            .eq_ignore_ascii_case(&source_meta.config.content_type)
        {
            return Err(Error::ContentTypeMismatch {
                expected: source_meta.config.content_type.clone(),
                actual: config.content_type.clone(),
            });
        }

        // Inherit content type from source (use source's exact casing)
        let fork_content_type = source_meta.config.content_type.clone();

        // Resolve TTL inheritance
        let (fork_ttl, fork_expires_at) = super::fork::resolve_fork_ttl(
            &source_meta.config,
            config.ttl_seconds,
            config.expires_at,
        );

        let fork_closed = config.created_closed;

        // --- Phase 2: Write fork meta + update source ref_count ---
        // Cross-shard concern: fork and source may be in different shards.
        // To avoid deadlocks, acquire shards in consistent order (lower index first).
        let fork_shard_idx = self.shard_index(name);
        let source_shard_idx = self.shard_index(source_name);

        if fork_shard_idx == source_shard_idx {
            // Same shard — single write transaction
            let shard = &self.shards[fork_shard_idx];
            let txn = Self::begin_write_txn(&shard.db)?;
            let mut streams = txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;

            // Re-read source meta inside write txn
            let mut src_meta = Self::read_stream_meta(&streams, source_name)?
                .ok_or_else(|| Error::NotFound(source_name.to_string()))?;

            super::fork::check_stream_access(&src_meta.config, src_meta.state, source_name)?;

            // Check idempotent create
            if let Some(existing) = Self::read_stream_meta(&streams, name)? {
                if super::is_stream_expired(&existing.config) {
                    let mut messages = txn
                        .open_table(MESSAGES)
                        .map_err(|e| Self::storage_err("failed to open messages table", e))?;
                    Self::delete_stream_messages(&mut messages, name)?;
                    drop(messages);
                    streams
                        .remove(name)
                        .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                    self.saturating_sub_total_bytes(existing.total_bytes);
                } else if existing.state == StreamState::Tombstone {
                    return Err(Error::StreamGone(name.to_string()));
                } else {
                    // Idempotent check: config must match
                    let expected_config =
                        Self::build_fork_config(&fork_content_type, fork_ttl, fork_expires_at, fork_closed);
                    if existing.config == expected_config {
                        return Ok(CreateStreamResult::AlreadyExists);
                    }
                    return Err(Error::ConfigMismatch);
                }
            }

            let (fork_read_seq, fork_byte_offset) =
                resolved_offset.parse_components().unwrap_or((0, 0));

            let fork_config =
                Self::build_fork_config(&fork_content_type, fork_ttl, fork_expires_at, fork_closed);

            let fork_meta = StoredStreamMeta {
                config: fork_config,
                closed: fork_closed,
                next_read_seq: fork_read_seq,
                next_byte_offset: fork_byte_offset,
                total_bytes: 0,
                created_at: Utc::now(),
                updated_at: None,
                last_seq: None,
                producers: HashMap::new(),
                fork_info: Some(ForkInfo {
                    source_name: source_name.to_string(),
                    fork_offset: resolved_offset,
                }),
                ref_count: 0,
                state: StreamState::Active,
            };

            Self::write_stream_meta(&mut streams, name, &fork_meta)?;

            // Increment source ref_count
            src_meta.ref_count += 1;
            Self::write_stream_meta(&mut streams, source_name, &src_meta)?;

            drop(streams);
            txn.commit()
                .map_err(|e| Self::storage_err("failed to commit create fork", e))?;
        } else {
            // Different shards — two write transactions.
            // Always acquire lower shard index first to avoid deadlocks.
            let (first_idx, second_idx) = if fork_shard_idx < source_shard_idx {
                (fork_shard_idx, source_shard_idx)
            } else {
                (source_shard_idx, fork_shard_idx)
            };

            let first_txn = Self::begin_write_txn(&self.shards[first_idx].db)?;
            let second_txn = Self::begin_write_txn(&self.shards[second_idx].db)?;

            // Get stream tables from both
            let mut first_streams = first_txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;
            let mut second_streams = second_txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;

            // Determine which table has source and which has fork
            let (source_streams, fork_streams) = if source_shard_idx == first_idx {
                (&mut first_streams, &mut second_streams)
            } else {
                (&mut second_streams, &mut first_streams)
            };

            // Re-read source meta inside write txn
            let mut src_meta = Self::read_stream_meta(source_streams, source_name)?
                .ok_or_else(|| Error::NotFound(source_name.to_string()))?;

            super::fork::check_stream_access(&src_meta.config, src_meta.state, source_name)?;

            // Check idempotent create
            if let Some(existing) = Self::read_stream_meta(fork_streams, name)? {
                if super::is_stream_expired(&existing.config) {
                    // Need messages table from fork shard
                    let fork_shard_txn = if fork_shard_idx == first_idx {
                        &first_txn
                    } else {
                        &second_txn
                    };
                    let mut messages = fork_shard_txn
                        .open_table(MESSAGES)
                        .map_err(|e| Self::storage_err("failed to open messages table", e))?;
                    Self::delete_stream_messages(&mut messages, name)?;
                    drop(messages);
                    fork_streams
                        .remove(name)
                        .map_err(|e| Self::storage_err("failed to remove expired stream", e))?;
                    self.saturating_sub_total_bytes(existing.total_bytes);
                } else if existing.state == StreamState::Tombstone {
                    return Err(Error::StreamGone(name.to_string()));
                } else {
                    let expected_config =
                        Self::build_fork_config(&fork_content_type, fork_ttl, fork_expires_at, fork_closed);
                    if existing.config == expected_config {
                        return Ok(CreateStreamResult::AlreadyExists);
                    }
                    return Err(Error::ConfigMismatch);
                }
            }

            let (fork_read_seq, fork_byte_offset) =
                resolved_offset.parse_components().unwrap_or((0, 0));

            let fork_config =
                Self::build_fork_config(&fork_content_type, fork_ttl, fork_expires_at, fork_closed);

            let fork_meta = StoredStreamMeta {
                config: fork_config,
                closed: fork_closed,
                next_read_seq: fork_read_seq,
                next_byte_offset: fork_byte_offset,
                total_bytes: 0,
                created_at: Utc::now(),
                updated_at: None,
                last_seq: None,
                producers: HashMap::new(),
                fork_info: Some(ForkInfo {
                    source_name: source_name.to_string(),
                    fork_offset: resolved_offset,
                }),
                ref_count: 0,
                state: StreamState::Active,
            };

            Self::write_stream_meta(fork_streams, name, &fork_meta)?;

            // Increment source ref_count
            src_meta.ref_count += 1;
            Self::write_stream_meta(source_streams, source_name, &src_meta)?;

            drop(first_streams);
            drop(second_streams);

            // Commit lower shard first, then higher
            first_txn
                .commit()
                .map_err(|e| Self::storage_err("failed to commit fork (first shard)", e))?;
            second_txn
                .commit()
                .map_err(|e| Self::storage_err("failed to commit fork (second shard)", e))?;
        }

        // Create notifier for the fork
        self.notifier_sender(name);

        Ok(CreateStreamResult::Created)
    }

    fn touch_ttl(&self, name: &str) -> Result<()> {
        let shard = self.shard(name);
        let write_txn = shard
            .db
            .begin_write()
            .map_err(|e| Self::storage_err("failed to begin write transaction", e))?;

        {
            let mut streams = write_txn
                .open_table(STREAMS)
                .map_err(|e| Self::storage_err("failed to open streams table", e))?;

            let mut meta = Self::read_stream_meta(&streams, name)?
                .ok_or_else(|| Error::NotFound(name.to_string()))?;

            super::fork::check_stream_access(&meta.config, meta.state, name)?;

            if let Some(ttl) = meta.config.ttl_seconds {
                meta.config.expires_at = Some(
                    Utc::now()
                        + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX)),
                );
                Self::write_stream_meta(&mut streams, name, &meta)?;
            }
        }

        write_txn
            .commit()
            .map_err(|e| Self::storage_err("failed to commit transaction", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    fn test_storage_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("ds-acid-storage-test-{stamp}-{pid}-{seq}"))
    }

    fn test_storage() -> AcidStorage {
        AcidStorage::new(
            test_storage_dir(),
            16,
            1024 * 1024,
            100 * 1024,
            AcidBackend::File,
        )
        .expect("acid storage should initialize")
    }

    // Restore-from-disk and producer-state durability tests live in
    // tests/acid_crash_recovery.rs and the storage_backend_contract suite.

    #[test]
    fn test_shard_routing_same_stream_is_stable() {
        let storage = test_storage();
        let a = storage.shard_index("same-stream");
        let b = storage.shard_index("same-stream");
        assert_eq!(a, b);
    }

    #[test]
    fn test_shard_distribution_uses_multiple_shards() {
        let storage = test_storage();
        let mut seen = std::collections::HashSet::new();
        for i in 0..256 {
            seen.insert(storage.shard_index(&format!("stream-{i}")));
        }
        assert!(seen.len() > 1);
    }

    #[test]
    fn test_startup_purges_expired_streams() {
        let root = test_storage_dir();
        {
            let storage =
                AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File)
                    .unwrap();
            let expires = Utc::now() + Duration::milliseconds(50);
            let cfg = StreamConfig::new("text/plain".to_string()).with_expires_at(expires);
            storage.create_stream("expiring", cfg).unwrap();
            storage
                .append("expiring", Bytes::from("x"), "text/plain")
                .unwrap();
        }

        std::thread::sleep(std::time::Duration::from_millis(100));

        let restored =
            AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
        assert!(!restored.exists("expiring"));
        assert!(matches!(
            restored.read("expiring", &Offset::start()),
            Err(Error::NotFound(_) | Error::StreamExpired)
        ));
    }

    #[test]
    fn test_global_cap_strict_under_concurrency() {
        let storage = Arc::new(
            AcidStorage::new(test_storage_dir(), 16, 120, 120, AcidBackend::File).unwrap(),
        );
        let shard_count = (0..8)
            .map(|i| storage.shard_index(&format!("s-{i}")))
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert!(
            shard_count > 1,
            "test streams must span multiple shards to validate cross-shard cap behavior"
        );

        for i in 0..8 {
            storage
                .create_stream(
                    &format!("s-{i}"),
                    StreamConfig::new("text/plain".to_string()),
                )
                .unwrap();
        }

        let mut handles = Vec::new();
        for i in 0..8 {
            let storage = Arc::clone(&storage);
            handles.push(thread::spawn(move || {
                storage.append(&format!("s-{i}"), Bytes::from(vec![0_u8; 40]), "text/plain")
            }));
        }

        for h in handles {
            let _ = h.join().unwrap();
        }

        assert!(storage.total_bytes() <= 120);
    }

    #[test]
    fn test_layout_manifest_mismatch_fails_fast() {
        let root = test_storage_dir();
        let first = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
        assert!(first.is_ok());

        let mismatch = AcidStorage::new(root, 8, 1024 * 1024, 100 * 1024, AcidBackend::File);
        assert!(matches!(mismatch, Err(Error::Storage(_))));
    }

    #[test]
    fn test_layout_manifest_invalid_json_fails_fast() {
        let root = test_storage_dir();
        let acid_dir = root.join("acid");
        fs::create_dir_all(&acid_dir).unwrap();
        fs::write(acid_dir.join("layout.json"), b"{invalid-json").unwrap();

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_layout_manifest_hash_policy_mismatch_fails_fast() {
        let root = test_storage_dir();
        let storage =
            AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
        drop(storage);

        let layout_path = root.join("acid").join("layout.json");
        let mut layout: serde_json::Value =
            serde_json::from_slice(&fs::read(&layout_path).unwrap()).unwrap();
        layout["hash_policy"] = serde_json::Value::String("tampered-hash-policy".to_string());
        fs::write(layout_path, serde_json::to_vec_pretty(&layout).unwrap()).unwrap();

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_corrupted_stream_metadata_fails_fast_on_startup() {
        let root = test_storage_dir();
        let storage =
            AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
        storage
            .create_stream("s", StreamConfig::new("text/plain".to_string()))
            .unwrap();
        storage
            .append("s", Bytes::from("payload"), "text/plain")
            .unwrap();

        // Simulate on-disk corruption of stream metadata.
        let shard_idx = storage.shard_index("s");
        let txn = AcidStorage::begin_write_txn(&storage.shards[shard_idx].db).unwrap();
        let mut streams = txn.open_table(STREAMS).unwrap();
        let corrupt = b"{not-json".to_vec();
        streams.insert("s", corrupt.as_slice()).unwrap();
        drop(streams);
        txn.commit().unwrap();
        drop(storage);

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_tampered_shard_file_fails_fast_on_startup() {
        let root = test_storage_dir();
        let storage =
            AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
        storage
            .create_stream("s", StreamConfig::new("text/plain".to_string()))
            .unwrap();
        storage
            .append("s", Bytes::from("payload"), "text/plain")
            .unwrap();
        let shard_idx = storage.shard_index("s");
        drop(storage);

        let shard_path = root
            .join("acid")
            .join(format!("shard_{shard_idx:02x}.redb"));
        fs::write(&shard_path, b"not-a-valid-redb-file").unwrap();

        let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
        assert!(matches!(reopened, Err(Error::Storage(_))));
    }

    #[test]
    fn test_in_memory_backend_create_append_read() {
        let storage = AcidStorage::new(
            test_storage_dir(),
            4,
            1024 * 1024,
            100 * 1024,
            AcidBackend::InMemory,
        )
        .expect("in-memory acid storage should initialize");

        let cfg = StreamConfig::new("text/plain".to_string());
        storage.create_stream("s", cfg).unwrap();
        storage
            .append("s", Bytes::from("hello"), "text/plain")
            .unwrap();
        storage
            .append("s", Bytes::from("world"), "text/plain")
            .unwrap();

        let read = storage.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 2);
        assert_eq!(read.messages[0], Bytes::from("hello"));
        assert_eq!(read.messages[1], Bytes::from("world"));

        let meta = storage.head("s").unwrap();
        assert_eq!(meta.message_count, 2);
        assert_eq!(meta.total_bytes, 10);
    }

    #[test]
    fn test_in_memory_backend_global_cap() {
        let storage = AcidStorage::new(test_storage_dir(), 4, 50, 50, AcidBackend::InMemory)
            .expect("in-memory acid storage should initialize");

        let cfg = StreamConfig::new("text/plain".to_string());
        storage.create_stream("s", cfg).unwrap();
        storage
            .append("s", Bytes::from(vec![0_u8; 40]), "text/plain")
            .unwrap();

        let result = storage.append("s", Bytes::from(vec![0_u8; 20]), "text/plain");
        assert!(result.is_err());
        assert_eq!(storage.total_bytes(), 40);
    }
}
