//! Local-filesystem storage built from one append-only log per stream.
//!
//! [`FileStorage`] is the implementation behind the config-level
//! [`crate::config::StorageMode::FileFast`] and
//! [`crate::config::StorageMode::FileDurable`] modes. Both use the same simple
//! layout on disk:
//!
//! - one directory per stream under the configured storage root
//! - `data.log` containing length-prefixed message payloads
//! - `meta.json` containing stream metadata, producer state, and fork lineage
//!
//! At runtime the backend rebuilds an in-memory index from `data.log` so reads
//! remain fast while the on-disk format stays straightforward to inspect and
//! recover.
//!
//! # FileFast vs FileDurable
//!
//! The two file modes differ only in how aggressively writes are forced to
//! stable storage:
//!
//! - `file-fast` skips `fsync`/`fdatasync` on each append and favors throughput
//!   and lower tail latency
//! - `file-durable` performs a sync on each append and favors simpler
//!   crash-recovery expectations at the cost of write latency
//!
//! In both cases the backend is still "plain files plus an in-memory index";
//! the difference is durability policy, not data model or on-disk shape.
//!
//! # When To Use This Backend
//!
//! Choose [`FileStorage`] when you want local persistence with minimal
//! operational overhead and a storage format that is easy to reason about.
//! This is often a good fit for:
//!
//! - single-node deployments
//! - development and staging environments
//! - simpler production setups where filesystem-backed append logs are
//!   sufficient
//!
//! If you also want filesystem persistence but need transactional updates
//! across metadata and messages, prefer [`super::acid::AcidStorage`] with
//! [`crate::config::AcidBackend::File`]. That mode is also disk-backed, but it
//! stores data in redb databases rather than per-stream log files and is aimed
//! at transactional durability rather than on-disk simplicity.

mod filesys;
mod reads;
mod recovery;
mod storage_impl;
mod writes;

#[cfg(test)]
mod tests;

use super::{
    CreateStreamResult, CreateWithDataResult, ForkInfo, NOTIFY_CHANNEL_CAPACITY,
    ProducerAppendResult, ProducerState, ReadResult, Storage, StreamConfig, StreamMetadata,
    StreamState,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

use self::filesys::retry_on_eintr;

/// Binary record header size: little-endian `u32` payload length.
const RECORD_HEADER_BYTES: usize = 4;
const INITIAL_INDEX_CAPACITY: usize = 256;
const INITIAL_PRODUCERS_CAPACITY: usize = 8;

#[derive(Debug, Clone)]
struct MessageIndex {
    offset: Offset,
    file_pos: u64,
    byte_len: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamMeta {
    name: String,
    config: StreamConfig,
    closed: bool,
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

struct StreamEntry {
    config: StreamConfig,
    index: Vec<MessageIndex>,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: DateTime<Utc>,
    updated_at: Option<DateTime<Utc>>,
    producers: HashMap<String, ProducerState>,
    notify: broadcast::Sender<()>,
    last_seq: Option<String>,
    file: File,
    file_len: u64,
    dir: PathBuf,
    fork_info: Option<ForkInfo>,
    ref_count: u32,
    state: StreamState,
}

impl StreamEntry {
    fn new(config: StreamConfig, file: File, dir: PathBuf) -> Self {
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        let file_len = file.metadata().map_or(0, |m| m.len());
        Self {
            config,
            index: Vec::with_capacity(INITIAL_INDEX_CAPACITY),
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
            file,
            file_len,
            dir,
            fork_info: None,
            ref_count: 0,
            state: StreamState::Active,
        }
    }
}

/// High-throughput file-backed storage.
///
/// Design:
/// - One append-only log file per stream (`data.log`)
/// - In-memory offset/file index for fast reads
/// - Stream-level write lock serializes appends and preserves monotonic offsets
/// - Batched write per append call reduces syscall overhead
///
/// `sync_on_append = false` prioritizes throughput and may lose recently
/// appended data on crash. `sync_on_append = true` trades latency for stronger
/// durability semantics.
#[allow(clippy::module_name_repetitions)]
pub struct FileStorage {
    streams: RwLock<HashMap<String, Arc<RwLock<StreamEntry>>>>,
    total_bytes: AtomicU64,
    max_total_bytes: u64,
    max_stream_bytes: u64,
    root_dir: PathBuf,
    root_dir_canonical: PathBuf,
    sync_on_append: bool,
}

impl FileStorage {
    /// Create or reopen a file-backed storage root.
    ///
    /// Existing streams under `root_dir` are discovered and indexed during
    /// startup so subsequent reads can serve offsets without rescanning files.
    ///
    /// # Errors
    ///
    /// Returns `Error::Storage` if the root directory cannot be created or
    /// existing streams fail to load from disk.
    pub fn new(
        root_dir: impl Into<PathBuf>,
        max_total_bytes: u64,
        max_stream_bytes: u64,
        sync_on_append: bool,
    ) -> Result<Self> {
        let root_dir = root_dir.into();
        retry_on_eintr(|| fs::create_dir_all(&root_dir)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "create storage directory",
                format!(
                    "failed to create storage directory {}: {e}",
                    root_dir.display()
                ),
                &e,
            )
        })?;

        let root_dir_canonical = fs::canonicalize(&root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to canonicalize storage directory {}: {e}",
                root_dir.display()
            ))
        })?;

        let storage = Self {
            streams: RwLock::new(HashMap::new()),
            total_bytes: AtomicU64::new(0),
            max_total_bytes,
            max_stream_bytes,
            root_dir,
            root_dir_canonical,
            sync_on_append,
        };
        storage.load_existing_streams()?;
        Ok(storage)
    }

    /// Return the currently tracked total payload bytes across all streams.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }
}
