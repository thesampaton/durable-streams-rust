//! File-backed storage using one append-only log per stream.
//!
//! This backend keeps an in-memory index for fast reads and stores stream data
//! beneath a caller-supplied root directory. It is a good fit when you want
//! local persistence without introducing an external database.

use super::{
    CreateStreamResult, CreateWithDataResult, ForkInfo, NOTIFY_CHANNEL_CAPACITY,
    ProducerAppendResult, ProducerCheck, ProducerState, ReadResult, Storage, StreamConfig,
    StreamMetadata, StreamState,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tracing::warn;

/// Binary record header size: little-endian `u32` payload length.
const RECORD_HEADER_BYTES: usize = 4;
const INITIAL_INDEX_CAPACITY: usize = 256;
const INITIAL_PRODUCERS_CAPACITY: usize = 8;

/// Re-issue a syscall when interrupted by a signal (`EINTR`).
///
/// Rust's standard library does not retry `fsync`, `rename`, or `mkdir` on
/// `EINTR`, so this wrapper handles the standard POSIX retry contract.
/// Unlike the previous `retry_on_eintr`, this never sleeps — `EINTR`
/// retries are immediate by convention. Other transient errors (`WouldBlock`,
/// `TimedOut`) propagate immediately and become 503 via error classification.
fn retry_on_eintr<T>(
    mut op: impl FnMut() -> std::result::Result<T, io::Error>,
) -> std::result::Result<T, io::Error> {
    loop {
        match op() {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

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

    /// Map a stream name to a directory path inside `root_dir`.
    ///
    /// Uses base64url encoding (alphabet `[A-Za-z0-9_-]`) so the output
    /// cannot contain path separators, but we verify containment anyway
    /// as defense in depth.
    fn stream_dir_for_name(&self, name: &str) -> Result<PathBuf> {
        let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(name.as_bytes());

        // Reject path traversal sequences and separators. Base64url encoding
        // (alphabet [A-Za-z0-9_-]) cannot produce these, but the explicit
        // checks act as defense in depth and satisfy static analysis (CodeQL
        // rust/path-injection).
        if encoded.contains("..") || encoded.contains('/') || encoded.contains('\\') {
            return Err(Error::Storage(
                "encoded stream name contains path traversal characters".to_string(),
            ));
        }
        if !encoded
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Error::Storage(
                "encoded stream directory contains invalid characters".to_string(),
            ));
        }

        let dir = self.root_dir.join(&encoded);
        if !dir.starts_with(&self.root_dir) {
            return Err(Error::Storage(format!(
                "stream directory escapes storage root: {encoded}"
            )));
        }
        Ok(dir)
    }

    fn validate_stream_dir(&self, dir: &Path) -> Result<()> {
        if !dir.starts_with(&self.root_dir) {
            return Err(Error::Storage(format!(
                "path escapes storage root: {}",
                dir.display()
            )));
        }

        let rel = dir.strip_prefix(&self.root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to validate storage path {}: {e}",
                dir.display()
            ))
        })?;
        if rel.components().count() != 1 {
            return Err(Error::Storage(format!(
                "invalid stream path depth: {}",
                dir.display()
            )));
        }

        if dir.exists() {
            let metadata = fs::symlink_metadata(dir).map_err(|e| {
                Error::Storage(format!(
                    "failed to stat stream directory {}: {e}",
                    dir.display()
                ))
            })?;
            if metadata.file_type().is_symlink() {
                return Err(Error::Storage(format!(
                    "stream directory cannot be a symlink: {}",
                    dir.display()
                )));
            }
            if !metadata.is_dir() {
                return Err(Error::Storage(format!(
                    "stream path is not a directory: {}",
                    dir.display()
                )));
            }

            let canonical = fs::canonicalize(dir).map_err(|e| {
                Error::Storage(format!(
                    "failed to canonicalize stream directory {}: {e}",
                    dir.display()
                ))
            })?;
            if !canonical.starts_with(&self.root_dir_canonical) {
                return Err(Error::Storage(format!(
                    "stream directory resolves outside storage root: {}",
                    dir.display()
                )));
            }
        }

        Ok(())
    }

    fn data_log_path(dir: &Path) -> PathBuf {
        dir.join("data.log")
    }

    fn meta_path(dir: &Path) -> PathBuf {
        dir.join("meta.json")
    }

    fn write_metadata_for(&self, name: &str, entry: &StreamEntry) -> Result<()> {
        self.validate_stream_dir(&entry.dir)?;
        let meta = StreamMeta {
            name: name.to_string(),
            config: entry.config.clone(),
            closed: entry.closed,
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            last_seq: entry.last_seq.clone(),
            producers: entry.producers.clone(),
            fork_info: entry.fork_info.clone(),
            ref_count: entry.ref_count,
            state: entry.state,
        };

        let meta_path = Self::meta_path(&entry.dir);
        let tmp_path = entry.dir.join("meta.json.tmp");
        let payload = serde_json::to_vec(&meta)
            .map_err(|e| Error::Storage(format!("failed to serialize stream metadata: {e}")))?;

        retry_on_eintr(|| fs::write(&tmp_path, payload.as_slice())).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "write stream metadata temp file",
                format!(
                    "failed to write metadata temp file {}: {e}",
                    tmp_path.display()
                ),
                &e,
            )
        })?;

        retry_on_eintr(|| fs::rename(&tmp_path, &meta_path)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "replace stream metadata",
                format!(
                    "failed to atomically replace metadata {}: {e}",
                    meta_path.display()
                ),
                &e,
            )
        })?;

        Ok(())
    }

    fn open_stream_file(&self, dir: &Path) -> Result<File> {
        self.validate_stream_dir(dir)?;
        let path = Self::data_log_path(dir);
        retry_on_eintr(|| {
            OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&path)
        })
        .map_err(|e| {
            Error::classify_io_failure(
                "file",
                "open stream log",
                format!("failed to open stream log {}: {e}", path.display()),
                &e,
            )
        })
    }

    fn rebuild_index(file: &mut File) -> Result<(Vec<MessageIndex>, u64, u64)> {
        let mut index = Vec::new();
        let mut next_read_seq = 0u64;
        let mut next_byte_offset = 0u64;

        let file_len = file
            .metadata()
            .map_err(|e| Error::Storage(format!("failed to stat stream log: {e}")))?
            .len();

        let mut cursor = 0u64;
        let mut header = [0u8; RECORD_HEADER_BYTES];

        while cursor < file_len {
            file.seek(SeekFrom::Start(cursor))
                .map_err(|e| Error::Storage(format!("failed to seek stream log: {e}")))?;

            let read = file
                .read(&mut header)
                .map_err(|e| Error::Storage(format!("failed to read stream log header: {e}")))?;

            if read == 0 {
                break;
            }

            if read < RECORD_HEADER_BYTES {
                file.set_len(cursor).map_err(|e| {
                    Error::Storage(format!("failed to truncate partial record: {e}"))
                })?;
                break;
            }

            let record_len = u64::from(u32::from_le_bytes(header));
            let record_end = cursor + RECORD_HEADER_BYTES as u64 + record_len;

            if record_end > file_len {
                file.set_len(cursor).map_err(|e| {
                    Error::Storage(format!("failed to truncate partial record: {e}"))
                })?;
                break;
            }

            index.push(MessageIndex {
                offset: Offset::new(next_read_seq, next_byte_offset),
                file_pos: cursor + RECORD_HEADER_BYTES as u64,
                byte_len: record_len,
            });

            next_read_seq += 1;
            next_byte_offset += record_len;
            cursor = record_end;
        }

        file.seek(SeekFrom::End(0))
            .map_err(|e| Error::Storage(format!("failed to seek end of stream log: {e}")))?;

        Ok((index, next_read_seq, next_byte_offset))
    }

    fn rollback_total_bytes(&self, bytes: u64) {
        self.total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
    }

    fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    fn append_records(
        &self,
        name: &str,
        stream: &mut StreamEntry,
        messages: &[Bytes],
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut total_batch_bytes = 0u64;
        let mut payload_bytes = 0u64;
        let mut sizes = Vec::with_capacity(messages.len());

        for msg in messages {
            let len = u64::try_from(msg.len()).unwrap_or(u64::MAX);
            if len > u64::from(u32::MAX) {
                return Err(Error::InvalidHeader {
                    header: "Content-Length".to_string(),
                    reason: "message too large for file record format".to_string(),
                });
            }
            payload_bytes += len;
            total_batch_bytes += len;
            sizes.push(len);
        }

        // Reserve global capacity first (preserves error precedence: global
        // limit takes priority over per-stream limit). Uses a CAS loop so
        // concurrent appends on different streams cannot exceed the cap.
        if self
            .total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(total_batch_bytes)
                    .filter(|next| *next <= self.max_total_bytes)
            })
            .is_err()
        {
            return Err(Error::MemoryLimitExceeded);
        }

        // Check per-stream limit; rollback global reservation on failure.
        if stream.total_bytes + total_batch_bytes > self.max_stream_bytes {
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::StreamSizeLimitExceeded);
        }

        let wire_overhead = RECORD_HEADER_BYTES.saturating_mul(messages.len());
        let mut write_buf =
            Vec::with_capacity(usize::try_from(payload_bytes).unwrap_or(0) + wire_overhead);
        for msg in messages {
            let len = u32::try_from(msg.len()).unwrap_or(u32::MAX);
            write_buf.extend_from_slice(&len.to_le_bytes());
            write_buf.extend_from_slice(msg);
        }

        let before_len = stream.file_len;

        if let Err(e) = retry_on_eintr(|| stream.file.write_all(&write_buf)) {
            // Write errors may still leave partial bytes on disk; refresh cached length.
            if let Ok(m) = stream.file.metadata() {
                stream.file_len = m.len();
            }
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::Storage(format!(
                "failed to append stream log for {name}: {e}"
            )));
        }

        if self.sync_on_append
            && let Err(e) = retry_on_eintr(|| stream.file.sync_data())
        {
            // Data may be written even if fsync fails; refresh cached length.
            if let Ok(m) = stream.file.metadata() {
                stream.file_len = m.len();
            }
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::classify_io_failure(
                "file",
                "sync stream log",
                format!("failed to sync stream log for {name}: {e}"),
                &e,
            ));
        }

        let mut cursor = before_len;
        for len in sizes {
            let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            stream.index.push(MessageIndex {
                offset,
                file_pos: cursor + RECORD_HEADER_BYTES as u64,
                byte_len: len,
            });
            stream.next_read_seq += 1;
            stream.next_byte_offset += len;
            stream.total_bytes += len;
            cursor += RECORD_HEADER_BYTES as u64 + len;
        }
        stream.file_len = cursor;

        let _ = stream.notify.send(());
        Ok(())
    }

    fn read_messages(file: &File, index_slice: &[MessageIndex]) -> Result<Vec<Bytes>> {
        if index_slice.is_empty() {
            return Ok(Vec::new());
        }

        let first_pos = index_slice[0].file_pos;
        let last = index_slice
            .last()
            .expect("index_slice non-empty due early return");
        let read_end = last.file_pos + last.byte_len;
        let read_len = read_end.saturating_sub(first_pos);

        // Use positional read (pread) to avoid the shared file-offset race
        // that occurs when multiple readers `try_clone()` the same file
        // descriptor and `seek()` concurrently.
        let mut raw = vec![0u8; usize::try_from(read_len).unwrap_or(usize::MAX)];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut raw, first_pos)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            file.seek_read(&mut raw, first_pos)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut reader = file
                .try_clone()
                .map_err(|e| Error::Storage(format!("failed to clone stream file handle: {e}")))?;
            reader
                .seek(SeekFrom::Start(first_pos))
                .map_err(|e| Error::Storage(format!("failed to seek message data: {e}")))?;
            reader
                .read_exact(&mut raw)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;
        }

        let shared = Bytes::from(raw);
        let mut messages = Vec::with_capacity(index_slice.len());
        for idx in index_slice {
            let rel_start =
                usize::try_from(idx.file_pos.saturating_sub(first_pos)).unwrap_or(usize::MAX);
            let rel_end = rel_start + usize::try_from(idx.byte_len).unwrap_or(usize::MAX);
            messages.push(shared.slice(rel_start..rel_end));
        }

        Ok(messages)
    }

    fn remove_stream_dir(&self, dir: &Path) -> Result<()> {
        self.validate_stream_dir(dir)?;
        retry_on_eintr(|| fs::remove_dir_all(dir)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "remove stream directory",
                format!("failed to remove stream directory {}: {e}", dir.display()),
                &e,
            )
        })
    }

    /// Walk up the fork chain after a hard-delete, decrementing `ref_count`s
    /// and garbage-collecting tombstoned ancestors with zero references.
    ///
    /// Must be called while holding the streams write lock.
    fn cascade_delete(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        parent_name: &str,
    ) {
        let mut current_parent = parent_name.to_string();
        loop {
            let Some(parent_arc) = streams.get(&current_parent) else {
                break;
            };
            let parent_arc = parent_arc.clone();
            let mut parent = parent_arc.write().expect("stream lock poisoned");
            parent.ref_count = parent.ref_count.saturating_sub(1);

            if parent.state == StreamState::Tombstone && parent.ref_count == 0 {
                // This parent can be garbage-collected
                let fi = parent.fork_info.clone();
                let dir = parent.dir.clone();
                let total = parent.total_bytes;
                // Persist updated ref_count before removing
                drop(parent);
                streams.remove(&current_parent);

                if let Err(e) = self.remove_stream_dir(&dir) {
                    warn!(%e, stream = current_parent.as_str(), "failed to remove tombstoned ancestor directory during cascade delete");
                } else {
                    self.rollback_total_bytes(total);
                }

                // Continue up the chain
                if let Some(fi) = fi {
                    current_parent = fi.source_name;
                } else {
                    break;
                }
            } else {
                // Parent still alive or has other children — persist ref_count update
                if let Err(e) = self.write_metadata_for(&current_parent, &parent) {
                    warn!(%e, stream = current_parent.as_str(), "failed to persist parent ref_count during cascade delete");
                }
                break;
            }
        }
    }

    /// Read messages from a source chain, following fork lineage upward.
    ///
    /// Reads all messages from `from_offset` up to (but not including) `up_to`
    /// across the full ancestor chain. This bypasses tombstone checks since
    /// source streams may be soft-deleted but their data must still be readable
    /// by forks.
    fn read_source_chain(
        &self,
        source_name: &str,
        from_offset: &Offset,
        up_to: &Offset,
    ) -> Result<Vec<Bytes>> {
        let streams = self.streams.read().expect("streams lock poisoned");

        // Build the ancestor chain from source to root
        let plan = super::fork::build_read_plan(source_name, |n| {
            streams.get(n).map(|arc| {
                let s = arc.read().expect("stream lock poisoned");
                s.fork_info.clone()
            })
        });

        let mut all_messages: Vec<Bytes> = Vec::new();

        for (i, segment) in plan.iter().enumerate() {
            let Some(seg_arc) = streams.get(&segment.name) else {
                continue;
            };
            let seg_stream = seg_arc.read().expect("stream lock poisoned");

            // Determine the effective upper bound for this segment
            let effective_up_to = if i == plan.len() - 1 {
                // Last segment (the direct source): read up to `up_to`
                Some(up_to)
            } else {
                // Intermediate segment: read up to this segment's read_up_to
                segment.read_up_to.as_ref()
            };

            // Determine start offset for this segment
            let effective_from = if i == 0 {
                from_offset
            } else {
                &Offset::start()
            };

            // Find start index in this segment's index
            let start_idx = if effective_from.is_start() {
                0
            } else {
                match seg_stream
                    .index
                    .binary_search_by(|m| m.offset.cmp(effective_from))
                {
                    Ok(idx) | Err(idx) => idx,
                }
            };

            // Find end index based on upper bound
            let end_idx = if let Some(bound) = effective_up_to {
                match seg_stream
                    .index
                    .binary_search_by(|m| m.offset.cmp(bound))
                {
                    Ok(idx) | Err(idx) => idx,
                }
            } else {
                seg_stream.index.len()
            };

            if start_idx < end_idx {
                let index_slice = &seg_stream.index[start_idx..end_idx];
                let msgs = Self::read_messages(&seg_stream.file, index_slice)?;
                all_messages.extend(msgs);
            }
        }

        Ok(all_messages)
    }

    fn load_existing_streams(&self) -> Result<()> {
        let entries = fs::read_dir(&self.root_dir).map_err(|e| {
            Error::Storage(format!(
                "failed to read storage directory {}: {e}",
                self.root_dir.display()
            ))
        })?;

        let mut streams_map = self.streams.write().expect("streams lock poisoned");
        let mut restored_total = 0u64;

        for dir_entry in entries {
            let dir_entry = dir_entry
                .map_err(|e| Error::Storage(format!("failed to inspect storage entry: {e}")))?;
            let path = dir_entry.path();
            if !path.is_dir() {
                continue;
            }
            if self.validate_stream_dir(&path).is_err() {
                continue;
            }

            let meta_path = Self::meta_path(&path);
            if !meta_path.exists() {
                continue;
            }

            let meta_payload = fs::read(&meta_path).map_err(|e| {
                Error::Storage(format!(
                    "failed to read stream metadata {}: {e}",
                    meta_path.display()
                ))
            })?;
            let meta: StreamMeta = serde_json::from_slice(&meta_payload).map_err(|e| {
                Error::Storage(format!(
                    "failed to parse stream metadata {}: {e}",
                    meta_path.display()
                ))
            })?;

            let mut file = self.open_stream_file(&path)?;
            let (index, next_read_seq, next_byte_offset) = Self::rebuild_index(&mut file)?;
            let total_bytes = next_byte_offset;
            let file_len = file
                .metadata()
                .map_err(|e| Error::Storage(format!("failed to stat stream log: {e}")))?
                .len();

            // Reconcile: data.log is the source of truth for message count
            // and byte offsets. If meta.json is stale (e.g. crash before
            // metadata flush), log a warning so operators can investigate.
            let log_msg_count = index.len() as u64;
            let meta_has_data =
                meta.closed || !meta.producers.is_empty() || meta.last_seq.is_some();
            if log_msg_count == 0 && meta_has_data {
                warn!(
                    stream = meta.name,
                    "meta.json indicates activity but data.log has 0 messages; \
                     data.log is authoritative"
                );
            }

            let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
            let mut entry = StreamEntry {
                config: meta.config,
                index,
                closed: meta.closed,
                next_read_seq,
                next_byte_offset,
                total_bytes,
                created_at: meta.created_at,
                updated_at: meta.updated_at,
                producers: meta.producers,
                notify,
                last_seq: meta.last_seq,
                file,
                file_len,
                dir: path,
                fork_info: meta.fork_info,
                ref_count: meta.ref_count,
                state: meta.state,
            };

            if super::is_stream_expired(&entry.config) {
                self.remove_stream_dir(&entry.dir)?;
                continue;
            }

            super::cleanup_stale_producers(&mut entry.producers);

            // Re-persist metadata if the on-disk copy may be stale so that
            // future restarts see a consistent snapshot. Best-effort: a
            // failure here is non-fatal since the data.log remains correct.
            if let Err(e) = self.write_metadata_for(&meta.name, &entry) {
                warn!(
                    %e,
                    stream = meta.name,
                    "failed to re-persist reconciled metadata during recovery"
                );
            }
            restored_total = restored_total.saturating_add(entry.total_bytes);
            streams_map.insert(meta.name, Arc::new(RwLock::new(entry)));
        }

        self.total_bytes.store(restored_total, Ordering::Release);

        Ok(())
    }
}

impl Storage for FileStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                let dir = stream.dir.clone();
                drop(stream);
                streams.remove(name);

                self.remove_stream_dir(&dir)?;
                self.rollback_total_bytes(stream_bytes);
            } else if stream.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else if stream.config == config {
                return Ok(CreateStreamResult::AlreadyExists);
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let dir = self.stream_dir_for_name(name)?;
        retry_on_eintr(|| fs::create_dir_all(&dir)).map_err(|e| {
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

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        super::validate_content_type(&stream.config.content_type, content_type)?;

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        self.append_records(name, &mut stream, &[data])?;
        stream.updated_at = Some(Utc::now());
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

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        super::validate_content_type(&stream.config.content_type, content_type)?;

        let pending_seq = super::validate_seq(stream.last_seq.as_deref(), seq)?;
        self.append_records(name, &mut stream, &messages)?;
        stream.updated_at = Some(Utc::now());
        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
            // Stream-Seq changed, persist metadata best-effort.
            if let Err(e) = self.write_metadata_for(name, &stream) {
                warn!(%e, stream = name, "metadata persist failed after batch append");
            }
        }

        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        if from_offset.is_now() {
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            });
        }

        // If stream has no fork lineage, use the fast path
        if stream.fork_info.is_none() {
            let start_idx = if from_offset.is_start() {
                0
            } else {
                match stream.index.binary_search_by(|m| m.offset.cmp(from_offset)) {
                    Ok(idx) | Err(idx) => idx,
                }
            };

            let index_slice = &stream.index[start_idx..];
            let messages = Self::read_messages(&stream.file, index_slice)?;
            let at_tail = start_idx + messages.len() >= stream.index.len();

            return Ok(ReadResult {
                messages,
                next_offset,
                at_tail,
                closed: stream.closed,
            });
        }

        // Fork-aware read: stitch messages from source chain + fork's own data
        let fi = stream.fork_info.clone().expect("checked above");
        let closed = stream.closed;

        // Read fork's own messages eagerly while we hold the lock
        let fork_local_messages: Vec<Bytes> = if from_offset.is_start()
            || *from_offset <= fi.fork_offset
        {
            // All fork messages
            Self::read_messages(&stream.file, &stream.index)?
        } else {
            // from_offset is past fork_offset — binary search within fork messages
            let start_idx =
                match stream
                    .index
                    .binary_search_by(|m| m.offset.cmp(from_offset))
                {
                    Ok(idx) | Err(idx) => idx,
                };
            Self::read_messages(&stream.file, &stream.index[start_idx..])?
        };
        drop(stream);

        let mut all_messages: Vec<Bytes> = Vec::new();

        // Read source data if from_offset is before the fork_offset
        if from_offset.is_start() || *from_offset < fi.fork_offset {
            let source_messages =
                self.read_source_chain(&fi.source_name, from_offset, &fi.fork_offset)?;
            all_messages.extend(source_messages);
        }

        // Add fork's own messages
        all_messages.extend(fork_local_messages);

        Ok(ReadResult {
            messages: all_messages,
            next_offset,
            at_tail: true,
            closed,
        })
    }

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let stream_arc = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?
            .clone();

        {
            let stream = stream_arc.read().expect("stream lock poisoned");

            // Already tombstoned — treat as gone
            if stream.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            }

            // Check expiry — let it fall through to removal
            if super::is_stream_expired(&stream.config) {
                // fall through
            }

            if stream.ref_count > 0 {
                // Has child forks — soft-delete (tombstone)
                drop(stream);
                let mut stream_w = stream_arc.write().expect("stream lock poisoned");
                stream_w.state = StreamState::Tombstone;
                if let Err(e) = self.write_metadata_for(name, &stream_w) {
                    warn!(%e, stream = name, "failed to persist tombstone state");
                }
                return Ok(());
            }
        }

        // No child forks — hard delete
        let fork_info = {
            let stream = stream_arc.read().expect("stream lock poisoned");
            let fi = stream.fork_info.clone();
            let dir = stream.dir.clone();
            let stream_bytes = stream.total_bytes;
            drop(stream);

            if let Err(e) = self.remove_stream_dir(&dir) {
                // Directory removal failed — stream still exists on disk
                return Err(e);
            }
            self.rollback_total_bytes(stream_bytes);
            fi
        };
        streams.remove(name);

        // Cascade: decrement parent ref_count, and if parent is tombstoned
        // with ref_count reaching 0, hard-delete it too (walk up the chain).
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

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        Ok(StreamMetadata {
            config: stream.config.clone(),
            next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
            closed: stream.closed,
            total_bytes: stream.total_bytes,
            message_count: u64::try_from(stream.index.len()).unwrap_or(u64::MAX),
            created_at: stream.created_at,
            updated_at: stream.updated_at,
        })
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        stream.closed = true;
        stream.updated_at = Some(Utc::now());
        self.write_metadata_for(name, &stream)?;

        let _ = stream.notify.send(());
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
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        super::cleanup_stale_producers(&mut stream.producers);

        if !messages.is_empty() {
            super::validate_content_type(&stream.config.content_type, content_type)?;
        }

        let now = Utc::now();

        match super::check_producer(
            stream.producers.get(producer.id.as_str()),
            producer,
            stream.closed,
        )? {
            ProducerCheck::Accept => {}
            ProducerCheck::Duplicate { epoch, seq } => {
                return Ok(ProducerAppendResult::Duplicate {
                    epoch,
                    seq,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            }
        }

        let pending_seq = super::validate_seq(stream.last_seq.as_deref(), seq)?;
        self.append_records(name, &mut stream, &messages)?;

        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
        }
        if should_close {
            stream.closed = true;
        }

        stream.producers.insert(
            producer.id.clone(),
            ProducerState {
                epoch: producer.epoch,
                last_seq: producer.seq,
                updated_at: now,
            },
        );
        stream.updated_at = Some(now);

        // Data is committed to the log; metadata write failure is non-fatal
        if let Err(e) = self.write_metadata_for(name, &stream) {
            warn!(%e, stream = name, "metadata persist failed after committed producer append");
        }

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

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                let dir = stream.dir.clone();
                drop(stream);
                streams.remove(name);

                self.remove_stream_dir(&dir)?;
                self.rollback_total_bytes(stream_bytes);
            } else if stream.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else if stream.config == config {
                return Ok(CreateWithDataResult {
                    status: CreateStreamResult::AlreadyExists,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            } else {
                return Err(Error::ConfigMismatch);
            }
        }

        let dir = self.stream_dir_for_name(name)?;
        retry_on_eintr(|| fs::create_dir_all(&dir)).map_err(|e| {
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
            !super::is_stream_expired(&stream.config) && stream.state == StreamState::Active
        } else {
            false
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let stream_arc = self.get_stream(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) || stream.state == StreamState::Tombstone {
            return None;
        }

        Some(stream.notify.subscribe())
    }

    fn cleanup_expired_streams(&self) -> usize {
        let mut streams = self.streams.write().expect("streams lock poisoned");
        let mut expired = Vec::new();

        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
            if super::is_stream_expired(&stream.config) {
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
        for (name, bytes, dir, ref_count, fork_info) in expired {
            if ref_count > 0 {
                // Has child forks — tombstone instead of hard-delete
                if let Some(arc) = streams.get(&name) {
                    let mut stream = arc.write().expect("stream lock poisoned");
                    stream.state = StreamState::Tombstone;
                    if let Err(e) = self.write_metadata_for(&name, &stream) {
                        warn!(%e, stream = name.as_str(), "failed to persist tombstone for expired stream");
                    }
                }
            } else {
                // No children — hard delete
                streams.remove(&name);
                if let Err(e) = self.remove_stream_dir(&dir) {
                    warn!(%e, stream = name.as_str(), "failed to remove expired stream directory");
                } else {
                    self.rollback_total_bytes(bytes);
                }
                // Cascade up fork chain
                if let Some(fi) = fork_info {
                    self.cascade_delete(&mut streams, &fi.source_name);
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
            if super::is_stream_expired(&stream.config)
                || stream.state == StreamState::Tombstone
            {
                continue;
            }
            result.push((
                name.clone(),
                StreamMetadata {
                    config: stream.config.clone(),
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                    total_bytes: stream.total_bytes,
                    message_count: u64::try_from(stream.index.len()).unwrap_or(u64::MAX),
                    created_at: stream.created_at,
                    updated_at: stream.updated_at,
                },
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

        // Look up source stream
        let source_arc = streams
            .get(source_name)
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?
            .clone();

        let source = source_arc.read().expect("stream lock poisoned");

        // Validate source: not expired, not tombstoned
        super::fork::check_stream_access(&source.config, source.state, source_name)?;

        // Resolve fork offset (defaults to source tail)
        let source_next_offset = Offset::new(source.next_read_seq, source.next_byte_offset);
        let resolved_offset =
            super::fork::resolve_fork_offset(fork_offset, &source_next_offset)?;

        // Validate content type: fork must match source
        if !config
            .content_type
            .eq_ignore_ascii_case(&source.config.content_type)
        {
            return Err(Error::ContentTypeMismatch {
                expected: source.config.content_type.clone(),
                actual: config.content_type.clone(),
            });
        }

        // Inherit content type from source (use source's exact casing)
        let fork_content_type = source.config.content_type.clone();

        // Resolve TTL inheritance
        let (fork_ttl, fork_expires_at) = super::fork::resolve_fork_ttl(
            &source.config,
            config.ttl_seconds,
            config.expires_at,
        );

        // Whether fork should be closed
        let fork_closed = config.created_closed;

        drop(source);

        // Check idempotent create — if fork already exists with matching config
        if let Some(existing_arc) = streams.get(name) {
            let existing = existing_arc.read().expect("stream lock poisoned");
            if super::is_stream_expired(&existing.config) {
                let existing_bytes = existing.total_bytes;
                let dir = existing.dir.clone();
                drop(existing);
                streams.remove(name);
                if let Err(e) = self.remove_stream_dir(&dir) {
                    warn!(%e, stream = name, "failed to remove expired fork directory");
                }
                self.rollback_total_bytes(existing_bytes);
            } else if existing.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else {
                // Idempotent check: config must match
                let mut expected_config = StreamConfig::new(fork_content_type.clone());
                if let Some(ttl) = fork_ttl {
                    expected_config = expected_config.with_ttl(ttl);
                }
                if let Some(ea) = fork_expires_at {
                    expected_config = expected_config.with_expires_at(ea);
                }
                if fork_closed {
                    expected_config = expected_config.with_created_closed(true);
                }
                if existing.config == expected_config {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                return Err(Error::ConfigMismatch);
            }
        }

        // Extract fork offset components to initialize the fork entry
        let (fork_read_seq, fork_byte_offset) = resolved_offset
            .parse_components()
            .unwrap_or((0, 0));

        // Build fork config
        let mut fork_config = StreamConfig::new(fork_content_type);
        if let Some(ttl) = fork_ttl {
            fork_config = fork_config.with_ttl(ttl);
        }
        if let Some(ea) = fork_expires_at {
            fork_config = fork_config.with_expires_at(ea);
        }
        if fork_closed {
            fork_config = fork_config.with_created_closed(true);
        }

        // Create fork directory and files on disk
        let dir = self.stream_dir_for_name(name)?;
        retry_on_eintr(|| fs::create_dir_all(&dir)).map_err(|e| {
            Error::classify_io_failure(
                "file",
                "create fork directory",
                format!("failed to create fork directory {}: {e}", dir.display()),
                &e,
            )
        })?;
        self.validate_stream_dir(&dir)?;
        let file = self.open_stream_file(&dir)?;

        // Create fork entry
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        let file_len = file.metadata().map_or(0, |m| m.len());
        let entry = StreamEntry {
            config: fork_config,
            index: Vec::with_capacity(INITIAL_INDEX_CAPACITY),
            closed: fork_closed,
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
                source_name: source_name.to_string(),
                fork_offset: resolved_offset,
            }),
            ref_count: 0,
            state: StreamState::Active,
        };

        // Write fork metadata to disk
        if let Err(e) = self.write_metadata_for(name, &entry) {
            if let Err(cleanup_err) = self.remove_stream_dir(&dir) {
                warn!(%cleanup_err, stream = name, "failed to clean up orphaned fork directory");
            }
            return Err(e);
        }

        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        // Increment source ref_count and persist
        if let Some(source_arc) = streams.get(source_name) {
            let mut source = source_arc.write().expect("stream lock poisoned");
            source.ref_count += 1;
            if let Err(e) = self.write_metadata_for(source_name, &source) {
                warn!(%e, stream = source_name, "failed to persist source ref_count after fork creation");
            }
        }

        Ok(CreateStreamResult::Created)
    }

    fn touch_ttl(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        if let Some(ttl) = stream.config.ttl_seconds {
            stream.config.expires_at = Some(
                Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX)),
            );
            self.write_metadata_for(name, &stream)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn test_storage_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("ds-file-storage-test-{stamp}-{pid}-{seq}"))
    }

    fn test_storage() -> FileStorage {
        FileStorage::new(test_storage_dir(), 1024 * 1024, 100 * 1024, false)
            .expect("file storage should initialize")
    }

    #[test]
    fn test_delete_removes_files() {
        let storage = test_storage();
        let config = StreamConfig::new("text/plain".to_string());
        storage.create_stream("test", config).unwrap();
        storage
            .append("test", Bytes::from("data"), "text/plain")
            .unwrap();

        let dir = storage.stream_dir_for_name("test").unwrap();
        assert!(dir.exists(), "stream directory should exist before delete");

        storage.delete("test").unwrap();
        assert!(
            !dir.exists(),
            "stream directory should be removed after delete"
        );
    }

    // Restore-from-disk and closed-stream durability tests live in
    // tests/crash_recovery.rs and the storage_backend_contract suite.

    #[test]
    fn test_partial_record_truncation_on_recovery() {
        let root = test_storage_dir();
        let config = StreamConfig::new("text/plain".to_string());

        {
            let storage = FileStorage::new(root.clone(), 1024 * 1024, 100 * 1024, false).unwrap();
            storage.create_stream("s", config.clone()).unwrap();
            storage
                .append("s", Bytes::from("good"), "text/plain")
                .unwrap();
        }

        // Append partial garbage to the data log (incomplete record header)
        let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode("s".as_bytes());
        let log_path = root.join(&encoded).join("data.log");
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        // Write a 2-byte partial header (less than the 4-byte record header)
        f.write_all(&[0xFF, 0xFF]).unwrap();
        drop(f);

        let restored = FileStorage::new(root, 1024 * 1024, 100 * 1024, false).unwrap();
        let read = restored.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 1);
        assert_eq!(read.messages[0], Bytes::from("good"));
    }
}
