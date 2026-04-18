//! Storage backends and the persistence contract used by the server.
//!
//! [`Storage`] is the central abstraction. The built-in implementations are:
//!
//! - [`memory::InMemoryStorage`] for ephemeral development and tests
//! - [`file::FileStorage`] for append-log persistence on the local filesystem
//! - [`acid::AcidStorage`] for crash-resilient redb-backed persistence
//!
//! The two disk-backed families serve different needs:
//!
//! - [`file::FileStorage`] is the simpler "one directory plus one log file per
//!   stream" backend used by [`crate::config::StorageMode::FileFast`] and
//!   [`crate::config::StorageMode::FileDurable`]
//! - [`acid::AcidStorage`] is the transactional backend used by
//!   [`crate::config::StorageMode::Acid`], with
//!   [`crate::config::AcidBackend::File`] persisting redb databases to disk
//!   and [`crate::config::AcidBackend::InMemory`] keeping those databases only
//!   in memory
//!
//! If you want the lowest operational complexity and are comfortable with a
//! file-log design, start with [`file::FileStorage`]. If you want stronger
//! transactional durability and recovery guarantees, prefer
//! [`acid::AcidStorage`].

pub mod acid;
pub mod file;
pub(crate) mod fork;
pub mod memory;
pub(crate) mod shared;

use crate::protocol::error::Result;
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::sync::broadcast;

// Re-export shared items so existing `super::` paths in backends still work.
pub(crate) use shared::{
    NOTIFY_CHANNEL_CAPACITY, ProducerAppendPrecheck, ProducerState, apply_append_metadata,
    build_stream_metadata, cleanup_stale_producers, is_stream_expired, is_stream_visible,
    precheck_append, precheck_batch_append, precheck_producer_append,
};

/// Immutable stream configuration captured at create time.
///
/// This is the durable metadata returned by `HEAD` and used for idempotent
/// create checks. The server treats it as part of the stream identity: recreating
/// an existing stream with a different configuration is a conflict.
///
/// Custom `PartialEq`: when `ttl_seconds` is `Some`, `expires_at` is
/// derived from `Utc::now()` and will drift between requests.  The
/// comparison therefore ignores `expires_at` in that case.  When
/// `ttl_seconds` is `None` and `expires_at` was set directly (via
/// `Expires-At` header), the parsed timestamp is stable so we compare it.
#[derive(Debug, Clone, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    /// Content-Type header value (normalized, lowercase)
    pub content_type: String,
    /// Time-to-live in seconds (optional)
    pub ttl_seconds: Option<u64>,
    /// Absolute expiration time (optional)
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether the stream was created closed
    pub created_closed: bool,
}

impl PartialEq for StreamConfig {
    fn eq(&self, other: &Self) -> bool {
        self.content_type == other.content_type
            && self.ttl_seconds == other.ttl_seconds
            && self.created_closed == other.created_closed
            && if self.ttl_seconds.is_some() {
                // TTL-derived expires_at drifts with Utc::now(); skip comparison
                true
            } else {
                self.expires_at == other.expires_at
            }
    }
}

impl StreamConfig {
    /// Create a config with a normalized content type and default flags.
    #[must_use]
    pub fn new(content_type: String) -> Self {
        Self {
            content_type,
            ttl_seconds: None,
            expires_at: None,
            created_closed: false,
        }
    }

    /// Set a relative time-to-live in seconds.
    #[must_use]
    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = Some(ttl_seconds);
        self
    }

    /// Set an absolute expiration timestamp.
    #[must_use]
    pub fn with_expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Record that the stream should be considered created in the closed state.
    #[must_use]
    pub fn with_created_closed(mut self, created_closed: bool) -> Self {
        self.created_closed = created_closed;
        self
    }
}

/// Fork lineage metadata for a stream created via `create_fork`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForkInfo {
    /// Name of the source stream this fork was created from.
    pub source_name: String,
    /// Offset at which this fork diverged from the source (serialized as string).
    #[serde(
        serialize_with = "crate::protocol::offset::serialize_offset",
        deserialize_with = "crate::protocol::offset::deserialize_offset"
    )]
    pub fork_offset: Offset,
}

/// Lifecycle state of a stream (active or soft-deleted tombstone).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StreamState {
    /// Normal operational state.
    #[default]
    Active,
    /// Soft-deleted: still held in memory for fork ref-count bookkeeping
    /// but invisible to regular operations.
    Tombstone,
}

/// Stored message plus bookkeeping metadata used by storage backends.
#[derive(Debug, Clone)]
pub struct Message {
    /// Message offset (unique identifier within stream)
    pub offset: Offset,
    /// Message data
    pub data: Bytes,
    /// Byte length (for memory tracking)
    pub byte_len: u64,
}

impl Message {
    /// Create a stored message and derive its tracked byte length.
    #[must_use]
    pub fn new(offset: Offset, data: Bytes) -> Self {
        let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
        Self {
            offset,
            data,
            byte_len,
        }
    }
}

/// Snapshot returned by [`Storage::read`].
///
/// Handlers map this directly into catch-up, long-poll, and SSE responses.
#[derive(Debug)]
pub struct ReadResult {
    /// Messages read
    pub messages: Vec<Bytes>,
    /// Next offset to read from (for resumption)
    pub next_offset: Offset,
    /// Whether we're at the end of the stream
    pub at_tail: bool,
    /// Whether the stream is closed
    pub closed: bool,
}

/// Stream-level metadata returned by [`Storage::head`].
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// Stream configuration
    pub config: StreamConfig,
    /// Next offset that will be assigned
    pub next_offset: Offset,
    /// Whether the stream is closed
    pub closed: bool,
    /// Total bytes stored in this stream
    pub total_bytes: u64,
    /// Number of messages in the stream
    pub message_count: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp (append, close, producer write)
    pub updated_at: Option<DateTime<Utc>>,
}

/// Outcome of [`Storage::create_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateStreamResult {
    /// A new stream was created.
    Created,
    /// Stream already existed with matching config (idempotent create).
    AlreadyExists,
}

/// Immutable snapshot of a fork create request after source-derived fields
/// have been resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForkCreateSpec {
    pub source_name: String,
    pub fork_offset: Offset,
    pub config: StreamConfig,
}

/// Outcome of [`Storage::create_stream_with_data`].
///
/// Bundles creation status with a metadata snapshot taken under the
/// same lock hold so the handler never needs a separate `head()` call.
#[derive(Debug)]
pub struct CreateWithDataResult {
    /// Whether the stream was newly created or already existed.
    pub status: CreateStreamResult,
    /// Next offset (for `Stream-Next-Offset` response header).
    pub next_offset: Offset,
    /// Whether the stream is closed (for `Stream-Closed` response header).
    pub closed: bool,
}

/// Outcome of [`Storage::append_with_producer`].
///
/// Includes a snapshot of stream state taken atomically with the operation
/// so handlers never need a separate `head()` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerAppendResult {
    /// New data accepted (200 OK)
    Accepted {
        epoch: u64,
        seq: u64,
        next_offset: Offset,
        closed: bool,
    },
    /// Duplicate detected, data already persisted (204 No Content)
    Duplicate {
        epoch: u64,
        seq: u64,
        next_offset: Offset,
        closed: bool,
    },
}

/// Persistence contract for Durable Streams server state.
///
/// Methods are intentionally synchronous. The server keeps async boundaries in
/// the HTTP and notification layers so storage implementations can focus on
/// atomicity, ordering, and recovery.
///
/// Implementations are expected to preserve these invariants:
///
/// - per-stream offsets are monotonic
/// - create, append, close, and delete are atomic at the stream level
/// - duplicate producer appends are idempotent
/// - reads observe a coherent snapshot
/// - expired streams behave as if they no longer exist
///
/// Implementations must also be thread-safe (`Send + Sync`), because the axum
/// server shares them across request handlers.
///
/// Error conditions are documented inline rather than in separate sections
/// to avoid repetitive documentation on internal trait methods.
#[allow(clippy::missing_errors_doc)]
pub trait Storage: Send + Sync {
    /// Create a stream entry with immutable configuration.
    ///
    /// Returns whether the stream was newly created or already existed with
    /// matching configuration.
    ///
    /// Returns `Err(Error::ConfigMismatch)` if stream exists with different config.
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult>;

    /// Append one message to an existing stream.
    ///
    /// Generates and returns the offset assigned to the appended message.
    /// Offsets must remain monotonically increasing within a stream.
    ///
    /// Returns `Err(Error::StreamClosed)` if stream is closed.
    /// Returns `Err(Error::ContentTypeMismatch)` if content type doesn't match.
    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset>;

    /// Append a batch of messages as one atomic operation.
    ///
    /// All messages are validated and committed as a single atomic operation.
    /// Either all messages are appended successfully, or none are.
    /// Returns the next offset (the offset that will be assigned to the
    /// next message appended after this batch).
    ///
    /// If `seq` is `Some`, validates lexicographic ordering against the
    /// stream's last seq and updates it on success.
    ///
    /// Returns `Err(Error::StreamClosed)` if stream is closed.
    /// Returns `Err(Error::ContentTypeMismatch)` if content type doesn't match.
    /// Returns `Err(Error::SeqOrderingViolation)` if seq <= last seq.
    /// Returns `Err(Error::MemoryLimitExceeded)` if batch would exceed limits.
    fn batch_append(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset>;

    /// Read from a stream starting at `from_offset`.
    ///
    /// `Offset::start()` reads from the beginning of the stream.
    /// `Offset::now()` positions the caller at the current tail and returns
    /// an empty catch-up result.
    ///
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    /// Returns `Err(Error::InvalidOffset)` if offset is invalid.
    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult>;

    /// Delete a stream and all of its persisted data.
    ///
    /// Returns `Ok(())` on successful deletion.
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    fn delete(&self, name: &str) -> Result<()>;

    /// Return stream metadata without reading message bodies.
    ///
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    fn head(&self, name: &str) -> Result<StreamMetadata>;

    /// Mark a stream closed so future appends are rejected.
    ///
    /// Prevents further appends.
    /// Returns `Ok(())` if already closed (idempotent).
    /// Returns `Err(Error::NotFound)` if stream doesn't exist.
    fn close_stream(&self, name: &str) -> Result<()>;

    /// Append with idempotent producer sequencing.
    ///
    /// Validates producer epoch/sequence, appends data if accepted, and
    /// optionally closes the stream — all within a single lock hold.
    ///
    /// Returns `ProducerAppendResult::Accepted` for new data (200 OK).
    /// Returns `ProducerAppendResult::Duplicate` for already-seen seq (204).
    /// Returns `Err(EpochFenced)` if epoch < current (403).
    /// Returns `Err(SequenceGap)` if seq > expected (409).
    /// Returns `Err(InvalidProducerState)` if epoch bump with seq != 0 (400).
    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult>;

    /// Atomically create a stream, optionally seed it with data, and optionally close it.
    ///
    /// Creates the stream, appends `messages` (if non-empty), and closes
    /// (if `should_close`) — all before the entry becomes visible to other
    /// operations. If `commit_messages` fails (e.g. memory limit), the
    /// stream is never created.
    ///
    /// For idempotent recreates (`AlreadyExists`), the body and close
    /// flag are ignored and existing metadata is returned.
    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult>;

    /// Check if a stream exists
    fn exists(&self, name: &str) -> bool;

    /// Subscribe to notifications for new data on a stream.
    ///
    /// Returns a broadcast receiver that fires when data is appended
    /// or the stream is closed. Returns `None` if the stream does not
    /// exist or has expired.
    ///
    /// The method itself is sync; the handler awaits on the receiver.
    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>>;

    /// Proactively remove all expired streams, returning the count deleted.
    ///
    /// By default, expired streams are only cleaned up lazily when accessed.
    /// This method sweeps all streams and deletes any that have expired,
    /// reclaiming their resources immediately.
    fn cleanup_expired_streams(&self) -> usize;

    /// List all non-expired streams with their metadata.
    ///
    /// Returns `(name, metadata)` pairs sorted by stream name.
    /// Expired streams are excluded from the listing.
    ///
    /// Returns `Err(Error::Storage)` if the underlying backend cannot be read.
    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>>;

    /// Create a fork of an existing stream.
    ///
    /// The fork inherits messages from the source up to `fork_offset` (or the
    /// source tail if `None`). Subsequent appends go into the fork's own
    /// storage. The source's `ref_count` is incremented so it cannot be
    /// garbage-collected while forks exist.
    ///
    /// Returns `Err(StreamGone)` if the source is tombstoned.
    /// Returns `Err(ForkOffsetBeyondTail)` if `fork_offset` exceeds the source tail.
    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult>;
}
