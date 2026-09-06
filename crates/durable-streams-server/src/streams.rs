//! Stream-domain operations and metadata above the storage contract.
//!
//! [`StreamService`] is the public consolidation point for stream operations.
//! It currently delegates atomic persistence operations to [`Storage`] and owns
//! the operator listing projection. HTTP handlers and CLI listing use this
//! surface; backend implementations retain locking, transaction, and recovery rules.

use crate::protocol::error::Result;
use crate::protocol::offset::Offset;
use crate::storage::{Storage, StreamConfig};
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Stream-level metadata returned by stream-domain and storage operations.
///
/// This is the canonical metadata type for the server. Storage backends fill it
/// from their backend-specific state, while handlers and operator tooling use
/// it through [`StreamService`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamMetadata {
    /// Stream configuration.
    pub config: StreamConfig,
    /// Next offset that will be assigned.
    pub next_offset: Offset,
    /// Whether the stream is closed.
    pub closed: bool,
    /// Total bytes stored in this stream.
    pub total_bytes: u64,
    /// Number of messages in the stream.
    pub message_count: u64,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp (append, close, producer write).
    pub updated_at: Option<DateTime<Utc>>,
}

impl StreamMetadata {
    /// Construct basic stream metadata; set usage and update fields from the same snapshot.
    #[must_use]
    pub fn new(
        config: StreamConfig,
        next_offset: Offset,
        closed: bool,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            config,
            next_offset,
            closed,
            created_at,
            total_bytes: 0,
            message_count: 0,
            updated_at: None,
        }
    }
}

/// Operator-facing representation of a stream returned by list operations.
///
/// Carries the fields an operator or admin tool needs to inspect streams:
/// status, message counts, byte totals, content type, timestamps, and TTL.
/// Serialises to JSON for `GET /admin/streams` when the optional admin router
/// is enabled, and is also used by the CLI for local-by-default inspection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct StreamListEntry {
    /// Stream name (the path segment used in protocol requests).
    pub name: String,
    /// Whether the stream is closed to further appends.
    pub closed: bool,
    /// Number of messages in the stream.
    pub message_count: u64,
    /// Total bytes stored across all messages.
    pub total_bytes: u64,
    /// Normalised content-type declared at create time.
    pub content_type: String,
    /// When the stream was created.
    pub created_at: DateTime<Utc>,
    /// When the stream was last written to (append, close, or producer write).
    pub updated_at: Option<DateTime<Utc>>,
    /// Relative TTL in seconds if one was set at create time.
    pub ttl_seconds: Option<u64>,
    /// Absolute expiration timestamp if one was set at create time.
    pub expires_at: Option<DateTime<Utc>>,
}

impl StreamListEntry {
    /// Build an entry from a storage-level `(name, metadata)` pair.
    #[must_use]
    pub fn from_metadata(name: String, meta: &StreamMetadata) -> Self {
        Self {
            name,
            closed: meta.closed,
            message_count: meta.message_count,
            total_bytes: meta.total_bytes,
            content_type: meta.config.content_type.clone(),
            created_at: meta.created_at,
            updated_at: meta.updated_at,
            ttl_seconds: meta.config.ttl_seconds,
            expires_at: meta.config.expires_at,
        }
    }
}

/// Shared stream operations used by HTTP handlers and operator tooling.
///
/// Clones share the same storage. The service does not start background tasks;
/// [`crate::router::Server`] owns HTTP configuration and worker lifecycle.
#[derive(Clone)]
pub struct StreamService {
    pub(crate) storage: Arc<dyn Storage>,
}

impl StreamService {
    /// Wrap a concrete or runtime-selected storage backend without starting workers.
    #[must_use]
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// List all non-expired streams with their metadata.
    ///
    /// This preserves the storage contract's `(name, metadata)` shape for
    /// callers that need full metadata snapshots.
    ///
    /// # Errors
    ///
    /// Returns the underlying storage error if stream metadata cannot be read.
    pub fn list(&self) -> Result<Vec<(String, StreamMetadata)>> {
        self.storage.list_streams()
    }

    /// List all non-expired streams as operator-facing entries.
    ///
    /// This is the preferred surface for CLI and admin list operations because
    /// it keeps projection rules in the stream domain rather than duplicating
    /// them in each transport or command path.
    ///
    /// # Errors
    ///
    /// Returns the underlying storage error if stream metadata cannot be read.
    pub fn list_entries(&self) -> Result<Vec<StreamListEntry>> {
        self.list().map(|streams| {
            streams
                .into_iter()
                .map(|(name, meta)| StreamListEntry::from_metadata(name, &meta))
                .collect()
        })
    }

    /// Return metadata for a single stream.
    ///
    /// Delegates to [`Storage::head`] today.
    ///
    /// # Errors
    ///
    /// Returns [`crate::protocol::error::Error::NotFound`] if the stream
    /// does not exist, `StreamExpired` if it has expired, or a storage error if metadata cannot be read.
    pub fn head(&self, name: &str) -> Result<StreamMetadata> {
        self.storage.head(name)
    }

    /// Create a stream with validated options and optional initial data.
    ///
    /// # Errors
    /// Returns configuration, capacity, conflict, or persistence failures without partial creation.
    pub fn create(
        &self,
        name: &str,
        options: crate::storage::StreamOptions,
        messages: Vec<bytes::Bytes>,
    ) -> Result<crate::storage::CreateWithDataResult> {
        let closed = options.closes_on_create();
        self.storage
            .create_stream_with_data(name, options, messages, closed)
    }

    /// Append one message with explicit starting and resume positions.
    ///
    /// # Errors
    /// Returns access, content-type, capacity, or persistence failures.
    pub fn append(
        &self,
        name: &str,
        data: bytes::Bytes,
        content_type: &str,
    ) -> Result<crate::storage::AppendResult> {
        self.storage.append(name, data, content_type)
    }

    /// Append and optionally close in one storage operation.
    ///
    /// # Errors
    /// Returns access, content-type, writer-ordering, capacity, or persistence failures.
    pub fn append_batch(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        seq: Option<&str>,
        close: bool,
    ) -> Result<crate::storage::AppendResult> {
        self.storage
            .append_batch(name, messages, content_type, seq, close)
    }

    /// Append with producer deduplication and optional closure.
    ///
    /// # Errors
    /// Returns producer fencing/sequence errors and ordinary append failures.
    pub fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        producer: &crate::protocol::producer::ProducerHeaders,
        close: bool,
        seq: Option<&str>,
    ) -> Result<crate::storage::ProducerAppendResult> {
        self.storage
            .append_with_producer(name, messages, content_type, producer, close, seq)
    }

    /// Read a coherent batch and its resume position.
    ///
    /// # Errors
    /// Returns access, offset, or persistence failures.
    pub fn read(&self, name: &str, offset: &Offset) -> Result<crate::storage::ReadResult> {
        self.storage.read(name, offset)
    }

    /// Delete a stream, retaining data needed by descendants.
    ///
    /// # Errors
    /// Returns access or persistence failures.
    pub fn delete(&self, name: &str) -> Result<()> {
        self.storage.delete(name)
    }

    /// Subscribe to data/closure notifications for an existing visible stream.
    ///
    /// # Errors
    /// Propagates backend failures rather than treating them as absence.
    pub fn subscribe(&self, name: &str) -> Result<Option<tokio::sync::broadcast::Receiver<()>>> {
        self.storage.subscribe(name)
    }

    /// Create a fork, inherited prefix, and initial body atomically.
    ///
    /// # Errors
    /// Returns invalid lineage, configuration, capacity, or persistence failures.
    pub fn create_fork_with_options(
        &self,
        name: &str,
        source: &str,
        offset: Option<&Offset>,
        config: crate::storage::StreamOptions,
        options: crate::storage::ForkOptions,
    ) -> Result<crate::storage::CreateStreamResult> {
        self.storage
            .create_fork_with_options(name, source, offset, config, options)
    }
}
