//! Stream-domain types and service exposed above storage plumbing.
//!
//! [`StreamService`] is the primary entry point for stream-domain operations.
//! It wraps a [`Storage`] backend and provides a stream-centric surface that
//! handlers, CLI tooling, and future admin/operator APIs can program against
//! without reaching through to backend-specific storage internals.
//!
//! Today the service covers metadata and listing. Over time this module is the
//! intended home for additional stream-domain concepts such as retention views,
//! read-admission snapshots, lifecycle state, and operator surfaces.

use crate::protocol::error::Result;
use crate::protocol::offset::Offset;
use crate::storage::{Storage, StreamConfig};
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Stream-level metadata returned by stream-domain and storage operations.
///
/// This is the canonical metadata type for the server. Storage backends fill it
/// from their backend-specific state, while handlers and operator tooling use
/// it through [`StreamService`]. The `storage` module re-exports this type for
/// compatibility, but it is owned here because it describes stream-domain
/// state rather than backend mechanics.
#[derive(Debug, Clone)]
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

/// Operator-facing representation of a stream returned by list operations.
///
/// Carries the fields an operator or admin tool needs to inspect streams:
/// status, message counts, byte totals, content type, timestamps, and TTL.
/// Serialises to JSON for `GET /admin/streams` when the optional admin router
/// is enabled, and is also used by the CLI for local-by-default inspection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Stream-domain service that fronts storage operations.
///
/// `StreamService` is intentionally generic over `S: Storage` so it can wrap
/// any backend. It holds an `Arc<S>` — the same ownership model handlers
/// already use — and adds a domain-typed API surface on top.
///
/// The service owns stream-domain projections such as list entries and metadata
/// views. Storage remains responsible for persistence and backend mechanics;
/// protocol and admin handlers call this service instead of reaching through to
/// backend-specific state.
pub struct StreamService<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StreamService<S> {
    /// Create a new stream service wrapping the given storage backend.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// List all non-expired streams with their metadata.
    ///
    /// This preserves the storage contract's `(name, metadata)` shape for
    /// callers that need full metadata snapshots.
    pub fn list(&self) -> Result<Vec<(String, StreamMetadata)>> {
        self.storage.list_streams()
    }

    /// List all non-expired streams as operator-facing entries.
    ///
    /// This is the preferred surface for CLI and admin list operations because
    /// it keeps projection rules in the stream domain rather than duplicating
    /// them in each transport or command path.
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
    pub fn head(&self, name: &str) -> Result<StreamMetadata> {
        self.storage.head(name)
    }
}

impl<S: Storage> Clone for StreamService<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}
