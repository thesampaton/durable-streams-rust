//! Serialisable types for the durable-streams export/import JSON format.
//!
//! The top-level [`ExportDocument`] wraps a versioned envelope around a list
//! of [`ExportedStream`] entries, each containing stream metadata and the
//! full message payload (base64-encoded for binary safety).

use crate::storage::StreamConfig;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Current format version written by [`super::export::export_streams`].
pub const FORMAT_VERSION: u32 = 1;

/// Top-level envelope for an export/import JSON file.
///
/// The `format_version` field enables forward-compatible schema evolution:
/// readers reject versions they do not understand.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportDocument {
    /// Schema version (currently [`FORMAT_VERSION`]).
    pub format_version: u32,
    /// Timestamp when the export was created.
    pub exported_at: DateTime<Utc>,
    /// Server crate version that produced the export.
    pub server_version: String,
    /// Exported streams with their messages.
    pub streams: Vec<ExportedStream>,
}

/// A single stream and all its messages within an [`ExportDocument`].
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedStream {
    /// Stream name (path-style, e.g. `"events/clicks"`).
    pub name: String,
    /// Immutable configuration captured at create time.
    pub config: StreamConfig,
    /// Whether the stream was closed at export time.
    pub closed: bool,
    /// When the stream was originally created.
    pub created_at: DateTime<Utc>,
    /// When the stream was last modified (`None` for legacy data).
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    /// Total payload bytes across all messages.
    pub total_bytes: u64,
    /// Number of messages in the stream.
    pub message_count: u64,
    /// Canonical next-offset string for reference; import generates fresh
    /// offsets so this is informational only.
    pub next_offset: String,
    /// Ordered list of messages with base64-encoded payloads.
    pub messages: Vec<ExportedMessage>,
}

/// A single message within an [`ExportedStream`].
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportedMessage {
    /// Original offset at export time (informational; import assigns new offsets).
    pub offset: String,
    /// Standard base64-encoded message payload.
    pub data_base64: String,
}
