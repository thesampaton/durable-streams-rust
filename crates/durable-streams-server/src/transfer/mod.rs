//! Export and import of stream data for backup, restore, and migration.
//!
//! This module provides a versioned JSON format that captures stream metadata
//! and message payloads (base64-encoded) so they can be moved between storage
//! backends, backed up to external systems, or restored after data loss.
//!
//! # Modules
//!
//! - [`mod@format`] — serde types for the JSON document schema
//! - [`export`] — reads streams from storage and writes JSON
//! - [`import`] — reads JSON and writes streams into storage

pub mod export;
pub mod format;
pub mod import;

/// Errors that can occur during export or import operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransferError {
    /// Underlying I/O failure (file create, write, etc.).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization failure.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// A message payload could not be decoded from base64.
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    /// The export document declares a format version this build cannot handle.
    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u32),

    /// The document contains duplicate names or invalid stream metadata.
    #[error("invalid import document: {0}")]
    InvalidDocument(String),

    /// Earlier streams committed before a later stream failed. Import is atomic per stream.
    #[error("import stopped at '{stream}' after {completed:?}: {source}")]
    PartialImport {
        /// Name of the stream that failed.
        stream: String,
        /// Counts of streams and messages committed or skipped before this failure.
        completed: import::ImportStats,
        /// Failure for the current stream.
        #[source]
        source: Box<TransferError>,
    },

    /// A storage operation failed during export or import.
    #[error("storage error: {0}")]
    Storage(#[from] crate::protocol::error::Error),

    /// A stream in the import already exists and the conflict policy forbids it.
    #[error("import conflict: stream '{0}' already exists")]
    Conflict(String),
}
