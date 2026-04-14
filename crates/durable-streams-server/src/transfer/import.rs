//! Stream import — reads a JSON export document and recreates streams
//! with their messages in a target storage backend.

use super::TransferError;
use super::format::{ExportDocument, FORMAT_VERSION};
use crate::storage::{Storage, StreamConfig};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use std::io::Read;

/// How to handle streams that already exist during import.
#[derive(Debug, Clone, Copy)]
pub enum ConflictPolicy {
    /// Skip streams that already exist (log a warning, continue).
    Skip,
    /// Fail the entire import if any stream already exists (pre-scans
    /// all names before writing).
    Fail,
    /// Delete and recreate existing streams from the export data.
    Replace,
}

/// Options controlling import behaviour.
pub struct ImportOptions {
    /// Strategy for streams whose names collide with existing data.
    pub conflict_policy: ConflictPolicy,
}

/// Counts returned after a successful import.
pub struct ImportStats {
    /// Number of streams created (or replaced) from the input.
    pub streams_imported: usize,
    /// Number of streams skipped because they already existed.
    pub streams_skipped: usize,
    /// Total messages written across all imported streams.
    pub messages_imported: usize,
}

/// Import streams from a JSON reader into storage.
///
/// The reader must contain a valid [`ExportDocument`]
/// whose `format_version` matches [`FORMAT_VERSION`].
/// Each stream is created atomically via
/// [`Storage::create_stream_with_data`].
///
/// # Errors
///
/// Returns [`TransferError`] on format version mismatch, storage errors,
/// base64 decode failures, or conflict policy violations.
pub fn import_streams<R: Read>(
    storage: &dyn Storage,
    reader: R,
    options: &ImportOptions,
) -> std::result::Result<ImportStats, TransferError> {
    let doc: ExportDocument = serde_json::from_reader(reader)?;

    if doc.format_version != FORMAT_VERSION {
        return Err(TransferError::UnsupportedVersion(doc.format_version));
    }

    // For Fail policy, pre-scan all stream names before writing anything.
    if matches!(options.conflict_policy, ConflictPolicy::Fail) {
        for stream in &doc.streams {
            if storage.exists(&stream.name) {
                return Err(TransferError::Conflict(stream.name.clone()));
            }
        }
    }

    let mut stats = ImportStats {
        streams_imported: 0,
        streams_skipped: 0,
        messages_imported: 0,
    };

    for stream in &doc.streams {
        if storage.exists(&stream.name) {
            match options.conflict_policy {
                ConflictPolicy::Skip => {
                    eprintln!("Skipping existing stream: {}", stream.name);
                    stats.streams_skipped += 1;
                    continue;
                }
                ConflictPolicy::Fail => {
                    return Err(TransferError::Conflict(stream.name.clone()));
                }
                ConflictPolicy::Replace => {
                    storage.delete(&stream.name)?;
                }
            }
        }

        let config = StreamConfig {
            content_type: stream.config.content_type.clone(),
            ttl_seconds: stream.config.ttl_seconds,
            expires_at: stream.config.expires_at,
            created_closed: stream.config.created_closed,
        };

        let mut messages = Vec::with_capacity(stream.messages.len());
        for msg in &stream.messages {
            let data = BASE64.decode(&msg.data_base64)?;
            messages.push(Bytes::from(data));
        }

        let msg_count = messages.len();
        storage.create_stream_with_data(&stream.name, config, messages, stream.closed)?;

        stats.streams_imported += 1;
        stats.messages_imported += msg_count;
    }

    Ok(stats)
}
