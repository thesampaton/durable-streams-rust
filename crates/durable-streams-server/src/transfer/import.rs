//! Stream import — reads a JSON export document and recreates streams
//! with their messages in a target storage backend.

use super::TransferError;
use super::format::{ExportDocument, FORMAT_VERSION};
use crate::storage::Storage;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use std::io::Read;

/// How to handle streams that already exist during import.
#[derive(Debug, Clone, Copy)]
pub enum ConflictPolicy {
    /// Skip streams that already exist and count them in the result.
    Skip,
    /// Fail the entire import if any stream already exists (pre-scans
    /// all names before writing).
    Fail,
    /// Atomically replace independent root streams; reject streams with fork lineage.
    Replace,
}

/// Options controlling import behaviour.
pub struct ImportOptions {
    /// Strategy for streams whose names collide with existing data.
    pub conflict_policy: ConflictPolicy,
}

/// Counts returned after a successful import.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
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
/// All payloads and creation options are validated before mutation. Each stream
/// commits independently; a later storage failure returns completed counts in
/// [`TransferError::PartialImport`]. Replacement requires staging capacity for
/// both old and new payloads and rejects streams involved in fork lineage.
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

    // Decode and validate every entry before the first write, including skipped entries.
    let mut names = std::collections::HashSet::new();
    let mut prepared = Vec::with_capacity(doc.streams.len());
    for stream in doc.streams {
        if stream.name.is_empty() || !names.insert(stream.name.clone()) {
            return Err(TransferError::InvalidDocument(
                "stream names must be non-empty and unique".into(),
            ));
        }
        let config = stream.config.creation_options();
        config.clone().resolve(chrono::Utc::now())?;
        let messages = stream
            .messages
            .iter()
            .map(|message| BASE64.decode(&message.data_base64).map(Bytes::from))
            .collect::<Result<Vec<_>, _>>()?;
        prepared.push((stream.name, config, messages, stream.closed));
    }
    if matches!(options.conflict_policy, ConflictPolicy::Fail) {
        for (name, ..) in &prepared {
            if storage.exists(name)? {
                return Err(TransferError::Conflict(name.clone()));
            }
        }
    }

    let mut stats = ImportStats::default();
    for (name, config, messages, closed) in prepared {
        let msg_count = messages.len();
        let result = (|| -> Result<bool, TransferError> {
            if storage.exists(&name)? {
                match options.conflict_policy {
                    ConflictPolicy::Skip => return Ok(false),
                    ConflictPolicy::Fail => return Err(TransferError::Conflict(name.clone())),
                    ConflictPolicy::Replace => {
                        storage.replace_stream(&name, config, messages, closed)?;
                        return Ok(true);
                    }
                }
            }
            let result = storage.create_stream_with_data(&name, config, messages, closed)?;
            if result.status == crate::storage::CreateStreamResult::AlreadyExists {
                // Another writer created the stream after the existence check.
                if matches!(options.conflict_policy, ConflictPolicy::Skip) {
                    return Ok(false);
                }
                return Err(TransferError::Conflict(name.clone()));
            }
            Ok(true)
        })();
        match result {
            Ok(true) => {
                stats.streams_imported += 1;
                stats.messages_imported += msg_count;
            }
            Ok(false) => stats.streams_skipped += 1,
            Err(source) => {
                return Err(TransferError::PartialImport {
                    stream: name,
                    completed: stats,
                    source: Box::new(source),
                });
            }
        }
    }
    Ok(stats)
}
