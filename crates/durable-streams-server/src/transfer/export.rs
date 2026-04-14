//! Stream export — reads all (or filtered) streams from storage and writes
//! a self-describing JSON document that can be imported elsewhere.

use super::TransferError;
use super::format::{ExportDocument, ExportedMessage, ExportedStream, FORMAT_VERSION};
use crate::protocol::offset::Offset;
use crate::storage::Storage;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use std::io::Write;

/// Options controlling which streams to export.
pub struct ExportOptions {
    /// If non-empty, only export streams whose names appear in this list.
    /// An empty list means "export everything".
    pub stream_names: Vec<String>,
}

/// Counts returned after a successful export.
pub struct ExportStats {
    /// Number of streams written to the output.
    pub streams_exported: usize,
    /// Total number of messages across all exported streams.
    pub messages_exported: usize,
}

/// Export streams from storage to a JSON writer.
///
/// Streams are listed via [`Storage::list_streams`], optionally filtered by
/// `options.stream_names`, then each stream's messages are read in full and
/// base64-encoded into the output document.
///
/// # Errors
///
/// Returns [`TransferError`] if a storage operation, JSON serialization, or
/// I/O write fails.
pub fn export_streams<W: Write>(
    storage: &dyn Storage,
    options: &ExportOptions,
    writer: W,
) -> std::result::Result<ExportStats, TransferError> {
    let all_streams = storage.list_streams()?;

    let mut exported_streams = Vec::new();
    let mut total_messages = 0usize;

    for (name, metadata) in &all_streams {
        if !options.stream_names.is_empty() && !options.stream_names.contains(name) {
            continue;
        }

        let read_result = storage.read(name, &Offset::start())?;

        let messages: Vec<ExportedMessage> = read_result
            .messages
            .iter()
            .enumerate()
            .map(|(i, data)| {
                let offset_str = Offset::new(i as u64, 0).to_string();
                ExportedMessage {
                    offset: offset_str,
                    data_base64: BASE64.encode(data),
                }
            })
            .collect();

        total_messages += messages.len();

        exported_streams.push(ExportedStream {
            name: name.clone(),
            config: metadata.config.clone(),
            closed: metadata.closed,
            created_at: metadata.created_at,
            updated_at: metadata.updated_at,
            total_bytes: metadata.total_bytes,
            message_count: metadata.message_count,
            next_offset: metadata.next_offset.to_string(),
            messages,
        });
    }

    let doc = ExportDocument {
        format_version: FORMAT_VERSION,
        exported_at: Utc::now(),
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        streams: exported_streams,
    };

    let streams_exported = doc.streams.len();
    serde_json::to_writer_pretty(writer, &doc)?;

    Ok(ExportStats {
        streams_exported,
        messages_exported: total_messages,
    })
}
