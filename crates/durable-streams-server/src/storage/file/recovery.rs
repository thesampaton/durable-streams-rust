use super::{
    FileStorage, MessageIndex, NOTIFY_CHANNEL_CAPACITY, RECORD_HEADER_BYTES, StreamEntry,
    StreamMeta,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use tracing::warn;

impl FileStorage {
    pub(super) fn rebuild_index(file: &mut File) -> Result<(Vec<MessageIndex>, u64, u64)> {
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

    pub(super) fn load_existing_streams(&self) -> Result<()> {
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

            self.recover_append(&path)?;
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
            let (mut index, mut next_read_seq, mut next_byte_offset) =
                Self::rebuild_index(&mut file)?;
            let total_bytes = next_byte_offset;
            if let Some(fork) = &meta.fork_info {
                let (seq, byte) = Self::restore_fork_offsets(&mut index, fork);
                next_read_seq += seq;
                next_byte_offset += byte;
            }
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
                unavailable: false,
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

            if super::super::is_stream_expired(&entry.config) {
                if entry.ref_count == 0 {
                    self.remove_stream_dir(&entry.dir)?;
                    continue;
                }
                entry.state = super::super::StreamState::Tombstone;
            }

            super::super::cleanup_stale_producers(&mut entry.producers);

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

impl FileStorage {
    fn restore_fork_offsets(index: &mut [MessageIndex], fork: &super::ForkInfo) -> (u64, u64) {
        let (seq, byte) = fork.fork_offset.parse_components().unwrap_or((0, 0));
        for entry in index {
            let (local_seq, local_byte) = entry.offset.parse_components().expect("rebuilt offset");
            entry.offset = Offset::new(seq + local_seq, byte + local_byte);
        }
        (seq, byte)
    }
}
