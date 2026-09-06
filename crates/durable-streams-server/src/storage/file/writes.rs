use super::{FileStorage, MessageIndex, RECORD_HEADER_BYTES, StreamEntry};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use tracing::warn;

impl FileStorage {
    pub(super) fn rollback_total_bytes(&self, bytes: u64) {
        self.total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
    }

    pub(super) fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    pub(super) fn hard_remove_stream(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        name: &str,
    ) -> Result<Option<super::ForkInfo>> {
        let Some(stream_arc) = streams.remove(name) else {
            return Ok(None);
        };
        let stream = stream_arc.read().expect("stream lock poisoned");
        let dir = stream.dir.clone();
        let total_bytes = stream.total_bytes;
        let fork_info = stream.fork_info.clone();
        drop(stream);

        self.remove_stream_dir(&dir)?;
        self.rollback_total_bytes(total_bytes);
        Ok(fork_info)
    }

    pub(super) fn remove_for_recreate(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        name: &str,
    ) -> Result<()> {
        if let Some(fork_info) = self.hard_remove_stream(streams, name)? {
            self.cascade_delete(streams, &fork_info.source_name);
        }
        Ok(())
    }

    /// Creation has no append journal, so sync its data before publishing metadata.
    /// The caller must discard the new entry and remove its directory on failure.
    pub(super) fn append_initial_records(
        &self,
        name: &str,
        stream: &mut StreamEntry,
        messages: &[Bytes],
    ) -> Result<()> {
        self.append_records(name, stream, messages)?;
        if !messages.is_empty()
            && let Err(e) = super::retry_on_eintr(|| stream.file.sync_data())
        {
            self.rollback_total_bytes(stream.total_bytes);
            return Err(Error::classify_io_failure(
                "file",
                "sync initial stream log",
                format!("failed to sync initial stream log for {name}: {e}"),
                &e,
            ));
        }
        Ok(())
    }

    /// Write records; the caller owns the commit sync and failure recovery.
    pub(super) fn append_records(
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

        if let Err(e) = super::retry_on_eintr(|| stream.file.write_all(&write_buf)) {
            if let Ok(m) = stream.file.metadata() {
                stream.file_len = m.len();
            }
            self.rollback_total_bytes(total_batch_bytes);
            return Err(Error::Storage(format!(
                "failed to append stream log for {name}: {e}"
            )));
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

    /// Walk up the fork chain after a hard-delete, decrementing `ref_count`s
    /// and garbage-collecting tombstoned ancestors with zero references.
    ///
    /// Must be called while holding the streams write lock.
    pub(super) fn cascade_delete(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        parent_name: &str,
    ) {
        let mut current_parent = parent_name.to_string();
        while let Some(parent_arc) = streams.get(&current_parent) {
            let parent_arc = parent_arc.clone();
            let mut parent = parent_arc.write().expect("stream lock poisoned");
            parent.ref_count = parent.ref_count.saturating_sub(1);

            if parent.state == super::StreamState::Tombstone && parent.ref_count == 0 {
                let next_parent = parent.fork_info.as_ref().map(|fi| fi.source_name.clone());
                let dir = parent.dir.clone();
                let total = parent.total_bytes;
                drop(parent);
                streams.remove(&current_parent);

                if let Err(e) = self.remove_stream_dir(&dir) {
                    warn!(%e, stream = current_parent.as_str(), "failed to remove tombstoned ancestor directory during cascade delete");
                } else {
                    self.rollback_total_bytes(total);
                }

                match next_parent {
                    Some(next) => current_parent = next,
                    None => break,
                }
            } else {
                if let Err(e) = self.write_metadata_for(&current_parent, &parent) {
                    warn!(%e, stream = current_parent.as_str(), "failed to persist parent ref_count during cascade delete");
                }
                break;
            }
        }
    }
}
