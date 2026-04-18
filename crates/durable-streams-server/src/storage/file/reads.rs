use super::{FileStorage, MessageIndex, StreamEntry};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use bytes::Bytes;
use std::fs::File;
#[cfg(not(any(unix, windows)))]
use std::io::{Read, Seek, SeekFrom};

impl FileStorage {
    pub(super) fn read_messages(file: &File, index_slice: &[MessageIndex]) -> Result<Vec<Bytes>> {
        if index_slice.is_empty() {
            return Ok(Vec::new());
        }

        let first_pos = index_slice[0].file_pos;
        let last = index_slice
            .last()
            .expect("index_slice non-empty due early return");
        let read_end = last.file_pos + last.byte_len;
        let read_len = read_end.saturating_sub(first_pos);

        // Use positional read (pread) to avoid the shared file-offset race
        // that occurs when multiple readers `try_clone()` the same file
        // descriptor and `seek()` concurrently.
        let mut raw = vec![0u8; usize::try_from(read_len).unwrap_or(usize::MAX)];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut raw, first_pos)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            file.seek_read(&mut raw, first_pos)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut reader = file
                .try_clone()
                .map_err(|e| Error::Storage(format!("failed to clone stream file handle: {e}")))?;
            reader
                .seek(SeekFrom::Start(first_pos))
                .map_err(|e| Error::Storage(format!("failed to seek message data: {e}")))?;
            reader
                .read_exact(&mut raw)
                .map_err(|e| Error::Storage(format!("failed to read message data: {e}")))?;
        }

        let shared = Bytes::from(raw);
        let mut messages = Vec::with_capacity(index_slice.len());
        for idx in index_slice {
            let rel_start =
                usize::try_from(idx.file_pos.saturating_sub(first_pos)).unwrap_or(usize::MAX);
            let rel_end = rel_start + usize::try_from(idx.byte_len).unwrap_or(usize::MAX);
            messages.push(shared.slice(rel_start..rel_end));
        }

        Ok(messages)
    }

    /// Read messages from a source chain, following fork lineage upward.
    ///
    /// Reads all messages from `from_offset` up to (but not including) `up_to`
    /// across the full ancestor chain. This bypasses tombstone checks since
    /// source streams may be soft-deleted but their data must still be readable
    /// by forks.
    pub(super) fn read_source_chain(
        &self,
        source_name: &str,
        from_offset: &Offset,
        up_to: &Offset,
    ) -> Result<Vec<Bytes>> {
        let streams = self.streams.read().expect("streams lock poisoned");

        // Build the ancestor chain from source to root
        let plan = super::super::fork::build_read_plan(source_name, |n| {
            streams.get(n).map(|arc| {
                let s = arc.read().expect("stream lock poisoned");
                s.fork_info.clone()
            })
        });

        let mut all_messages: Vec<Bytes> = Vec::new();

        for (i, segment) in plan.iter().enumerate() {
            let Some(seg_arc) = streams.get(&segment.name) else {
                continue;
            };
            let seg_stream = seg_arc.read().expect("stream lock poisoned");

            let effective_up_to = if i == plan.len() - 1 {
                Some(up_to)
            } else {
                segment.read_up_to.as_ref()
            };

            let effective_from = if i == 0 {
                from_offset
            } else {
                &Offset::start()
            };

            let start_idx = if effective_from.is_start() {
                0
            } else {
                match seg_stream
                    .index
                    .binary_search_by(|m| m.offset.cmp(effective_from))
                {
                    Ok(idx) | Err(idx) => idx,
                }
            };

            let end_idx = if let Some(bound) = effective_up_to {
                match seg_stream.index.binary_search_by(|m| m.offset.cmp(bound)) {
                    Ok(idx) | Err(idx) => idx,
                }
            } else {
                seg_stream.index.len()
            };

            if start_idx < end_idx {
                let index_slice = &seg_stream.index[start_idx..end_idx];
                let msgs = Self::read_messages(&seg_stream.file, index_slice)?;
                all_messages.extend(msgs);
            }
        }

        Ok(all_messages)
    }

    /// Read messages from a non-forked stream using the in-memory index.
    pub(super) fn read_local_file_messages(
        stream: &StreamEntry,
        from_offset: &Offset,
        next_offset: Offset,
    ) -> Result<super::ReadResult> {
        let start_idx = if from_offset.is_start() {
            0
        } else {
            match stream.index.binary_search_by(|m| m.offset.cmp(from_offset)) {
                Ok(idx) | Err(idx) => idx,
            }
        };

        let index_slice = &stream.index[start_idx..];
        let messages = Self::read_messages(&stream.file, index_slice)?;
        let at_tail = start_idx + messages.len() >= stream.index.len();

        Ok(super::ReadResult {
            messages,
            next_offset,
            at_tail,
            closed: stream.closed,
        })
    }

    /// Read the local portion of a forked stream's messages from disk.
    pub(super) fn read_fork_local_messages(
        stream: &StreamEntry,
        from_offset: &Offset,
        fork_offset: &Offset,
    ) -> Result<Vec<Bytes>> {
        if from_offset.is_start() || *from_offset <= *fork_offset {
            Self::read_messages(&stream.file, &stream.index)
        } else {
            let start_idx = match stream.index.binary_search_by(|m| m.offset.cmp(from_offset)) {
                Ok(idx) | Err(idx) => idx,
            };
            Self::read_messages(&stream.file, &stream.index[start_idx..])
        }
    }

    /// Combine source chain messages with fork-local messages into a read result.
    pub(super) fn assemble_fork_read(
        &self,
        from_offset: &Offset,
        fi: &super::ForkInfo,
        fork_local_messages: Vec<Bytes>,
        next_offset: Offset,
        closed: bool,
    ) -> Result<super::ReadResult> {
        let mut all_messages: Vec<Bytes> = Vec::new();
        if from_offset.is_start() || *from_offset < fi.fork_offset {
            let source_messages =
                self.read_source_chain(&fi.source_name, from_offset, &fi.fork_offset)?;
            all_messages.extend(source_messages);
        }
        all_messages.extend(fork_local_messages);

        Ok(super::ReadResult {
            messages: all_messages,
            next_offset,
            at_tail: true,
            closed,
        })
    }
}
