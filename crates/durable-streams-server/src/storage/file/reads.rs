use super::super::shared::PendingRead;
use super::{FileStorage, MessageIndex, StreamEntry};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use bytes::Bytes;
use std::fs::File;
#[cfg(not(any(unix, windows)))]
use std::io::{Read, Seek, SeekFrom};

impl FileStorage {
    /// Capture either a complete read or the local half of a fork while locked.
    pub(super) fn prepare_read(stream: &StreamEntry, from_offset: &Offset) -> Result<PendingRead> {
        stream.ensure_available()?;
        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        if from_offset.is_now() {
            return Ok(PendingRead::Complete(super::ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            }));
        }
        match &stream.fork_info {
            None => Self::read_local_file_messages(stream, from_offset, next_offset)
                .map(PendingRead::Complete),
            Some(info) => Ok(PendingRead::Fork {
                info: info.clone(),
                local: Self::read_local_file_messages(stream, from_offset, next_offset)?,
            }),
        }
    }

    /// Ancestor lookup acquires the stream map, so no stream lock may be held.
    pub(super) fn finish_read(
        &self,
        from_offset: &Offset,
        pending: PendingRead,
    ) -> Result<super::ReadResult> {
        pending.finish(from_offset, |source, from, up_to| {
            self.read_source_chain(source, from, up_to)
        })
    }

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
            Ok(streams
                .get(n)
                .and_then(|arc| arc.read().expect("stream lock poisoned").fork_info.clone()))
        })?;

        let mut all_messages: Vec<Bytes> = Vec::new();

        for segment in &plan {
            let Some(seg_arc) = streams.get(&segment.name) else {
                continue;
            };
            let seg_stream = seg_arc.read().expect("stream lock poisoned");
            seg_stream.ensure_available()?;

            let up_to = segment
                .read_up_to
                .as_ref()
                .map_or(up_to, |bound| bound.min(up_to));
            let range = super::super::fork::message_range(
                &seg_stream.index,
                from_offset,
                Some(up_to),
                |m| &m.offset,
            );
            all_messages.extend(Self::read_messages(
                &seg_stream.file,
                &seg_stream.index[range],
            )?);
        }

        Ok(all_messages)
    }

    /// Read a stream's local suffix using its in-memory index.
    pub(super) fn read_local_file_messages(
        stream: &StreamEntry,
        from_offset: &Offset,
        next_offset: Offset,
    ) -> Result<super::ReadResult> {
        let range =
            super::super::fork::message_range(&stream.index, from_offset, None, |m| &m.offset);
        let messages = Self::read_messages(&stream.file, &stream.index[range])?;

        Ok(super::ReadResult {
            messages,
            next_offset,
            at_tail: true,
            closed: stream.closed,
        })
    }
}
