//! In-memory storage used for tests, development, and ephemeral deployments.
//!
//! [`InMemoryStorage`] keeps the full stream set in process memory and shares
//! the same [`super::Storage`] contract as the disk-backed backends, but
//! without persistence across restarts.

use super::shared::PendingRead;
use super::{
    CreateStreamResult, ForkInfo, Message, NOTIFY_CHANNEL_CAPACITY, ProducerAppendResult,
    ProducerState, ReadResult, StreamConfig, StreamMetadata, StreamState,
};
use crate::Storage;
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use crate::storage::shared::{payload_bytes, release_bytes};
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;

const INITIAL_MESSAGES_CAPACITY: usize = 256;
const INITIAL_PRODUCERS_CAPACITY: usize = 8;

/// Internal stream entry
struct StreamEntry {
    config: StreamConfig,
    messages: Vec<Message>,
    closed: bool,
    next_read_seq: u64,
    next_byte_offset: u64,
    total_bytes: u64,
    created_at: chrono::DateTime<Utc>,
    updated_at: Option<chrono::DateTime<Utc>>,
    /// Per-producer state for idempotent producer support
    producers: HashMap<String, ProducerState>,
    /// Broadcast sender for notifying long-poll/SSE subscribers
    notify: broadcast::Sender<()>,
    /// Last Stream-Seq value received (lexicographic ordering)
    last_seq: Option<String>,
    /// Fork lineage metadata (None for root streams)
    fork_info: Option<ForkInfo>,
    /// Number of forks that reference this stream as their source
    ref_count: u32,
    /// Lifecycle state (Active or Tombstone)
    state: StreamState,
}

impl StreamEntry {
    fn metadata(&self) -> super::StreamMetadata {
        super::build_stream_metadata(
            self.config.clone(),
            self.next_read_seq,
            self.next_byte_offset,
            self.closed,
            self.total_bytes,
            u64::try_from(self.messages.len()).unwrap_or(u64::MAX),
            self.created_at,
            self.updated_at,
        )
    }
    fn new(config: StreamConfig) -> Self {
        // Creation commits any initial messages and closure before publishing the entry.
        // The `created_closed` flag in config is stored for idempotent checks only.
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        Self {
            config,
            messages: Vec::with_capacity(INITIAL_MESSAGES_CAPACITY),
            closed: false,
            next_read_seq: 0,
            next_byte_offset: 0,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
            fork_info: None,
            ref_count: 0,
            state: StreamState::Active,
        }
    }
}

/// In-memory storage implementation
///
/// Thread-safe storage with:
/// - `RwLock<HashMap>` for stream lookup (concurrent reads)
/// - Per-stream `RwLock` for exclusive write access (offset monotonicity)
/// - Memory limit enforcement (global and per-stream)
///
/// # Concurrency Model
///
/// Multiple readers can access different streams concurrently.
/// Appends to the same stream are serialized via `RwLock::write()`.
/// Appends to different streams can proceed concurrently.
pub struct InMemoryStorage {
    subscription_state: RwLock<Option<Vec<u8>>>,
    streams: RwLock<HashMap<String, Arc<RwLock<StreamEntry>>>>,
    total_bytes: AtomicU64,
    max_total_bytes: u64,
    max_stream_bytes: u64,
}

impl InMemoryStorage {
    /// Create a new in-memory storage with memory limits
    #[must_use]
    pub fn new(max_total_bytes: u64, max_stream_bytes: u64) -> Self {
        Self {
            subscription_state: RwLock::new(None),
            streams: RwLock::new(HashMap::new()),
            total_bytes: AtomicU64::new(0),
            max_total_bytes,
            max_stream_bytes,
        }
    }

    /// Return the currently tracked total payload bytes across all streams.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Acquire)
    }

    fn get_stream(&self, name: &str) -> Option<Arc<RwLock<StreamEntry>>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        streams.get(name).map(Arc::clone)
    }

    fn hard_remove_stream(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        name: &str,
    ) -> Option<ForkInfo> {
        let stream_arc = streams.remove(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");
        release_bytes(&self.total_bytes, stream.total_bytes);
        stream.fork_info.clone()
    }

    fn remove_for_recreate(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        name: &str,
    ) {
        if let Some(fork_info) = self.hard_remove_stream(streams, name) {
            self.cascade_delete(streams, &fork_info.source_name);
        }
    }

    /// Capture either a complete read or the local half of a fork while locked.
    fn prepare_read(stream: &StreamEntry, from_offset: &Offset) -> PendingRead {
        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        if from_offset.is_now() {
            return PendingRead::Complete(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            });
        }
        match &stream.fork_info {
            None => {
                PendingRead::Complete(Self::read_local_messages(stream, from_offset, next_offset))
            }
            Some(info) => PendingRead::Fork {
                info: info.clone(),
                local: Self::read_local_messages(stream, from_offset, next_offset),
            },
        }
    }

    /// Ancestor lookup acquires the stream map, so no stream lock may be held.
    fn finish_read(&self, from_offset: &Offset, pending: PendingRead) -> Result<ReadResult> {
        pending.finish(from_offset, |source, from, up_to| {
            self.read_source_chain(source, from, up_to)
        })
    }

    /// Read messages from a stream's local storage (no fork traversal).
    fn read_local_messages(
        stream: &StreamEntry,
        from_offset: &Offset,
        next_offset: Offset,
    ) -> ReadResult {
        let range = super::fork::message_range(&stream.messages, from_offset, None, |m| &m.offset);
        let messages = stream.messages[range]
            .iter()
            .map(|m| m.data.clone())
            .collect();

        ReadResult {
            messages,
            next_offset,
            at_tail: true,
            closed: stream.closed,
        }
    }

    /// Walk up the fork chain after a hard-delete, decrementing `ref_count`s
    /// and garbage-collecting tombstoned ancestors with zero references.
    ///
    /// Must be called while holding the streams write lock.
    fn cascade_delete(
        &self,
        streams: &mut HashMap<String, Arc<RwLock<StreamEntry>>>,
        parent_name: &str,
    ) {
        let mut current_parent = parent_name.to_string();
        while let Some(parent_arc) = streams.get(&current_parent) {
            let parent_arc = parent_arc.clone();
            let mut parent = parent_arc.write().expect("stream lock poisoned");
            parent.ref_count = parent.ref_count.saturating_sub(1);

            if parent.state == StreamState::Tombstone && parent.ref_count == 0 {
                let next_parent = parent.fork_info.as_ref().map(|fi| fi.source_name.clone());
                release_bytes(&self.total_bytes, parent.total_bytes);
                drop(parent);
                streams.remove(&current_parent);

                match next_parent {
                    Some(next) => current_parent = next,
                    None => break,
                }
            } else {
                break;
            }
        }
    }

    /// Read messages from a source chain, following fork lineage upward.
    ///
    /// Reads all messages from `from_offset` up to (but not including) `up_to`
    /// across the full ancestor chain. This bypasses tombstone checks since
    /// source streams may be soft-deleted but their data must still be readable
    /// by forks.
    fn read_source_chain(
        &self,
        source_name: &str,
        from_offset: &Offset,
        up_to: &Offset,
    ) -> Result<Vec<Bytes>> {
        let streams = self.streams.read().expect("streams lock poisoned");

        // Build the ancestor chain from source to root
        let plan = super::fork::build_read_plan(source_name, |n| {
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

            let up_to = segment
                .read_up_to
                .as_ref()
                .map_or(up_to, |bound| bound.min(up_to));
            let range =
                super::fork::message_range(&seg_stream.messages, from_offset, Some(up_to), |m| {
                    &m.offset
                });
            all_messages.extend(seg_stream.messages[range].iter().map(|m| m.data.clone()));
        }

        Ok(all_messages)
    }

    /// Commit messages to a stream, checking memory limits first.
    ///
    /// Caller must hold the stream write lock. Updates both stream-level
    /// and global memory counters atomically.
    fn commit_messages(&self, stream: &mut StreamEntry, messages: Vec<Bytes>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let total_batch_bytes = payload_bytes(&messages);

        // Reserve global bytes atomically (global precedence before per-stream).
        crate::storage::shared::reserve_bytes(
            &self.total_bytes,
            self.max_total_bytes,
            total_batch_bytes,
        )?;
        if stream.total_bytes + total_batch_bytes > self.max_stream_bytes {
            release_bytes(&self.total_bytes, total_batch_bytes);
            return Err(Error::StreamSizeLimitExceeded);
        }

        for data in messages {
            let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
            let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
            stream.next_read_seq += 1;
            stream.next_byte_offset += byte_len;
            stream.total_bytes += byte_len;
            let message = Message::new(offset, data);
            stream.messages.push(message);
        }

        // Notify long-poll/SSE subscribers that new data is available.
        // Ignore errors (no active receivers is fine).
        let _ = stream.notify.send(());

        Ok(())
    }
}

impl Storage for InMemoryStorage {
    fn append_batch(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
        close: bool,
    ) -> Result<crate::storage::AppendResult> {
        super::shared::validate_batch_shape(&messages, close)?;

        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        let pending_seq = super::precheck_batch_append(
            &stream.config,
            stream.state,
            stream.closed,
            stream.last_seq.as_deref(),
            name,
            (!messages.is_empty()).then_some(content_type),
            seq,
        )?;

        let start_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        let mut next_config = stream.config.clone();
        let mut next_seq = stream.last_seq.clone();
        let mut next_updated_at = stream.updated_at;
        super::apply_append_metadata(
            &mut next_config,
            &mut next_seq,
            &mut next_updated_at,
            pending_seq,
            Utc::now(),
        )?;
        self.commit_messages(&mut stream, messages)?;
        stream.config = next_config;
        stream.last_seq = next_seq;
        stream.updated_at = next_updated_at;

        stream.closed |= close;
        let _ = stream.notify.send(());
        Ok(crate::storage::AppendResult::new(
            start_offset,
            Offset::new(stream.next_read_seq, stream.next_byte_offset),
            stream.closed,
        ))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        let stream = stream_arc.read().expect("stream lock poisoned");
        super::fork::check_stream_access(&stream.config, stream.state, name)?;
        if stream.config.ttl_seconds.is_none() {
            let pending = Self::prepare_read(&stream, from_offset);
            drop(stream);
            return self.finish_read(from_offset, pending);
        }
        drop(stream);

        let mut stream = stream_arc.write().expect("stream lock poisoned");
        super::fork::check_stream_access(&stream.config, stream.state, name)?;
        let result = match Self::prepare_read(&stream, from_offset) {
            PendingRead::Complete(result) => result,
            pending @ PendingRead::Fork { .. } => {
                drop(stream);
                let result = self.finish_read(from_offset, pending)?;
                stream = stream_arc.write().expect("stream lock poisoned");
                result
            }
        };
        super::fork::renew_ttl(&mut stream.config)?;
        Ok(result)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let stream_arc = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?
            .clone();

        {
            let stream = stream_arc.read().expect("stream lock poisoned");

            match super::fork::evaluate_delete(name, stream.state, stream.ref_count)? {
                super::fork::DeleteDisposition::Tombstone => {
                    drop(stream);
                    let mut stream_w = stream_arc.write().expect("stream lock poisoned");
                    stream_w.state = StreamState::Tombstone;
                    return Ok(());
                }
                super::fork::DeleteDisposition::HardDelete => {}
            }
        }

        let fork_info = self.hard_remove_stream(&mut streams, name);

        if let Some(fi) = fork_info {
            self.cascade_delete(&mut streams, &fi.source_name);
        }

        Ok(())
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        Ok(stream.metadata())
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        let pending_seq = {
            let StreamEntry {
                config,
                state,
                closed,
                last_seq,
                producers,
                next_read_seq,
                next_byte_offset,
                ..
            } = &mut *stream;

            match super::precheck_producer_append(
                config,
                *state,
                *closed,
                last_seq.as_deref(),
                producers,
                name,
                content_type,
                producer,
                &messages,
                seq,
            )? {
                super::ProducerAppendPrecheck::Accept { pending_seq } => pending_seq,
                super::ProducerAppendPrecheck::Duplicate { epoch, seq } => {
                    return Ok(ProducerAppendResult::Duplicate {
                        epoch,
                        seq,
                        next_offset: Offset::new(*next_read_seq, *next_byte_offset),
                        closed: *closed,
                    });
                }
            }
        };

        let now = Utc::now();
        let mut next_config = stream.config.clone();
        let mut next_seq = stream.last_seq.clone();
        let mut next_updated_at = stream.updated_at;
        super::apply_append_metadata(
            &mut next_config,
            &mut next_seq,
            &mut next_updated_at,
            pending_seq,
            now,
        )?;
        self.commit_messages(&mut stream, messages)?;

        if should_close {
            stream.closed = true;
        }

        stream.config = next_config;
        stream.last_seq = next_seq;
        stream.updated_at = next_updated_at;

        stream.producers.insert(
            producer.id.clone(),
            ProducerState {
                epoch: producer.epoch,
                last_seq: producer.seq,
                updated_at: now,
            },
        );

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        let closed = stream.closed;

        Ok(ProducerAppendResult::Accepted {
            epoch: producer.epoch,
            seq: producer.seq,
            next_offset,
            closed,
        })
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: crate::storage::StreamOptions,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<super::CreateWithDataResult> {
        let config = config.resolve(Utc::now())?;
        let should_close = should_close || config.created_closed;
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            match super::fork::evaluate_root_create(
                name,
                &stream.config,
                stream.state,
                stream.ref_count,
                &config,
            ) {
                super::fork::ExistingCreateDisposition::RemoveExpired => {
                    drop(stream);
                    self.remove_for_recreate(&mut streams, name);
                }
                super::fork::ExistingCreateDisposition::AlreadyExists => {
                    return Ok(super::CreateWithDataResult {
                        status: CreateStreamResult::AlreadyExists,
                        next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                        closed: stream.closed,
                    });
                }
                super::fork::ExistingCreateDisposition::Conflict(err) => {
                    return Err(err);
                }
            }
        }

        let mut entry = StreamEntry::new(config);

        if !messages.is_empty() {
            self.commit_messages(&mut entry, messages)?;
        }

        if should_close {
            entry.closed = true;
        }

        let next_offset = Offset::new(entry.next_read_seq, entry.next_byte_offset);
        let closed = entry.closed;

        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(super::CreateWithDataResult {
            status: CreateStreamResult::Created,
            next_offset,
            closed,
        })
    }

    fn replace_stream(
        &self,
        name: &str,
        config: crate::storage::StreamOptions,
        messages: Vec<Bytes>,
        closed: bool,
    ) -> Result<crate::storage::AppendResult> {
        let config = config.resolve(Utc::now())?;
        let closed = closed || config.created_closed;
        let streams = self.streams.write().expect("streams lock poisoned");
        let current = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.into()))?;
        let mut current = current.write().expect("stream lock poisoned");
        super::fork::check_replace(current.ref_count, current.fork_info.as_ref())?;
        let mut replacement = StreamEntry::new(config);
        // Reserve the staged payload before releasing any bytes from the original.
        self.commit_messages(&mut replacement, messages)?;
        replacement.closed = closed;
        replacement.notify = current.notify.clone();
        release_bytes(&self.total_bytes, current.total_bytes);
        let result = crate::storage::AppendResult::new(
            Offset::new(0, 0),
            Offset::new(replacement.next_read_seq, replacement.next_byte_offset),
            closed,
        );
        *current = replacement;
        let _ = current.notify.send(());
        Ok(result)
    }

    fn subscribe(&self, name: &str) -> Result<Option<broadcast::Receiver<()>>> {
        let Some(stream_arc) = self.get_stream(name) else {
            return Ok(None);
        };
        let stream = stream_arc.read().expect("stream lock poisoned");

        if !super::is_stream_visible(&stream.config, stream.state) {
            return Ok(None);
        }

        Ok(Some(stream.notify.subscribe()))
    }

    fn cleanup_expired_streams(&self) -> usize {
        let mut streams = self.streams.write().expect("streams lock poisoned");
        let mut expired = Vec::new();

        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
            if super::is_stream_expired(&stream.config) {
                expired.push((name.clone(), stream.ref_count));
            }
        }

        let removed_count = expired.len();
        for (name, ref_count) in expired {
            match super::fork::evaluate_expired_cleanup(ref_count) {
                super::fork::DeleteDisposition::Tombstone => {
                    if let Some(stream_arc) = streams.get(&name) {
                        let mut stream = stream_arc.write().expect("stream lock poisoned");
                        stream.state = StreamState::Tombstone;
                    }
                }
                super::fork::DeleteDisposition::HardDelete => {
                    self.remove_for_recreate(&mut streams, &name);
                }
            }
        }

        removed_count
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        let mut result = Vec::new();
        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
            if !super::is_stream_visible(&stream.config, stream.state) {
                continue;
            }
            result.push((name.clone(), stream.metadata()));
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn load_subscription_state(&self) -> Result<Option<Vec<u8>>> {
        Ok(self
            .subscription_state
            .read()
            .expect("subscription lock poisoned")
            .clone())
    }

    fn save_subscription_state(&self, state: &[u8]) -> Result<()> {
        *self
            .subscription_state
            .write()
            .expect("subscription lock poisoned") = Some(state.to_vec());
        Ok(())
    }

    fn create_fork_with_options(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: crate::storage::StreamOptions,
        options: super::ForkOptions,
    ) -> Result<CreateStreamResult> {
        let mut config = config.resolve(Utc::now())?;
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let source_arc = streams
            .get(source_name)
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?
            .clone();

        let (mut fork_spec, resolved_offset) = {
            let source = source_arc.read().expect("stream lock poisoned");
            if options.inherit_content_type {
                config.content_type.clone_from(&source.config.content_type);
            }
            let source_next_offset = Offset::new(source.next_read_seq, source.next_byte_offset);
            super::fork::prepare_fork_spec(
                source_name,
                &source.config,
                source.state,
                &source_next_offset,
                fork_offset,
                &config,
            )?
        };

        fork_spec.sub_offset = options.sub_offset;
        let initial_messages = Self::fork_initial_messages(
            &streams,
            source_name,
            &resolved_offset,
            &fork_spec.config,
            &options,
        )?;

        if let Some(existing_arc) = streams.get(name) {
            let existing = existing_arc.read().expect("stream lock poisoned");
            match super::fork::evaluate_fork_create(
                name,
                &existing.config,
                existing.fork_info.as_ref(),
                existing.state,
                existing.ref_count,
                &fork_spec,
            ) {
                super::fork::ExistingCreateDisposition::RemoveExpired => {
                    drop(existing);
                    self.remove_for_recreate(&mut streams, name);
                }
                super::fork::ExistingCreateDisposition::AlreadyExists => {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                super::fork::ExistingCreateDisposition::Conflict(err) => {
                    return Err(err);
                }
            }
        }

        // Extract fork offset components to initialize the fork entry
        let (fork_read_seq, fork_byte_offset) =
            resolved_offset.parse_components().unwrap_or((0, 0));

        let mut entry = StreamEntry::new(fork_spec.config);
        entry.closed = config.created_closed;
        entry.next_read_seq = fork_read_seq;
        entry.next_byte_offset = fork_byte_offset;
        entry.fork_info = Some(ForkInfo::new(
            fork_spec.source_name,
            resolved_offset,
            options.sub_offset,
        ));

        self.commit_messages(&mut entry, initial_messages)?;
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        // Increment source ref_count
        if let Some(source_arc) = streams.get(source_name) {
            let mut source = source_arc.write().expect("stream lock poisoned");
            source.ref_count += 1;
        }

        Ok(CreateStreamResult::Created)
    }
}

// Concurrent producer and Storage trait contract tests live in the
// integration test suite (storage_backend_contract, concurrent_stress).

impl InMemoryStorage {
    fn fork_initial_messages(
        streams: &HashMap<String, Arc<RwLock<StreamEntry>>>,
        source_name: &str,
        resolved_offset: &Offset,
        config: &StreamConfig,
        options: &super::ForkOptions,
    ) -> Result<Vec<Bytes>> {
        let mut source_messages = Vec::new();
        if options.sub_offset > 0 {
            let plan = super::fork::build_read_plan(source_name, |n| {
                Ok(streams
                    .get(n)
                    .and_then(|arc| arc.read().expect("stream lock poisoned").fork_info.clone()))
            })?;
            for segment in plan {
                let arc = streams
                    .get(&segment.name)
                    .ok_or_else(|| Error::NotFound(segment.name.clone()))?;
                let stream = arc.read().expect("stream lock poisoned");
                let range = super::fork::message_range(
                    &stream.messages,
                    resolved_offset,
                    segment.read_up_to.as_ref(),
                    |m| &m.offset,
                );
                source_messages.extend(stream.messages[range].iter().map(|m| m.data.clone()));
            }
        }
        super::fork::initial_fork_messages(config, options, source_messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StreamOptions;

    #[test]
    fn fork_read_retains_local_snapshot_across_append_and_close() {
        let storage = InMemoryStorage::new(1024, 1024);
        storage
            .create_stream_with_data(
                "source",
                StreamOptions::new("text/plain"),
                vec![Bytes::from_static(b"a")],
                false,
            )
            .unwrap();
        storage
            .create_fork("fork", "source", None, StreamOptions::new("text/plain"))
            .unwrap();
        storage
            .append_batch(
                "fork",
                vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")],
                "text/plain",
                None,
                false,
            )
            .unwrap();
        let from = Offset::new(2, 2);
        let stream = storage.get_stream("fork").unwrap();
        let pending = InMemoryStorage::prepare_read(&stream.read().unwrap(), &from);
        // Deterministically interleave a writer after the local snapshot is unlocked.
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    storage
                        .append_batch(
                            "fork",
                            vec![Bytes::from_static(b"d")],
                            "text/plain",
                            None,
                            true,
                        )
                        .unwrap()
                })
                .join()
                .unwrap();
        });
        let read = storage.finish_read(&from, pending).unwrap();
        assert_eq!(read.messages, vec![Bytes::from_static(b"c")]);
        assert_eq!(read.next_offset, Offset::new(3, 3));
        assert!(!read.closed);
        let current = storage.read("fork", &from).unwrap();
        assert_eq!(
            current.messages,
            vec![Bytes::from_static(b"c"), Bytes::from_static(b"d")]
        );
        assert_eq!(current.next_offset, Offset::new(4, 4));
        assert!(current.closed);
    }
}
