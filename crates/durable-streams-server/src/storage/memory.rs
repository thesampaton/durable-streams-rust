//! In-memory storage used for tests, development, and ephemeral deployments.
//!
//! [`InMemoryStorage`] keeps the full stream set in process memory and shares
//! the same [`super::Storage`] contract as the disk-backed backends, but
//! without persistence across restarts.

use super::{
    CreateStreamResult, ForkInfo, Message, NOTIFY_CHANNEL_CAPACITY, ProducerAppendResult,
    ProducerState, ReadResult, Storage, StreamConfig, StreamMetadata, StreamState,
};
use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
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
    fn new(config: StreamConfig) -> Self {
        // Stream starts open; the handler closes it after any initial appends.
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

    fn saturating_sub_total_bytes(&self, bytes: u64) {
        self.total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
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
        self.saturating_sub_total_bytes(stream.total_bytes);
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

    /// Read messages from a stream's local storage (no fork traversal).
    #[allow(clippy::unnecessary_wraps)]
    fn read_local_messages(
        stream: &StreamEntry,
        from_offset: &Offset,
        next_offset: Offset,
    ) -> Result<ReadResult> {
        let start_idx = if from_offset.is_start() {
            0
        } else {
            match stream
                .messages
                .binary_search_by(|m| m.offset.cmp(from_offset))
            {
                Ok(idx) | Err(idx) => idx,
            }
        };

        let messages: Vec<Bytes> = stream.messages[start_idx..]
            .iter()
            .map(|m| m.data.clone())
            .collect();

        let at_tail = start_idx + messages.len() >= stream.messages.len();

        Ok(ReadResult {
            messages,
            next_offset,
            at_tail,
            closed: stream.closed,
        })
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
                self.saturating_sub_total_bytes(parent.total_bytes);
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
    ) -> Vec<Bytes> {
        let streams = self.streams.read().expect("streams lock poisoned");

        // Build the ancestor chain from source to root
        let plan = super::fork::build_read_plan(source_name, |n| {
            streams.get(n).map(|arc| {
                let s = arc.read().expect("stream lock poisoned");
                s.fork_info.clone()
            })
        });

        let mut all_messages: Vec<Bytes> = Vec::new();

        for segment in &plan {
            let Some(seg_arc) = streams.get(&segment.name) else {
                continue;
            };
            let seg_stream = seg_arc.read().expect("stream lock poisoned");

            // Determine the effective upper bound for this segment
            let effective_up_to = Some(
                segment
                    .read_up_to
                    .as_ref()
                    .map_or(up_to, |bound| bound.min(up_to)),
            );

            // Determine start offset for this segment
            let effective_from = from_offset;

            // Collect messages in range
            let start_idx = if effective_from.is_start() {
                0
            } else {
                match seg_stream
                    .messages
                    .binary_search_by(|m| m.offset.cmp(effective_from))
                {
                    Ok(idx) | Err(idx) => idx,
                }
            };

            for msg in &seg_stream.messages[start_idx..] {
                if effective_up_to.is_some_and(|bound| msg.offset >= *bound) {
                    break;
                }
                all_messages.push(msg.data.clone());
            }
        }

        all_messages
    }

    /// Commit messages to a stream, checking memory limits first.
    ///
    /// Caller must hold the stream write lock. Updates both stream-level
    /// and global memory counters atomically.
    fn commit_messages(&self, stream: &mut StreamEntry, messages: Vec<Bytes>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut total_batch_bytes = 0u64;
        let mut message_sizes = Vec::with_capacity(messages.len());
        for data in &messages {
            let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
            message_sizes.push(byte_len);
            total_batch_bytes += byte_len;
        }

        // Reserve global bytes atomically (global precedence before per-stream).
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
            self.saturating_sub_total_bytes(total_batch_bytes);
            return Err(Error::StreamSizeLimitExceeded);
        }

        for (data, byte_len) in messages.into_iter().zip(message_sizes) {
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

    /// Read messages from a forked stream by combining source chain with local data.
    ///
    /// The caller must have already extracted fork info and local messages from
    /// the stream entry (and dropped the lock if needed before calling this).
    fn assemble_fork_read(
        &self,
        name: &str,
        from_offset: &Offset,
        fi: &super::ForkInfo,
        fork_messages_data: Vec<Bytes>,
        next_offset: Offset,
        closed: bool,
    ) -> Result<ReadResult> {
        let mut all_messages: Vec<Bytes> = Vec::new();
        if from_offset.is_start() || *from_offset < fi.fork_offset {
            let source_messages =
                self.read_source_chain(&fi.source_name, from_offset, &fi.fork_offset);
            all_messages.extend(source_messages);
        }

        if from_offset.is_start() || *from_offset <= fi.fork_offset {
            all_messages.extend(fork_messages_data);
        } else {
            let stream_arc = self
                .get_stream(name)
                .ok_or_else(|| Error::NotFound(name.to_string()))?;
            let stream = stream_arc.read().expect("stream lock poisoned");
            let start_idx = match stream
                .messages
                .binary_search_by(|m| m.offset.cmp(from_offset))
            {
                Ok(idx) | Err(idx) => idx,
            };
            let msgs: Vec<Bytes> = stream.messages[start_idx..]
                .iter()
                .map(|m| m.data.clone())
                .collect();
            all_messages.extend(msgs);
        }

        Ok(ReadResult {
            messages: all_messages,
            next_offset,
            at_tail: true,
            closed,
        })
    }
}

impl Storage for InMemoryStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
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
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                super::fork::ExistingCreateDisposition::Conflict(err) => {
                    return Err(err);
                }
            }
        }

        let entry = StreamEntry::new(config);
        streams.insert(name.to_string(), Arc::new(RwLock::new(entry)));

        Ok(CreateStreamResult::Created)
    }

    fn append(&self, name: &str, data: Bytes, content_type: &str) -> Result<Offset> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::precheck_append(
            &stream.config,
            stream.state,
            stream.closed,
            name,
            content_type,
        )?;

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        self.commit_messages(&mut stream, vec![data])?;
        {
            let StreamEntry {
                config,
                last_seq,
                updated_at,
                ..
            } = &mut *stream;
            super::apply_append_metadata(config, last_seq, updated_at, None, Utc::now());
        }

        Ok(offset)
    }

    fn batch_append(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset> {
        if messages.is_empty() {
            return Err(Error::InvalidHeader {
                header: "Content-Length".to_string(),
                reason: "batch cannot be empty".to_string(),
            });
        }

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
            content_type,
            seq,
        )?;

        self.commit_messages(&mut stream, messages)?;
        {
            let StreamEntry {
                config,
                last_seq,
                updated_at,
                ..
            } = &mut *stream;
            super::apply_append_metadata(config, last_seq, updated_at, pending_seq, Utc::now());
        }

        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let needs_ttl_renewal = {
            let stream = stream_arc.read().expect("stream lock poisoned");
            super::fork::check_stream_access(&stream.config, stream.state, name)?;
            stream.config.ttl_seconds.is_some()
        };

        if !needs_ttl_renewal {
            let stream = stream_arc.read().expect("stream lock poisoned");
            let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

            if from_offset.is_now() {
                return Ok(ReadResult {
                    messages: Vec::new(),
                    next_offset,
                    at_tail: true,
                    closed: stream.closed,
                });
            }

            if stream.fork_info.is_none() {
                return Self::read_local_messages(&stream, from_offset, next_offset);
            }

            let fi = stream.fork_info.clone().expect("checked above");
            let closed = stream.closed;
            let fork_messages_data: Vec<Bytes> =
                stream.messages.iter().map(|m| m.data.clone()).collect();
            drop(stream);

            return self.assemble_fork_read(
                name,
                from_offset,
                &fi,
                fork_messages_data,
                next_offset,
                closed,
            );
        }

        let mut stream = stream_arc.write().expect("stream lock poisoned");
        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
        let result = if from_offset.is_now() {
            ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            }
        } else if stream.fork_info.is_none() {
            Self::read_local_messages(&stream, from_offset, next_offset)?
        } else {
            let fi = stream.fork_info.clone().expect("checked above");
            let closed = stream.closed;
            let fork_messages_data: Vec<Bytes> =
                stream.messages.iter().map(|m| m.data.clone()).collect();
            drop(stream);

            let result = self.assemble_fork_read(
                name,
                from_offset,
                &fi,
                fork_messages_data,
                next_offset,
                closed,
            )?;

            stream = stream_arc.write().expect("stream lock poisoned");
            result
        };

        super::fork::renew_ttl(&mut stream.config);
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

        Ok(super::build_stream_metadata(
            stream.config.clone(),
            stream.next_read_seq,
            stream.next_byte_offset,
            stream.closed,
            stream.total_bytes,
            u64::try_from(stream.messages.len()).unwrap_or(u64::MAX),
            stream.created_at,
            stream.updated_at,
        ))
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        stream.closed = true;
        stream.updated_at = Some(Utc::now());
        super::fork::renew_ttl(&mut stream.config);

        let _ = stream.notify.send(());

        Ok(())
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
        self.commit_messages(&mut stream, messages)?;

        if should_close {
            stream.closed = true;
        }

        {
            let StreamEntry {
                config,
                last_seq,
                updated_at,
                ..
            } = &mut *stream;
            super::apply_append_metadata(config, last_seq, updated_at, pending_seq, now);
        }

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
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<super::CreateWithDataResult> {
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

    fn exists(&self, name: &str) -> bool {
        let streams = self.streams.read().expect("streams lock poisoned");
        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");
            super::is_stream_visible(&stream.config, stream.state)
        } else {
            false
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let stream_arc = self.get_stream(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");

        if !super::is_stream_visible(&stream.config, stream.state) {
            return None;
        }

        Some(stream.notify.subscribe())
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
            result.push((
                name.clone(),
                super::build_stream_metadata(
                    stream.config.clone(),
                    stream.next_read_seq,
                    stream.next_byte_offset,
                    stream.closed,
                    stream.total_bytes,
                    u64::try_from(stream.messages.len()).unwrap_or(u64::MAX),
                    stream.created_at,
                    stream.updated_at,
                ),
            ));
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        self.create_fork_with_options(
            name,
            source_name,
            fork_offset,
            config,
            super::ForkOptions::default(),
        )
    }

    fn create_fork_with_options(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        mut config: StreamConfig,
        options: super::ForkOptions,
    ) -> Result<CreateStreamResult> {
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

        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        let mut entry = StreamEntry {
            config: fork_spec.config,
            messages: Vec::with_capacity(INITIAL_MESSAGES_CAPACITY),
            closed: config.created_closed,
            next_read_seq: fork_read_seq,
            next_byte_offset: fork_byte_offset,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
            fork_info: Some(ForkInfo {
                sub_offset: options.sub_offset,
                source_name: fork_spec.source_name,
                fork_offset: resolved_offset,
            }),
            ref_count: 0,
            state: StreamState::Active,
        };

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
                streams
                    .get(n)
                    .map(|arc| arc.read().expect("stream lock poisoned").fork_info.clone())
            });
            for segment in plan {
                let arc = streams
                    .get(&segment.name)
                    .ok_or_else(|| Error::NotFound(segment.name.clone()))?;
                let stream = arc.read().expect("stream lock poisoned");
                source_messages.extend(
                    stream
                        .messages
                        .iter()
                        .filter(|m| {
                            m.offset >= *resolved_offset
                                && segment
                                    .read_up_to
                                    .as_ref()
                                    .is_none_or(|bound| m.offset < *bound)
                        })
                        .map(|m| m.data.clone()),
                );
            }
        }
        super::fork::initial_fork_messages(config, options, source_messages)
    }
}
