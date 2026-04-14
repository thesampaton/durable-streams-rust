use super::{
    CreateStreamResult, ForkInfo, Message, NOTIFY_CHANNEL_CAPACITY, ProducerAppendResult,
    ProducerCheck, ProducerState, ReadResult, Storage, StreamConfig, StreamMetadata, StreamState,
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
        loop {
            let Some(parent_arc) = streams.get(&current_parent) else {
                break;
            };
            let parent_arc = parent_arc.clone();
            let mut parent = parent_arc.write().expect("stream lock poisoned");
            parent.ref_count = parent.ref_count.saturating_sub(1);

            if parent.state == StreamState::Tombstone && parent.ref_count == 0 {
                // This parent can be garbage-collected
                let fi = parent.fork_info.clone();
                self.saturating_sub_total_bytes(parent.total_bytes);
                drop(parent);
                streams.remove(&current_parent);

                // Continue up the chain
                if let Some(fi) = fi {
                    current_parent = fi.source_name;
                } else {
                    break;
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

        for (i, segment) in plan.iter().enumerate() {
            let Some(seg_arc) = streams.get(&segment.name) else {
                continue;
            };
            let seg_stream = seg_arc.read().expect("stream lock poisoned");

            // Determine the effective upper bound for this segment
            let effective_up_to = if i == plan.len() - 1 {
                // Last segment (the direct source): read up to `up_to`
                Some(up_to)
            } else {
                // Intermediate segment: read up to this segment's read_up_to
                segment.read_up_to.as_ref()
            };

            // Determine start offset for this segment
            let effective_from = if i == 0 {
                from_offset
            } else {
                &Offset::start()
            };

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
}

impl Storage for InMemoryStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        if let Some(stream_arc) = streams.get(name) {
            let stream = stream_arc.read().expect("stream lock poisoned");

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                drop(stream);
                streams.remove(name);

                self.total_bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        Some(current.saturating_sub(stream_bytes))
                    })
                    .ok();
            } else if stream.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else {
                if stream.config == config {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                return Err(Error::ConfigMismatch);
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

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        super::validate_content_type(&stream.config.content_type, content_type)?;

        let byte_len = u64::try_from(data.len()).unwrap_or(u64::MAX);

        if self
            .total_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(byte_len)
                    .filter(|next| *next <= self.max_total_bytes)
            })
            .is_err()
        {
            return Err(Error::MemoryLimitExceeded);
        }

        if stream.total_bytes + byte_len > self.max_stream_bytes {
            self.saturating_sub_total_bytes(byte_len);
            return Err(Error::StreamSizeLimitExceeded);
        }

        let offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        stream.next_read_seq += 1;
        stream.next_byte_offset += byte_len;
        stream.total_bytes += byte_len;

        let message = Message::new(offset.clone(), data);
        stream.messages.push(message);
        stream.updated_at = Some(Utc::now());

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

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        if stream.closed {
            return Err(Error::StreamClosed);
        }

        super::validate_content_type(&stream.config.content_type, content_type)?;

        let pending_seq = super::validate_seq(stream.last_seq.as_deref(), seq)?;

        self.commit_messages(&mut stream, messages)?;
        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
        }
        stream.updated_at = Some(Utc::now());

        Ok(Offset::new(stream.next_read_seq, stream.next_byte_offset))
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let stream = stream_arc.read().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);

        if from_offset.is_now() {
            return Ok(ReadResult {
                messages: Vec::new(),
                next_offset,
                at_tail: true,
                closed: stream.closed,
            });
        }

        // If stream has no fork lineage, use the fast path
        if stream.fork_info.is_none() {
            return Self::read_local_messages(&stream, from_offset, next_offset);
        }

        // Fork-aware read: stitch messages from source chain + fork's own data
        let fi = stream.fork_info.clone().expect("checked above");
        let closed = stream.closed;
        let fork_messages_data: Vec<Bytes> =
            stream.messages.iter().map(|m| m.data.clone()).collect();
        drop(stream);

        let mut all_messages: Vec<Bytes> = Vec::new();

        // Read source data if from_offset is before the fork_offset
        if from_offset.is_start() || *from_offset < fi.fork_offset {
            let source_messages =
                self.read_source_chain(&fi.source_name, from_offset, &fi.fork_offset);
            all_messages.extend(source_messages);
        }

        // Add fork's own messages
        if from_offset.is_start() || *from_offset <= fi.fork_offset {
            // All fork messages (from_offset is in ancestor range or at boundary)
            all_messages.extend(fork_messages_data);
        } else {
            // from_offset is past fork_offset — binary search within fork messages
            let stream_arc2 = self
                .get_stream(name)
                .ok_or_else(|| Error::NotFound(name.to_string()))?;
            let stream2 = stream_arc2.read().expect("stream lock poisoned");
            let start_idx = match stream2
                .messages
                .binary_search_by(|m| m.offset.cmp(from_offset))
            {
                Ok(idx) | Err(idx) => idx,
            };
            let msgs: Vec<Bytes> = stream2.messages[start_idx..]
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

    fn delete(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        let stream_arc = streams
            .get(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?
            .clone();

        {
            let stream = stream_arc.read().expect("stream lock poisoned");

            // Already tombstoned — treat as gone
            if stream.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            }

            // Check expiry
            if super::is_stream_expired(&stream.config) {
                // Let it fall through to removal
            }

            if stream.ref_count > 0 {
                // Has child forks — soft-delete (tombstone)
                drop(stream);
                let mut stream_w = stream_arc.write().expect("stream lock poisoned");
                stream_w.state = StreamState::Tombstone;
                return Ok(());
            }
        }

        // No child forks — hard delete
        let fork_info = {
            let stream = stream_arc.read().expect("stream lock poisoned");
            let fi = stream.fork_info.clone();
            self.saturating_sub_total_bytes(stream.total_bytes);
            fi
        };
        streams.remove(name);

        // Cascade: decrement parent ref_count, and if parent is tombstoned
        // with ref_count reaching 0, hard-delete it too (walk up the chain).
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

        Ok(StreamMetadata {
            config: stream.config.clone(),
            next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
            closed: stream.closed,
            total_bytes: stream.total_bytes,
            message_count: u64::try_from(stream.messages.len()).unwrap_or(u64::MAX),
            created_at: stream.created_at,
            updated_at: stream.updated_at,
        })
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        stream.closed = true;
        stream.updated_at = Some(Utc::now());

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

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        super::cleanup_stale_producers(&mut stream.producers);

        if !messages.is_empty() {
            super::validate_content_type(&stream.config.content_type, content_type)?;
        }

        let now = Utc::now();

        match super::check_producer(stream.producers.get(&producer.id), producer, stream.closed)? {
            ProducerCheck::Accept => {}
            ProducerCheck::Duplicate { epoch, seq } => {
                return Ok(ProducerAppendResult::Duplicate {
                    epoch,
                    seq,
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                });
            }
        }

        let pending_seq = super::validate_seq(stream.last_seq.as_deref(), seq)?;

        self.commit_messages(&mut stream, messages)?;
        if let Some(new_seq) = pending_seq {
            stream.last_seq = Some(new_seq);
        }

        if should_close {
            stream.closed = true;
        }

        stream.updated_at = Some(now);

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

            if super::is_stream_expired(&stream.config) {
                let stream_bytes = stream.total_bytes;
                drop(stream);
                streams.remove(name);
                self.saturating_sub_total_bytes(stream_bytes);
            } else if stream.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else if stream.config == config {
                let next_offset = Offset::new(stream.next_read_seq, stream.next_byte_offset);
                let closed = stream.closed;
                return Ok(super::CreateWithDataResult {
                    status: CreateStreamResult::AlreadyExists,
                    next_offset,
                    closed,
                });
            } else {
                return Err(Error::ConfigMismatch);
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
            !super::is_stream_expired(&stream.config) && stream.state == StreamState::Active
        } else {
            false
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        let stream_arc = self.get_stream(name)?;
        let stream = stream_arc.read().expect("stream lock poisoned");

        if super::is_stream_expired(&stream.config) || stream.state == StreamState::Tombstone {
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
                expired.push((name.clone(), stream.total_bytes));
            }
        }

        for (name, bytes) in &expired {
            streams.remove(name);
            self.saturating_sub_total_bytes(*bytes);
        }

        expired.len()
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        let streams = self.streams.read().expect("streams lock poisoned");
        let mut result = Vec::new();
        for (name, stream_arc) in streams.iter() {
            let stream = stream_arc.read().expect("stream lock poisoned");
            if super::is_stream_expired(&stream.config)
                || stream.state == StreamState::Tombstone
            {
                continue;
            }
            result.push((
                name.clone(),
                StreamMetadata {
                    config: stream.config.clone(),
                    next_offset: Offset::new(stream.next_read_seq, stream.next_byte_offset),
                    closed: stream.closed,
                    total_bytes: stream.total_bytes,
                    message_count: u64::try_from(stream.messages.len()).unwrap_or(u64::MAX),
                    created_at: stream.created_at,
                    updated_at: stream.updated_at,
                },
            ));
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn touch_ttl(&self, name: &str) -> Result<()> {
        let stream_arc = self
            .get_stream(name)
            .ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut stream = stream_arc.write().expect("stream lock poisoned");

        super::fork::check_stream_access(&stream.config, stream.state, name)?;

        if let Some(ttl) = stream.config.ttl_seconds {
            stream.config.expires_at = Some(
                Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX)),
            );
        }

        Ok(())
    }

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&Offset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        let mut streams = self.streams.write().expect("streams lock poisoned");

        // Look up source stream
        let source_arc = streams
            .get(source_name)
            .ok_or_else(|| Error::NotFound(source_name.to_string()))?
            .clone();

        let source = source_arc.read().expect("stream lock poisoned");

        // Validate source: not expired, not tombstoned
        super::fork::check_stream_access(&source.config, source.state, source_name)?;

        // Resolve fork offset (defaults to source tail)
        let source_next_offset = Offset::new(source.next_read_seq, source.next_byte_offset);
        let resolved_offset =
            super::fork::resolve_fork_offset(fork_offset, &source_next_offset)?;

        // Validate content type: fork must match source
        if !config.content_type.eq_ignore_ascii_case(&source.config.content_type) {
            return Err(Error::ContentTypeMismatch {
                expected: source.config.content_type.clone(),
                actual: config.content_type.clone(),
            });
        }

        // Inherit content type from source (use source's exact casing)
        let fork_content_type = source.config.content_type.clone();

        // Resolve TTL inheritance
        let (fork_ttl, fork_expires_at) = super::fork::resolve_fork_ttl(
            &source.config,
            config.ttl_seconds,
            config.expires_at,
        );

        // Whether fork should be closed: inherit source's closed state if
        // the fork was requested closed, otherwise start open
        let fork_closed = config.created_closed;

        drop(source);

        // Check idempotent create — if fork already exists with matching config
        if let Some(existing_arc) = streams.get(name) {
            let existing = existing_arc.read().expect("stream lock poisoned");
            if super::is_stream_expired(&existing.config) {
                let existing_bytes = existing.total_bytes;
                drop(existing);
                streams.remove(name);
                self.saturating_sub_total_bytes(existing_bytes);
            } else if existing.state == StreamState::Tombstone {
                return Err(Error::StreamGone(name.to_string()));
            } else {
                // Idempotent check: config must match
                let mut expected_config = StreamConfig::new(fork_content_type.clone());
                if let Some(ttl) = fork_ttl {
                    expected_config = expected_config.with_ttl(ttl);
                }
                if let Some(ea) = fork_expires_at {
                    expected_config = expected_config.with_expires_at(ea);
                }
                if fork_closed {
                    expected_config = expected_config.with_created_closed(true);
                }
                if existing.config == expected_config {
                    return Ok(CreateStreamResult::AlreadyExists);
                }
                return Err(Error::ConfigMismatch);
            }
        }

        // Extract fork offset components to initialize the fork entry
        let (fork_read_seq, fork_byte_offset) = resolved_offset
            .parse_components()
            .unwrap_or((0, 0));

        // Build fork config
        let mut fork_config = StreamConfig::new(fork_content_type);
        if let Some(ttl) = fork_ttl {
            fork_config = fork_config.with_ttl(ttl);
        }
        if let Some(ea) = fork_expires_at {
            fork_config = fork_config.with_expires_at(ea);
        }
        if fork_closed {
            fork_config = fork_config.with_created_closed(true);
        }

        // Create fork entry
        let (notify, _) = broadcast::channel(NOTIFY_CHANNEL_CAPACITY);
        let entry = StreamEntry {
            config: fork_config,
            messages: Vec::with_capacity(INITIAL_MESSAGES_CAPACITY),
            closed: fork_closed,
            next_read_seq: fork_read_seq,
            next_byte_offset: fork_byte_offset,
            total_bytes: 0,
            created_at: Utc::now(),
            updated_at: None,
            producers: HashMap::with_capacity(INITIAL_PRODUCERS_CAPACITY),
            notify,
            last_seq: None,
            fork_info: Some(ForkInfo {
                source_name: source_name.to_string(),
                fork_offset: resolved_offset,
            }),
            ref_count: 0,
            state: StreamState::Active,
        };

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
