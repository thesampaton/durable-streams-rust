//! Shared constants, types, and validation helpers used by all storage backends.
//!
//! Extracted from `storage/mod.rs` to keep the top-level module focused on the
//! `Storage` trait definition and public types.

use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::protocol::producer::ProducerHeaders;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::{StreamConfig, StreamMetadata, StreamState, fork};

/// Duration after which stale producer state is cleaned up (7 days).
pub(crate) const PRODUCER_STATE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Broadcast channel capacity for long-poll/SSE notifications.
/// Small because notifications are hints (no payload), not data delivery.
pub(crate) const NOTIFY_CHANNEL_CAPACITY: usize = 16;

/// Per-producer state tracked within a stream.
///
/// Shared between storage implementations. Includes serde derives
/// for the file-backed storage which persists this to disk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProducerState {
    pub epoch: u64,
    pub last_seq: u64,
    pub updated_at: DateTime<Utc>,
}

/// Outcome of producer validation before any mutation.
pub(crate) enum ProducerCheck {
    /// Request is valid; proceed with append.
    Accept,
    /// Request is a duplicate; return idempotent success.
    Duplicate { epoch: u64, seq: u64 },
}

/// Check if a stream has expired based on its configuration.
pub(crate) fn is_stream_expired(config: &StreamConfig) -> bool {
    config
        .expires_at
        .is_some_and(|expires_at| Utc::now() >= expires_at)
}

/// Validate content-type matches the stream's configured type (case-insensitive).
pub(crate) fn validate_content_type(stream_ct: &str, request_ct: &str) -> Result<()> {
    if !request_ct.eq_ignore_ascii_case(stream_ct) {
        return Err(Error::ContentTypeMismatch {
            expected: stream_ct.to_string(),
            actual: request_ct.to_string(),
        });
    }
    Ok(())
}

/// Validate Stream-Seq ordering and return the pending value to commit.
///
/// Returns `Err(SeqOrderingViolation)` if the new seq is not strictly
/// greater than the last seq (lexicographic comparison).
pub(crate) fn validate_seq(
    last_seq: Option<&str>,
    new_seq: Option<&str>,
) -> Result<Option<String>> {
    if let Some(new) = new_seq {
        if let Some(last) = last_seq
            && new <= last
        {
            return Err(Error::SeqOrderingViolation {
                last: last.to_string(),
                received: new.to_string(),
            });
        }
        return Ok(Some(new.to_string()));
    }
    Ok(None)
}

/// Remove producer state entries older than `PRODUCER_STATE_TTL_SECS`.
pub(crate) fn cleanup_stale_producers(producers: &mut HashMap<String, ProducerState>) {
    let cutoff = Utc::now()
        - chrono::TimeDelta::try_seconds(PRODUCER_STATE_TTL_SECS)
            .expect("7 days fits in TimeDelta");
    producers.retain(|_, state| state.updated_at > cutoff);
}

/// Return `true` when a stream is visible to callers — not expired and not tombstoned.
///
/// Used by `exists`, `subscribe`, and `list_streams` to filter out streams that
/// should no longer appear to external observers even if they are still held in
/// memory for fork bookkeeping.
pub(crate) fn is_stream_visible(config: &StreamConfig, state: StreamState) -> bool {
    !is_stream_expired(config) && state == StreamState::Active
}

/// Assemble a [`StreamMetadata`] from primitive fields.
///
/// The three backends persist stream state in different shapes
/// (`memory::StreamEntry`, `file::StreamEntry`, `acid::StoredStreamMeta`) — they
/// cannot share a struct because each carries backend-local resources (notifier
/// channels, file handles, index vectors). This helper keeps the projection
/// into the shared [`StreamMetadata`] return type in one place so a field
/// addition or rename to [`StreamMetadata`] is a single edit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_stream_metadata(
    config: StreamConfig,
    next_read_seq: u64,
    next_byte_offset: u64,
    closed: bool,
    total_bytes: u64,
    message_count: u64,
    created_at: DateTime<Utc>,
    updated_at: Option<DateTime<Utc>>,
) -> StreamMetadata {
    StreamMetadata {
        config,
        next_offset: Offset::new(next_read_seq, next_byte_offset),
        closed,
        total_bytes,
        message_count,
        created_at,
        updated_at,
    }
}

/// Shared pre-persistence validation for `append` / `batch_append`.
///
/// Runs in the canonical order so backends return the same errors in the same
/// precedence (access → closed → content-type). Extracted so the order lives in
/// one place and business-rule tweaks no longer need per-backend edits.
pub(crate) fn precheck_append(
    config: &StreamConfig,
    state: StreamState,
    closed: bool,
    name: &str,
    content_type: &str,
) -> Result<()> {
    fork::check_stream_access(config, state, name)?;
    if closed {
        return Err(Error::StreamClosed);
    }
    validate_content_type(&config.content_type, content_type)?;
    Ok(())
}

/// Shared pre-persistence validation for `batch_append`.
///
/// Extends [`precheck_append`] with the sequence-ordering check and returns the
/// pending `Stream-Seq` value to commit on success.
pub(crate) fn precheck_batch_append(
    config: &StreamConfig,
    state: StreamState,
    closed: bool,
    last_seq: Option<&str>,
    name: &str,
    content_type: &str,
    new_seq: Option<&str>,
) -> Result<Option<String>> {
    precheck_append(config, state, closed, name, content_type)?;
    validate_seq(last_seq, new_seq)
}

/// Outcome of producer-append pre-checks.
pub(crate) enum ProducerAppendPrecheck {
    /// Proceed with persistence using `pending_seq` as the new `Stream-Seq`.
    Accept { pending_seq: Option<String> },
    /// Duplicate request — backend must return `ProducerAppendResult::Duplicate`.
    Duplicate { epoch: u64, seq: u64 },
}

/// Shared pre-persistence validation for `append_with_producer`.
///
/// Performs the full producer-append validation sequence in the canonical order:
/// access → cleanup-stale-producers → content-type (non-empty) → producer check
/// → seq-ordering. Mutates `producers` to prune stale entries before validation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn precheck_producer_append(
    config: &StreamConfig,
    state: StreamState,
    closed: bool,
    last_seq: Option<&str>,
    producers: &mut HashMap<String, ProducerState>,
    name: &str,
    content_type: &str,
    producer: &ProducerHeaders,
    messages: &[Bytes],
    new_seq: Option<&str>,
) -> Result<ProducerAppendPrecheck> {
    fork::check_stream_access(config, state, name)?;
    cleanup_stale_producers(producers);

    if !messages.is_empty() {
        validate_content_type(&config.content_type, content_type)?;
    }

    match check_producer(producers.get(&producer.id), producer, closed)? {
        ProducerCheck::Accept => {}
        ProducerCheck::Duplicate { epoch, seq } => {
            return Ok(ProducerAppendPrecheck::Duplicate { epoch, seq });
        }
    }

    let pending_seq = validate_seq(last_seq, new_seq)?;
    Ok(ProducerAppendPrecheck::Accept { pending_seq })
}

/// Apply the post-persistence metadata updates that every append variant performs.
///
/// Sets `updated_at`, commits `pending_seq` into `last_seq`, and renews the
/// sliding TTL in-place. Returns `true` when the TTL value changed — file and
/// acid backends use that signal to decide whether metadata needs re-persisting.
pub(crate) fn apply_append_metadata(
    config: &mut StreamConfig,
    last_seq_field: &mut Option<String>,
    updated_at: &mut Option<DateTime<Utc>>,
    pending_seq: Option<String>,
    now: DateTime<Utc>,
) -> Result<bool> {
    *updated_at = Some(now);
    if let Some(new_seq) = pending_seq {
        *last_seq_field = Some(new_seq);
    }
    fork::renew_ttl(config)
}

/// Validate producer epoch/sequence against existing state.
///
/// Implements the standard validation order:
///   1. Epoch fencing (403)
///   2. Duplicate detection (204) — before closed check so retries work
///   3. Closed check (409) — blocks new sequences on closed streams
///   4. Gap / epoch-bump validation
pub(crate) fn check_producer(
    existing: Option<&ProducerState>,
    producer: &ProducerHeaders,
    stream_closed: bool,
) -> Result<ProducerCheck> {
    if let Some(state) = existing {
        if producer.epoch < state.epoch {
            return Err(Error::EpochFenced {
                current: state.epoch,
                received: producer.epoch,
            });
        }

        if producer.epoch == state.epoch && producer.seq <= state.last_seq {
            return Ok(ProducerCheck::Duplicate {
                epoch: state.epoch,
                seq: state.last_seq,
            });
        }

        // Not a duplicate — if stream is closed, reject
        if stream_closed {
            return Err(Error::StreamClosed);
        }

        if producer.epoch > state.epoch {
            if producer.seq != 0 {
                return Err(Error::InvalidProducerState(
                    "new epoch must start at seq 0".to_string(),
                ));
            }
        } else if producer.seq > state.last_seq + 1 {
            return Err(Error::SequenceGap {
                expected: state.last_seq + 1,
                actual: producer.seq,
            });
        }
    } else {
        // New producer
        if stream_closed {
            return Err(Error::StreamClosed);
        }
        if producer.seq != 0 {
            return Err(Error::SequenceGap {
                expected: 0,
                actual: producer.seq,
            });
        }
    }
    Ok(ProducerCheck::Accept)
}

/// Owned read data captured under a backend's stream lock. Ancestor traversal
/// must happen after releasing that lock, so a fork retains its local snapshot.
pub(crate) enum PendingRead {
    Complete(super::ReadResult),
    Fork {
        info: super::ForkInfo,
        local: super::ReadResult,
    },
}
