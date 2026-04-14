//! Shared constants, types, and validation helpers used by all storage backends.
//!
//! Extracted from `storage/mod.rs` to keep the top-level module focused on the
//! `Storage` trait definition and public types.

use crate::protocol::error::{Error, Result};
use crate::protocol::producer::ProducerHeaders;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::StreamConfig;

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
