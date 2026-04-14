//! Shared fork logic used by all storage backends.
//!
//! Contains helpers for fork offset resolution, TTL inheritance,
//! read-plan construction, and access validation.

use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::storage::{ForkInfo, StreamConfig, StreamState};
use chrono::{DateTime, Utc};

/// Combined expiry + tombstone check for stream access.
///
/// Returns `Err(StreamExpired)` if the stream's TTL has elapsed, or
/// `Err(StreamGone)` if the stream has been soft-deleted (tombstoned).
pub(crate) fn check_stream_access(
    config: &StreamConfig,
    state: StreamState,
    name: &str,
) -> Result<()> {
    if super::shared::is_stream_expired(config) {
        return Err(Error::StreamExpired);
    }
    if state == StreamState::Tombstone {
        return Err(Error::StreamGone(name.to_string()));
    }
    Ok(())
}

/// Resolve fork TTL inheritance from source.
///
/// Priority: explicit fork TTL > explicit fork Expires-At > source TTL > source Expires-At.
pub(crate) fn resolve_fork_ttl(
    source_config: &StreamConfig,
    fork_ttl: Option<u64>,
    fork_expires_at: Option<DateTime<Utc>>,
) -> (Option<u64>, Option<DateTime<Utc>>) {
    // If fork specifies its own TTL or Expires-At, use that
    if let Some(ttl) = fork_ttl {
        let expires_at =
            Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX));
        return (Some(ttl), Some(expires_at));
    }
    if let Some(ea) = fork_expires_at {
        return (None, Some(ea));
    }
    // Inherit from source
    if let Some(ttl) = source_config.ttl_seconds {
        let expires_at =
            Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX));
        return (Some(ttl), Some(expires_at));
    }
    if let Some(ea) = source_config.expires_at {
        return (None, Some(ea));
    }
    (None, None)
}

/// Resolve fork offset, defaulting to source tail.
///
/// Returns `Err(ForkOffsetBeyondTail)` if the requested offset exceeds the
/// source stream's current next offset.
pub(crate) fn resolve_fork_offset(
    requested: Option<&Offset>,
    source_next_offset: &Offset,
) -> Result<Offset> {
    match requested {
        Some(offset) => {
            if offset > source_next_offset {
                Err(Error::ForkOffsetBeyondTail)
            } else {
                Ok(offset.clone())
            }
        }
        None => Ok(source_next_offset.clone()),
    }
}

/// One segment of a fork read plan.
pub(crate) struct ReadSegment {
    /// Stream name to read from.
    pub name: String,
    /// Read messages with offsets strictly less than this value.
    /// `None` means read all available messages from this stream.
    pub read_up_to: Option<Offset>,
}

/// Build the read plan for a potentially forked stream.
///
/// Returns segments from root (oldest ancestor) to leaf (the requested stream).
/// Each segment's `read_up_to` is the offset at which to stop reading from
/// that segment — determined by the `fork_offset` of the child that forks from it.
/// The leaf segment has `read_up_to = None` (read all its messages).
pub(crate) fn build_read_plan(
    name: &str,
    lookup: impl Fn(&str) -> Option<Option<ForkInfo>>,
) -> Vec<ReadSegment> {
    // Walk from leaf to root, collecting (name, fork_offset_from_parent).
    // fork_offset_from_parent is the offset at which this entry forked from
    // its parent — i.e., the boundary where the parent should stop.
    let mut chain: Vec<(String, Option<Offset>)> = Vec::new();
    let mut current = name.to_string();

    loop {
        if let Some(Some(fork_info)) = lookup(&current) {
            chain.push((current.clone(), Some(fork_info.fork_offset.clone())));
            current.clone_from(&fork_info.source_name);
        } else {
            chain.push((current, None));
            break;
        }
    }

    // chain is [leaf, ..., root] with each entry's fork_offset from its parent.
    // Reverse to get [root, ..., leaf].
    chain.reverse();

    // Now build segments. For segment i, read_up_to is the fork_offset of
    // segment i+1 (the child that forked from segment i). The last segment
    // (leaf) has read_up_to = None.
    let len = chain.len();
    let mut segments = Vec::with_capacity(len);

    for i in 0..len {
        let (ref seg_name, _) = chain[i];
        let read_up_to = if i + 1 < len {
            // The next segment's fork_offset_from_parent tells us where to
            // stop reading from this segment.
            chain[i + 1].1.clone()
        } else {
            // Leaf: read all messages
            None
        };
        segments.push(ReadSegment {
            name: seg_name.clone(),
            read_up_to,
        });
    }

    segments
}
