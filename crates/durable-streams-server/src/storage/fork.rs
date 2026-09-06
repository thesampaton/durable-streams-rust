//! Shared fork lifecycle logic used by all storage backends.
//!
//! This module owns policy decisions around source access, create-time
//! idempotency, soft-delete/tombstone handling, TTL renewal, and fork read
//! planning. Backends should only implement persistence and synchronization.

use crate::protocol::error::{Error, Result};
use crate::protocol::offset::Offset;
use crate::storage::{ForkCreateSpec, ForkInfo, StreamConfig, StreamState};
use chrono::{DateTime, Utc};

/// Combined expiry + tombstone check for direct stream access.
///
/// Returns `Err(StreamExpired)` if the stream's TTL has elapsed, or
/// `Err(StreamGone)` if the stream has been soft-deleted (tombstoned).
pub(crate) fn check_stream_access(
    config: &StreamConfig,
    state: StreamState,
    name: &str,
) -> Result<()> {
    if state == StreamState::Tombstone {
        return Err(Error::StreamGone(name.to_string()));
    }
    if super::shared::is_stream_expired(config) {
        return Err(Error::StreamExpired);
    }
    Ok(())
}

/// Source-stream access check for fork creation.
///
/// Direct access to a tombstoned stream is `410 Gone`, but attempting to fork
/// from that deleted source is a create-time conflict.
pub(crate) fn check_fork_source_access(
    config: &StreamConfig,
    state: StreamState,
    name: &str,
) -> Result<()> {
    if state == StreamState::Tombstone {
        return Err(Error::ForkFromTombstone(name.to_string()));
    }
    if super::shared::is_stream_expired(config) {
        return Err(Error::StreamExpired);
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
) -> Result<(Option<u64>, Option<DateTime<Utc>>)> {
    if let Some(ttl) = fork_ttl {
        return Ok((Some(ttl), Some(super::ttl_deadline(ttl, Utc::now())?)));
    }
    if let Some(deadline) = fork_expires_at {
        return Ok((None, Some(deadline)));
    }
    if let Some(ttl) = source_config.ttl_seconds {
        return Ok((Some(ttl), Some(super::ttl_deadline(ttl, Utc::now())?)));
    }
    Ok((None, source_config.expires_at))
}

/// Build the effective fork configuration after inheriting source fields.
pub(crate) fn build_fork_config(
    source_config: &StreamConfig,
    requested_config: &StreamConfig,
) -> Result<StreamConfig> {
    let (ttl_seconds, expires_at) = resolve_fork_ttl(
        source_config,
        requested_config.ttl_seconds,
        requested_config.expires_at,
    )?;
    Ok(StreamConfig {
        content_type: source_config.content_type.clone(),
        ttl_seconds,
        expires_at,
        created_closed: requested_config.created_closed,
    })
}

/// Resolve a fork create spec once source-derived values are known.
pub(crate) fn build_fork_create_spec(
    source_name: &str,
    source_config: &StreamConfig,
    requested_config: &StreamConfig,
    fork_offset: Offset,
) -> Result<ForkCreateSpec> {
    Ok(ForkCreateSpec {
        sub_offset: 0,
        source_name: source_name.to_string(),
        fork_offset,
        config: build_fork_config(source_config, requested_config)?,
    })
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

/// Result of evaluating an existing path during create.
pub(crate) enum ExistingCreateDisposition {
    RemoveExpired,
    AlreadyExists,
    Conflict(Error),
}

/// Decide how `PUT` should behave for an existing non-fork stream path.
#[must_use]
pub(crate) fn evaluate_root_create(
    name: &str,
    existing_config: &StreamConfig,
    existing_state: StreamState,
    existing_ref_count: u32,
    requested_config: &StreamConfig,
) -> ExistingCreateDisposition {
    if super::shared::is_stream_expired(existing_config) {
        if existing_ref_count > 0 {
            return ExistingCreateDisposition::Conflict(Error::StreamPathBlocked(name.to_string()));
        }
        return ExistingCreateDisposition::RemoveExpired;
    }

    if existing_state == StreamState::Tombstone {
        return ExistingCreateDisposition::Conflict(Error::StreamPathBlocked(name.to_string()));
    }

    if existing_config == requested_config {
        ExistingCreateDisposition::AlreadyExists
    } else {
        ExistingCreateDisposition::Conflict(Error::ConfigMismatch)
    }
}

/// Perform the source-side validation and spec construction for a fork create.
///
/// Runs `check_fork_source_access` → `resolve_fork_offset` → content-type check
/// (case-insensitive) → `build_fork_create_spec`. Returns the resolved fork
/// offset alongside the spec so callers can initialize the fork entry without
/// re-parsing it.
pub(crate) fn prepare_fork_spec(
    source_name: &str,
    source_config: &StreamConfig,
    source_state: StreamState,
    source_next_offset: &Offset,
    requested_fork_offset: Option<&Offset>,
    requested_config: &StreamConfig,
) -> Result<(ForkCreateSpec, Offset)> {
    check_fork_source_access(source_config, source_state, source_name)?;
    let resolved_offset = resolve_fork_offset(requested_fork_offset, source_next_offset)?;
    if !requested_config
        .content_type
        .eq_ignore_ascii_case(&source_config.content_type)
    {
        return Err(Error::ContentTypeMismatch {
            expected: source_config.content_type.clone(),
            actual: requested_config.content_type.clone(),
        });
    }
    let spec = build_fork_create_spec(
        source_name,
        source_config,
        requested_config,
        resolved_offset.clone(),
    )?;
    Ok((spec, resolved_offset))
}

/// Decide how `PUT` should behave for an existing fork path.
#[must_use]
pub(crate) fn evaluate_fork_create(
    name: &str,
    existing_config: &StreamConfig,
    existing_fork_info: Option<&ForkInfo>,
    existing_state: StreamState,
    existing_ref_count: u32,
    requested_spec: &ForkCreateSpec,
) -> ExistingCreateDisposition {
    if super::shared::is_stream_expired(existing_config) {
        if existing_ref_count > 0 {
            return ExistingCreateDisposition::Conflict(Error::StreamPathBlocked(name.to_string()));
        }
        return ExistingCreateDisposition::RemoveExpired;
    }

    if existing_state == StreamState::Tombstone {
        return ExistingCreateDisposition::Conflict(Error::StreamPathBlocked(name.to_string()));
    }

    let expected_fork_info = ForkInfo {
        source_name: requested_spec.source_name.clone(),
        fork_offset: requested_spec.fork_offset.clone(),
        sub_offset: requested_spec.sub_offset,
    };

    if existing_config == &requested_spec.config && existing_fork_info == Some(&expected_fork_info)
    {
        ExistingCreateDisposition::AlreadyExists
    } else {
        ExistingCreateDisposition::Conflict(Error::ConfigMismatch)
    }
}

/// Decide whether a delete/expiry operation should tombstone or hard-delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeleteDisposition {
    Tombstone,
    HardDelete,
}

/// Delete policy for direct client-initiated deletes.
pub(crate) fn evaluate_delete(
    name: &str,
    state: StreamState,
    ref_count: u32,
) -> Result<DeleteDisposition> {
    if state == StreamState::Tombstone {
        return Err(Error::StreamGone(name.to_string()));
    }
    if ref_count > 0 {
        Ok(DeleteDisposition::Tombstone)
    } else {
        Ok(DeleteDisposition::HardDelete)
    }
}

/// Delete policy for expired-stream cleanup.
#[must_use]
pub(crate) fn evaluate_expired_cleanup(ref_count: u32) -> DeleteDisposition {
    if ref_count > 0 {
        DeleteDisposition::Tombstone
    } else {
        DeleteDisposition::HardDelete
    }
}

/// Renew a sliding TTL in-place.
///
/// Returns `true` when the config was updated and should be persisted.
pub(crate) fn renew_ttl(config: &mut StreamConfig) -> Result<bool> {
    if let Some(ttl) = config.ttl_seconds {
        config.expires_at = Some(super::ttl_deadline(ttl, Utc::now())?);
        Ok(true)
    } else {
        Ok(false)
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

    chain.reverse();

    let len = chain.len();
    let mut segments = Vec::with_capacity(len);

    for i in 0..len {
        let (ref seg_name, _) = chain[i];
        let read_up_to = if i + 1 < len {
            chain[i + 1].1.clone()
        } else {
            None
        };
        segments.push(ReadSegment {
            name: seg_name.clone(),
            read_up_to,
        });
    }

    let mut bound: Option<Offset> = None;
    for segment in segments.iter_mut().rev() {
        if let Some(local) = &segment.read_up_to {
            bound = Some(bound.map_or_else(|| local.clone(), |b| b.min(local.clone())));
        }
        segment.read_up_to.clone_from(&bound);
    }
    segments
}

/// Validate and materialize the partial inherited prefix and initial body.
pub(crate) fn initial_fork_messages(
    config: &StreamConfig,
    options: &super::ForkOptions,
    source_messages: Vec<bytes::Bytes>,
) -> Result<Vec<bytes::Bytes>> {
    let invalid = || Error::InvalidHeader {
        header: "Stream-Fork-Sub-Offset".to_string(),
        reason: "sub-offset exceeds the available data after the anchor".to_string(),
    };
    let count = usize::try_from(options.sub_offset).map_err(|_| invalid())?;
    let json = crate::protocol::json_mode::is_json_content_type(&config.content_type);
    let mut messages = Vec::new();
    if count > 0 {
        if json {
            if count > source_messages.len() {
                return Err(invalid());
            }
            messages.extend(source_messages.into_iter().take(count));
        } else {
            let first = source_messages.first().ok_or_else(invalid)?;
            if count > first.len() {
                return Err(invalid());
            }
            // Own the prefix so a small fork does not retain a large source allocation.
            messages.push(bytes::Bytes::copy_from_slice(&first[..count]));
        }
    }
    if !options.initial_body.is_empty() {
        if json {
            messages.extend(crate::protocol::json_mode::process_append(
                &options.initial_body,
            )?);
        } else {
            messages.push(options.initial_body.clone());
        }
    }
    Ok(messages)
}

/// Replacing lineage in place would change bytes already inherited by descendants.
pub(crate) fn check_replace(ref_count: u32, fork: Option<&ForkInfo>) -> Result<()> {
    if ref_count != 0 || fork.is_some() {
        return Err(Error::ReplacementHasForks);
    }
    Ok(())
}
