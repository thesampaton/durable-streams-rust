use crate::handlers::common::{
    StreamResponse, extract_messages, parse_stream_closed, read_body, with_instance,
};
use crate::middleware::proxy_trust::ProxyTrustResult;
use crate::protocol::error::Error;
use crate::protocol::headers::{self, names};
use crate::protocol::offset::Offset;
use crate::protocol::problem::{ProblemResponse, ProblemResult};
use crate::protocol::stream_name::StreamName;
use crate::router::StreamBasePath;
use crate::storage::{
    CreateStreamResult, CreateWithDataResult, ForkOptions, Storage, StreamConfig,
};
use axum::{
    Extension,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use std::str::FromStr;
use std::sync::Arc;

/// PUT handler for creating streams
///
/// Creates a new stream with the specified configuration.
/// Returns 201 Created for new streams, 200 OK for idempotent recreates.
/// Optionally accepts a body with initial data for the stream.
///
/// # Errors
///
/// Returns error if Content-Type is explicitly provided but empty,
/// both TTL and Expires-At are provided, TTL format is invalid, or stream
/// exists with different configuration.
pub async fn create_stream<S: Storage>(
    State(storage): State<Arc<S>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
    Extension(StreamBasePath(stream_base_path)): Extension<StreamBasePath>,
    Extension(request_origin): Extension<ProxyTrustResult>,
    headers: HeaderMap,
    body: Body,
) -> ProblemResult<Response> {
    with_instance(original_uri, || async move {
        let body_bytes = read_body(body).await?;
        let normalized_ct = parse_content_type(&headers)?;
        let created_closed = parse_stream_closed(&headers);
        let config = build_config(&headers, normalized_ct.clone(), created_closed)?;

        let sub_offset = headers
            .get(names::STREAM_FORK_SUB_OFFSET)
            .map(|value| {
                let raw = value.to_str().map_err(|_| Error::InvalidHeader {
                    header: "Stream-Fork-Sub-Offset".into(),
                    reason: "expected a non-negative integer".into(),
                })?;
                if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(Error::InvalidHeader {
                        header: "Stream-Fork-Sub-Offset".into(),
                        reason: "expected a non-negative integer".into(),
                    });
                }
                raw.parse::<u64>().map_err(|_| Error::InvalidHeader {
                    header: "Stream-Fork-Sub-Offset".into(),
                    reason: "integer out of range".into(),
                })
            })
            .transpose()?;
        if !headers.contains_key(names::STREAM_FORKED_FROM)
            && (sub_offset.is_some() || headers.contains_key(names::STREAM_FORK_OFFSET))
        {
            return Err(Error::InvalidHeader {
                header: "Stream-Forked-From".into(),
                reason: "required with fork offsets".into(),
            }
            .into());
        }
        let options = ForkOptions {
            sub_offset: sub_offset.unwrap_or(0),
            inherit_content_type: !headers.contains_key("content-type"),
            initial_body: body_bytes.clone(),
        };
        let location = build_location_url(&request_origin, &stream_base_path, &name);

        if let Some(forked_from) = headers
            .get(names::STREAM_FORKED_FROM)
            .and_then(|v| v.to_str().ok())
        {
            create_fork_stream(
                &storage,
                &name,
                (
                    forked_from,
                    headers
                        .get(names::STREAM_FORK_OFFSET)
                        .and_then(|v| v.to_str().ok()),
                ),
                &stream_base_path,
                config,
                &location,
                options,
            )
        } else {
            create_standard_stream(
                &storage,
                &name,
                body_bytes,
                &normalized_ct,
                config,
                created_closed,
                &location,
            )
        }
    })
    .await
}

/// Parse Content-Type, defaulting to application/octet-stream when missing,
/// rejecting an explicitly empty value.
fn parse_content_type(headers: &HeaderMap) -> ProblemResult<String> {
    let raw = headers.get("content-type").and_then(|v| v.to_str().ok());
    if let Some(ct) = raw
        && ct.trim().is_empty()
    {
        return Err(ProblemResponse::from(Error::InvalidHeader {
            header: "Content-Type".to_string(),
            reason: "empty value".to_string(),
        }));
    }
    Ok(raw.map_or_else(
        || "application/octet-stream".to_string(),
        headers::normalize_content_type,
    ))
}

/// Build the `StreamConfig` from headers, enforcing the TTL/Expires-At
/// mutual exclusion.
fn build_config(
    headers: &HeaderMap,
    content_type: String,
    created_closed: bool,
) -> ProblemResult<StreamConfig> {
    let ttl_seconds = match headers.get(names::STREAM_TTL).and_then(|v| v.to_str().ok()) {
        Some(value) => Some(headers::parse_ttl(value)?),
        None => None,
    };
    let expires_at = match headers
        .get(names::STREAM_EXPIRES_AT)
        .and_then(|v| v.to_str().ok())
    {
        Some(value) => Some(headers::parse_expires_at(value)?),
        None => None,
    };
    if ttl_seconds.is_some() && expires_at.is_some() {
        return Err(ProblemResponse::from(Error::ConflictingExpiration));
    }

    let mut config = StreamConfig::new(content_type);
    if let Some(ttl) = ttl_seconds {
        let computed_expires =
            Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX));
        config = config.with_expires_at(computed_expires).with_ttl(ttl);
    } else if let Some(expires) = expires_at {
        config = config.with_expires_at(expires);
    }

    if created_closed {
        config = config.with_created_closed(true);
    }

    Ok(config)
}

/// Handle a fork-create request (Stream-Forked-From present).
fn create_fork_stream<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    (forked_from, fork_offset_raw): (&str, Option<&str>),
    stream_base_path: &str,
    config: StreamConfig,
    location: &str,
    options: ForkOptions,
) -> ProblemResult<Response> {
    let source_name = strip_stream_base_path(forked_from, stream_base_path);
    let fork_offset = match fork_offset_raw {
        Some(raw) => Some(Offset::from_str(raw)?),
        None => None,
    };

    let create_result = storage
        .create_fork_with_options(name, &source_name, fork_offset.as_ref(), config, options)
        .map_err(ProblemResponse::from)?;

    let meta = storage.head(name)?;
    Ok(StreamResponse::new(created_status(create_result))
        .content_type(&meta.config.content_type)
        .next_offset(&meta.next_offset)
        .location(location)
        .closed_if(meta.closed)
        .into_response())
}

/// Handle a standard (non-fork) create request.
fn create_standard_stream<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    body: bytes::Bytes,
    normalized_ct: &str,
    config: StreamConfig,
    created_closed: bool,
    location: &str,
) -> ProblemResult<Response> {
    // Parse body into messages BEFORE creating the stream so that failures
    // (e.g. invalid JSON) never leave an orphaned stream.
    let messages = extract_messages(body, normalized_ct)?;

    let CreateWithDataResult {
        status: create_status,
        next_offset,
        closed,
    } = storage
        .create_stream_with_data(name, config, messages, created_closed)
        .map_err(ProblemResponse::from)?;

    Ok(StreamResponse::new(created_status(create_status))
        .content_type(normalized_ct)
        .next_offset(&next_offset)
        .location(location)
        .closed_if(closed)
        .into_response())
}

fn created_status(result: CreateStreamResult) -> StatusCode {
    if matches!(result, CreateStreamResult::Created) {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    }
}

/// Strip the stream base path prefix from a fork source header value.
///
/// The conformance tests send the `Stream-Forked-From` header as the full
/// URL path (e.g., `/v1/stream/source-name`). This strips the leading
/// `stream_base_path + "/"` to get just the stream name.
fn strip_stream_base_path(value: &str, stream_base_path: &str) -> String {
    let prefix = if stream_base_path == "/" {
        "/".to_string()
    } else {
        format!("{stream_base_path}/")
    };

    value
        .strip_prefix(&prefix)
        .map_or_else(|| value.to_string(), str::to_string)
}

/// Build an absolute Location URL from the trusted request origin.
fn build_location_url(origin: &ProxyTrustResult, stream_base_path: &str, name: &str) -> String {
    let scheme = origin.scheme.as_str();
    let host = origin.authority.as_deref().unwrap_or("localhost");

    if stream_base_path == "/" {
        format!("{scheme}://{host}/{name}")
    } else {
        format!("{scheme}://{host}{stream_base_path}/{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_location_prefers_x_forwarded_host() {
        let location = build_location_url(
            &ProxyTrustResult {
                peer_ip: None,
                trusted: true,
                scheme: "https".to_string(),
                authority: Some("proxy.example.com".to_string()),
                client_address: None,
            },
            "/v1/stream",
            "orders",
        );
        assert_eq!(location, "https://proxy.example.com/v1/stream/orders");
    }

    #[test]
    fn test_build_location_falls_back_to_host_and_http() {
        let location = build_location_url(
            &ProxyTrustResult {
                peer_ip: None,
                trusted: false,
                scheme: "http".to_string(),
                authority: Some("localhost:4437".to_string()),
                client_address: None,
            },
            "/v1/stream",
            "orders",
        );
        assert_eq!(location, "http://localhost:4437/v1/stream/orders");
    }

    #[test]
    fn test_build_location_supports_custom_base_path() {
        let location = build_location_url(
            &ProxyTrustResult {
                peer_ip: None,
                trusted: false,
                scheme: "http".to_string(),
                authority: Some("localhost:4437".to_string()),
                client_address: None,
            },
            "/streams",
            "orders",
        );
        assert_eq!(location, "http://localhost:4437/streams/orders");
    }

    #[test]
    fn test_build_location_supports_root_base_path() {
        let location = build_location_url(
            &ProxyTrustResult {
                peer_ip: None,
                trusted: false,
                scheme: "http".to_string(),
                authority: Some("localhost:4437".to_string()),
                client_address: None,
            },
            "/",
            "orders",
        );
        assert_eq!(location, "http://localhost:4437/orders");
    }
}
