use crate::protocol::error::Error;
use crate::protocol::headers::{self, names};
use crate::protocol::json_mode;
use crate::protocol::offset::Offset;
use crate::protocol::problem::{ProblemResponse, Result, request_instance};
use crate::protocol::stream_name::StreamName;
use crate::router::StreamBasePath;
use crate::storage::{CreateStreamResult, CreateWithDataResult, Storage, StreamConfig};
use axum::{
    Extension,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, HeaderValue, StatusCode},
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
///
/// # Panics
///
/// Panics if validated content-type or offset strings fail to parse into
/// header values, which should never happen with valid inputs.
#[allow(clippy::too_many_lines)]
pub async fn create_stream<S: Storage>(
    State(storage): State<Arc<S>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
    Extension(StreamBasePath(stream_base_path)): Extension<StreamBasePath>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response> {
    let instance = request_instance(&original_uri);
    let result = async {
        // Read body (PUT may include initial data)
        let body_bytes =
            axum::body::to_bytes(body, usize::MAX)
                .await
                .map_err(|e| Error::InvalidHeader {
                    header: "Content-Length".to_string(),
                    reason: format!("Failed to read body: {e}"),
                })?;

        // Parse Content-Type: optional, defaults to application/octet-stream
        let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());

        // Reject explicitly-provided but empty Content-Type
        if let Some(ct) = content_type
            && ct.trim().is_empty()
        {
            return Err(ProblemResponse::from(Error::InvalidHeader {
                header: "Content-Type".to_string(),
                reason: "empty value".to_string(),
            }));
        }

        let normalized_ct = content_type.map_or_else(
            || "application/octet-stream".to_string(),
            headers::normalize_content_type,
        );

        // Parse optional TTL
        let ttl_seconds =
            if let Some(ttl_value) = headers.get(names::STREAM_TTL).and_then(|v| v.to_str().ok()) {
                Some(headers::parse_ttl(ttl_value)?)
            } else {
                None
            };

        // Parse optional Expires-At
        let expires_at = if let Some(expires_value) = headers
            .get(names::STREAM_EXPIRES_AT)
            .and_then(|v| v.to_str().ok())
        {
            Some(headers::parse_expires_at(expires_value)?)
        } else {
            None
        };

        // Reject both TTL and Expires-At
        if ttl_seconds.is_some() && expires_at.is_some() {
            return Err(ProblemResponse::from(Error::ConflictingExpiration));
        }

        // Parse optional Stream-Closed
        let created_closed = headers
            .get(names::STREAM_CLOSED)
            .and_then(|v| v.to_str().ok())
            .is_some_and(headers::parse_bool);

        // Build stream config
        let mut config = StreamConfig::new(normalized_ct.clone());

        if let Some(ttl) = ttl_seconds {
            let expires_at =
                Utc::now() + chrono::Duration::seconds(i64::try_from(ttl).unwrap_or(i64::MAX));
            config = config.with_expires_at(expires_at);
            config = config.with_ttl(ttl);
        } else if let Some(expires) = expires_at {
            config = config.with_expires_at(expires);
        }

        if created_closed {
            config = config.with_created_closed(true);
        }

        // ── Fork branch ──────────────────────────────────────────────
        let forked_from = headers
            .get(names::STREAM_FORKED_FROM)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let fork_offset_raw = headers
            .get(names::STREAM_FORK_OFFSET)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        if let Some(forked_from_value) = forked_from {
            // Strip stream_base_path prefix to get the source stream name
            let source_name = strip_stream_base_path(&forked_from_value, &stream_base_path);

            // Parse optional fork offset
            let fork_offset = if let Some(ref raw) = fork_offset_raw {
                Some(Offset::from_str(raw)?)
            } else {
                None
            };

            let create_result = storage
                .create_fork(&name, &source_name, fork_offset.as_ref(), config)
                .map_err(|e| match e {
                    // StreamGone from create_fork should be 409 Conflict, not 410
                    Error::StreamGone(_) => ProblemResponse::from(Error::ConfigMismatch),
                    other => ProblemResponse::from(other),
                })?;

            // After fork creation, read the fork metadata for response headers
            let meta = storage.head(&name)?;

            let status = if matches!(create_result, CreateStreamResult::Created) {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };

            let location = build_location_url(&headers, &stream_base_path, &name);

            let mut response_headers = HeaderMap::new();
            response_headers.insert(
                "content-type",
                meta.config.content_type.parse().unwrap(),
            );
            response_headers.insert(
                names::STREAM_NEXT_OFFSET,
                HeaderValue::from_bytes(meta.next_offset.as_str().as_bytes()).unwrap(),
            );
            response_headers.insert("location", location.parse().unwrap());

            if meta.closed {
                response_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
            }

            return Ok((status, response_headers).into_response());
        }

        // ── Standard (non-fork) create branch ────────────────────────

        // Parse body into messages BEFORE creating the stream so that
        // failures (e.g. invalid JSON) never leave an orphaned stream.
        let messages = if body_bytes.is_empty() {
            vec![]
        } else if json_mode::is_json_content_type(&normalized_ct) {
            // Empty JSON arrays produce no messages — that's fine for PUT
            json_mode::process_append(&body_bytes)?
        } else {
            vec![body_bytes]
        };

        // Atomic create + append + close. If commit_messages fails (e.g.
        // memory limit), the stream is never inserted. If created_closed,
        // the entry is closed before it becomes visible to other operations.
        let create_with_data_result = storage
            .create_stream_with_data(&name, config, messages, created_closed)
            .map_err(|e| match e {
                // StreamGone from create_stream_with_data should be 409 Conflict
                Error::StreamGone(_) => ProblemResponse::from(Error::ConfigMismatch),
                other => ProblemResponse::from(other),
            })?;

        let CreateWithDataResult {
            status: create_status,
            next_offset,
            closed,
        } = create_with_data_result;

        let status = if matches!(create_status, CreateStreamResult::Created) {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        };

        // Build absolute Location URL
        let location = build_location_url(&headers, &stream_base_path, &name);

        let mut response_headers = HeaderMap::new();
        response_headers.insert("content-type", normalized_ct.parse().unwrap());
        response_headers.insert(
            names::STREAM_NEXT_OFFSET,
            HeaderValue::from_bytes(next_offset.as_str().as_bytes()).unwrap(),
        );
        response_headers.insert("location", location.parse().unwrap());

        if closed {
            response_headers.insert(names::STREAM_CLOSED, "true".parse().unwrap());
        }

        Ok((status, response_headers).into_response())
    }
    .await;

    result.map_err(|problem| problem.with_instance(instance))
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

    if let Some(stripped) = value.strip_prefix(&prefix) {
        stripped.to_string()
    } else {
        // If the value doesn't start with the prefix, use it as-is
        value.to_string()
    }
}

/// Build an absolute Location URL from request headers.
///
/// Uses `X-Forwarded-Host`/`Host` for the authority and `X-Forwarded-Proto`
/// for the scheme.
/// Falls back to `http` and `localhost` when headers are absent.
fn build_location_url(headers: &HeaderMap, stream_base_path: &str, name: &str) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");

    let host = headers
        .get("x-forwarded-host")
        .and_then(|v| v.to_str().ok())
        .or_else(|| headers.get("host").and_then(|v| v.to_str().ok()))
        .unwrap_or("localhost");

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
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert("x-forwarded-host", "proxy.example.com".parse().unwrap());
        headers.insert("host", "internal.local".parse().unwrap());

        let location = build_location_url(&headers, "/v1/stream", "orders");
        assert_eq!(location, "https://proxy.example.com/v1/stream/orders");
    }

    #[test]
    fn test_build_location_falls_back_to_host_and_http() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:4437".parse().unwrap());

        let location = build_location_url(&headers, "/v1/stream", "orders");
        assert_eq!(location, "http://localhost:4437/v1/stream/orders");
    }

    #[test]
    fn test_build_location_supports_custom_base_path() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:4437".parse().unwrap());

        let location = build_location_url(&headers, "/streams", "orders");
        assert_eq!(location, "http://localhost:4437/streams/orders");
    }

    #[test]
    fn test_build_location_supports_root_base_path() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:4437".parse().unwrap());

        let location = build_location_url(&headers, "/", "orders");
        assert_eq!(location, "http://localhost:4437/orders");
    }
}
