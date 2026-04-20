use crate::handlers::common::{StreamResponse, header_value, with_instance};
use crate::protocol::cursor;
use crate::protocol::error::Error;
use crate::protocol::json_mode;
use crate::protocol::offset::Offset;
use crate::protocol::problem::ProblemResult;
use crate::protocol::sse::{self, ControlPayload};
use crate::protocol::stream_name::StreamName;
use crate::router::ReadStreamConfig;
use crate::storage::{ReadResult, Storage};
use axum::{
    Extension,
    body::Body,
    extract::{OriginalUri, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    /// Starting offset (None when not provided; defaults vary by mode)
    offset: Option<String>,
    /// Live mode: "long-poll" for long-polling
    live: Option<String>,
    /// Cursor echoed from previous long-poll response. Parsed by axum/serde
    /// so the query param is accepted, but the server doesn't use it — it
    /// exists for CDN intermediaries to collapse identical polling requests.
    #[serde(rename = "cursor")]
    _cursor: Option<String>,
}

/// GET handler for reading stream data
///
/// Supports three modes:
/// - Catch-up (no `live` param): immediate read, returns all available data
/// - Long-poll (`live=long-poll`): waits for new data at tail
/// - SSE (`live=sse`): streaming Server-Sent Events
///
/// # Errors
///
/// Returns error if stream doesn't exist, offset is invalid,
/// or storage operation fails.
pub async fn read_stream<S: Storage + 'static>(
    State(storage): State<Arc<S>>,
    StreamName(name): StreamName,
    original_uri: OriginalUri,
    Query(query): Query<ReadQuery>,
    Extension(read_config): Extension<ReadStreamConfig>,
    headers: HeaderMap,
) -> ProblemResult<Response> {
    let ReadStreamConfig {
        long_poll_timeout: timeout,
        sse_reconnect_interval_secs: reconnect_interval_secs,
        shutdown,
    } = read_config;
    with_instance(original_uri, || async move {
        let raw_offset = resolve_offset(&query)?;
        let offset = Offset::from_str(&raw_offset)?;
        let metadata = storage.head(&name)?;
        let content_type = metadata.config.content_type.clone();

        let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());

        match query.live.as_deref() {
            None => read_catch_up(
                &storage,
                &name,
                &offset,
                &raw_offset,
                if_none_match,
                &content_type,
            ),
            Some("long-poll") => {
                read_long_poll(
                    &storage,
                    &ReadContext {
                        name: &name,
                        offset: &offset,
                        raw_offset: &raw_offset,
                        if_none_match,
                        content_type: &content_type,
                    },
                    timeout,
                    shutdown,
                )
                .await
            }
            Some("sse") => read_sse(
                storage,
                name,
                &offset,
                &content_type,
                reconnect_interval_secs,
                shutdown,
            ),
            Some(other) => Err(Error::InvalidHeader {
                header: "live".to_string(),
                reason: format!("unsupported live mode: {other}"),
            }
            .into()),
        }
    })
    .await
}

/// Resolve the starting offset from query params.
///
/// Live modes require an explicit `offset`; catch-up defaults to `-1`.
fn resolve_offset(query: &ReadQuery) -> ProblemResult<String> {
    if let Some(ref live) = query.live {
        query.offset.clone().ok_or_else(|| {
            Error::InvalidHeader {
                header: "offset".to_string(),
                reason: format!("offset query parameter is required for live={live} mode"),
            }
            .into()
        })
    } else {
        Ok(query
            .offset
            .clone()
            .unwrap_or_else(|| Offset::START.to_string()))
    }
}

/// Catch-up mode: immediate read of all available data.
fn read_catch_up<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    offset: &Offset,
    raw_offset: &str,
    if_none_match: Option<&str>,
    content_type: &str,
) -> ProblemResult<Response> {
    let read_result = storage.read(name, offset)?;
    let (etag, not_modified) = compute_etag(&read_result, raw_offset, if_none_match);
    if let Some(response) = not_modified {
        return Ok(response);
    }
    Ok(build_data_response(&read_result, content_type, &etag, None))
}

struct ReadContext<'a> {
    name: &'a str,
    offset: &'a Offset,
    raw_offset: &'a str,
    if_none_match: Option<&'a str>,
    content_type: &'a str,
}

/// Long-poll mode: wait for new data at tail, return immediately if data exists.
async fn read_long_poll<S: Storage>(
    storage: &Arc<S>,
    ctx: &ReadContext<'_>,
    timeout: Duration,
    shutdown: CancellationToken,
) -> ProblemResult<Response> {
    let ReadContext {
        name,
        offset,
        raw_offset,
        if_none_match,
        content_type,
    } = *ctx;
    // Subscribe BEFORE read to avoid missing notifications between read and subscribe
    let mut receiver = storage
        .subscribe(name)
        .ok_or_else(|| Error::NotFound(name.to_string()))?;

    let read_result = storage.read(name, offset)?;

    let (etag, not_modified) = compute_etag(&read_result, raw_offset, if_none_match);
    if let Some(response) = not_modified {
        return Ok(response);
    }

    if !read_result.messages.is_empty() {
        let cursor_val = cursor::generate(&read_result.next_offset);
        return Ok(build_data_response(
            &read_result,
            content_type,
            &etag,
            Some(&cursor_val),
        ));
    }

    // At tail + closed → immediate 204 (MUST NOT wait)
    if read_result.closed && read_result.at_tail {
        return Ok(build_204_response(&read_result.next_offset, true));
    }

    // At tail + open → wait for notification or timeout.
    // Capture the concrete tail offset for re-reads. The original `offset`
    // may be a sentinel (e.g. `now`) which always returns empty on re-read;
    // using the resolved position ensures we pick up data that arrived.
    let tail_offset = read_result.next_offset.clone();
    let tail_offset_str = tail_offset.to_string();

    tokio::select! {
        _ = receiver.recv() => {
            handle_long_poll_wake(storage, name, &tail_offset, &tail_offset_str, content_type)
        }
        () = tokio::time::sleep(timeout) => {
            let read_result = storage.read(name, &tail_offset)?;
            let is_closed = read_result.closed && read_result.at_tail;
            Ok(build_204_response(&read_result.next_offset, is_closed))
        }
        () = shutdown.cancelled() => {
            let read_result = storage.read(name, &tail_offset)?;
            let is_closed = read_result.closed && read_result.at_tail;
            Ok(build_204_response(&read_result.next_offset, is_closed))
        }
    }
}

/// SSE mode: stream data as Server-Sent Events (PROTOCOL.md §5.8).
///
/// Validates preconditions (stream existence, offset) eagerly before
/// starting the stream. Uses raw byte streaming for full control over
/// the SSE wire format. Once streaming begins, errors are silently
/// dropped (SSE has no error frame).
fn read_sse<S: Storage + 'static>(
    storage: Arc<S>,
    name: String,
    offset: &Offset,
    content_type: &str,
    reconnect_interval_secs: u64,
    shutdown: CancellationToken,
) -> ProblemResult<Response> {
    let encoding = SseEncoding {
        is_binary: sse::is_binary_content_type(content_type),
        is_json: json_mode::is_json_content_type(content_type),
    };

    let receiver = storage
        .subscribe(&name)
        .ok_or_else(|| Error::NotFound(name.clone()))?;

    let read_result = storage.read(&name, offset)?;

    let byte_stream = build_sse_byte_stream(
        storage,
        name,
        read_result,
        receiver,
        encoding,
        reconnect_interval_secs,
        shutdown,
    );

    let mut response = StreamResponse::new(StatusCode::OK)
        .content_type("text/event-stream")
        .body(Body::from_stream(byte_stream));

    if encoding.is_binary {
        response = response.header("stream-sse-data-encoding", header_value("base64"));
    }

    Ok(response.into_response())
}

/// SSE data encoding derived from the stream's content type.
#[derive(Clone, Copy)]
struct SseEncoding {
    is_binary: bool,
    is_json: bool,
}

/// Encode a read result as SSE frames: optional `data` frame + `control` frame.
fn encode_read_as_sse_frames(read_result: &ReadResult, encoding: SseEncoding) -> Vec<String> {
    let mut frames = Vec::with_capacity(2);
    let data_frames =
        sse::format_data_frames(&read_result.messages, encoding.is_binary, encoding.is_json);
    if !data_frames.is_empty() {
        frames.push(data_frames);
    }
    frames.push(sse::format_control_frame(&build_sse_control(read_result)));
    frames
}

/// Build a byte stream that yields raw SSE frame strings.
///
/// Manages keep-alive, idle timeout, and the subscribe-before-read pattern.
fn build_sse_byte_stream<S: Storage + 'static>(
    storage: Arc<S>,
    name: String,
    initial_read: ReadResult,
    mut receiver: tokio::sync::broadcast::Receiver<()>,
    encoding: SseEncoding,
    reconnect_interval_secs: u64,
    shutdown: CancellationToken,
) -> impl futures_util::stream::Stream<Item = std::result::Result<String, std::convert::Infallible>> + Send
{
    async_stream::stream! {
        let read_result = initial_read;

        for frame in encode_read_as_sse_frames(&read_result, encoding) {
            yield Ok(frame);
        }

        // If closed at tail, we're done
        if read_result.closed && read_result.at_tail {
            return;
        }

        // Enter wait loop at tail
        let mut tail_offset = read_result.next_offset;
        let idle_timeout = if reconnect_interval_secs > 0 {
            Some(Duration::from_secs(reconnect_interval_secs))
        } else {
            None
        };
        let mut idle_deadline = idle_timeout.map(|timeout| Instant::now() + timeout);

        let keepalive_interval = Duration::from_secs(15);

        loop {
            tokio::select! {
                recv_result = receiver.recv() => {
                    match recv_result {
                        Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // New data or we fell behind — re-read from tail
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            // Channel closed — final read + emit + end
                            if let Ok(rr) = storage.read(&name, &tail_offset) {
                                for frame in encode_read_as_sse_frames(&rr, encoding) {
                                    yield Ok(frame);
                                }
                            }
                            return;
                        }
                    }
                }
                () = tokio::time::sleep(keepalive_interval) => {
                    yield Ok(sse::format_keepalive_frame().to_string());
                    continue;
                }
                () = async {
                    match idle_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Idle close per PROTOCOL.md §5.8
                    return;
                }
                () = shutdown.cancelled() => {
                    return;
                }
            }

            // Re-read from tail position
            let Ok(rr) = storage.read(&name, &tail_offset) else {
                return;
            };

            for frame in encode_read_as_sse_frames(&rr, encoding) {
                yield Ok(frame);
            }

            // Keep idle-close tied to stream activity (new data), not keepalive ticks.
            if !rr.messages.is_empty()
                && let Some(timeout) = idle_timeout
            {
                idle_deadline = Some(Instant::now() + timeout);
            }

            if rr.closed && rr.at_tail {
                return;
            }

            tail_offset = rr.next_offset;
        }
    }
}

/// Build a `ControlPayload` from a `ReadResult`.
fn build_sse_control(read_result: &ReadResult) -> ControlPayload {
    let is_closed_at_tail = read_result.closed && read_result.at_tail;

    ControlPayload {
        stream_next_offset: read_result.next_offset.to_string(),
        stream_cursor: if is_closed_at_tail {
            None
        } else {
            Some(cursor::generate(&read_result.next_offset))
        },
        up_to_date: if read_result.at_tail {
            Some(true)
        } else {
            None
        },
        stream_closed: if is_closed_at_tail { Some(true) } else { None },
    }
}

/// Handle wake-up from broadcast in long-poll mode.
///
/// Re-reads from storage to get the actual data that triggered the notification.
fn handle_long_poll_wake<S: Storage>(
    storage: &Arc<S>,
    name: &str,
    offset: &Offset,
    raw_offset: &str,
    content_type: &str,
) -> ProblemResult<Response> {
    let read_result = storage.read(name, offset)?;

    if read_result.messages.is_empty() {
        // Woke up but no data (e.g., stream was closed)
        let is_closed = read_result.closed && read_result.at_tail;
        return Ok(build_204_response(&read_result.next_offset, is_closed));
    }

    let etag = generate_etag(raw_offset, &read_result);
    let cursor_val = cursor::generate(&read_result.next_offset);
    Ok(build_data_response(
        &read_result,
        content_type,
        &etag,
        Some(&cursor_val),
    ))
}

/// Compute the etag for this read and a 304 response if the client's
/// `If-None-Match` already matches.
fn compute_etag(
    read_result: &ReadResult,
    raw_offset: &str,
    if_none_match: Option<&str>,
) -> (String, Option<Response>) {
    let etag = generate_etag(raw_offset, read_result);
    let not_modified = match if_none_match {
        Some(client) if client == etag => Some(build_304_response(read_result)),
        _ => None,
    };
    (etag, not_modified)
}

/// Generate `ETag` from read result.
///
/// Format: `"{start_offset}:{end_offset}"` or `"{start_offset}:{end_offset}:c"` if closed at tail.
fn generate_etag(start_offset: &str, read_result: &ReadResult) -> String {
    let end_offset = read_result.next_offset.as_str();
    if read_result.closed && read_result.at_tail {
        format!("\"{start_offset}:{end_offset}:c\"")
    } else {
        format!("\"{start_offset}:{end_offset}\"")
    }
}

/// Build a 304 Not Modified response.
fn build_304_response(read_result: &ReadResult) -> Response {
    StreamResponse::new(StatusCode::NOT_MODIFIED)
        .next_offset(&read_result.next_offset)
        .up_to_date(true)
        .into_response()
}

/// Build a 200 OK response with message data.
///
/// If `cursor_val` is `Some`, includes `Stream-Cursor` header (long-poll mode).
fn build_data_response(
    read_result: &ReadResult,
    content_type: &str,
    etag: &str,
    cursor_val: Option<&str>,
) -> Response {
    let body = build_body(read_result, content_type);
    let is_closed_at_tail = read_result.closed && read_result.at_tail;

    let mut response = StreamResponse::new(StatusCode::OK)
        .content_type(content_type)
        .next_offset(&read_result.next_offset)
        .up_to_date(read_result.at_tail)
        .etag(etag)
        .closed_if(is_closed_at_tail)
        .body(Body::from(body));

    if let Some(c) = cursor_val {
        response = response.cursor(c);
    }

    response.into_response()
}

/// Build a 204 No Content response for long-poll timeout or closed stream.
fn build_204_response(next_offset: &Offset, is_closed: bool) -> Response {
    let cursor_val = cursor::generate(next_offset);
    StreamResponse::new(StatusCode::NO_CONTENT)
        .next_offset(next_offset)
        .up_to_date(true)
        .closed_if(is_closed)
        // Cursor MAY be omitted when closed per spec, but including it is harmless.
        .cursor(&cursor_val)
        .into_response()
}

/// Build response body from read result messages.
fn build_body(read_result: &ReadResult, content_type: &str) -> bytes::Bytes {
    if json_mode::is_json_content_type(content_type) {
        json_mode::wrap_read_iter(read_result.messages.iter())
    } else if read_result.messages.is_empty() {
        bytes::Bytes::new()
    } else {
        let total_len: usize = read_result.messages.iter().map(bytes::Bytes::len).sum();
        let mut buf = BytesMut::with_capacity(total_len);
        for message in &read_result.messages {
            buf.put(message.clone());
        }
        buf.freeze()
    }
}
