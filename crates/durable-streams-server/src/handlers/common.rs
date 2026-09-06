//! Shared helpers for the protocol handlers.
//!
//! Centralizes body reading, header parsing, JSON-mode message extraction,
//! and response construction so each handler can focus on its own flow
//! rather than re-implementing the same HTTP plumbing.

use crate::protocol::headers::{self, names};
use crate::protocol::offset::Offset;
use crate::protocol::problem::{ProblemDetails, ProblemResponse, ProblemResult, request_instance};
use axum::body::{Body, Bytes};
use axum::extract::OriginalUri;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header::IntoHeaderName};
use axum::response::{IntoResponse, Response};
use std::future::Future;

/// Read a request body fully into bytes.
///
/// Body I/O failures surface as a dedicated invalid-body problem response
/// without widening the public [`crate::protocol::error::Error`] enum.
pub(crate) async fn read_body(body: Body, limit: usize) -> ProblemResult<Bytes> {
    axum::body::to_bytes(body, limit).await.map_err(|e| {
        use std::error::Error as _;
        if e.source()
            .is_some_and(<dyn std::error::Error>::is::<http_body_util::LengthLimitError>)
        {
            return ProblemResponse::new(
                ProblemDetails::new(
                    "/errors/request-body-too-large",
                    "Request Body Too Large",
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "REQUEST_BODY_TOO_LARGE",
                )
                .with_detail(format!("request body exceeds the {limit}-byte limit")),
            );
        }
        ProblemResponse::new(
            ProblemDetails::new(
                "/errors/invalid-body",
                "Invalid Body",
                StatusCode::BAD_REQUEST,
                "INVALID_BODY",
            )
            .with_detail(format!("failed to read request body: {e}")),
        )
    })
}

/// Check whether the `Stream-Closed` request header is truthy.
#[must_use]
pub fn parse_stream_closed(headers: &HeaderMap) -> bool {
    headers
        .get(names::STREAM_CLOSED)
        .and_then(|v| v.to_str().ok())
        .is_some_and(headers::parse_bool)
}

/// Split a request body into messages based on content type.
///
/// - Empty body → `vec![]`
/// - JSON content type → flattened JSON array elements
/// - Anything else → single-element vec containing the raw body
///
/// Empty JSON arrays return `Ok(vec![])`; callers enforce their own
/// policy (PUT accepts them; POST rejects unless closing).
pub fn extract_messages(body: Bytes, normalized_ct: &str) -> ProblemResult<Vec<Bytes>> {
    crate::protocol::extract_messages(body, normalized_ct).map_err(Into::into)
}

/// Attach the request instance to any problem-response produced inside `f`.
///
/// All handlers share the same "run the body, tag failures with the
/// request path" boilerplate; this helper does the wrapping once.
pub async fn with_instance<F, Fut>(original_uri: OriginalUri, f: F) -> ProblemResult<Response>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ProblemResult<Response>>,
{
    let instance = request_instance(&original_uri);
    f().await.map_err(|problem| problem.with_instance(instance))
}

/// Build a [`HeaderValue`] from bytes that are guaranteed to be a valid
/// header value (ASCII offset strings, hardcoded protocol literals, etc.).
///
/// Collapses the `.parse().unwrap()` / `HeaderValue::from_bytes(...).unwrap()`
/// sprinkled across every response builder into one place.
///
/// # Panics
///
/// Panics if the input is not valid for a `HeaderValue`. Callers pass
/// hardcoded strings or already-validated offsets, so this is unreachable
/// in practice.
#[must_use]
pub fn header_value(value: impl AsRef<[u8]>) -> HeaderValue {
    HeaderValue::from_bytes(value.as_ref()).expect("handler header values are validated upstream")
}

/// Fluent builder for protocol responses.
///
/// Centralizes the `Stream-Next-Offset`, `Stream-Closed`, and related
/// headers so each handler does not re-implement them.
#[must_use]
pub struct StreamResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Option<Body>,
}

impl StreamResponse {
    pub fn new(status: StatusCode) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: None,
        }
    }

    pub fn body(mut self, body: Body) -> Self {
        self.body = Some(body);
        self
    }

    pub fn header<K: IntoHeaderName>(mut self, name: K, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    pub fn content_type(self, content_type: &str) -> Self {
        self.header("content-type", header_value(content_type))
    }

    pub fn next_offset(self, offset: &Offset) -> Self {
        self.header(names::STREAM_NEXT_OFFSET, header_value(offset.as_str()))
    }

    pub fn closed_if(self, closed: bool) -> Self {
        if closed {
            self.header(names::STREAM_CLOSED, header_value("true"))
        } else {
            self
        }
    }

    pub fn up_to_date(self, value: bool) -> Self {
        let text = if value { "true" } else { "false" };
        self.header(names::STREAM_UP_TO_DATE, header_value(text))
    }

    pub fn cursor(self, cursor: &str) -> Self {
        self.header(names::STREAM_CURSOR, header_value(cursor))
    }

    pub fn etag(self, etag: &str) -> Self {
        self.header("etag", header_value(etag))
    }

    pub fn location(self, location: &str) -> Self {
        self.header("location", header_value(location))
    }
}

impl IntoResponse for StreamResponse {
    fn into_response(self) -> Response {
        match self.body {
            Some(body) => (self.status, self.headers, body).into_response(),
            None => (self.status, self.headers).into_response(),
        }
    }
}
