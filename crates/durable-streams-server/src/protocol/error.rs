//! Domain errors and their HTTP problem-response mappings.

use crate::protocol::problem::{ProblemDetails, ProblemResponse, ProblemTelemetry};
use axum::http::{HeaderValue, StatusCode, header::RETRY_AFTER};
use std::io;
use thiserror::Error;

/// Default `Retry-After` value for temporary backend unavailability.
pub const DEFAULT_STORAGE_RETRY_AFTER_SECS: u32 = 1;

/// Classification used for storage HTTP responses and telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFailureClass {
    /// Temporary backend failure, mapped to HTTP 503.
    Unavailable,
    /// Backend capacity exhaustion, mapped to HTTP 507.
    InsufficientStorage,
}

impl StorageFailureClass {
    /// Stable telemetry label for this failure class.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::InsufficientStorage => "insufficient_storage",
        }
    }
}

/// Backend failure context retained for diagnostics and retry guidance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageFailure {
    /// Response and telemetry classification.
    pub class: StorageFailureClass,
    /// Backend identifier used in diagnostics.
    pub backend: &'static str,
    /// Operation that failed, such as reading metadata or committing an append.
    pub operation: String,
    /// Internal diagnostic detail; classified responses keep this out of the public body.
    pub detail: String,
    /// Suggested delay in seconds before retrying, if available.
    pub retry_after_secs: Option<u32>,
}

impl std::fmt::Display for StorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", self.backend, self.operation, self.detail)
    }
}

/// Domain errors for stream storage and protocol operations.
///
/// This module defines the HTTP status, problem details, and telemetry for each
/// variant. Handlers attach request context and operation-specific headers.
#[derive(Debug, Error)]
pub enum Error {
    /// Stream not found (404)
    #[error("Stream not found: {0}")]
    NotFound(String),

    /// Stream already exists with different config (409)
    #[error("Stream already exists with different configuration")]
    ConfigMismatch,

    /// Invalid offset format (400)
    #[error("Invalid offset format: {0}")]
    InvalidOffset(String),

    /// Content type mismatch (409)
    #[error("Content type mismatch: expected {expected}, got {actual}")]
    ContentTypeMismatch {
        /// Content type fixed when the stream was created.
        expected: String,
        /// Content type supplied by the request.
        actual: String,
    },

    /// Stream is closed (409)
    #[error("Stream is closed")]
    StreamClosed,

    /// Producer sequence gap (409)
    #[error("Producer sequence gap: expected {expected}, got {actual}")]
    SequenceGap {
        /// Next producer sequence expected by the server.
        expected: u64,
        /// Producer sequence supplied by the request.
        actual: u64,
    },

    /// Producer epoch fenced (403)
    #[error("Producer epoch fenced: current {current}, received {received}")]
    EpochFenced {
        /// Current producer epoch.
        current: u64,
        /// Producer epoch supplied by the request.
        received: u64,
    },

    /// Invalid producer state (400)
    #[error("Invalid producer state: {0}")]
    InvalidProducerState(String),

    /// Memory limit exceeded (413)
    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,

    /// Stream size limit exceeded (413)
    #[error("Stream size limit exceeded")]
    StreamSizeLimitExceeded,

    /// Invalid TTL format (400)
    #[error("Invalid TTL format: {0}")]
    InvalidTtl(String),

    /// Both TTL and Expires-At provided (400)
    #[error("Cannot specify both TTL and Expires-At")]
    ConflictingExpiration,

    /// Stream has expired (404)
    #[error("Stream has expired")]
    StreamExpired,

    /// Invalid JSON (400)
    #[error("Invalid JSON: {0}")]
    InvalidJson(String),

    /// Empty request body when data is required (400)
    #[error("Empty request body requires Stream-Closed: true")]
    EmptyBody,

    /// Empty JSON array append body (400)
    #[error("Empty JSON arrays are not permitted for append")]
    EmptyArray,

    /// Invalid header value (400)
    #[error("Invalid header value for {header}: {reason}")]
    InvalidHeader {
        /// Header whose value is invalid.
        header: String,
        /// Why the header value was rejected.
        reason: String,
    },

    /// Invalid stream name (400)
    #[error("Invalid stream name: {0}")]
    InvalidStreamName(String),

    /// Stream-Seq ordering violation (409)
    #[error("Stream-Seq ordering violation: last={last}, received={received}")]
    SeqOrderingViolation {
        /// Last accepted writer sequence.
        last: String,
        /// Writer sequence supplied by the request.
        received: String,
    },

    /// Storage backend is temporarily unavailable (503)
    #[error("Storage temporarily unavailable: {0}")]
    Unavailable(StorageFailure),

    /// Storage backend has insufficient capacity (507)
    #[error("Storage capacity exhausted: {0}")]
    InsufficientStorage(StorageFailure),

    /// Stream has been deleted (tombstoned) and is gone (410)
    #[error("Stream is gone: {0}")]
    StreamGone(String),

    /// Stream path is blocked because a soft-deleted lineage still owns it (409)
    #[error("Stream path is reserved by a soft-deleted lineage: {0}")]
    StreamPathBlocked(String),

    /// Fork offset exceeds the source stream's tail (400)
    #[error("Fork offset is beyond the source stream's tail")]
    ForkOffsetBeyondTail,

    /// Cannot fork from a tombstoned stream (409)
    #[error("Cannot fork from deleted stream: {0}")]
    ForkFromTombstone(String),

    /// Storage backend I/O or serialization error (500)
    #[error("Storage error: {0}")]
    Storage(String),
}

/// Static response metadata; public detail overrides keep storage internals out
/// of the wire response without losing them from telemetry.
struct ProblemDefinition {
    status: StatusCode,
    type_uri: &'static str,
    title: &'static str,
    code: &'static str,
    detail: Option<&'static str>,
}

impl ProblemDefinition {
    fn new(
        status: StatusCode,
        type_uri: &'static str,
        title: &'static str,
        code: &'static str,
    ) -> Self {
        Self {
            status,
            type_uri,
            title,
            code,
            detail: None,
        }
    }

    fn with_detail(mut self, detail: &'static str) -> Self {
        self.detail = Some(detail);
        self
    }
}

impl Error {
    /// Map an error to its HTTP status using the same definition as its body.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        self.problem_definition().status
    }

    /// One exhaustive mapping defines status, type, title, code and safe detail.
    /// Adding an error variant requires defining its response here.
    #[allow(clippy::too_many_lines)] // Keep the complete wire mapping exhaustive and together.
    fn problem_definition(&self) -> ProblemDefinition {
        match self {
            Self::NotFound(_) | Self::StreamExpired => ProblemDefinition::new(
                StatusCode::NOT_FOUND,
                "/errors/not-found",
                "Stream Not Found",
                "NOT_FOUND",
            ),
            Self::ConfigMismatch => ProblemDefinition::new(
                StatusCode::CONFLICT,
                "/errors/already-exists",
                "Stream Already Exists",
                "ALREADY_EXISTS",
            ),
            Self::ContentTypeMismatch { .. } => ProblemDefinition::new(
                StatusCode::CONFLICT,
                "/errors/content-type-mismatch",
                "Content Type Mismatch",
                "CONTENT_TYPE_MISMATCH",
            ),
            Self::StreamClosed => ProblemDefinition::new(
                StatusCode::CONFLICT,
                "/errors/stream-closed",
                "Stream Closed",
                "STREAM_CLOSED",
            ),
            Self::SequenceGap { .. } | Self::SeqOrderingViolation { .. } => ProblemDefinition::new(
                StatusCode::CONFLICT,
                "/errors/sequence-conflict",
                "Sequence Conflict",
                "SEQUENCE_CONFLICT",
            ),
            Self::ForkFromTombstone(_) => ProblemDefinition::new(
                StatusCode::CONFLICT,
                "/errors/fork-from-tombstone",
                "Fork From Deleted Stream",
                "FORK_FROM_TOMBSTONE",
            ),
            Self::StreamPathBlocked(_) => ProblemDefinition::new(
                StatusCode::CONFLICT,
                "/errors/path-blocked",
                "Stream Path Blocked",
                "PATH_BLOCKED",
            ),
            Self::InvalidOffset(_) => ProblemDefinition::new(
                StatusCode::BAD_REQUEST,
                "/errors/invalid-offset",
                "Invalid Offset",
                "INVALID_OFFSET",
            ),
            Self::InvalidStreamName(_) => ProblemDefinition::new(
                StatusCode::BAD_REQUEST,
                "/errors/invalid-stream-name",
                "Invalid Stream Name",
                "INVALID_STREAM_NAME",
            ),
            Self::InvalidJson(_) => ProblemDefinition::new(
                StatusCode::BAD_REQUEST,
                "/errors/invalid-json",
                "Invalid JSON",
                "INVALID_JSON",
            ),
            Self::EmptyBody => ProblemDefinition::new(
                StatusCode::BAD_REQUEST,
                "/errors/empty-body",
                "Empty Body",
                "EMPTY_BODY",
            ),
            Self::EmptyArray => ProblemDefinition::new(
                StatusCode::BAD_REQUEST,
                "/errors/empty-array",
                "Empty Array",
                "EMPTY_ARRAY",
            ),
            Self::EpochFenced { .. } => ProblemDefinition::new(
                StatusCode::FORBIDDEN,
                "/errors/producer-epoch-fenced",
                "Producer Epoch Fenced",
                "PRODUCER_EPOCH_FENCED",
            ),
            Self::ForkOffsetBeyondTail
            | Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidHeader { .. } => ProblemDefinition::new(
                StatusCode::BAD_REQUEST,
                "/errors/bad-request",
                "Bad Request",
                "BAD_REQUEST",
            ),
            Self::MemoryLimitExceeded | Self::StreamSizeLimitExceeded => ProblemDefinition::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "/errors/payload-too-large",
                "Payload Too Large",
                "PAYLOAD_TOO_LARGE",
            ),
            Self::Unavailable(_) => ProblemDefinition::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "/errors/unavailable",
                "Service Unavailable",
                "UNAVAILABLE",
            )
            .with_detail("The server is temporarily unable to complete the request."),
            Self::InsufficientStorage(_) => ProblemDefinition::new(
                StatusCode::INSUFFICIENT_STORAGE,
                "/errors/insufficient-storage",
                "Insufficient Storage",
                "INSUFFICIENT_STORAGE",
            )
            .with_detail(
                "The server does not have enough storage capacity to complete the request.",
            ),
            Self::Storage(_) => ProblemDefinition::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "/errors/internal",
                "Internal Server Error",
                "INTERNAL_ERROR",
            )
            .with_detail("The server encountered an internal error."),
            Self::StreamGone(_) => {
                ProblemDefinition::new(StatusCode::GONE, "/errors/gone", "Stream Gone", "GONE")
            }
        }
    }

    #[must_use]
    fn problem_details(&self) -> ProblemDetails {
        let definition = self.problem_definition();
        ProblemDetails::new(
            definition.type_uri,
            definition.title,
            definition.status,
            definition.code,
        )
        .with_detail(
            definition
                .detail
                .map_or_else(|| self.to_string(), str::to_owned),
        )
    }

    /// Build a temporary failure with the default retry delay and internal diagnostic context.
    #[must_use]
    pub fn storage_unavailable(
        backend: &'static str,
        operation: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::Unavailable(StorageFailure {
            class: StorageFailureClass::Unavailable,
            backend,
            operation: operation.into(),
            detail: detail.into(),
            retry_after_secs: Some(DEFAULT_STORAGE_RETRY_AFTER_SECS),
        })
    }

    /// Build a capacity failure retaining backend and operation details for telemetry.
    #[must_use]
    pub fn storage_insufficient(
        backend: &'static str,
        operation: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::InsufficientStorage(StorageFailure {
            class: StorageFailureClass::InsufficientStorage,
            backend,
            operation: operation.into(),
            detail: detail.into(),
            retry_after_secs: None,
        })
    }

    /// Classify transient and capacity I/O failures; preserve other failures as storage errors.
    #[must_use]
    pub fn classify_io_failure(
        backend: &'static str,
        operation: impl Into<String>,
        detail: impl Into<String>,
        error: &io::Error,
    ) -> Self {
        match error.kind() {
            io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
                Self::storage_unavailable(backend, operation, detail)
            }
            io::ErrorKind::StorageFull
            | io::ErrorKind::QuotaExceeded
            | io::ErrorKind::FileTooLarge => Self::storage_insufficient(backend, operation, detail),
            _ => Self::Storage(detail.into()),
        }
    }

    /// Whether the I/O kind indicates interruption, a would-block condition, or timeout.
    #[must_use]
    pub fn is_retryable_io_error(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        )
    }

    /// Build telemetry metadata for storage-related errors.
    ///
    /// Derives the base fields from [`Self::problem_details`] so that the
    /// type URI, code, title, and detail stay in sync automatically, then
    /// overlays storage-specific context that is only emitted to logs.
    #[must_use]
    fn telemetry(&self, problem: &ProblemDetails) -> Option<ProblemTelemetry> {
        match self {
            Self::Unavailable(failure) | Self::InsufficientStorage(failure) => {
                let mut t = ProblemTelemetry::from(problem);
                t.error_class = Some(failure.class.as_str().to_string());
                t.storage_backend = Some(failure.backend.to_string());
                t.storage_operation = Some(failure.operation.clone());
                t.internal_detail = Some(failure.detail.clone());
                if let Self::Unavailable(f) = self {
                    t.retry_after_secs = f.retry_after_secs;
                }
                Some(t)
            }
            Self::Storage(detail) => {
                let mut t = ProblemTelemetry::from(problem);
                t.error_class = Some("internal".to_string());
                t.internal_detail = Some(detail.clone());
                Some(t)
            }
            _ => None,
        }
    }
}

/// Result type alias for storage and protocol operations
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for ProblemResponse {
    fn from(error: Error) -> Self {
        let problem = error.problem_details();
        let telemetry = error.telemetry(&problem);
        let mut response = ProblemResponse::new(problem);

        if let Some(retry_after_secs) = match &error {
            Error::Unavailable(failure) => failure.retry_after_secs,
            _ => None,
        } {
            response = response.with_header(
                RETRY_AFTER,
                HeaderValue::from_str(&retry_after_secs.to_string())
                    .expect("retry-after header value must be valid"),
            );
        }

        if let Some(telemetry) = telemetry {
            response = response.with_telemetry(telemetry);
        }

        response
    }
}

/// Convert Error to HTTP response
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        ProblemResponse::from(self).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use axum::http::HeaderValue;
    use axum::response::IntoResponse;
    use std::io;

    #[test]
    fn classify_io_failure_maps_transient_errors_to_503() {
        let error = io::Error::new(io::ErrorKind::TimedOut, "backend timed out");
        let response = Error::classify_io_failure(
            "file",
            "append stream log",
            "failed to append stream log: backend timed out",
            &error,
        )
        .into_response();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.headers().get("retry-after").unwrap(),
            &HeaderValue::from_static("1")
        );
    }

    #[test]
    fn classify_io_failure_maps_capacity_errors_to_507() {
        let error = io::Error::new(io::ErrorKind::StorageFull, "disk full");
        let response = Error::classify_io_failure(
            "file",
            "sync stream log",
            "failed to sync stream log: disk full",
            &error,
        )
        .into_response();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::from_u16(507).unwrap()
        );
        assert!(response.headers().get("retry-after").is_none());
    }
    /// Issue #13: preserve the wire contract and private telemetry for every
    /// error variant while replacing the response dispatch machinery.
    #[tokio::test]
    async fn test_error_response_wire_contract() {
        use crate::protocol::problem::ProblemTelemetry;
        use serde_json::{Value, json};
        let errors = [
            Error::NotFound("missing".into()),
            Error::ConfigMismatch,
            Error::InvalidOffset("bad".into()),
            Error::ContentTypeMismatch {
                expected: "text/plain".into(),
                actual: "application/json".into(),
            },
            Error::StreamClosed,
            Error::SequenceGap {
                expected: 3,
                actual: 5,
            },
            Error::EpochFenced {
                current: 3,
                received: 1,
            },
            Error::InvalidProducerState("incomplete headers".into()),
            Error::MemoryLimitExceeded,
            Error::StreamSizeLimitExceeded,
            Error::InvalidTtl("bad".into()),
            Error::ConflictingExpiration,
            Error::StreamExpired,
            Error::InvalidJson("bad".into()),
            Error::EmptyBody,
            Error::EmptyArray,
            Error::InvalidHeader {
                header: "Stream-Seq".into(),
                reason: "bad".into(),
            },
            Error::InvalidStreamName("bad".into()),
            Error::SeqOrderingViolation {
                last: "b".into(),
                received: "a".into(),
            },
            Error::storage_unavailable("file", "append", "private timeout"),
            Error::storage_insufficient("acid", "commit", "private disk path"),
            Error::StreamGone("deleted".into()),
            Error::StreamPathBlocked("reserved".into()),
            Error::ForkOffsetBeyondTail,
            Error::ForkFromTombstone("deleted".into()),
            Error::Storage("private internal failure".into()),
        ];
        let mut actual = Vec::new();
        for error in errors {
            let expected_status = error.status_code();
            let response = error.into_response();
            assert_eq!(response.status(), expected_status);
            let telemetry = response.extensions().get::<ProblemTelemetry>().unwrap();
            let record = json!({
                "status": response.status().as_u16(),
                "content_type": response.headers()["content-type"].to_str().unwrap(),
                "retry_after": response.headers().get("retry-after").map(|v| v.to_str().unwrap()),
                "telemetry": {
                    "type": telemetry.problem_type, "code": telemetry.code,
                    "title": telemetry.title, "detail": telemetry.detail,
                    "class": telemetry.error_class, "backend": telemetry.storage_backend,
                    "operation": telemetry.storage_operation, "internal": telemetry.internal_detail,
                    "retry_after": telemetry.retry_after_secs,
                },
            });
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let mut record = record;
            record["body"] = serde_json::from_slice::<Value>(&body).unwrap();
            actual.push(record);
        }
        let actual = Value::Array(actual);
        let expected: Value =
            serde_json::from_str(include_str!("../../tests/snapshots/error-responses.json"))
                .unwrap();
        assert_eq!(actual, expected);
    }
}
