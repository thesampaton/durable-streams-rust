use crate::protocol::problem::{ProblemDetails, ProblemResponse, ProblemTelemetry};
use axum::http::{HeaderValue, StatusCode, header::RETRY_AFTER};
use std::io;
use thiserror::Error;

/// Default `Retry-After` value for temporary backend unavailability.
pub const DEFAULT_STORAGE_RETRY_AFTER_SECS: u32 = 1;

/// Internal classification for storage-originated failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFailureClass {
    Unavailable,
    InsufficientStorage,
}

impl StorageFailureClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::InsufficientStorage => "insufficient_storage",
        }
    }
}

/// Internal metadata retained for storage-related failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageFailure {
    pub class: StorageFailureClass,
    pub backend: &'static str,
    pub operation: String,
    pub detail: String,
    pub retry_after_secs: Option<u32>,
}

impl std::fmt::Display for StorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", self.backend, self.operation, self.detail)
    }
}

/// Single error type for all storage and protocol operations
///
/// Maps to HTTP status codes in handlers. Following the single error enum
/// pattern to avoid error type proliferation.
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
    ContentTypeMismatch { expected: String, actual: String },

    /// Stream is closed (409)
    #[error("Stream is closed")]
    StreamClosed,

    /// Producer sequence gap (409)
    #[error("Producer sequence gap: expected {expected}, got {actual}")]
    SequenceGap { expected: u64, actual: u64 },

    /// Producer epoch fenced (403)
    #[error("Producer epoch fenced: current {current}, received {received}")]
    EpochFenced { current: u64, received: u64 },

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
    InvalidHeader { header: String, reason: String },

    /// Invalid stream name (400)
    #[error("Invalid stream name: {0}")]
    InvalidStreamName(String),

    /// Stream-Seq ordering violation (409)
    #[error("Stream-Seq ordering violation: last={last}, received={received}")]
    SeqOrderingViolation { last: String, received: String },

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

impl Error {
    /// Map error to HTTP status code
    ///
    /// This is the single place where errors are mapped to status codes.
    /// Handlers should use this method to determine the response code.
    ///
    /// # Panics
    ///
    /// Panics if HTTP status code 507 cannot be constructed, which should
    /// never happen since 507 is a valid IANA-registered status code.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) | Self::StreamExpired => StatusCode::NOT_FOUND,
            Self::StreamGone(_) => StatusCode::GONE,
            Self::StreamPathBlocked(_)
            | Self::ForkFromTombstone(_)
            | Self::ConfigMismatch
            | Self::ContentTypeMismatch { .. }
            | Self::StreamClosed
            | Self::SequenceGap { .. }
            | Self::SeqOrderingViolation { .. } => StatusCode::CONFLICT,
            Self::EpochFenced { .. } => StatusCode::FORBIDDEN,
            Self::MemoryLimitExceeded | Self::StreamSizeLimitExceeded => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Self::ForkOffsetBeyondTail
            | Self::InvalidOffset(_)
            | Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidJson(_)
            | Self::InvalidHeader { .. }
            | Self::InvalidStreamName(_)
            | Self::EmptyBody
            | Self::EmptyArray => StatusCode::BAD_REQUEST,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::InsufficientStorage(_) => {
                StatusCode::from_u16(507).expect("507 is a valid status code")
            }
            Self::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Build a [`ProblemDetails`] using `self.to_string()` as the detail message.
    ///
    /// This covers the common case where the `Display` impl already provides a
    /// sufficiently descriptive detail string.
    fn simple_problem(
        &self,
        type_uri: &'static str,
        title: &'static str,
        code: &'static str,
    ) -> ProblemDetails {
        ProblemDetails::new(type_uri, title, self.status_code(), code).with_detail(self.to_string())
    }

    fn conflict_problem(&self) -> ProblemDetails {
        match self {
            Self::ConfigMismatch => self.simple_problem(
                "/errors/already-exists",
                "Stream Already Exists",
                "ALREADY_EXISTS",
            ),
            Self::ContentTypeMismatch { .. } => self.simple_problem(
                "/errors/content-type-mismatch",
                "Content Type Mismatch",
                "CONTENT_TYPE_MISMATCH",
            ),
            Self::StreamClosed => {
                self.simple_problem("/errors/stream-closed", "Stream Closed", "STREAM_CLOSED")
            }
            Self::SequenceGap { .. } | Self::SeqOrderingViolation { .. } => self.simple_problem(
                "/errors/sequence-conflict",
                "Sequence Conflict",
                "SEQUENCE_CONFLICT",
            ),
            Self::ForkFromTombstone(_) => self.simple_problem(
                "/errors/fork-from-tombstone",
                "Fork From Deleted Stream",
                "FORK_FROM_TOMBSTONE",
            ),
            Self::StreamPathBlocked(name) => ProblemDetails::new(
                "/errors/path-blocked",
                "Stream Path Blocked",
                self.status_code(),
                "PATH_BLOCKED",
            )
            .with_detail(format!(
                "Stream path is reserved by a soft-deleted lineage: {name}"
            )),
            _ => unreachable!("conflict_problem called with non-conflict error"),
        }
    }

    fn client_problem(&self) -> ProblemDetails {
        match self {
            Self::InvalidOffset(_) => {
                self.simple_problem("/errors/invalid-offset", "Invalid Offset", "INVALID_OFFSET")
            }
            Self::InvalidStreamName(_) => self.simple_problem(
                "/errors/invalid-stream-name",
                "Invalid Stream Name",
                "INVALID_STREAM_NAME",
            ),
            Self::InvalidJson(_) => {
                self.simple_problem("/errors/invalid-json", "Invalid JSON", "INVALID_JSON")
            }
            Self::EmptyBody => {
                self.simple_problem("/errors/empty-body", "Empty Body", "EMPTY_BODY")
            }
            Self::EmptyArray => {
                self.simple_problem("/errors/empty-array", "Empty Array", "EMPTY_ARRAY")
            }
            Self::EpochFenced { .. } => self.simple_problem(
                "/errors/producer-epoch-fenced",
                "Producer Epoch Fenced",
                "PRODUCER_EPOCH_FENCED",
            ),
            Self::ForkOffsetBeyondTail
            | Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidHeader { .. } => {
                self.simple_problem("/errors/bad-request", "Bad Request", "BAD_REQUEST")
            }
            _ => unreachable!("client_problem called with unsupported error"),
        }
    }

    fn storage_problem(&self) -> ProblemDetails {
        match self {
            Self::Unavailable(_) => ProblemDetails::new(
                "/errors/unavailable",
                "Service Unavailable",
                self.status_code(),
                "UNAVAILABLE",
            )
            .with_detail("The server is temporarily unable to complete the request."),
            Self::InsufficientStorage(_) => ProblemDetails::new(
                "/errors/insufficient-storage",
                "Insufficient Storage",
                self.status_code(),
                "INSUFFICIENT_STORAGE",
            )
            .with_detail(
                "The server does not have enough storage capacity to complete the request.",
            ),
            Self::Storage(_) => ProblemDetails::new(
                "/errors/internal",
                "Internal Server Error",
                self.status_code(),
                "INTERNAL_ERROR",
            )
            .with_detail("The server encountered an internal error."),
            _ => unreachable!("storage_problem called with non-storage error"),
        }
    }

    #[must_use]
    fn problem_details(&self) -> ProblemDetails {
        match self {
            Self::NotFound(name) => ProblemDetails::new(
                "/errors/not-found",
                "Stream Not Found",
                self.status_code(),
                "NOT_FOUND",
            )
            .with_detail(format!("Stream not found: {name}")),
            Self::ConfigMismatch
            | Self::ContentTypeMismatch { .. }
            | Self::StreamClosed
            | Self::SequenceGap { .. }
            | Self::SeqOrderingViolation { .. }
            | Self::ForkFromTombstone(_)
            | Self::StreamPathBlocked(_) => self.conflict_problem(),
            Self::InvalidOffset(_)
            | Self::EpochFenced { .. }
            | Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidJson(_)
            | Self::EmptyBody
            | Self::EmptyArray
            | Self::InvalidHeader { .. }
            | Self::InvalidStreamName(_)
            | Self::ForkOffsetBeyondTail => self.client_problem(),
            Self::MemoryLimitExceeded | Self::StreamSizeLimitExceeded => self.simple_problem(
                "/errors/payload-too-large",
                "Payload Too Large",
                "PAYLOAD_TOO_LARGE",
            ),
            Self::Unavailable(_) | Self::InsufficientStorage(_) | Self::Storage(_) => {
                self.storage_problem()
            }
            Self::StreamExpired => {
                self.simple_problem("/errors/not-found", "Stream Not Found", "NOT_FOUND")
            }
            Self::StreamGone(name) => {
                ProblemDetails::new("/errors/gone", "Stream Gone", self.status_code(), "GONE")
                    .with_detail(format!("Stream is gone: {name}"))
            }
        }
    }

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
    fn telemetry(&self) -> Option<ProblemTelemetry> {
        match self {
            Self::Unavailable(failure) | Self::InsufficientStorage(failure) => {
                let mut t = ProblemTelemetry::from(&self.problem_details());
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
                let mut t = ProblemTelemetry::from(&self.problem_details());
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
        let telemetry = error.telemetry();
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
}
