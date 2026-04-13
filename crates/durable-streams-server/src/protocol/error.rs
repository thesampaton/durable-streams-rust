use crate::protocol::problem::{ProblemDetails, ProblemResponse};
use axum::http::StatusCode;
use thiserror::Error;

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

    /// Stream-Seq ordering violation (409)
    #[error("Stream-Seq ordering violation: last={last}, received={received}")]
    SeqOrderingViolation { last: String, received: String },

    /// Storage backend I/O or serialization error (500)
    #[error("Storage error: {0}")]
    Storage(String),
}

impl Error {
    /// Map error to HTTP status code
    ///
    /// This is the single place where errors are mapped to status codes.
    /// Handlers should use this method to determine the response code.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) | Self::StreamExpired => StatusCode::NOT_FOUND,
            Self::ConfigMismatch
            | Self::ContentTypeMismatch { .. }
            | Self::StreamClosed
            | Self::SequenceGap { .. }
            | Self::SeqOrderingViolation { .. } => StatusCode::CONFLICT,
            Self::EpochFenced { .. } => StatusCode::FORBIDDEN,
            Self::MemoryLimitExceeded | Self::StreamSizeLimitExceeded => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Self::InvalidOffset(_)
            | Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidJson(_)
            | Self::InvalidHeader { .. }
            | Self::EmptyBody
            | Self::EmptyArray => StatusCode::BAD_REQUEST,
            Self::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
            Self::ConfigMismatch => ProblemDetails::new(
                "/errors/already-exists",
                "Stream Already Exists",
                self.status_code(),
                "ALREADY_EXISTS",
            )
            .with_detail(self.to_string()),
            Self::InvalidOffset(_) => ProblemDetails::new(
                "/errors/invalid-offset",
                "Invalid Offset",
                self.status_code(),
                "INVALID_OFFSET",
            )
            .with_detail(self.to_string()),
            Self::ContentTypeMismatch { .. } => ProblemDetails::new(
                "/errors/content-type-mismatch",
                "Content Type Mismatch",
                self.status_code(),
                "CONTENT_TYPE_MISMATCH",
            )
            .with_detail(self.to_string()),
            Self::StreamClosed => ProblemDetails::new(
                "/errors/stream-closed",
                "Stream Closed",
                self.status_code(),
                "STREAM_CLOSED",
            )
            .with_detail(self.to_string()),
            Self::SequenceGap { .. } | Self::SeqOrderingViolation { .. } => ProblemDetails::new(
                "/errors/sequence-conflict",
                "Sequence Conflict",
                self.status_code(),
                "SEQUENCE_CONFLICT",
            )
            .with_detail(self.to_string()),
            Self::EpochFenced { .. } => ProblemDetails::new(
                "/errors/producer-epoch-fenced",
                "Producer Epoch Fenced",
                self.status_code(),
                "PRODUCER_EPOCH_FENCED",
            )
            .with_detail(self.to_string()),
            Self::InvalidProducerState(_)
            | Self::InvalidTtl(_)
            | Self::ConflictingExpiration
            | Self::InvalidHeader { .. } => ProblemDetails::new(
                "/errors/bad-request",
                "Bad Request",
                self.status_code(),
                "BAD_REQUEST",
            )
            .with_detail(self.to_string()),
            Self::InvalidJson(_) => ProblemDetails::new(
                "/errors/invalid-json",
                "Invalid JSON",
                self.status_code(),
                "INVALID_JSON",
            )
            .with_detail(self.to_string()),
            Self::EmptyBody => ProblemDetails::new(
                "/errors/empty-body",
                "Empty Body",
                self.status_code(),
                "EMPTY_BODY",
            )
            .with_detail(self.to_string()),
            Self::EmptyArray => ProblemDetails::new(
                "/errors/empty-array",
                "Empty Array",
                self.status_code(),
                "EMPTY_ARRAY",
            )
            .with_detail(self.to_string()),
            Self::MemoryLimitExceeded | Self::StreamSizeLimitExceeded => ProblemDetails::new(
                "/errors/payload-too-large",
                "Payload Too Large",
                self.status_code(),
                "PAYLOAD_TOO_LARGE",
            )
            .with_detail(self.to_string()),
            Self::StreamExpired => ProblemDetails::new(
                "/errors/not-found",
                "Stream Not Found",
                self.status_code(),
                "NOT_FOUND",
            )
            .with_detail(self.to_string()),
            Self::Storage(_) => ProblemDetails::new(
                "/errors/internal",
                "Internal Server Error",
                self.status_code(),
                "INTERNAL_ERROR",
            )
            .with_detail("The server encountered an internal error."),
        }
    }
}

/// Result type alias for storage and protocol operations
pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for ProblemResponse {
    fn from(error: Error) -> Self {
        ProblemResponse::new(error.problem_details())
    }
}

/// Convert Error to HTTP response
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        ProblemResponse::from(self).into_response()
    }
}
