//! Error types exposed by the client crate.
//!
//! [`enum@Error`] is the main application-facing type. [`HttpError`] preserves
//! protocol details from non-success responses when callers need to branch on
//! offsets, stream state, or producer metadata.

use reqwest::StatusCode;
use std::collections::HashMap;
use thiserror::Error;

/// High-level client error.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid input before any HTTP call.
    #[error("{0}")]
    InvalidArgument(String),
    /// Configuration loading or validation failure.
    #[error("{0}")]
    Config(String),
    /// Network or HTTP client error.
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    /// HTTP response error with protocol context.
    #[error("{0}")]
    Http(Box<HttpError>),
    /// I/O failure used by configuration and conformance glue.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// JSON serialization or parsing failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// TOML parsing failure.
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    /// URL parsing failure.
    #[error(transparent)]
    Url(#[from] url::ParseError),
    /// Base64 decoding failure.
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    /// Protocol-level parse failure.
    #[error("{0}")]
    Parse(String),
}

impl Error {
    /// Construct an invalid-argument error.
    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    /// Construct a configuration error.
    #[must_use]
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// Construct a protocol parse error.
    #[must_use]
    pub fn parse(message: impl Into<String>) -> Self {
        Self::Parse(message.into())
    }

    /// Return the normalized error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidArgument(_) | Self::Url(_) => ErrorKind::InvalidArgument,
            Self::Config(_) | Self::Toml(_) => ErrorKind::Config,
            Self::Transport(error) if error.is_timeout() => ErrorKind::Timeout,
            Self::Transport(_) => ErrorKind::Network,
            Self::Http(error) => error.kind,
            Self::Io(_) => ErrorKind::Io,
            Self::Json(_) | Self::Parse(_) | Self::Base64(_) => ErrorKind::Parse,
        }
    }

    /// Return whether the error is a candidate for retry.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(error) => error.is_timeout() || error.is_connect(),
            Self::Http(error) => error.is_retryable(),
            _ => false,
        }
    }
}

/// Normalized error category used across transport and protocol failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidArgument,
    Config,
    Network,
    Timeout,
    NotFound,
    Conflict,
    StreamClosed,
    InvalidOffset,
    Forbidden,
    RateLimited,
    UnexpectedStatus,
    Parse,
    Io,
}

/// Coarse error code used by the conformance adapter and simple callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    NetworkError,
    Timeout,
    Conflict,
    NotFound,
    SequenceConflict,
    StreamClosed,
    InvalidOffset,
    UnexpectedStatus,
    ParseError,
    InternalError,
    NotSupported,
    InvalidArgument,
}

/// HTTP error with preserved protocol details.
///
/// This retains parsed response headers and server-reported sequencing metadata
/// so higher-level code can make conflict or recovery decisions.
#[derive(Debug)]
pub struct HttpError {
    pub status: StatusCode,
    pub kind: ErrorKind,
    pub message: String,
    pub headers: HashMap<String, String>,
    pub next_offset: Option<String>,
    pub stream_closed: bool,
    pub stream_cursor: Option<String>,
    pub producer_epoch: Option<i64>,
    pub producer_seq: Option<i64>,
    pub producer_expected_seq: Option<i64>,
    pub producer_received_seq: Option<i64>,
}

impl HttpError {
    /// Return whether this HTTP status is typically safe to retry.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
    }
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "http {}: {}", self.status.as_u16(), self.message)
    }
}

impl std::error::Error for HttpError {}

impl From<HttpError> for Error {
    fn from(error: HttpError) -> Self {
        Self::Http(Box::new(error))
    }
}
