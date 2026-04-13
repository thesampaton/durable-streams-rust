//! Stream name extraction and validation.
//!
//! [`StreamName`] is an axum extractor that captures catch-all path parameters,
//! strips the leading slash, and validates length and segment depth against
//! configurable limits injected via [`StreamNameLimits`].

use crate::protocol::error::Error;
use crate::protocol::problem::ProblemResponse;
use axum::{
    Extension,
    extract::{Path, rejection::PathRejection},
    http::request::Parts,
    response::{IntoResponse, Response},
};

/// Configurable limits for stream name validation, injected as an axum
/// `Extension` by the router.
#[derive(Debug, Clone, Copy)]
pub struct StreamNameLimits {
    /// Maximum byte length of the stream name.
    pub max_bytes: usize,
    /// Maximum number of `/`-separated segments in the stream name.
    pub max_segments: usize,
}

/// Validated stream name extracted from the request path.
///
/// Replaces `Path<String>` in handler signatures. The catch-all wildcard
/// `/{*name}` may include a leading `/` which this extractor strips before
/// validation.
pub struct StreamName(pub String);

impl<S> axum::extract::FromRequestParts<S> for StreamName
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let raw_name = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|e| path_rejection_to_response(&e))?
            .0;

        let name = raw_name.strip_prefix('/').unwrap_or(&raw_name);

        if name.is_empty() {
            return Err(problem_response("stream name cannot be empty"));
        }

        let Extension(limits) =
            Extension::<StreamNameLimits>::from_request_parts(parts, state)
                .await
                .map_err(|_| {
                    problem_response("server misconfiguration: stream name limits not set")
                })?;

        if name.len() > limits.max_bytes {
            return Err(problem_response(&format!(
                "stream name is {} bytes, which exceeds the maximum of {} bytes",
                name.len(),
                limits.max_bytes
            )));
        }

        let segment_count = name.split('/').count();
        if segment_count > limits.max_segments {
            return Err(problem_response(&format!(
                "stream name has {} path segments, which exceeds the maximum of {}",
                segment_count, limits.max_segments
            )));
        }

        Ok(Self(name.to_string()))
    }
}

fn problem_response(reason: &str) -> Response {
    ProblemResponse::from(Error::InvalidStreamName(reason.to_string())).into_response()
}

fn path_rejection_to_response(rejection: &PathRejection) -> Response {
    ProblemResponse::from(Error::InvalidStreamName(rejection.to_string())).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Validate the name extraction and stripping logic directly.
    fn validate_name(raw: &str, limits: &StreamNameLimits) -> Result<String, String> {
        let name = raw.strip_prefix('/').unwrap_or(raw);
        if name.is_empty() {
            return Err("stream name cannot be empty".to_string());
        }
        if name.len() > limits.max_bytes {
            return Err(format!(
                "stream name is {} bytes, which exceeds the maximum of {} bytes",
                name.len(),
                limits.max_bytes
            ));
        }
        let segment_count = name.split('/').count();
        if segment_count > limits.max_segments {
            return Err(format!(
                "stream name has {} path segments, which exceeds the maximum of {}",
                segment_count, limits.max_segments
            ));
        }
        Ok(name.to_string())
    }

    fn default_limits() -> StreamNameLimits {
        StreamNameLimits {
            max_bytes: 1024,
            max_segments: 8,
        }
    }

    #[test]
    fn flat_name_passes() {
        let result = validate_name("my-stream", &default_limits());
        assert_eq!(result.unwrap(), "my-stream");
    }

    #[test]
    fn nested_name_passes() {
        let result = validate_name("a/b/c", &default_limits());
        assert_eq!(result.unwrap(), "a/b/c");
    }

    #[test]
    fn leading_slash_stripped() {
        let result = validate_name("/my-stream", &default_limits());
        assert_eq!(result.unwrap(), "my-stream");
    }

    #[test]
    fn leading_slash_stripped_nested() {
        let result = validate_name("/slides/abc123", &default_limits());
        assert_eq!(result.unwrap(), "slides/abc123");
    }

    #[test]
    fn empty_name_rejected() {
        let result = validate_name("", &default_limits());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn slash_only_rejected() {
        let result = validate_name("/", &default_limits());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn exceeds_byte_limit_rejected() {
        let limits = StreamNameLimits {
            max_bytes: 10,
            max_segments: 8,
        };
        let result = validate_name("this-is-way-too-long", &limits);
        let err = result.unwrap_err();
        assert!(err.contains("20 bytes"), "should include actual length: {err}");
        assert!(
            err.contains("maximum of 10 bytes"),
            "should include limit: {err}"
        );
    }

    #[test]
    fn exceeds_segment_limit_rejected() {
        let limits = StreamNameLimits {
            max_bytes: 1024,
            max_segments: 3,
        };
        let result = validate_name("a/b/c/d", &limits);
        let err = result.unwrap_err();
        assert!(
            err.contains("4 path segments"),
            "should include actual count: {err}"
        );
        assert!(
            err.contains("maximum of 3"),
            "should include limit: {err}"
        );
    }

    #[test]
    fn exactly_at_segment_limit_passes() {
        let limits = StreamNameLimits {
            max_bytes: 1024,
            max_segments: 3,
        };
        let result = validate_name("a/b/c", &limits);
        assert_eq!(result.unwrap(), "a/b/c");
    }

    #[test]
    fn exactly_at_byte_limit_passes() {
        let limits = StreamNameLimits {
            max_bytes: 5,
            max_segments: 8,
        };
        let result = validate_name("abcde", &limits);
        assert_eq!(result.unwrap(), "abcde");
    }
}
