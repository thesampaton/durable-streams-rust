//! Stream name extraction and validation.
//!
//! [`StreamName`] is an axum extractor that captures catch-all path parameters,
//! strips the leading slash, and validates length, segment depth, and segment
//! content against configurable limits injected via [`StreamNameLimits`].

use crate::protocol::error::Error;
use crate::protocol::problem::ProblemResponse;
use axum::{
    Extension,
    extract::{OriginalUri, Path, rejection::PathRejection},
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

/// Structural predicates shared by literal names and subscription patterns.
/// Callers retain their own limits, wildcard policy, and error envelopes.
pub(crate) fn is_reserved(name: &str) -> bool {
    name.split('/').next() == Some("__ds")
}

pub(crate) fn invalid_segment(name: &str) -> Option<&str> {
    name.split('/')
        .find(|segment| segment.is_empty() || *segment == "." || *segment == "..")
}

/// Validate a stream name against the configured limits.
///
/// Checks (in order):
/// 1. Name is non-empty
/// 2. No trailing slash
/// 3. No empty segments (consecutive slashes)
/// 4. No `.` or `..` segments (path traversal)
/// 5. Byte length within limit
/// 6. Segment count within limit
fn validate(name: &str, limits: &StreamNameLimits) -> Result<(), String> {
    if is_reserved(name) {
        return Err("__ds is reserved for control APIs".into());
    }
    if name.is_empty() {
        return Err("stream name cannot be empty".to_string());
    }

    if name.ends_with('/') {
        return Err("stream name must not end with '/'".to_string());
    }

    if let Some(segment) = invalid_segment(name) {
        if segment.is_empty() {
            return Err(
                "stream name contains empty segments (consecutive '/' characters)".to_string(),
            );
        }
        if segment == "." || segment == ".." {
            return Err(format!("stream name contains invalid segment '{segment}'"));
        }
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

    Ok(())
}

impl<S> axum::extract::FromRequestParts<S> for StreamName
where
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // Use OriginalUri to get the full request path; `parts.uri` is
        // stripped by axum's `.nest()` and would omit the base path prefix.
        let instance = OriginalUri::from_request_parts(parts, state)
            .await
            .ok()
            .and_then(|OriginalUri(uri)| uri.path_and_query().map(|pq| pq.as_str().to_string()));

        let raw_name = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|e| path_rejection_to_response(&e, instance.as_deref()))?
            .0;

        let name = raw_name.strip_prefix('/').unwrap_or(&raw_name);

        let Extension(limits) = Extension::<StreamNameLimits>::from_request_parts(parts, state)
            .await
            .map_err(|_| {
                problem_response(
                    "server misconfiguration: stream name limits not set",
                    instance.as_deref(),
                )
            })?;

        validate(name, &limits).map_err(|reason| problem_response(&reason, instance.as_deref()))?;

        Ok(Self(name.to_string()))
    }
}

fn problem_response(reason: &str, instance: Option<&str>) -> Response {
    let mut response = ProblemResponse::from(Error::InvalidStreamName(reason.to_string()));
    if let Some(inst) = instance {
        response = response.with_instance(inst);
    }
    response.into_response()
}

fn path_rejection_to_response(rejection: &PathRejection, instance: Option<&str>) -> Response {
    let mut response = ProblemResponse::from(Error::InvalidStreamName(rejection.to_string()));
    if let Some(inst) = instance {
        response = response.with_instance(inst);
    }
    response.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_limits() -> StreamNameLimits {
        StreamNameLimits {
            max_bytes: 1024,
            max_segments: 8,
        }
    }

    /// Helper: strip leading slash then validate, mirroring the extractor flow.
    fn strip_and_validate(raw: &str, limits: &StreamNameLimits) -> Result<String, String> {
        let name = raw.strip_prefix('/').unwrap_or(raw);
        validate(name, limits)?;
        Ok(name.to_string())
    }

    #[test]
    fn flat_name_passes() {
        let result = strip_and_validate("my-stream", &default_limits());
        assert_eq!(result.unwrap(), "my-stream");
    }

    #[test]
    fn nested_name_passes() {
        let result = strip_and_validate("a/b/c", &default_limits());
        assert_eq!(result.unwrap(), "a/b/c");
    }

    #[test]
    fn leading_slash_stripped() {
        let result = strip_and_validate("/my-stream", &default_limits());
        assert_eq!(result.unwrap(), "my-stream");
    }

    #[test]
    fn leading_slash_stripped_nested() {
        let result = strip_and_validate("/slides/abc123", &default_limits());
        assert_eq!(result.unwrap(), "slides/abc123");
    }

    #[test]
    fn empty_name_rejected() {
        let result = strip_and_validate("", &default_limits());
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn slash_only_rejected() {
        let result = strip_and_validate("/", &default_limits());
        assert!(result.unwrap_err().contains("cannot be empty"));
    }

    #[test]
    fn exceeds_byte_limit_rejected() {
        let limits = StreamNameLimits {
            max_bytes: 10,
            max_segments: 8,
        };
        let err = strip_and_validate("this-is-way-too-long", &limits).unwrap_err();
        assert!(
            err.contains("20 bytes"),
            "should include actual length: {err}"
        );
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
        let err = strip_and_validate("a/b/c/d", &limits).unwrap_err();
        assert!(
            err.contains("4 path segments"),
            "should include actual count: {err}"
        );
        assert!(err.contains("maximum of 3"), "should include limit: {err}");
    }

    #[test]
    fn exactly_at_segment_limit_passes() {
        let limits = StreamNameLimits {
            max_bytes: 1024,
            max_segments: 3,
        };
        assert_eq!(strip_and_validate("a/b/c", &limits).unwrap(), "a/b/c");
    }

    #[test]
    fn exactly_at_byte_limit_passes() {
        let limits = StreamNameLimits {
            max_bytes: 5,
            max_segments: 8,
        };
        assert_eq!(strip_and_validate("abcde", &limits).unwrap(), "abcde");
    }

    // --- segment validation ---

    #[test]
    fn trailing_slash_rejected() {
        let err = strip_and_validate("slides/abc123/", &default_limits()).unwrap_err();
        assert!(err.contains("must not end with '/'"), "got: {err}");
    }

    #[test]
    fn empty_segment_rejected() {
        let err = strip_and_validate("a//b", &default_limits()).unwrap_err();
        assert!(err.contains("empty segments"), "got: {err}");
    }

    #[test]
    fn dot_segment_rejected() {
        let err = strip_and_validate("a/./b", &default_limits()).unwrap_err();
        assert!(err.contains("invalid segment '.'"), "got: {err}");
    }

    #[test]
    fn dot_dot_segment_rejected() {
        let err = strip_and_validate("a/../b", &default_limits()).unwrap_err();
        assert!(err.contains("invalid segment '..'"), "got: {err}");
    }

    #[test]
    fn leading_dot_dot_rejected() {
        let err = strip_and_validate("../etc/passwd", &default_limits()).unwrap_err();
        assert!(err.contains("invalid segment '..'"), "got: {err}");
    }

    #[test]
    fn dot_in_segment_name_allowed() {
        // "v1.2" contains a dot but is not "." or ".."
        let result = strip_and_validate("releases/v1.2", &default_limits());
        assert_eq!(result.unwrap(), "releases/v1.2");
    }

    #[test]
    fn dotdot_prefix_in_segment_allowed() {
        // "..foo" starts with dots but is not ".."
        let result = strip_and_validate("a/..foo/b", &default_limits());
        assert_eq!(result.unwrap(), "a/..foo/b");
    }
}
