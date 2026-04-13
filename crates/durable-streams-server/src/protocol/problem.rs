use axum::{
    body::Body,
    extract::OriginalUri,
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CONTENT_TYPE, IntoHeaderName},
    },
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

/// RFC 9457 problem details payload used for server error responses.
///
/// The field names intentionally match the wire format so the same type can be
/// reused by future client-side parsing without another translation layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProblemDetails {
    /// URI identifying the problem type.
    #[serde(rename = "type")]
    pub problem_type: String,
    /// Short, human-readable summary of the problem type.
    pub title: String,
    /// HTTP status code. Must match the response status line.
    pub status: u16,
    /// Machine-readable error code.
    pub code: String,
    /// Per-instance explanation of the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Request path or URI for the failing resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
}

impl ProblemDetails {
    /// Create a new problem details payload with the required RFC 9457 fields.
    #[must_use]
    pub fn new(
        problem_type: &'static str,
        title: &'static str,
        status: StatusCode,
        code: &'static str,
    ) -> Self {
        Self {
            problem_type: problem_type.to_string(),
            title: title.to_string(),
            status: status.as_u16(),
            code: code.to_string(),
            detail: None,
            instance: None,
        }
    }

    /// Attach a problem-specific detail string.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Attach the request instance path/URI.
    #[must_use]
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }
}

/// Telemetry metadata copied from the final problem response.
#[derive(Debug, Clone)]
pub struct ProblemTelemetry {
    pub problem_type: String,
    pub code: String,
    pub title: String,
    pub detail: Option<String>,
    pub error_class: Option<String>,
    pub storage_backend: Option<String>,
    pub storage_operation: Option<String>,
    pub internal_detail: Option<String>,
    pub retry_after_secs: Option<u32>,
}

impl From<&ProblemDetails> for ProblemTelemetry {
    fn from(problem: &ProblemDetails) -> Self {
        Self {
            problem_type: problem.problem_type.clone(),
            code: problem.code.clone(),
            title: problem.title.clone(),
            detail: problem.detail.clone(),
            error_class: None,
            storage_backend: None,
            storage_operation: None,
            internal_detail: None,
            retry_after_secs: None,
        }
    }
}

/// Builder for structured error responses with protocol-specific headers.
#[derive(Debug, Clone)]
pub struct ProblemResponse {
    problem: ProblemDetails,
    headers: HeaderMap,
    telemetry: Option<ProblemTelemetry>,
}

/// Response result alias for handlers that emit structured problem details.
pub type Result<T> = std::result::Result<T, ProblemResponse>;

impl ProblemResponse {
    /// Create a problem response from an RFC 9457 payload.
    #[must_use]
    pub fn new(problem: ProblemDetails) -> Self {
        Self {
            problem,
            headers: HeaderMap::new(),
            telemetry: None,
        }
    }

    /// Attach a problem detail.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.problem.detail = Some(detail.into());
        self
    }

    /// Attach the request instance path/URI.
    #[must_use]
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.problem.instance = Some(instance.into());
        self
    }

    /// Attach an additional response header.
    #[must_use]
    pub fn with_header<K>(mut self, key: K, value: HeaderValue) -> Self
    where
        K: IntoHeaderName,
    {
        self.headers.insert(key, value);
        self
    }

    /// Attach explicit telemetry metadata that differs from the public problem payload.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: ProblemTelemetry) -> Self {
        self.telemetry = Some(telemetry);
        self
    }
}

/// Convert the actual request target into a relative `instance` reference.
///
/// This intentionally excludes scheme/authority information so error payloads
/// do not expose proxy or load-balancer host configuration to clients.
#[must_use]
pub fn request_instance(OriginalUri(uri): &OriginalUri) -> String {
    uri.path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| uri.path().to_string())
}

impl IntoResponse for ProblemResponse {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.problem.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let telemetry = self
            .telemetry
            .unwrap_or_else(|| ProblemTelemetry::from(&self.problem));
        let body = match serde_json::to_vec(&self.problem) {
            Ok(body) => body,
            Err(_) => br#"{"type":"/errors/internal","title":"Internal Server Error","status":500,"code":"INTERNAL_ERROR"}"#.to_vec(),
        };

        let mut response = Response::new(Body::from(body));
        *response.status_mut() = status;

        let headers = response.headers_mut();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        headers.extend(self.headers);

        response.extensions_mut().insert(telemetry);
        response
    }
}
