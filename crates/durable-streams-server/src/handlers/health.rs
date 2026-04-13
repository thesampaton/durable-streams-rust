use crate::protocol::problem::{ProblemDetails, ProblemResponse};
use axum::Extension;
use axum::extract::OriginalUri;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Health check endpoint
///
/// Returns 200 OK with body "ok" to indicate the server is running.
/// This endpoint is outside the protocol namespace (/v1/stream) as it's
/// an infrastructure concern, not part of the durable streams protocol.
pub async fn health_check() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}

/// Readiness check endpoint
///
/// Returns 200 OK when the server is ready to accept traffic (storage
/// initialization is complete). Returns 503 Service Unavailable otherwise.
/// Useful for Kubernetes readiness probes and load balancer health checks.
pub async fn readiness_check(
    Extension(ready): Extension<Arc<AtomicBool>>,
    original_uri: OriginalUri,
) -> impl IntoResponse {
    if ready.load(Ordering::Acquire) {
        (StatusCode::OK, "ready").into_response()
    } else {
        ProblemResponse::new(
            ProblemDetails::new(
                "/errors/unavailable",
                "Service Unavailable",
                StatusCode::SERVICE_UNAVAILABLE,
                "UNAVAILABLE",
            )
            .with_detail("The server is not ready to accept traffic.")
            .with_instance(crate::protocol::problem::request_instance(&original_uri)),
        )
        .into_response()
    }
}
