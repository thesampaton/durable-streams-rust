use axum::Extension;
use axum::http::StatusCode;
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
) -> (StatusCode, &'static str) {
    if ready.load(Ordering::Acquire) {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}
