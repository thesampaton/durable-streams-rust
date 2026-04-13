//! Axum router construction for the Durable Streams HTTP surface.
//!
//! [`build_router`] is the main embedding entry point for library consumers.

use crate::config::{Config, LongPollTimeout, SseReconnectInterval};
use crate::{handlers, middleware, storage::Storage};
use axum::http::HeaderValue;
use axum::{Extension, Router, middleware as axum_middleware, routing::get};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Build the application router with storage state.
///
/// Routes:
/// - `GET /healthz`  – Liveness probe (always 200)
/// - `GET /readyz`   – Readiness probe (200 when `ready` is true, 503 otherwise)
/// - `/v1/stream/*`  – Protocol routes
///
/// The `ready` flag is typically set to `true` after storage initialization
/// completes. Pass `None` to omit the readiness endpoint entirely (the
/// health endpoint is always present).
pub fn build_router<S: Storage + 'static>(storage: Arc<S>, config: &Config) -> Router {
    build_router_with_ready(storage, config, None)
}

/// Build the router with an explicit readiness flag.
///
/// When `ready` is `Some`, the `/readyz` endpoint is registered and returns
/// 200 only after the flag is set to `true`. When `None`, the endpoint is
/// not registered (backwards-compatible).
pub fn build_router_with_ready<S: Storage + 'static>(
    storage: Arc<S>,
    config: &Config,
    ready: Option<Arc<AtomicBool>>,
) -> Router {
    let mut app = Router::new()
        .route("/healthz", get(handlers::health::health_check))
        .nest("/v1/stream", protocol_routes(storage, config));

    if let Some(flag) = ready {
        app = app
            .route("/readyz", get(handlers::health::readiness_check))
            .layer(Extension(flag));
    }

    app.layer(cors_layer(&config.cors_origins))
}

/// Build a CORS layer from the configured origins string.
///
/// Accepts `"*"` for permissive (any origin) or a comma-separated list of
/// allowed origins (e.g. `"http://localhost:3000,https://app.example.com"`).
fn cors_layer(origins: &str) -> CorsLayer {
    let allow_origin = if origins == "*" {
        AllowOrigin::any()
    } else {
        let values: Vec<HeaderValue> = origins
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        AllowOrigin::list(values)
    };

    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
        .expose_headers(tower_http::cors::Any)
}

/// Protocol routes under /v1/stream
///
/// All protocol routes have security headers applied via middleware.
fn protocol_routes<S: Storage + 'static>(storage: Arc<S>, config: &Config) -> Router {
    Router::new()
        .route(
            "/{name}",
            get(handlers::get::read_stream::<S>)
                .put(handlers::put::create_stream::<S>)
                .head(handlers::head::stream_metadata::<S>)
                .post(handlers::post::append_data::<S>)
                .delete(handlers::delete::delete_stream::<S>),
        )
        .layer(Extension(SseReconnectInterval(
            config.sse_reconnect_interval_secs,
        )))
        .layer(Extension(LongPollTimeout(config.long_poll_timeout)))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
        .with_state(storage)
}
