//! Axum router construction for the Durable Streams HTTP surface.
//!
//! [`build_router`] is the main embedding entry point for library consumers.

use crate::config::{Config, LongPollTimeout, SseReconnectInterval};
use crate::protocol::stream_name::StreamNameLimits;
use crate::{handlers, middleware, storage::Storage};
use axum::http::HeaderValue;
use axum::{Extension, Router, middleware as axum_middleware, routing::get};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Wrapper around [`CancellationToken`] for axum `Extension` extraction.
///
/// Long-poll and SSE handlers observe this token so they can drain cleanly
/// when the server begins a graceful shutdown.
#[derive(Clone)]
pub struct ShutdownToken(pub CancellationToken);

/// Default mount path for the Durable Streams protocol routes.
pub const DEFAULT_STREAM_BASE_PATH: &str = "/v1/stream";

/// Wrapper around the configured stream route mount path.
#[derive(Clone)]
pub(crate) struct StreamBasePath(pub Arc<str>);

/// Build the application router with storage state.
///
/// Routes:
/// - `GET /healthz`        – Liveness probe (always 200)
/// - `GET/PUT/... <path>`  – Protocol routes mounted at the configured
///   `config.stream_base_path` (default [`DEFAULT_STREAM_BASE_PATH`])
///
/// Uses a no-op cancellation token (never cancelled). For production
/// use with graceful shutdown, prefer [`build_router_with_ready`].
pub fn build_router<S: Storage + 'static>(storage: Arc<S>, config: &Config) -> Router {
    build_router_with_ready(storage, config, None, CancellationToken::new())
}

/// Build the router with readiness flag and shutdown token.
///
/// When `ready` is `Some`, the `/readyz` endpoint is registered and returns
/// 200 only after the flag is set to `true`. When `None`, the endpoint is
/// not registered (backwards-compatible).
///
/// The `shutdown` token is propagated to long-poll and SSE handlers so they
/// can observe server shutdown and drain in-flight connections cleanly.
pub fn build_router_with_ready<S: Storage + 'static>(
    storage: Arc<S>,
    config: &Config,
    ready: Option<Arc<AtomicBool>>,
    shutdown: CancellationToken,
) -> Router {
    let stream_base_path = Arc::<str>::from(config.http.stream_base_path.as_str());
    let mut app = Router::new()
        .route("/healthz", get(handlers::health::health_check))
        .nest(
            stream_base_path.as_ref(),
            protocol_routes(storage, config, shutdown, Arc::clone(&stream_base_path)),
        );

    if let Some(flag) = ready {
        app = app
            .route("/readyz", get(handlers::health::readiness_check))
            .layer(Extension(flag));
    }

    app.layer(axum_middleware::from_fn(
        middleware::telemetry::track_requests,
    ))
    .layer(cors_layer(&config.http.cors_origins))
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
fn protocol_routes<S: Storage + 'static>(
    storage: Arc<S>,
    config: &Config,
    shutdown: CancellationToken,
    stream_base_path: Arc<str>,
) -> Router {
    Router::new()
        .route(
            "/{*name}",
            get(handlers::get::read_stream::<S>)
                .put(handlers::put::create_stream::<S>)
                .head(handlers::head::stream_metadata::<S>)
                .post(handlers::post::append_data::<S>)
                .delete(handlers::delete::delete_stream::<S>),
        )
        .layer(Extension(StreamNameLimits {
            max_bytes: config.limits.max_stream_name_bytes,
            max_segments: config.limits.max_stream_name_segments,
        }))
        .layer(Extension(ShutdownToken(shutdown)))
        .layer(Extension(SseReconnectInterval(
            config.transport.connection.sse_reconnect_interval_secs,
        )))
        .layer(Extension(LongPollTimeout(config.long_poll_timeout())))
        .layer(Extension(StreamBasePath(stream_base_path)))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
        .with_state(storage)
}
