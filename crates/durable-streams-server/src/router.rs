//! Axum router construction for the Durable Streams HTTP surfaces.
//!
//! [`build_router`] is the main embedding entry point for library consumers.
//! Protocol routes are always mounted at `Config::http.stream_base_path`;
//! optional admin/operator routes are mounted separately at
//! `Config::admin.base_path` only when `admin.enabled = true`.

use crate::config::Config;
use crate::middleware::proxy_trust::ProxyTrustState;
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

/// Combined read-stream configuration extracted as a single axum `Extension`.
///
/// Groups the long-poll timeout, SSE reconnect interval, and shutdown token
/// so handlers that need all three only consume one extractor slot.
#[derive(Clone)]
pub(crate) struct ReadStreamConfig {
    pub(crate) long_poll_timeout: std::time::Duration,
    pub(crate) sse_reconnect_interval_secs: u64,
    pub(crate) shutdown: CancellationToken,
}

/// Default mount path for the Durable Streams protocol routes.
pub const DEFAULT_STREAM_BASE_PATH: &str = "/v1/stream";

/// Wrapper around the configured stream route mount path.
#[derive(Clone)]
pub(crate) struct StreamBasePath(pub Arc<str>);

/// Build the application router with storage state.
///
/// Routes:
/// - `GET /healthz`         – Liveness probe (always 200)
/// - `GET/PUT/... <path>`   – Protocol routes mounted at the configured
///   `config.http.stream_base_path` (default [`DEFAULT_STREAM_BASE_PATH`])
/// - `GET <admin>/streams`  – Optional operator list route, mounted only when
///   `config.admin.enabled` is true
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
///
/// The admin router is composed separately from the protocol router so future
/// operators or embedders can apply different Tower layers to admin traffic
/// (filtering, rate limiting, load shedding, audit logging, IP restrictions)
/// without contaminating the Durable Streams protocol surface. The server does
/// not implement authentication or authorization for admin routes; enable them
/// only behind a trusted network, reverse proxy, or external access-control
/// layer.
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
            protocol_routes(
                Arc::clone(&storage),
                config,
                shutdown,
                Arc::clone(&stream_base_path),
            ),
        );

    if config.admin.enabled {
        app = app.nest(config.admin.base_path.as_str(), admin_routes(storage));
    }

    if let Some(flag) = ready {
        app = app
            .route("/readyz", get(handlers::health::readiness_check))
            .layer(Extension(flag));
    }

    let proxy_trust_state = Arc::new(ProxyTrustState::from_config(config));

    app.layer(axum_middleware::from_fn(
        middleware::telemetry::track_requests,
    ))
    .layer(cors_layer(&config.http.cors_origins))
    .layer(axum_middleware::from_fn(move |request, next| {
        middleware::proxy_trust::enforce_proxy_trust(proxy_trust_state.clone(), request, next)
    }))
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

/// Protocol routes under /v1/stream.
///
/// This router owns only the Durable Streams protocol surface. Operator/admin
/// routes are kept out of this tree so protocol handlers do not need to know
/// about admin enablement or policy.
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
        .layer(Extension(ReadStreamConfig {
            long_poll_timeout: config.long_poll_timeout(),
            sse_reconnect_interval_secs: config.transport.connection.sse_reconnect_interval_secs,
            shutdown,
        }))
        .layer(Extension(StreamBasePath(stream_base_path)))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
        .with_state(storage)
}

/// Admin routes mounted under `admin.base_path`.
///
/// These routes are operator-focused and opt-in. Keep them in a separate
/// subrouter so different Tower middleware can be layered around admin traffic
/// without changing protocol behaviour.
fn admin_routes<S: Storage + 'static>(storage: Arc<S>) -> Router {
    Router::new()
        .route("/streams", get(handlers::list::list_streams::<S>))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
        .with_state(storage)
}
