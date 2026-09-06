//! HTTP route groups and explicit subscription-worker lifecycle.
//!
//! Construct [`Server`] outside or inside Tokio, then call [`Server::start`]
//! inside the serving runtime. Clone [`RunningServer`] or its routers to expose
//! multiple listeners with the same initialized state.

use crate::config::Config;
use crate::execution::{AsyncStreams, Execution};
use crate::middleware::proxy_trust::ProxyTrustState;
use crate::protocol::stream_name::StreamNameLimits;
use crate::{handlers, middleware, storage::Storage};
use axum::http::HeaderValue;
use axum::{Extension, Router, middleware as axum_middleware, routing::get};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowOrigin, CorsLayer};

#[cfg(test)]
mod tests;

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

#[derive(Clone, Copy)]
pub(crate) struct RequestBodyLimit(pub(crate) usize);

/// Default mount path for the Durable Streams protocol routes.
pub const DEFAULT_STREAM_BASE_PATH: &str = "/v1/stream";

/// Wrapper around the configured stream route mount path.
#[derive(Clone)]
pub(crate) struct StreamBasePath(pub Arc<str>);

/// Optional readiness and cancellation hooks for [`Server`].
///
/// By default `/readyz` is omitted. [`RunningServer::shutdown`] joins the worker
/// and drains admitted storage jobs. Supply an external token with [`Self::with_shutdown`] to integrate
/// cancellation with caller-owned HTTP listeners.
#[derive(Debug, Clone, Default)]
pub struct RouterOptions {
    ready: Option<Arc<AtomicBool>>,
    shutdown: CancellationToken,
}

impl RouterOptions {
    /// Register `/readyz`, returning 200 when the shared flag is true and 503
    /// while it is false. The caller owns changes to the readiness flag.
    #[must_use]
    pub fn with_readiness(mut self, ready: Arc<AtomicBool>) -> Self {
        self.ready = Some(ready);
        self
    }

    /// Stop storage admission and cancel long-poll, SSE, and subscription workers.
    /// The caller must also stop its HTTP listener during shutdown.
    #[must_use]
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }
}

/// Construction or startup failure for an embedded server.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// An HTTP route or middleware setting is invalid.
    #[error("invalid HTTP configuration: {0}")]
    Configuration(#[from] crate::config::ConfigValidationError),
    /// An independent server already owns this storage instance.
    #[error("storage already has a server owner; clone the existing server or its routers")]
    StorageAlreadyOwned,
    /// A backend operation failed while loading or persisting control state.
    #[error("server storage initialization failed: {0}")]
    Storage(#[from] crate::protocol::error::Error),
    /// Persisted subscription state or signing-key initialization failed.
    #[error("subscription initialization failed: {0}")]
    Initialization(String),
    /// Worker startup requires an entered Tokio runtime.
    #[error("start the server inside the Tokio runtime that will serve it")]
    RuntimeRequired,
    /// The supplied cancellation source was already cancelled before startup.
    #[error("server shutdown was requested before startup")]
    ShutdownRequested,
}

pub(crate) struct StorageLease(Arc<dyn Storage>);
static OWNERS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<usize>>> =
    std::sync::OnceLock::new();
impl StorageLease {
    pub(crate) fn acquire(storage: &Arc<dyn Storage>) -> Result<Self, ServerError> {
        let identity = Arc::as_ptr(storage).cast::<()>() as usize;
        let mut owners = OWNERS
            .get_or_init(Default::default)
            .lock()
            .expect("storage owner registry lock poisoned");
        if !owners.insert(identity) {
            return Err(ServerError::StorageAlreadyOwned);
        }
        Ok(Self(storage.clone()))
    }
}
impl Drop for StorageLease {
    fn drop(&mut self) {
        if let Some(owners) = OWNERS.get() {
            owners
                .lock()
                .expect("storage owner registry lock poisoned")
                .remove(&(Arc::as_ptr(&self.0).cast::<()>() as usize));
        }
    }
}

struct ServerState {
    service: Arc<AsyncStreams>,
    subscriptions: Arc<crate::subscriptions::Service<dyn Storage>>,
    config: Config,
    options: RouterOptions,
    worker: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    lease: Arc<StorageLease>,
}
impl Drop for ServerState {
    fn drop(&mut self) {
        self.service.execution.close();
        self.options.shutdown.cancel();
    }
}

/// Validated HTTP configuration and initialized control state, without running tasks.
///
/// Construction performs synchronous storage initialization. It can run before
/// Tokio starts. One server may own a given storage `Arc`; use clones of the
/// running server for multiple listeners. Distinct wrappers around the same
/// underlying database must still respect that backend's exclusive ownership.
pub struct Server {
    state: ServerState,
}
impl Server {
    /// Validate HTTP settings and load control state before making routes available.
    ///
    /// # Errors
    /// Rejects invalid route settings, competing ownership, and initialization failures.
    pub fn new(
        service: crate::streams::StreamService,
        config: &Config,
        options: RouterOptions,
    ) -> Result<Self, ServerError> {
        config.validate_router()?;
        let options = RouterOptions {
            shutdown: options.shutdown.child_token(),
            ..options
        };
        let lease = Arc::new(StorageLease::acquire(&service.storage)?);
        let execution = Execution::new(
            config.limits.max_storage_jobs,
            options.shutdown.clone(),
            lease.clone(),
        );
        let subscriptions =
            crate::subscriptions::initialize(service.storage.clone(), config, execution.clone())?;
        Ok(Self {
            state: ServerState {
                service: Arc::new(AsyncStreams {
                    service: Arc::new(service),
                    execution,
                }),
                subscriptions,
                config: config.clone(),
                options,
                worker: tokio::sync::Mutex::new(None),
                lease,
            },
        })
    }

    /// Bind storage execution and start the subscription worker in the entered Tokio runtime.
    ///
    /// # Errors
    /// Returns [`ServerError::RuntimeRequired`] outside Tokio.
    pub fn start(mut self) -> Result<RunningServer, ServerError> {
        if self.state.options.shutdown.is_cancelled() {
            return Err(ServerError::ShutdownRequested);
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| ServerError::RuntimeRequired)?;
        self.state.service.execution.start(runtime.clone());
        let weak = Arc::downgrade(&self.state.subscriptions);
        let shutdown = self.state.options.shutdown.clone();
        let lease = self.state.lease.clone();
        let worker = runtime.spawn(async move {
            let _lease = lease;
            crate::subscriptions::run(weak, shutdown).await;
        });
        *self.state.worker.get_mut() = Some(worker);
        Ok(RunningServer {
            state: Arc::new(self.state),
        })
    }
}

/// Cloneable server owner with composable HTTP surfaces and explicit shutdown.
///
/// Routers retain this owner's state. Dropping the final handle/router closes
/// storage admission and cancels the worker. Use [`Self::shutdown`] to await
/// worker completion and every admitted storage job. Stop HTTP
/// listeners separately and drain them using the same cancellation source.
#[derive(Clone)]
pub struct RunningServer {
    state: Arc<ServerState>,
}
impl RunningServer {
    /// Access synchronous stream operations without backend-specific types.
    /// These direct calls use the caller's thread and are outside server admission/drain accounting.
    #[must_use]
    pub fn streams(&self) -> &crate::streams::StreamService {
        &self.state.service.service
    }

    /// Combined protocol, subscriptions, optional admin, and probe routes.
    pub fn router(&self) -> Router {
        self.finish(
            self.protocol_group()
                .merge(self.admin_group())
                .merge(self.probe_group()),
        )
    }

    /// Protocol and reserved subscription routes at the configured stream mount.
    pub fn protocol_router(&self) -> Router {
        self.finish(self.protocol_group())
    }

    /// Admin routes at their configured mount, or an empty router when disabled.
    /// Apply admin authentication middleware to this router before merging groups.
    pub fn admin_router(&self) -> Router {
        self.finish(self.admin_group())
    }

    /// Health and optional readiness probes, without protocol or admin routes.
    pub fn probe_router(&self) -> Router {
        self.finish(self.probe_group())
    }

    /// Close storage admission, cancel live reads/workers, and drain admitted storage jobs.
    ///
    /// Concurrent callers all wait for completion. Cancelling this future leaves the
    /// drain available to a subsequent caller. Jobs retain their capacity and storage
    /// ownership after a request disconnects. Keep the serving Tokio runtime alive
    /// until draining completes; a deadline cannot stop synchronous filesystem calls.
    /// HTTP listeners are caller-owned. Completion does not imply every operation succeeded.
    /// # Errors
    /// Returns the worker's Tokio join error after storage drain if it panicked or was aborted.
    pub async fn shutdown(&self) -> Result<(), tokio::task::JoinError> {
        self.state.service.execution.close();
        self.state.options.shutdown.cancel();
        let mut worker = self.state.worker.lock().await;
        let result = if let Some(task) = worker.as_mut() {
            let result = task.await;
            if let Err(error) = &result {
                tracing::error!(%error, "subscription worker stopped unexpectedly; draining storage");
            }
            *worker = None;
            result
        } else {
            Ok(())
        };
        self.state.service.execution.drain().await;
        result
    }

    fn protocol_group(&self) -> Router {
        let config = &self.state.config;
        let base_path = Arc::<str>::from(config.http.stream_base_path.as_str());
        let routes = protocol_routes(
            self.state.service.clone(),
            config,
            self.state.options.shutdown.clone(),
            base_path.clone(),
            self.state.subscriptions.clone(),
        );
        if base_path.as_ref() == "/" {
            routes
        } else {
            Router::new().nest(base_path.as_ref(), routes)
        }
    }
    fn admin_group(&self) -> Router {
        if self.state.config.admin.enabled {
            Router::new().nest(
                &self.state.config.admin.base_path,
                admin_routes(self.state.service.clone()),
            )
        } else {
            Router::new()
        }
    }
    fn probe_group(&self) -> Router {
        let mut app = Router::new().route("/healthz", get(handlers::health::health_check));
        if let Some(flag) = &self.state.options.ready {
            app = app
                .route("/readyz", get(handlers::health::readiness_check))
                .layer(Extension(flag.clone()));
        }
        app
    }
    fn finish(&self, router: Router) -> Router {
        let proxy = Arc::new(ProxyTrustState::from_config(&self.state.config));
        router
            .layer(Extension(self.state.clone()))
            .layer(axum_middleware::from_fn(
                middleware::telemetry::track_requests,
            ))
            .layer(cors_layer(&self.state.config.http.cors_origins))
            .layer(axum_middleware::from_fn(move |request, next| {
                middleware::proxy_trust::enforce_proxy_trust(proxy.clone(), request, next)
            }))
    }
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
fn protocol_routes(
    storage: Arc<AsyncStreams>,
    config: &Config,
    shutdown: CancellationToken,
    stream_base_path: Arc<str>,
    subscriptions: Arc<crate::subscriptions::Service<dyn Storage>>,
) -> Router {
    Router::new()
        .route(
            "/{*name}",
            get(handlers::get::read_stream)
                .put(handlers::put::create_stream)
                .head(handlers::head::stream_metadata)
                .post(handlers::post::append_data)
                .delete(handlers::delete::delete_stream),
        )
        .layer(Extension(RequestBodyLimit(
            config.limits.max_request_body_bytes,
        )))
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
        .with_state(storage)
        .merge(crate::subscriptions::routes(subscriptions))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
}

/// Admin routes mounted under `admin.base_path`.
///
/// These routes are operator-focused and opt-in. Keep them in a separate
/// subrouter so different Tower middleware can be layered around admin traffic
/// without changing protocol behaviour.
fn admin_routes(storage: Arc<AsyncStreams>) -> Router {
    Router::new()
        .route("/streams", get(handlers::list::list_streams))
        .layer(axum_middleware::from_fn(
            middleware::security::add_security_headers,
        ))
        .with_state(storage)
}
