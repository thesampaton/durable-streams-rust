use durable_streams_server::config::{AcidBackend, Config, StorageMode};
use durable_streams_server::protocol::problem::ProblemDetails;
use durable_streams_server::storage::{Storage, acid::AcidStorage, memory::InMemoryStorage};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

static STREAM_COUNTER: AtomicU16 = AtomicU16::new(0);
static HTTP_STORAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique stream name for testing
///
/// Uses a global atomic counter to ensure unique names across all tests.
pub fn unique_stream_name() -> String {
    let id = STREAM_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test-stream-{id}")
}

#[derive(Debug, Clone, Copy)]
pub enum HttpTestBackend {
    Memory,
    Acid,
}

/// Spawn a test server on a random available port
///
/// Returns the bound address and port number.
pub async fn spawn_test_server() -> (String, u16) {
    spawn_test_server_with_limits(1024 * 1024 * 100, 1024 * 1024 * 10).await
}

/// Spawn a test server with custom memory limits.
pub async fn spawn_test_server_with_limits(
    max_total_bytes: u64,
    max_stream_bytes: u64,
) -> (String, u16) {
    let mut config = Config::default();
    config.limits.max_memory_bytes = max_total_bytes;
    config.limits.max_stream_bytes = max_stream_bytes;
    spawn_test_server_with_config(config).await
}

/// Spawn a test server with a custom long-poll timeout.
pub async fn spawn_test_server_with_timeout(timeout: Duration) -> (String, u16) {
    let mut config = Config::default();
    config.transport.connection.long_poll_timeout_secs = timeout.as_secs();
    spawn_test_server_with_config(config).await
}

/// Spawn a test server for a specific backend under test.
pub async fn spawn_test_server_for_backend(backend: HttpTestBackend) -> (String, u16) {
    match backend {
        HttpTestBackend::Memory => spawn_test_server_with_config(Config::default()).await,
        HttpTestBackend::Acid => spawn_test_server_acid().await,
    }
}

/// Spawn a test server with a full Config.
pub async fn spawn_test_server_with_config(config: Config) -> (String, u16) {
    let storage = Arc::new(InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));
    spawn_test_server_with_storage(storage, config).await
}

/// Spawn a test server in acid mode (sharded redb).
pub async fn spawn_test_server_acid() -> (String, u16) {
    let storage_dir = unique_storage_dir("acid-http");
    let mut config = Config::default();
    config.storage.mode = StorageMode::Acid;
    config.storage.data_dir = storage_dir.to_string_lossy().into_owned();
    config.storage.acid_shard_count = 16;
    let storage = Arc::new(
        AcidStorage::new(
            &config.storage.data_dir,
            config.storage.acid_shard_count,
            config.limits.max_memory_bytes,
            config.limits.max_stream_bytes,
            AcidBackend::File,
        )
        .expect("Failed to initialize acid test storage"),
    );
    spawn_test_server_with_storage(storage, config).await
}

pub async fn spawn_test_server_with_storage<S>(storage: Arc<S>, config: Config) -> (String, u16)
where
    S: Storage + 'static,
{
    // Bind to port 0 to get a random available port
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test server");

    let addr = listener.local_addr().expect("Failed to get local addr");
    let port = addr.port();

    // Build and spawn server — ConnectInfo<SocketAddr> is required by proxy
    // trust middleware to identify the peer IP.
    let app = durable_streams_server::router::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default(),
    );

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .expect("Test server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (format!("http://127.0.0.1:{port}"), port)
}

/// Spawn a test server with a readiness flag.
///
/// Returns `(base_url, port, ready_flag)`. The ready flag starts `false`;
/// callers can flip it to `true` to make `/readyz` return 200.
pub async fn spawn_test_server_with_readyz() -> (String, u16, Arc<std::sync::atomic::AtomicBool>) {
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let config = Config::default();
    let storage = Arc::new(InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test server");
    let addr = listener.local_addr().expect("Failed to get local addr");
    let port = addr.port();

    let app = durable_streams_server::router::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default().with_readiness(Arc::clone(&ready)),
    );

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .expect("Test server failed");
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (format!("http://127.0.0.1:{port}"), port, ready)
}

/// Spawn a test server with a shutdown token for graceful drain testing.
///
/// Returns `(base_url, port, shutdown_token)`.
pub async fn spawn_test_server_with_shutdown() -> (String, u16, tokio_util::sync::CancellationToken)
{
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut config = Config::default();
    config.transport.connection.long_poll_timeout_secs = 30;
    let storage = Arc::new(InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind test server");
    let addr = listener.local_addr().expect("Failed to get local addr");
    let port = addr.port();

    let app = durable_streams_server::router::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default().with_shutdown(shutdown.clone()),
    );

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .expect("Test server failed");
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (format!("http://127.0.0.1:{port}"), port, shutdown)
}

/// Create an HTTP client for testing
pub fn test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("Failed to build test client")
}

/// Create an HTTP client with a custom timeout for long-poll tests
pub fn test_client_with_timeout(timeout_secs: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .expect("Failed to build test client")
}

pub async fn read_problem(response: reqwest::Response) -> ProblemDetails {
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("application/problem+json"),
        "expected application/problem+json, got {content_type}"
    );

    let body = response
        .text()
        .await
        .expect("failed to read problem details response");
    serde_json::from_str::<ProblemDetails>(&body)
        .expect("failed to decode problem details response")
}

fn unique_storage_dir(prefix: &str) -> std::path::PathBuf {
    let seq = HTTP_STORAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    std::env::temp_dir().join(format!("ds-{prefix}-storage-test-{pid}-{ts}-{seq}"))
}
