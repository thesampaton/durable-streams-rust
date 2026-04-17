#![allow(dead_code)]
// Shared across many independent integration-test crates; each crate only uses
// a subset of helpers, so items appear unused when compiled per-test target.

use durable_streams_server::config::{AcidBackend, Config, StorageMode};
use durable_streams_server::protocol::error::Result;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::offset::Offset as StorageOffset;
use durable_streams_server::protocol::problem::ProblemDetails;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{
    CreateStreamResult, CreateWithDataResult, ProducerAppendResult, ReadResult, Storage,
    StreamConfig, StreamMetadata, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

/// Global counter for generating unique stream names in tests
static STREAM_COUNTER: AtomicU16 = AtomicU16::new(0);
static STORAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique stream name for testing
///
/// Uses a global atomic counter to ensure unique names across all tests.
pub fn unique_stream_name() -> String {
    let id = STREAM_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("test-stream-{id}")
}

#[derive(Debug, Clone, Copy)]
pub enum StorageTestBackend {
    Memory,
    FileDurable,
    Acid,
    AcidInMemory,
}

impl StorageTestBackend {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::FileDurable => "file-durable",
            Self::Acid => "acid",
            Self::AcidInMemory => "acid-in-memory",
        }
    }
}

/// Expand a block of tests into one `mod` per backend mapping.
///
/// This supports small backend subsets that do not use `StorageTestBackend`
/// directly but still benefit from per-backend test enumeration.
#[macro_export]
macro_rules! backend_tests {
    (
        type $backend_ty:ty;
        $($name:ident => $backend:path),+ $(,)?;
        $($body:tt)*
    ) => {
        $crate::backend_tests! {
            @expand
            [$backend_ty]
            [$($body)*]
            $($name => $backend),+
        }
    };
    (
        @expand
        [$backend_ty:ty]
        [$($body:tt)*]
        $name:ident => $backend:path $(, $rest_name:ident => $rest_backend:path)*
    ) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $backend_ty = $backend;
            $($body)*
        }
        $crate::backend_tests! {
            @expand
            [$backend_ty]
            [$($body)*]
            $($rest_name => $rest_backend),*
        }
    };
    (
        @expand
        [$backend_ty:ty]
        [$($body:tt)*]
    ) => {
    };
}

/// Expand a block of `#[test]` functions into one `mod` per storage backend.
///
/// Each generated module defines a `const BACKEND: StorageTestBackend = ...`
/// identifying the backend under test, so the body can call
/// `create_test_storage(BACKEND)` etc. without a closure parameter.
///
/// `cargo test` then reports each `(test, backend)` pair as its own case
/// (e.g. `memory::my_test`, `acid::my_test`), which isolates failures and
/// drops the manual `"backend={}"` assertion suffixes.
#[macro_export]
macro_rules! storage_backend_tests {
    ($($body:tt)*) => {
        mod memory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::Memory;
            $($body)*
        }
        mod file_durable {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::FileDurable;
            $($body)*
        }
        mod acid {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::Acid;
            $($body)*
        }
        mod acid_in_memory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::StorageTestBackend =
                $crate::common::StorageTestBackend::AcidInMemory;
            $($body)*
        }
    };
}

/// Expand a block of `#[tokio::test]` functions into one `mod` per HTTP
/// backend (the subset of storage backends that participate in HTTP parity).
///
/// Each generated module defines a `const BACKEND: HttpTestBackend = ...`
/// that the body can feed to `spawn_test_server_for_backend(BACKEND)`.
#[macro_export]
macro_rules! http_backend_tests {
    ($($body:tt)*) => {
        mod memory {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::HttpTestBackend =
                $crate::common::HttpTestBackend::Memory;
            $($body)*
        }
        mod acid {
            #[allow(unused_imports)]
            use super::*;
            #[allow(dead_code)]
            const BACKEND: $crate::common::HttpTestBackend =
                $crate::common::HttpTestBackend::Acid;
            $($body)*
        }
    };
}

pub enum TestStorage {
    Memory(InMemoryStorage),
    File(FileStorage),
    Acid(AcidStorage),
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TestStorage>();
};

impl Storage for TestStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        match self {
            Self::Memory(inner) => inner.create_stream(name, config),
            Self::File(inner) => inner.create_stream(name, config),
            Self::Acid(inner) => inner.create_stream(name, config),
        }
    }

    fn append(&self, name: &str, data: bytes::Bytes, content_type: &str) -> Result<Offset> {
        match self {
            Self::Memory(inner) => inner.append(name, data, content_type),
            Self::File(inner) => inner.append(name, data, content_type),
            Self::Acid(inner) => inner.append(name, data, content_type),
        }
    }

    fn batch_append(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset> {
        match self {
            Self::Memory(inner) => inner.batch_append(name, messages, content_type, seq),
            Self::File(inner) => inner.batch_append(name, messages, content_type, seq),
            Self::Acid(inner) => inner.batch_append(name, messages, content_type, seq),
        }
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        match self {
            Self::Memory(inner) => inner.read(name, from_offset),
            Self::File(inner) => inner.read(name, from_offset),
            Self::Acid(inner) => inner.read(name, from_offset),
        }
    }

    fn delete(&self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.delete(name),
            Self::File(inner) => inner.delete(name),
            Self::Acid(inner) => inner.delete(name),
        }
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        match self {
            Self::Memory(inner) => inner.head(name),
            Self::File(inner) => inner.head(name),
            Self::Acid(inner) => inner.head(name),
        }
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.close_stream(name),
            Self::File(inner) => inner.close_stream(name),
            Self::Acid(inner) => inner.close_stream(name),
        }
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        match self {
            Self::Memory(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
            Self::File(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
            Self::Acid(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
        }
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<bytes::Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        match self {
            Self::Memory(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
            Self::File(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
            Self::Acid(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
        }
    }

    fn exists(&self, name: &str) -> bool {
        match self {
            Self::Memory(inner) => inner.exists(name),
            Self::File(inner) => inner.exists(name),
            Self::Acid(inner) => inner.exists(name),
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        match self {
            Self::Memory(inner) => inner.subscribe(name),
            Self::File(inner) => inner.subscribe(name),
            Self::Acid(inner) => inner.subscribe(name),
        }
    }

    fn cleanup_expired_streams(&self) -> usize {
        match self {
            Self::Memory(inner) => inner.cleanup_expired_streams(),
            Self::File(inner) => inner.cleanup_expired_streams(),
            Self::Acid(inner) => inner.cleanup_expired_streams(),
        }
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        match self {
            Self::Memory(inner) => inner.list_streams(),
            Self::File(inner) => inner.list_streams(),
            Self::Acid(inner) => inner.list_streams(),
        }
    }

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&StorageOffset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        match self {
            Self::Memory(inner) => inner.create_fork(name, source_name, fork_offset, config),
            Self::File(inner) => inner.create_fork(name, source_name, fork_offset, config),
            Self::Acid(inner) => inner.create_fork(name, source_name, fork_offset, config),
        }
    }
}

impl TestStorage {
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        match self {
            Self::Memory(inner) => inner.total_bytes(),
            Self::File(inner) => inner.total_bytes(),
            Self::Acid(inner) => inner.total_bytes(),
        }
    }
}

pub struct TestStorageHandle {
    pub storage: TestStorage,
    _storage_dir: Option<PathBuf>,
}

#[must_use]
pub fn create_test_storage(backend: StorageTestBackend) -> TestStorageHandle {
    create_test_storage_with_limits(backend, 1024 * 1024, 100 * 1024)
}

#[must_use]
pub fn create_test_storage_with_limits(
    backend: StorageTestBackend,
    max_total_bytes: u64,
    max_stream_bytes: u64,
) -> TestStorageHandle {
    match backend {
        StorageTestBackend::Memory => TestStorageHandle {
            storage: TestStorage::Memory(InMemoryStorage::new(max_total_bytes, max_stream_bytes)),
            _storage_dir: None,
        },
        StorageTestBackend::FileDurable => {
            let storage_dir = unique_storage_dir("file");
            let storage = FileStorage::new(&storage_dir, max_total_bytes, max_stream_bytes, true)
                .expect("failed to initialize test file storage");
            TestStorageHandle {
                storage: TestStorage::File(storage),
                _storage_dir: Some(storage_dir),
            }
        }
        StorageTestBackend::Acid => {
            let storage_dir = unique_storage_dir("acid");
            let storage = AcidStorage::new(
                &storage_dir,
                16,
                max_total_bytes,
                max_stream_bytes,
                AcidBackend::File,
            )
            .expect("failed to initialize test acid storage");
            TestStorageHandle {
                storage: TestStorage::Acid(storage),
                _storage_dir: Some(storage_dir),
            }
        }
        StorageTestBackend::AcidInMemory => {
            let storage_dir = unique_storage_dir("acid-mem");
            let storage = AcidStorage::new(
                &storage_dir,
                16,
                max_total_bytes,
                max_stream_bytes,
                AcidBackend::InMemory,
            )
            .expect("failed to initialize test acid in-memory storage");
            TestStorageHandle {
                storage: TestStorage::Acid(storage),
                _storage_dir: Some(storage_dir),
            }
        }
    }
}

fn unique_storage_dir(prefix: &str) -> PathBuf {
    let seq = STORAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    std::env::temp_dir().join(format!("ds-{prefix}-storage-test-{pid}-{ts}-{seq}"))
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

#[derive(Debug, Clone, Copy)]
pub enum HttpTestBackend {
    Memory,
    Acid,
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
    let app = durable_streams_server::router::build_router(storage, &config);

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

    let app = durable_streams_server::router::build_router_with_ready(
        storage,
        &config,
        Some(Arc::clone(&ready)),
        tokio_util::sync::CancellationToken::new(),
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

    let app = durable_streams_server::router::build_router_with_ready(
        storage,
        &config,
        None,
        shutdown.clone(),
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
