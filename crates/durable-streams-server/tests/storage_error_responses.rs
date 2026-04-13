mod common;

use bytes::Bytes;
use common::{read_problem, spawn_test_server_with_storage, test_client, unique_stream_name};
use durable_streams_server::config::Config;
use durable_streams_server::protocol::error::{Error, Result};
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::InMemoryStorage;
use durable_streams_server::storage::{
    CreateStreamResult, CreateWithDataResult, ProducerAppendResult, ReadResult, Storage,
    StreamConfig, StreamMetadata,
};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy)]
enum FailureMode {
    Unavailable,
    InsufficientStorage,
}

struct FailingAppendStorage {
    inner: InMemoryStorage,
    failure_mode: FailureMode,
}

impl FailingAppendStorage {
    fn new(failure_mode: FailureMode) -> Self {
        Self {
            inner: InMemoryStorage::new(1024 * 1024, 1024 * 1024),
            failure_mode,
        }
    }

    fn append_error(&self) -> Error {
        match self.failure_mode {
            FailureMode::Unavailable => Error::storage_unavailable(
                "file",
                "sync stream log",
                "failed to sync stream log for test-stream: backend timed out",
            ),
            FailureMode::InsufficientStorage => Error::storage_insufficient(
                "file",
                "sync stream log",
                "failed to sync stream log for test-stream: disk full",
            ),
        }
    }
}

impl Storage for FailingAppendStorage {
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        self.inner.create_stream(name, config)
    }

    fn append(&self, _name: &str, _data: Bytes, _content_type: &str) -> Result<Offset> {
        Err(self.append_error())
    }

    fn batch_append(
        &self,
        _name: &str,
        _messages: Vec<Bytes>,
        _content_type: &str,
        _seq: Option<&str>,
    ) -> Result<Offset> {
        Err(self.append_error())
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        self.inner.read(name, from_offset)
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.inner.delete(name)
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        self.inner.head(name)
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        self.inner.close_stream(name)
    }

    fn append_with_producer(
        &self,
        _name: &str,
        _messages: Vec<Bytes>,
        _content_type: &str,
        _producer: &ProducerHeaders,
        _should_close: bool,
        _seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        Err(self.append_error())
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        self.inner
            .create_stream_with_data(name, config, messages, should_close)
    }

    fn exists(&self, name: &str) -> bool {
        self.inner.exists(name)
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        self.inner.subscribe(name)
    }

    fn cleanup_expired_streams(&self) -> usize {
        self.inner.cleanup_expired_streams()
    }
}

#[tokio::test]
async fn test_append_transient_storage_failure_returns_503_with_retry_after() {
    let storage = Arc::new(FailingAppendStorage::new(FailureMode::Unavailable));
    let (base_url, _port) = spawn_test_server_with_storage(storage, Config::default()).await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 503);
    assert_eq!(
        response.headers().get("retry-after").unwrap(),
        &reqwest::header::HeaderValue::from_static("1")
    );

    let problem = read_problem(response).await;
    assert_eq!(problem.code, "UNAVAILABLE");
    assert_eq!(problem.title, "Service Unavailable");
    assert_eq!(problem.status, 503);
    assert_eq!(
        problem.detail.as_deref(),
        Some("The server is temporarily unable to complete the request.")
    );
}

#[tokio::test]
async fn test_append_storage_capacity_failure_returns_507() {
    let storage = Arc::new(FailingAppendStorage::new(FailureMode::InsufficientStorage));
    let (base_url, _port) = spawn_test_server_with_storage(storage, Config::default()).await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 507);
    assert!(response.headers().get("retry-after").is_none());

    let problem = read_problem(response).await;
    assert_eq!(problem.code, "INSUFFICIENT_STORAGE");
    assert_eq!(problem.title, "Insufficient Storage");
    assert_eq!(problem.status, 507);
    assert_eq!(
        problem.detail.as_deref(),
        Some("The server does not have enough storage capacity to complete the request.")
    );
}
