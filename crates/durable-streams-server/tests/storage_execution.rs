//! HTTP regressions for docs/design/blocking-execution-boundary.md.
#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

mod common;
#[path = "common/controlled_storage.rs"]
mod controlled_storage;

use bytes::Bytes;
use common::test_client;
use controlled_storage::ControlledStorage;
use durable_streams_server::{
    Config, FileStorage, RouterOptions, RunningServer, Server, Storage, StreamService,
};
use durable_streams_server::{protocol::offset::Offset, storage::StreamOptions};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Fixture {
    root: tempfile::TempDir,
    storage: Arc<ControlledStorage>,
    running: RunningServer,
    url: String,
    stop: CancellationToken,
    listener: Option<tokio::task::JoinHandle<()>>,
}

impl Fixture {
    async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = Arc::new(ControlledStorage::new(root.path()));
        storage
            .create_stream("s", StreamOptions::new("text/plain"))
            .unwrap();
        storage
            .create_stream("wake", StreamOptions::new("application/json"))
            .unwrap();
        let mut config = Config::default();
        config.limits.max_storage_jobs = 1;
        config.admin.enabled = true;
        config.transport.connection.long_poll_timeout_secs = 1;
        let stop = CancellationToken::new();
        let running = Server::new(
            StreamService::new(storage.clone()),
            &config,
            RouterOptions::default().with_shutdown(stop.clone()),
        )
        .unwrap()
        .start()
        .unwrap();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        let router = running.router();
        let shutdown = stop.clone();
        let listener = tokio::spawn(async move {
            axum::serve(
                tcp,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
        });
        Self {
            root,
            storage,
            running,
            url,
            stop,
            listener: Some(listener),
        }
    }

    fn stream(&self) -> String {
        format!("{}/v1/stream/s", self.url)
    }

    async fn shutdown(&mut self) {
        self.stop.cancel();
        self.running.shutdown().await.unwrap();
        if let Some(listener) = self.listener.take() {
            listener.await.unwrap();
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}

// Tests use one slot; the idle reconciliation pass can briefly hold it at startup.
async fn admitted(request: reqwest::RequestBuilder) -> reqwest::Response {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = request.try_clone().unwrap().send().await.unwrap();
            if response.status() != StatusCode::SERVICE_UNAVAILABLE {
                return response;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

async fn assert_busy_routes(fixture: &Fixture) {
    let client = test_client();
    for (method, path) in [
        (Method::PUT, "/v1/stream/other"),
        (Method::POST, "/v1/stream/s"),
        (Method::GET, "/v1/stream/s?offset=-1"),
        (Method::HEAD, "/v1/stream/s"),
        (Method::DELETE, "/v1/stream/s"),
        (Method::GET, "/admin/streams"),
        (Method::GET, "/v1/stream/__ds/subscriptions/pending"),
    ] {
        let head = method == Method::HEAD;
        let response = client
            .request(method, format!("{}{path}", fixture.url))
            .header("content-type", "text/plain")
            .body("blocked")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503, "{path}");
        assert_eq!(response.headers()["retry-after"], "1");
        if head {
            assert!(response.bytes().await.unwrap().is_empty());
        } else {
            let body: Value = response.json().await.unwrap();
            let code = if path.contains("/__ds/") {
                &body["error"]["code"]
            } else {
                &body["code"]
            };
            assert_eq!(code, "UNAVAILABLE", "{path}: {body}");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn paused_detached_append_keeps_capacity_and_shutdown_waits_for_durable_close() {
    let mut fixture = Fixture::start().await;
    let (observed, release) = fixture.storage.arm("append");
    let request = test_client()
        .post(fixture.stream())
        .header("content-type", "text/plain")
        .header("stream-closed", "true")
        .body("committed");
    let caller = tokio::spawn(admitted(request));
    observed.await.unwrap();
    let health = tokio::time::timeout(
        Duration::from_millis(500),
        test_client().get(format!("{}/healthz", fixture.url)).send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(health.status(), 200);
    assert_busy_routes(&fixture).await;
    assert_eq!(fixture.storage.inner.head("s").unwrap().message_count, 0);
    assert!(!fixture.storage.inner.exists("other").unwrap());
    caller.abort();
    let _ = caller.await;
    let a = fixture.running.clone();
    let b = fixture.running.clone();
    let mut first = Box::pin(a.shutdown());
    let mut second = Box::pin(b.shutdown());
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    release.send(()).unwrap();
    first.await.unwrap();
    second.await.unwrap();
    fixture.shutdown().await;
    let reopened =
        FileStorage::new(fixture.root.path().to_owned(), 1024 * 1024, 1024 * 1024).unwrap();
    let read = reopened.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages, vec![Bytes::from_static(b"committed")]);
    assert!(read.closed && read.at_tail);
}

#[tokio::test(flavor = "current_thread")]
async fn detached_subscription_save_keeps_durable_and_cached_state_together() {
    let mut fixture = Fixture::start().await;
    let (observed, release) = fixture.storage.arm("subscription save");
    let url = format!("{}/v1/stream/__ds/subscriptions/pending", fixture.url);
    let caller = tokio::spawn(admitted(test_client().put(&url).json(&json!({
        "type":"pull-wake", "pattern":"events/**", "wake_stream":"wake", "lease_ttl_ms":1000
    }))));
    observed.await.unwrap(); // Persisted, with the cache update still in the accepted job.
    let saved: Value = serde_json::from_slice(
        &fixture
            .storage
            .inner
            .load_subscription_state()
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(saved["subscriptions"]["pending"].is_object());
    assert_busy_routes(&fixture).await;
    caller.abort();
    let _ = caller.await;
    release.send(()).unwrap();
    let response = admitted(test_client().get(url)).await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Value>().await.unwrap()["pattern"],
        "events/**"
    );
    fixture.shutdown().await;
    let reopened =
        FileStorage::new(fixture.root.path().to_owned(), 1024 * 1024, 1024 * 1024).unwrap();
    let saved: Value =
        serde_json::from_slice(&reopened.load_subscription_state().unwrap().unwrap()).unwrap();
    assert!(saved["subscriptions"]["pending"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn idle_sse_releases_capacity_and_busy_reread_ends_without_skipping_data() {
    let mut fixture = Fixture::start().await;
    let mut sse =
        admitted(test_client().get(format!("{}?offset=now&live=sse", fixture.stream()))).await;
    assert_eq!(sse.status(), 200);
    let initial = sse.chunk().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&initial).contains("streamNextOffset"));
    let (observed, release) = fixture.storage.arm("append");
    let caller = tokio::spawn(admitted(
        test_client()
            .post(fixture.stream())
            .header("content-type", "text/plain")
            .body("second"),
    ));
    observed.await.unwrap(); // An idle SSE response did not retain the only slot.
    fixture
        .storage
        .inner
        .append("s", Bytes::from_static(b"first"), "text/plain")
        .unwrap();
    let rest = tokio::time::timeout(Duration::from_secs(2), sse.bytes())
        .await
        .unwrap()
        .unwrap();
    assert!(
        rest.is_empty(),
        "a rejected reread must not emit an advanced control frame"
    );
    release.send(()).unwrap();
    assert_eq!(caller.await.unwrap().status(), 204);
    let response = admitted(test_client().get(format!("{}?offset=-1", fixture.stream()))).await;
    assert_eq!(response.bytes().await.unwrap(), "firstsecond");
    fixture.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn long_poll_timeout_delivers_data_even_when_notification_is_lost() {
    let mut fixture = Fixture::start().await;
    fixture
        .storage
        .mute_notifications
        .store(true, Ordering::Relaxed);
    let (observed, release) = fixture.storage.arm("read");
    let caller = tokio::spawn(admitted(
        test_client().get(format!("{}?offset=now&live=long-poll", fixture.stream())),
    ));
    observed.await.unwrap(); // Initial empty snapshot captured; a wake is intentionally suppressed.
    let result = fixture
        .storage
        .inner
        .append("s", Bytes::from_static(b"late"), "text/plain")
        .unwrap();
    release.send(()).unwrap();
    let response = caller.await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["stream-next-offset"],
        result.next_offset.as_str()
    );
    assert_eq!(response.bytes().await.unwrap(), "late");
    fixture.shutdown().await;
}
