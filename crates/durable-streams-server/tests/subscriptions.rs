//! Integration coverage for subscriptions.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

//! Protocol sections 6–7: control namespace, durable delivery, and worker fencing.
mod common;

use axum::{Json, Router, routing::post};
use bytes::Bytes;
use common::{StorageTestBackend, TestStorage, create_test_storage};
use durable_streams_server::{
    Config, Storage, build_router,
    config::AcidBackend,
    storage::{acid::AcidStorage, file::FileStorage},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

struct Server {
    url: String,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    storage: Arc<TestStorage>,
}
impl Server {
    async fn start(storage: TestStorage) -> Self {
        let storage = Arc::new(storage);
        let mut config = Config::default();
        config.http.allow_insecure_webhooks = true;
        let shutdown = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/stream", listener.local_addr().unwrap());
        let app = build_router(
            storage.clone(),
            &config,
            durable_streams_server::RouterOptions::default().with_shutdown(shutdown.clone()),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url,
            shutdown,
            task,
            storage,
        }
    }
    async fn stop(self) {
        self.shutdown.cancel();
        self.task.abort();
        let _ = self.task.await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&self.storage) > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    fn sub(&self, id: &str) -> String {
        format!("{}/__ds/subscriptions/{id}", self.url)
    }
}

async fn create_pull(server: &Server, id: &str) {
    let client = reqwest::Client::new();
    assert!(
        client
            .put(format!("{}/wake", server.url))
            .header("content-type", "application/json")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert_eq!(client.put(server.sub(id)).json(&json!({"type":"pull-wake", "pattern":"events/**", "wake_stream":"wake", "lease_ttl_ms":1000})).send().await.unwrap().status(),201);
}
async fn append(server: &Server, path: &str) -> String {
    let response = reqwest::Client::new()
        .put(format!("{}/{path}", server.url))
        .header("content-type", "application/json")
        .body("[1]")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    response.headers()["stream-next-offset"]
        .to_str()
        .unwrap()
        .into()
}
async fn claim(server: &Server, id: &str) -> Value {
    let response = reqwest::Client::new()
        .post(format!("{}/claim", server.sub(id)))
        .json(&json!({"worker":"one"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}
fn ack_body(claim: &Value, tail: &str) -> Value {
    json!({"wake_id":claim["wake_id"],"generation":claim["generation"],"acks":[{"stream":"events/item","offset":tail}],"done":true})
}

#[tokio::test]
async fn test_subscription_fencing_and_ack_validation_across_backends() {
    let client = reqwest::Client::new();
    for backend in [
        StorageTestBackend::Memory,
        StorageTestBackend::FileDurable,
        StorageTestBackend::Acid,
        StorageTestBackend::AcidInMemory,
    ] {
        let server = Server::start(create_test_storage(backend).storage).await;
        let jwks = client
            .get(format!("{}/__ds/jwks.json", server.url))
            .send()
            .await
            .unwrap();
        assert_eq!(jwks.headers()["cache-control"], "public, max-age=300");
        assert_eq!(jwks.headers()["x-content-type-options"], "nosniff");
        create_pull(&server, "sub").await;
        let tail = append(&server, "events/item").await;
        let claimed = claim(&server, "sub").await;
        let endpoint = format!("{}/ack", server.sub("sub"));
        let token = claimed["token"].as_str().unwrap();
        let mut invalid = ack_body(&claimed, &tail);
        invalid["acks"]
            .as_array_mut()
            .unwrap()
            .push(json!({"stream":"unlinked","offset":tail}));
        assert_eq!(
            client
                .post(&endpoint)
                .bearer_auth(token)
                .json(&invalid)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        let current: Value = client
            .get(server.sub("sub"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_ne!(current["streams"][0]["acked_offset"], tail);
        assert_eq!(
            client
                .post(&endpoint)
                .bearer_auth(format!("{token}x"))
                .json(&ack_body(&claimed, &tail))
                .send()
                .await
                .unwrap()
                .status(),
            409
        );
        assert_eq!(
            client
                .post(&endpoint)
                .bearer_auth(token)
                .json(&ack_body(&claimed, &tail))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            client
                .post(&endpoint)
                .bearer_auth(token)
                .json(&ack_body(&claimed, &tail))
                .send()
                .await
                .unwrap()
                .status(),
            409
        );
        // Neither direct stream creation nor forking can expose control state.
        assert!(
            !client
                .put(format!("{}/__ds/private", server.url))
                .body("secret")
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        assert_eq!(
            client
                .put(format!("{}/leak", server.url))
                .header("Stream-Forked-From", "/v1/stream/__ds/subscriptions/sub")
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        server.stop().await;
    }
}

#[tokio::test]
async fn test_lease_expiry_heartbeat_and_deleted_subscription_fence_tokens() {
    let server = Server::start(create_test_storage(StorageTestBackend::Memory).storage).await;
    create_pull(&server, "sub").await;
    let tail = append(&server, "events/item").await;
    let first = claim(&server, "sub").await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = claim(&server, "sub").await;
    assert!(second["generation"].as_u64().unwrap() > first["generation"].as_u64().unwrap());
    let client = reqwest::Client::new();
    let endpoint = format!("{}/ack", server.sub("sub"));
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(first["token"].as_str().unwrap())
            .json(&ack_body(&first, &tail))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let heartbeat = json!({"wake_id":second["wake_id"],"generation":second["generation"]});
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(second["token"].as_str().unwrap())
            .json(&heartbeat)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        client
            .post(format!("{}/claim", server.sub("sub")))
            .json(&json!({"worker":"two"}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        client
            .delete(server.sub("sub"))
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(second["token"].as_str().unwrap())
            .json(&ack_body(&second, &tail))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    server.stop().await;
}

fn persistent_storage(path: &std::path::Path, acid: bool) -> TestStorage {
    if acid {
        TestStorage::Acid(
            AcidStorage::new(path, 2, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap(),
        )
    } else {
        TestStorage::File(FileStorage::new(path, 1024 * 1024, 100 * 1024, true).unwrap())
    }
}

#[tokio::test]
async fn test_signing_key_claim_and_cursor_survive_disk_backend_restart() {
    let client = reqwest::Client::new();
    for acid in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let server = Server::start(persistent_storage(dir.path(), acid)).await;
        create_pull(&server, "sub").await;
        let tail = append(&server, "events/item").await;
        let claimed = claim(&server, "sub").await;
        let key: Value = client
            .get(format!("{}/__ds/jwks.json", server.url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        server.stop().await;
        let server = Server::start(persistent_storage(dir.path(), acid)).await;
        let restored: Value = client
            .get(format!("{}/__ds/jwks.json", server.url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(restored, key);
        assert_eq!(
            client
                .post(format!("{}/ack", server.sub("sub")))
                .bearer_auth(claimed["token"].as_str().unwrap())
                .json(&ack_body(&claimed, &tail))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        server.stop().await;
        let server = Server::start(persistent_storage(dir.path(), acid)).await;
        let restored: Value = client
            .get(server.sub("sub"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(restored["streams"][0]["acked_offset"], tail);
        server.stop().await;
    }
}

#[tokio::test]
async fn test_failed_webhook_retry_deadline_survives_restart() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let webhook = format!("http://{}/", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/",
        post(move || {
            let observed = observed.clone();
            async move {
                let n = observed.fetch_add(1, Ordering::SeqCst);
                (
                    if n == 0 {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        axum::http::StatusCode::OK
                    },
                    Json(json!({"done":true})),
                )
            }
        }),
    );
    let receiver = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(persistent_storage(dir.path(), true)).await;
    let client = reqwest::Client::new();
    assert_eq!(client.put(server.sub("retry")).json(&json!({"type":"webhook","pattern":"events/**","webhook":{"url":webhook},"lease_ttl_ms":30000})).send().await.unwrap().status(),201);
    let tail = append(&server, "events/item").await;
    let deadline = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state: Value =
                serde_json::from_slice(&server.storage.load_subscription_state().unwrap().unwrap())
                    .unwrap();
            if state["subscriptions"]["retry"]["failed"] == true {
                break state["subscriptions"]["retry"]["next_attempt_at"]
                    .as_i64()
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    server.stop().await;
    let server = Server::start(persistent_storage(dir.path(), true)).await;
    if chrono::Utc::now().timestamp_millis() < deadline {
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current: Value = client
                .get(server.sub("retry"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if current["streams"][0]["acked_offset"] == tail {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    server.stop().await;
    receiver.abort();
}

#[test]
fn test_file_fork_offsets_and_partial_prefix_survive_restart() {
    use durable_streams_server::{
        protocol::offset::Offset,
        storage::{ForkOptions, StreamConfig},
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = persistent_storage(dir.path(), false);
    let config = StreamConfig::new("text/plain".into());
    storage
        .create_stream_with_data("source", config.clone(), vec![Bytes::from("first")], false)
        .unwrap();
    let anchor = storage.head("source").unwrap().next_offset;
    storage
        .append("source", Bytes::from("second"), "text/plain")
        .unwrap();
    storage
        .create_fork_with_options(
            "fork",
            "source",
            Some(&anchor),
            config,
            ForkOptions {
                sub_offset: 3,
                initial_body: Bytes::from("!"),
                ..ForkOptions::default()
            },
        )
        .unwrap();
    let tail = storage.head("fork").unwrap().next_offset;
    storage.delete("source").unwrap();
    drop(storage);
    let storage = persistent_storage(dir.path(), false);
    assert_eq!(storage.head("fork").unwrap().next_offset, tail);
    assert_eq!(
        storage
            .read("fork", &Offset::start())
            .unwrap()
            .messages
            .concat(),
        b"firstsec!"
    );
    assert!(storage.read("fork", &tail).unwrap().messages.is_empty());
    storage
        .append("fork", Bytes::from("next"), "text/plain")
        .unwrap();
    assert_eq!(
        storage.read("fork", &tail).unwrap().messages.concat(),
        b"next"
    );
}
