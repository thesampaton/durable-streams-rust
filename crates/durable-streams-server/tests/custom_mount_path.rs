//! Integration coverage for custom mount path.

mod common;

use common::test_client;
use durable_streams_server::{
    Config, InMemoryStorage, protocol::problem::ProblemDetails, router::DEFAULT_STREAM_BASE_PATH,
};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test]
async fn custom_mount_path_updates_location_and_problem_instance() {
    let mut config = Config::default();
    config.http.stream_base_path = "/streams".to_string();
    let storage = Arc::new(InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind test listener");
    let addr = listener.local_addr().expect("failed to read local addr");
    let app = durable_streams_server::Server::new(
        durable_streams_server::StreamService::new(storage),
        &config,
        durable_streams_server::RouterOptions::default(),
    )
    .expect("server initialization")
    .start()
    .expect("server startup")
    .router();

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server failed");
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = test_client();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    let create = client
        .put(format!("{base_url}/streams/orders"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);
    assert_eq!(
        create
            .headers()
            .get("location")
            .expect("missing location header")
            .to_str()
            .expect("location header should be utf-8"),
        format!("{base_url}/streams/orders")
    );

    let error = client
        .get(format!("{base_url}/streams/orders?offset=-1&live=invalid"))
        .send()
        .await
        .expect("invalid live request failed");
    assert_eq!(error.status(), 400);

    let problem = serde_json::from_str::<ProblemDetails>(
        &error.text().await.expect("failed to read problem body"),
    )
    .expect("problem response should parse");
    assert_eq!(
        problem.instance.as_deref(),
        Some("/streams/orders?offset=-1&live=invalid")
    );

    let default_create = client
        .put(format!("{base_url}{DEFAULT_STREAM_BASE_PATH}/fallback"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("default mount request failed");
    assert_eq!(default_create.status(), 404);
}
