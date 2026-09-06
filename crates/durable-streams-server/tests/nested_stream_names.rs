mod common;

use common::{spawn_test_server, test_client};
use durable_streams_server::Config;
use durable_streams_server::protocol::problem::ProblemDetails;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test]
async fn nested_stream_name_full_lifecycle() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();

    let stream_name = "slides/abc123";

    // PUT — create stream
    let create = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);

    let location = create
        .headers()
        .get("location")
        .expect("missing location header")
        .to_str()
        .unwrap();
    assert!(
        location.ends_with("/v1/stream/slides/abc123"),
        "location should preserve nested name, got: {location}"
    );

    // POST — append data
    let append = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("hello nested world")
        .send()
        .await
        .expect("append request failed");
    assert_eq!(append.status(), 204);

    // HEAD — check metadata
    let head = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .expect("head request failed");
    assert_eq!(head.status(), 200);
    assert_eq!(
        head.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/plain"
    );

    // GET — read data back
    let read = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .expect("read request failed");
    assert_eq!(read.status(), 200);
    let body = read.text().await.expect("failed to read body");
    assert!(body.contains("hello nested world"));

    // DELETE — remove stream
    let delete = client
        .delete(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .expect("delete request failed");
    assert_eq!(delete.status(), 204);

    // HEAD after delete — should be 404
    let head_after = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .expect("head-after-delete request failed");
    assert_eq!(head_after.status(), 404);
}

#[tokio::test]
async fn deeply_nested_stream_within_limits() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();

    // Default max_stream_name_segments is 8, so 4 segments is fine
    let stream_name = "org/project/type/resource-42";

    let create = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);
}

#[tokio::test]
async fn stream_name_exceeding_segment_limit_rejected() {
    // Use a low segment limit for testing
    let mut config = Config::default();
    config.limits.max_stream_name_segments = 3;
    let storage = Arc::new(durable_streams_server::InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to read local addr");
    let app = durable_streams_server::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default(),
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server failed");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = test_client();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    // 4 segments exceeds limit of 3
    let create = client
        .put(format!("{base_url}/v1/stream/a/b/c/d"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 400);

    let problem: ProblemDetails =
        serde_json::from_str(&create.text().await.unwrap()).expect("problem parse failed");
    assert_eq!(problem.code, "INVALID_STREAM_NAME");
    let detail = problem.detail.as_deref().unwrap();
    assert!(
        detail.contains("4 path segments") && detail.contains("maximum of 3"),
        "expected detail with actual count and limit, got: {detail}"
    );

    // 3 segments should still be fine
    let create_ok = client
        .put(format!("{base_url}/v1/stream/a/b/c"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create_ok.status(), 201);
}

#[tokio::test]
async fn stream_name_exceeding_byte_limit_rejected() {
    let mut config = Config::default();
    config.limits.max_stream_name_bytes = 20;
    let storage = Arc::new(durable_streams_server::InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to read local addr");
    let app = durable_streams_server::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default(),
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server failed");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = test_client();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    // Name longer than 20 bytes
    let long_name = "this-name-is-definitely-too-long";
    let create = client
        .put(format!("{base_url}/v1/stream/{long_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 400);

    let problem: ProblemDetails =
        serde_json::from_str(&create.text().await.unwrap()).expect("problem parse failed");
    assert_eq!(problem.code, "INVALID_STREAM_NAME");
    let detail = problem.detail.as_deref().unwrap();
    assert!(
        detail.contains("bytes") && detail.contains("maximum of 20"),
        "expected detail with actual length and limit, got: {detail}"
    );
}

/// Note: `.` and `..` path traversal segments are validated in unit tests
/// (`protocol::stream_name::tests`). HTTP clients normalise these before
/// sending, so they cannot be tested end-to-end. Empty segments (`a//b`)
/// and trailing slashes are preserved by clients and tested here instead.
#[tokio::test]
async fn empty_segment_rejected() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();

    let response = client
        .put(format!("{base_url}/v1/stream/a//b"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 400);

    let problem: ProblemDetails =
        serde_json::from_str(&response.text().await.unwrap()).expect("problem parse failed");
    assert_eq!(problem.code, "INVALID_STREAM_NAME");
    assert!(
        problem
            .detail
            .as_deref()
            .unwrap()
            .contains("empty segments"),
        "expected empty segments detail, got: {:?}",
        problem.detail
    );
}

#[tokio::test]
async fn error_response_includes_instance() {
    let mut config = Config::default();
    config.limits.max_stream_name_bytes = 5;
    let storage = Arc::new(durable_streams_server::InMemoryStorage::new(
        config.limits.max_memory_bytes,
        config.limits.max_stream_bytes,
    ));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("failed to read local addr");
    let app = durable_streams_server::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default(),
    );

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server failed");
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = test_client();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    let response = client
        .put(format!("{base_url}/v1/stream/too-long-name"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("request failed");
    assert_eq!(response.status(), 400);

    let problem: ProblemDetails =
        serde_json::from_str(&response.text().await.unwrap()).expect("problem parse failed");
    assert!(
        problem.instance.is_some(),
        "rejection response should include instance field"
    );
    assert!(
        problem
            .instance
            .as_deref()
            .unwrap()
            .contains("/v1/stream/too-long-name"),
        "instance should contain the request path, got: {:?}",
        problem.instance
    );
}

#[test]
fn config_validate_rejects_zero_max_stream_name_bytes() {
    let mut config = Config::default();
    config.limits.max_stream_name_bytes = 0;
    let err = config.validate().unwrap_err();
    assert_eq!(
        err,
        durable_streams_server::config::ConfigValidationError::MaxStreamNameBytesTooSmall
    );
}

#[test]
fn config_validate_rejects_zero_max_stream_name_segments() {
    let mut config = Config::default();
    config.limits.max_stream_name_segments = 0;
    let err = config.validate().unwrap_err();
    assert_eq!(
        err,
        durable_streams_server::config::ConfigValidationError::MaxStreamNameSegmentsTooSmall
    );
}

#[tokio::test]
async fn flat_stream_names_still_work() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();

    let create = client
        .put(format!("{base_url}/v1/stream/simple-name"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);

    let head = client
        .head(format!("{base_url}/v1/stream/simple-name"))
        .send()
        .await
        .expect("head request failed");
    assert_eq!(head.status(), 200);
}
