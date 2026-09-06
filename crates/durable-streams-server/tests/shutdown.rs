//! Integration coverage for shutdown.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

//! Graceful shutdown tests.
//!
//! Verify that in-flight long-poll and SSE connections complete cleanly
//! when the server's shutdown token is cancelled, rather than being
//! abruptly terminated with connection resets.

mod common;

use common::{spawn_test_server_with_shutdown, test_client, test_client_with_timeout};
use reqwest::StatusCode;

/// Create a stream on the test server for shutdown tests.
async fn create_stream(base_url: &str, name: &str) {
    let client = test_client();
    let resp = client
        .put(format!("{base_url}/v1/stream/{name}"))
        .header("content-type", "text/plain")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream creation failed: {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 1. Long-poll returns 204 on shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn long_poll_returns_204_on_shutdown() {
    let (base_url, _port, shutdown) = spawn_test_server_with_shutdown().await;
    let client = test_client_with_timeout(10);

    create_stream(&base_url, "s").await;

    // Start a long-poll that will block (no data, stream is empty at tail)
    let poll_handle = tokio::spawn({
        let url = format!("{base_url}/v1/stream/s?offset=now&live=long-poll");
        let client = client.clone();
        async move { client.get(url).send().await }
    });

    // Give the long-poll time to enter the wait state
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Signal shutdown
    shutdown.cancel();

    // The long-poll should complete with 204 (no content), not a connection error
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(5), poll_handle)
        .await
        .expect("long-poll should complete within timeout")
        .expect("task should not panic");

    let resp = result.expect("request should not fail with connection error");
    assert_eq!(
        resp.status(),
        StatusCode::NO_CONTENT,
        "long-poll should return 204 on shutdown, got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 2. SSE stream ends cleanly on shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sse_stream_ends_cleanly_on_shutdown() {
    let (base_url, _port, shutdown) = spawn_test_server_with_shutdown().await;
    let client = test_client_with_timeout(10);

    create_stream(&base_url, "s").await;

    // Start an SSE connection at the tail
    let sse_handle = tokio::spawn({
        let url = format!("{base_url}/v1/stream/s?offset=now&live=sse");
        let client = client.clone();
        async move { client.get(url).send().await }
    });

    // Give the SSE connection time to establish
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Signal shutdown
    shutdown.cancel();

    // The SSE response should complete (stream ends) rather than error
    let result = tokio::time::timeout(tokio::time::Duration::from_secs(5), sse_handle)
        .await
        .expect("SSE should complete within timeout")
        .expect("task should not panic");

    let resp = result.expect("SSE request should not fail with connection error");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "SSE should have returned 200"
    );

    // The body should be readable (not a broken pipe)
    let body = resp.text().await.expect("should be able to read SSE body");
    // Should contain at least the initial control frame
    assert!(
        body.contains("event: control"),
        "SSE body should contain initial control frame, got: {body}"
    );
}

// ---------------------------------------------------------------------------
// 3. Catch-up reads are unaffected by shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catch_up_read_works_during_shutdown() {
    let (base_url, _port, shutdown) = spawn_test_server_with_shutdown().await;
    let client = test_client();

    create_stream(&base_url, "s").await;

    // Append some data
    client
        .post(format!("{base_url}/v1/stream/s"))
        .header("content-type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();

    // Signal shutdown
    shutdown.cancel();

    // Catch-up reads should still work (they don't wait)
    let resp = client
        .get(format!("{base_url}/v1/stream/s"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), b"hello");
}

// ---------------------------------------------------------------------------
// 4. Multiple long-polls all drain on shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_long_polls_drain_on_shutdown() {
    let (base_url, _port, shutdown) = spawn_test_server_with_shutdown().await;
    let client = test_client_with_timeout(10);

    // Create 3 streams
    for i in 0..3 {
        create_stream(&base_url, &format!("s{i}")).await;
    }

    // Start 3 concurrent long-polls
    let mut handles = Vec::new();
    for i in 0..3 {
        let url = format!("{base_url}/v1/stream/s{i}?offset=now&live=long-poll");
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.get(url).send().await }));
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Signal shutdown
    shutdown.cancel();

    // All should complete with 204
    for (i, handle) in handles.into_iter().enumerate() {
        let result = tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
            .await
            .unwrap_or_else(|_| panic!("long-poll {i} should complete within timeout"))
            .unwrap_or_else(|_| panic!("task {i} should not panic"));

        let resp = result.unwrap_or_else(|e| panic!("long-poll {i} should not fail: {e}"));
        assert_eq!(
            resp.status(),
            StatusCode::NO_CONTENT,
            "long-poll {i} should return 204"
        );
    }
}
