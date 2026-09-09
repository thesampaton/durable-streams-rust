//! Integration coverage for ttl expiry.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

mod common;

use common::{spawn_test_server, test_client, unique_stream_name};
use std::time::Duration;
use tokio::time::sleep;

/// Validates spec: 09-ttl-expiry.md#expiration-behavior
///
/// HEAD requests to expired streams return 404.
#[tokio::test]
async fn test_head_expired_stream_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with 1 second TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "1")
        .send()
        .await
        .unwrap();

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;

    // HEAD should return 404
    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

/// Validates spec: 09-ttl-expiry.md#expiration-behavior
///
/// GET requests to expired streams return 404.
#[tokio::test]
async fn test_get_expired_stream_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with 1 second TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "1")
        .send()
        .await
        .unwrap();

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;

    // GET should return 404
    let response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

/// Validates spec: 09-ttl-expiry.md#expiration-behavior
///
/// POST requests to expired streams return 404.
#[tokio::test]
async fn test_post_expired_stream_returns_404() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with 1 second TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "1")
        .send()
        .await
        .unwrap();

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;

    // POST should return 404
    let response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("data")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

/// Validates spec: 09-ttl-expiry.md#expiration-behavior
///
/// DELETE requests to expired streams return 204 (idempotent).
#[tokio::test]
async fn test_delete_expired_stream_returns_204() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with 1 second TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "1")
        .send()
        .await
        .unwrap();

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;

    // DELETE should return 204 (idempotent)
    let response = client
        .delete(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);
}

/// Validates spec: 09-ttl-expiry.md#remaining-ttl
///
/// HEAD responses show decreasing remaining TTL over time.
#[tokio::test]
async fn test_remaining_ttl_decreases() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with 10 second TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "10")
        .send()
        .await
        .unwrap();

    // Get initial TTL
    let response1 = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let ttl1: u64 = response1
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    // Wait 2 seconds
    sleep(Duration::from_secs(2)).await;

    // Get TTL again
    let response2 = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    let ttl2: u64 = response2
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    // TTL should have decreased
    assert!(
        ttl2 < ttl1,
        "TTL should decrease over time: {ttl1} -> {ttl2}"
    );
    assert!(
        ttl1 - ttl2 >= 1 && ttl1 - ttl2 <= 3,
        "TTL should decrease by ~2 seconds"
    );
}

/// Validates spec: 09-ttl-expiry.md#sliding-ttl
///
/// Successful GET requests renew the remaining TTL window.
#[tokio::test]
async fn test_get_renews_ttl() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "4")
        .send()
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let get_response = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_response.status(), 200);

    let head_response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let ttl: u64 = head_response
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    assert!(
        ttl >= 3,
        "GET should renew TTL close to the configured window, got {ttl}"
    );
}

/// Validates spec: 09-ttl-expiry.md#sliding-ttl
///
/// Successful POST requests renew the remaining TTL window.
#[tokio::test]
async fn test_post_renews_ttl() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "4")
        .send()
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let post_response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(post_response.status(), 204);

    let head_response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let ttl: u64 = head_response
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    assert!(
        ttl >= 3,
        "POST should renew TTL close to the configured window, got {ttl}"
    );
}

/// Validates spec: 09-ttl-expiry.md#sliding-ttl
///
/// Failed POST requests do not renew the remaining TTL window.
#[tokio::test]
async fn test_failed_post_does_not_renew_ttl() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "4")
        .send()
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let post_response = client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "application/json")
        .body("payload")
        .send()
        .await
        .unwrap();
    assert_eq!(post_response.status(), 400);

    let head_response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let ttl: u64 = head_response
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    assert!(ttl <= 2, "failed POST should not renew TTL, got {ttl}");
}

/// Validates spec: 09-ttl-expiry.md#sliding-ttl
///
/// HEAD requests observe but do not renew the TTL window.
#[tokio::test]
async fn test_head_does_not_renew_ttl() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "4")
        .send()
        .await
        .unwrap();

    sleep(Duration::from_secs(2)).await;

    let response1 = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let ttl1: u64 = response1
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    let response2 = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();
    let ttl2: u64 = response2
        .headers()
        .get("Stream-TTL")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();

    assert!(ttl2 < ttl1, "HEAD should not renew TTL: {ttl1} -> {ttl2}");
}

/// Validates spec: 09-ttl-expiry.md#idempotent-create-with-ttl
///
/// Creating the same stream after expiry creates a new stream (201).
#[tokio::test]
async fn test_recreate_after_expiry() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream with 1 second TTL
    let response1 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "1")
        .send()
        .await
        .unwrap();

    assert_eq!(response1.status(), 201);

    // Append data
    client
        .post(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .body("original data")
        .send()
        .await
        .unwrap();

    // Wait for expiration
    sleep(Duration::from_secs(2)).await;

    // Recreate should return 201 (new stream)
    let response2 = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .header("Stream-TTL", "10")
        .send()
        .await
        .unwrap();

    assert_eq!(response2.status(), 201);

    // Verify new stream is empty
    let response3 = client
        .get(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response3.status(), 200);
    let body = response3.text().await.unwrap();
    assert_eq!(body, "", "New stream should be empty");
}

/// Validates spec: 09-ttl-expiry.md#edge-cases
///
/// Streams without TTL should not include TTL headers in HEAD response.
#[tokio::test]
async fn test_no_ttl_no_headers() {
    let (base_url, _port) = spawn_test_server().await;
    let client = test_client();
    let stream_name = unique_stream_name();

    // Create stream without TTL
    client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("Content-Type", "text/plain")
        .send()
        .await
        .unwrap();

    // HEAD should not include TTL headers
    let response = client
        .head(format!("{base_url}/v1/stream/{stream_name}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert!(
        response.headers().get("Stream-TTL").is_none(),
        "Should not include Stream-TTL header"
    );
    assert!(
        response.headers().get("Stream-Expires-At").is_none(),
        "Should not include Stream-Expires-At header"
    );
}
