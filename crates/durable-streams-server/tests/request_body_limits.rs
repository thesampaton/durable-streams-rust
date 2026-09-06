//! Request-body limits apply while collecting both fixed and chunked bodies.
#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]
mod common;

#[tokio::test]
async fn bounded_bodies_preserve_storage_and_return_413() {
    let mut config = durable_streams_server::Config::default();
    config.limits.max_request_body_bytes = 4;
    let (base, _) = common::spawn_test_server_with_config(config).await;
    let client = common::test_client();
    let url = format!("{base}/v1/stream/limited");
    assert_eq!(
        client
            .put(&url)
            .header("content-type", "text/plain")
            .body("12345")
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    assert_eq!(client.head(&url).send().await.unwrap().status(), 404);
    assert_eq!(
        client
            .put(&url)
            .header("content-type", "text/plain")
            .body("1234")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    let chunks = futures_util::stream::iter([
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"123")),
        Ok(bytes::Bytes::from_static(b"45")),
    ]);
    let response = client
        .post(&url)
        .header("content-type", "text/plain")
        .body(reqwest::Body::wrap_stream(chunks))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "REQUEST_BODY_TOO_LARGE");
    let response = client.get(format!("{url}?offset=-1")).send().await.unwrap();
    assert_eq!(response.text().await.unwrap(), "1234");
}

#[tokio::test]
async fn oversized_ttl_returns_400_without_creating_a_stream() {
    let (base, _) = common::spawn_test_server().await;
    let client = common::test_client();
    let url = format!("{base}/v1/stream/invalid-ttl");
    let response = client
        .put(&url)
        .header("stream-ttl", u64::MAX.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert_eq!(client.head(&url).send().await.unwrap().status(), 404);
}

// PROTOCOL.md §9.1.3: [] is invalid in POST, including requests carrying closure.
#[tokio::test]
async fn empty_json_array_cannot_close_a_stream() {
    let (base, _) = common::spawn_test_server().await;
    let client = common::test_client();
    let url = format!("{base}/v1/stream/json-close");
    assert_eq!(
        client
            .put(&url)
            .header("content-type", "application/json")
            .body("[]")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        client
            .post(&url)
            .header("content-type", "application/json")
            .header("stream-closed", "true")
            .body("[]")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_ne!(
        client
            .head(&url)
            .send()
            .await
            .unwrap()
            .headers()
            .get("stream-closed")
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
    assert_eq!(
        client
            .post(&url)
            .header("stream-closed", "true")
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
}
