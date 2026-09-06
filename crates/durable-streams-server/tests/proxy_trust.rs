//! Integration coverage for proxy trust.

mod common;

use common::{spawn_test_server, spawn_test_server_with_config, test_client, unique_stream_name};
use durable_streams_server::config::{
    Config, ForwardedHeadersMode, ProxyIdentityMode, TransportMode,
};

/// Extract the `Location` header from a PUT response.
///
/// The PUT handler builds the Location URL using `x-forwarded-host` and
/// `x-forwarded-proto` if present — giving us an observable side-effect
/// for testing whether the proxy trust middleware strips or passes them.
async fn put_and_get_location(base_url: &str, headers: Vec<(&str, &str)>) -> String {
    let client = test_client();
    let stream_name = unique_stream_name();
    let url = format!("{base_url}/v1/stream/{stream_name}");

    let mut req = client.put(&url).header("content-type", "text/plain");
    for (key, value) in headers {
        req = req.header(key, value);
    }
    let resp = req.send().await.expect("PUT request failed");

    assert_eq!(resp.status(), 201, "expected 201 Created");
    resp.headers()
        .get("location")
        .expect("missing Location header")
        .to_str()
        .expect("non-UTF-8 Location")
        .to_string()
}

// ── Forwarded headers stripped when proxy disabled (default) ───────

#[tokio::test]
async fn forwarded_headers_stripped_when_proxy_disabled() {
    // Default config: proxy.enabled = false
    let (base_url, _) = spawn_test_server().await;

    let location = put_and_get_location(
        &base_url,
        vec![
            ("x-forwarded-host", "evil.example.com"),
            ("x-forwarded-proto", "https"),
            ("host", "localhost"),
        ],
    )
    .await;

    assert!(
        !location.contains("evil.example.com"),
        "x-forwarded-host should be stripped when proxy is disabled, but Location was: {location}"
    );
    assert!(
        location.starts_with("http://"),
        "x-forwarded-proto should be stripped (no https), but Location was: {location}"
    );
}

// ── Forwarded headers accepted from trusted peer ──────────────────

#[tokio::test]
async fn forwarded_headers_accepted_from_trusted_peer() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::XForwarded;
    config.proxy.trusted_proxies = vec!["127.0.0.1".to_string()];

    let (base_url, _) = spawn_test_server_with_config(config).await;

    let location = put_and_get_location(
        &base_url,
        vec![
            ("x-forwarded-host", "proxy.example.com"),
            ("x-forwarded-proto", "https"),
        ],
    )
    .await;

    assert!(
        location.contains("proxy.example.com"),
        "x-forwarded-host should be passed from trusted peer, but Location was: {location}"
    );
    assert!(
        location.starts_with("https://"),
        "x-forwarded-proto should be passed from trusted peer, but Location was: {location}"
    );
}

// ── Forwarded headers stripped from untrusted peer ────────────────

#[tokio::test]
async fn forwarded_headers_stripped_from_untrusted_peer() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::XForwarded;
    // Only trust 10.0.0.1 — test client connects from 127.0.0.1
    config.proxy.trusted_proxies = vec!["10.0.0.1".to_string()];

    let (base_url, _) = spawn_test_server_with_config(config).await;

    let location = put_and_get_location(
        &base_url,
        vec![
            ("x-forwarded-host", "evil.example.com"),
            ("x-forwarded-proto", "https"),
        ],
    )
    .await;

    assert!(
        !location.contains("evil.example.com"),
        "x-forwarded-host should be stripped from untrusted peer, but Location was: {location}"
    );
    assert!(
        location.starts_with("http://"),
        "x-forwarded-proto should be stripped from untrusted peer, but Location was: {location}"
    );
}

// ── RFC Forwarded header stripped when mode is XForwarded ──────────

#[tokio::test]
async fn rfc_forwarded_stripped_when_mode_is_xforwarded() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::XForwarded;
    config.proxy.trusted_proxies = vec!["127.0.0.1".to_string()];

    let (base_url, _) = spawn_test_server_with_config(config).await;

    // Send both header families; only x-forwarded-* should survive.
    let location = put_and_get_location(
        &base_url,
        vec![
            ("x-forwarded-host", "proxy.example.com"),
            ("x-forwarded-proto", "https"),
            ("forwarded", "host=rfc-evil.example.com;proto=https"),
        ],
    )
    .await;

    // x-forwarded-host should be kept (trusted + correct mode).
    assert!(
        location.contains("proxy.example.com"),
        "x-forwarded-host should survive in XForwarded mode, but Location was: {location}"
    );
}

// ── X-Forwarded-* stripped when mode is Forwarded ─────────────────

#[tokio::test]
async fn x_forwarded_stripped_when_mode_is_forwarded() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::Forwarded;
    config.proxy.trusted_proxies = vec!["127.0.0.1".to_string()];

    let (base_url, _) = spawn_test_server_with_config(config).await;

    // x-forwarded-* should be stripped even from a trusted peer when
    // mode is Forwarded (only RFC 7239 Forwarded is kept).
    let location = put_and_get_location(
        &base_url,
        vec![
            ("x-forwarded-host", "evil.example.com"),
            ("x-forwarded-proto", "https"),
        ],
    )
    .await;

    assert!(
        !location.contains("evil.example.com"),
        "x-forwarded-host should be stripped in Forwarded mode, but Location was: {location}"
    );
    assert!(
        location.starts_with("http://"),
        "x-forwarded-proto should be stripped in Forwarded mode, but Location was: {location}"
    );
}

#[tokio::test]
async fn forwarded_header_accepted_from_trusted_peer() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::Forwarded;
    config.proxy.trusted_proxies = vec!["127.0.0.1".to_string()];

    let (base_url, _) = spawn_test_server_with_config(config).await;

    let location = put_and_get_location(
        &base_url,
        vec![
            (
                "forwarded",
                "for=203.0.113.44;host=proxy.example.com;proto=https",
            ),
            ("x-forwarded-host", "ignored.example.com"),
            ("x-forwarded-proto", "http"),
        ],
    )
    .await;

    assert!(
        location.contains("proxy.example.com"),
        "trusted Forwarded host should be used, but Location was: {location}"
    );
    assert!(
        location.starts_with("https://"),
        "trusted Forwarded proto should be used, but Location was: {location}"
    );
}

// ── Identity header stripped from untrusted peer ──────────────────

#[tokio::test]
async fn identity_header_stripped_from_untrusted_peer() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::XForwarded;
    // Trust 10.0.0.1 only; test client connects from 127.0.0.1
    config.proxy.trusted_proxies = vec!["10.0.0.1".to_string()];
    // The embedding declares its authenticated transport; the test listener exercises middleware.
    config.transport.mode = TransportMode::Mtls;
    config.proxy.identity.mode = ProxyIdentityMode::Header;
    config.proxy.identity.header_name = Some("x-client-identity".to_string());

    let (base_url, _) = spawn_test_server_with_config(config).await;

    let client = test_client();
    let stream_name = unique_stream_name();

    // The middleware should strip x-client-identity from untrusted peers.
    // The server should still respond normally (the header is just removed).
    let resp = client
        .put(format!("{base_url}/v1/stream/{stream_name}"))
        .header("content-type", "text/plain")
        .header("x-client-identity", "spoofed-user")
        .send()
        .await
        .expect("PUT with identity header failed");

    assert_eq!(
        resp.status(),
        201,
        "server should still respond after stripping identity header"
    );
}

// ── CIDR trust matching ───────────────────────────────────────────

#[tokio::test]
async fn cidr_trust_matching() {
    let mut config = Config::default();
    config.proxy.enabled = true;
    config.proxy.forwarded_headers = ForwardedHeadersMode::XForwarded;
    // 127.0.0.0/8 matches all loopback addresses including 127.0.0.1.
    config.proxy.trusted_proxies = vec!["127.0.0.0/8".to_string()];

    let (base_url, _) = spawn_test_server_with_config(config).await;

    let location = put_and_get_location(
        &base_url,
        vec![
            ("x-forwarded-host", "proxy.example.com"),
            ("x-forwarded-proto", "https"),
        ],
    )
    .await;

    assert!(
        location.contains("proxy.example.com"),
        "CIDR 127.0.0.0/8 should trust 127.0.0.1, but Location was: {location}"
    );
}
