mod common;

use axum_server::{Handle, tls_rustls::RustlsConfig};
use common::{test_client, unique_stream_name};
use durable_streams_server::{
    config::{AlpnProtocol, Config, ConfigValidationError, HttpVersion, TlsVersion, TransportMode},
    router,
    startup::build_tls_server_config,
    storage::memory::InMemoryStorage,
};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use rustls_pemfile::certs;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const CERT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/ds-server-cert.pem"
);
const KEY_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/ds-server-key.pem"
);
const CA_CERT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/ds-ca-cert.pem");
const CLIENT_CERT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/ds-client-cert.pem"
);
const CLIENT_KEY_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/ds-client-key.pem"
);
const UNTRUSTED_CLIENT_CERT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/ds-untrusted-client-cert.pem"
);
const UNTRUSTED_CLIENT_KEY_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/ds-untrusted-client-key.pem"
);

static TLS_PROVIDER: Once = Once::new();

fn install_tls_provider() {
    TLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Build a server config for TLS mode using the startup module.
fn tls_server_config() -> Config {
    let mut config = Config::default();
    config.transport.mode = TransportMode::Tls;
    config.transport.tls.cert_path = Some(CERT_PATH.to_string());
    config.transport.tls.key_path = Some(KEY_PATH.to_string());
    config
}

/// Build a server config for mTLS mode using the startup module.
fn mtls_server_config() -> Config {
    let mut config = tls_server_config();
    config.transport.mode = TransportMode::Mtls;
    config.transport.tls.client_ca_path = Some(CA_CERT_PATH.to_string());
    config
}

/// Spawn a TLS test server using explicit transport-mode bootstrap.
async fn spawn_tls_server() -> u16 {
    install_tls_provider();
    let config = tls_server_config();
    spawn_server_with_tls_config(config).await
}

/// Spawn an mTLS test server using explicit transport-mode bootstrap.
async fn spawn_mtls_server() -> u16 {
    install_tls_provider();
    let config = mtls_server_config();
    spawn_server_with_tls_config(config).await
}

/// Common server spawner that uses `build_tls_server_config` from startup module.
async fn spawn_server_with_tls_config(config: Config) -> u16 {
    let storage = Arc::new(InMemoryStorage::new(100 * 1024 * 1024, 10 * 1024 * 1024));
    let app = router::build_router(
        storage,
        &config,
        durable_streams_server::RouterOptions::default(),
    );

    let server_config =
        build_tls_server_config(&config).expect("failed to build TLS server config");
    let tls = RustlsConfig::from_config(Arc::new(server_config));

    let handle = Handle::new();
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));

    let server_handle = handle.clone();
    tokio::spawn(async move {
        axum_server::bind_rustls(addr, tls)
            .handle(server_handle)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("TLS test server failed");
    });

    let listening = tokio::time::timeout(Duration::from_secs(5), handle.listening())
        .await
        .expect("TLS server did not start listening within 5s")
        .expect("server never reported listening address");
    listening.port()
}

/// Build a rustls `ClientConfig` that trusts the test CA (no client cert).
fn build_ca_trusting_client_config() -> ClientConfig {
    install_tls_provider();
    let roots = load_ca_roots();
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// Build a rustls `ClientConfig` with the trusted client cert (for mTLS).
fn build_mtls_client_config() -> ClientConfig {
    install_tls_provider();
    let roots = load_ca_roots();
    let client_certs = load_pem_certs(CLIENT_CERT_PATH);
    let client_key = load_pem_key(CLIENT_KEY_PATH);
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certs, client_key)
        .expect("failed to build mTLS client config")
}

/// Build a rustls `ClientConfig` with an untrusted client cert (self-signed).
fn build_untrusted_client_config() -> ClientConfig {
    install_tls_provider();
    let roots = load_ca_roots();
    let client_certs = load_pem_certs(UNTRUSTED_CLIENT_CERT_PATH);
    let client_key = load_pem_key(UNTRUSTED_CLIENT_KEY_PATH);
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_certs, client_key)
        .expect("failed to build untrusted client config")
}

fn https_test_client() -> reqwest::Client {
    let ca_pem = std::fs::read(CA_CERT_PATH).expect("failed to read CA cert");
    let ca = reqwest::Certificate::from_pem(&ca_pem).expect("failed to parse CA cert");
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .tls_certs_only([ca])
        .tls_danger_accept_invalid_hostnames(true)
        .build()
        .expect("failed to build HTTPS test client")
}

fn load_ca_roots() -> RootCertStore {
    let cert_pem = std::fs::read(CA_CERT_PATH).expect("failed to read CA cert");
    let mut reader = BufReader::new(cert_pem.as_slice());
    let cert_chain: Vec<_> = certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse CA certs");
    let mut roots = RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).expect("failed to add CA cert");
    }
    roots
}

fn load_pem_certs(path: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    let data = std::fs::read(path).expect("failed to read cert file");
    rustls_pemfile::certs(&mut data.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .expect("failed to parse PEM certs")
}

fn load_pem_key(path: &str) -> rustls::pki_types::PrivateKeyDer<'static> {
    let data = std::fs::read(path).expect("failed to read key file");
    rustls_pemfile::private_key(&mut data.as_slice())
        .expect("failed to parse PEM key")
        .expect("no key found in file")
}

/// Do a raw TLS GET /healthz using a specific `ClientConfig`.
async fn tls_raw_get_health(port: u16, client_config: ClientConfig) -> std::io::Result<String> {
    let connector = TlsConnector::from(Arc::new(client_config));
    let tcp = TcpStream::connect(("127.0.0.1", port)).await?;
    let server_name = ServerName::try_from("localhost")
        .map_err(|e| std::io::Error::other(format!("invalid server name: {e}")))?;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(std::io::Error::other)?;

    tls.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    tls.flush().await?;

    let mut bytes = Vec::new();
    tls.read_to_end(&mut bytes).await?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

// ── TLS server tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_https_health_check_with_custom_ca() {
    let port = spawn_tls_server().await;
    let client_config = build_ca_trusting_client_config();
    let raw = tls_raw_get_health(port, client_config)
        .await
        .expect("HTTPS health check request failed");
    assert!(raw.starts_with("HTTP/1.1 200 OK"));
    assert!(raw.ends_with("\r\n\r\nok") || raw.ends_with("\n\nok"));
}

#[tokio::test]
async fn test_https_put_location_uses_https_scheme() {
    let port = spawn_tls_server().await;
    let client = https_test_client();
    let stream_name = unique_stream_name();

    let response = client
        .put(format!("https://localhost:{port}/v1/stream/{stream_name}"))
        .header("content-type", "text/plain")
        .body("hello")
        .send()
        .await
        .expect("HTTPS PUT failed");

    assert_eq!(response.status(), 201);
    let location = response
        .headers()
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("missing Location header");
    assert!(
        location.starts_with("https://localhost:"),
        "expected HTTPS Location, got: {location}"
    );
}

#[test]
fn test_tls_config_validation_requires_pair() {
    let mut config = Config::default();
    config.transport.mode = TransportMode::Tls;
    config.transport.tls.cert_path = Some(CERT_PATH.to_string());
    assert_eq!(
        config.validate().unwrap_err(),
        ConfigValidationError::MissingTlsField {
            mode: TransportMode::Tls,
            field: "key_path",
        }
    );

    config.transport.tls.cert_path = None;
    config.transport.tls.key_path = Some(KEY_PATH.to_string());
    assert_eq!(
        config.validate().unwrap_err(),
        ConfigValidationError::MissingTlsField {
            mode: TransportMode::Tls,
            field: "cert_path",
        }
    );
}

#[tokio::test]
async fn test_http_client_fails_against_https_endpoint() {
    let port = spawn_tls_server().await;

    let err = test_client()
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .expect_err("plaintext client unexpectedly succeeded against tls endpoint");

    assert!(
        err.is_request(),
        "expected request error from protocol mismatch, got: {err:#}"
    );
}

// ── TLS version and ALPN tests ─────────────────────────────────────

#[test]
fn test_build_tls_server_config_tls13_only() {
    install_tls_provider();

    let mut config = tls_server_config();
    config.transport.tls.min_version = TlsVersion::V1_3;
    config.transport.tls.max_version = TlsVersion::V1_3;
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1];
    let sc = build_tls_server_config(&config).expect("should build TLS 1.3 only config");
    // Verify by checking the ALPN was applied (sanity) — version enforcement
    // is tested via live connection below.
    assert!(!sc.alpn_protocols.is_empty());
}

#[test]
fn test_build_tls_server_config_tls12_plus_tls13() {
    install_tls_provider();

    let mut config = tls_server_config();
    config.transport.tls.min_version = TlsVersion::V1_2;
    config.transport.tls.max_version = TlsVersion::V1_3;
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1];
    let sc = build_tls_server_config(&config).expect("should build TLS 1.2+1.3 config");
    assert!(!sc.alpn_protocols.is_empty());
}

#[tokio::test]
async fn test_tls13_only_server_rejects_tls12_client() {
    install_tls_provider();

    let mut config = tls_server_config();
    config.transport.tls.min_version = TlsVersion::V1_3;
    config.transport.tls.max_version = TlsVersion::V1_3;
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1];
    let port = spawn_server_with_tls_config(config).await;

    // Build a client that only speaks TLS 1.2
    let roots = load_ca_roots();
    let mut client_config =
        ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
            .with_root_certificates(roots)
            .with_no_client_auth();
    client_config.alpn_protocols = vec![b"http/1.1".to_vec()];

    let result = tls_raw_get_health(port, client_config).await;
    assert!(
        result.is_err(),
        "expected TLS 1.2-only client to fail against TLS 1.3-only server, got: {:?}",
        result.unwrap()
    );
}

#[test]
fn test_build_tls_server_config_applies_alpn() {
    install_tls_provider();

    let mut config = tls_server_config();
    config.transport.http.versions = vec![HttpVersion::Http1, HttpVersion::Http2];
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1, AlpnProtocol::H2];
    let sc = build_tls_server_config(&config).expect("should build config with ALPN");
    assert_eq!(
        sc.alpn_protocols,
        vec![b"http/1.1".to_vec(), b"h2".to_vec()]
    );
}

// ── mTLS tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_mtls_accepts_trusted_client_cert() {
    let port = spawn_mtls_server().await;
    let client_config = build_mtls_client_config();
    let raw = tls_raw_get_health(port, client_config)
        .await
        .expect("mTLS health check with trusted cert failed");
    assert!(
        raw.starts_with("HTTP/1.1 200 OK"),
        "expected 200 OK, got: {raw}"
    );
}

#[tokio::test]
async fn test_mtls_rejects_untrusted_client_cert() {
    let port = spawn_mtls_server().await;
    let client_config = build_untrusted_client_config();
    let result = tls_raw_get_health(port, client_config).await;
    assert!(
        result.is_err(),
        "expected TLS handshake to fail with untrusted cert, but got: {:?}",
        result.unwrap()
    );
}

#[tokio::test]
async fn test_mtls_rejects_no_client_cert() {
    let port = spawn_mtls_server().await;
    let client_config = build_ca_trusting_client_config(); // no client cert
    let result = tls_raw_get_health(port, client_config).await;
    assert!(
        result.is_err(),
        "expected TLS handshake to fail without client cert, but got: {:?}",
        result.unwrap()
    );
}

// ── HTTP/2 ALPN negotiation tests ─────────────────────────────────

/// Connect over TLS and return the negotiated ALPN protocol, or an error
/// if the TLS handshake fails.
async fn tls_negotiated_alpn(
    port: u16,
    client_config: ClientConfig,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    let connector = TlsConnector::from(Arc::new(client_config));
    let tcp = TcpStream::connect(("127.0.0.1", port)).await?;
    let server_name = ServerName::try_from("localhost")
        .map_err(|e| std::io::Error::other(format!("invalid server name: {e}")))?;
    let tls = connector.connect(server_name, tcp).await?;
    Ok(tls.get_ref().1.alpn_protocol().map(Vec::from))
}

#[tokio::test]
async fn test_h2_alpn_negotiated_when_enabled() {
    install_tls_provider();

    let mut config = tls_server_config();
    config.transport.http.versions = vec![HttpVersion::Http1, HttpVersion::Http2];
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1, AlpnProtocol::H2];
    let port = spawn_server_with_tls_config(config).await;

    // Client offering only h2 should negotiate h2.
    let roots = load_ca_roots();
    let mut client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec()];

    let negotiated = tls_negotiated_alpn(port, client_config)
        .await
        .expect("TLS connect should succeed when h2 is enabled");
    assert_eq!(
        negotiated.as_deref(),
        Some(b"h2".as_slice()),
        "expected h2 ALPN negotiation, got: {negotiated:?}"
    );
}

#[tokio::test]
async fn test_h1_alpn_works_when_enabled() {
    install_tls_provider();

    let mut config = tls_server_config();
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1];
    let port = spawn_server_with_tls_config(config).await;

    let client_config = build_ca_trusting_client_config();
    let raw = tls_raw_get_health(port, client_config)
        .await
        .expect("HTTP/1.1 health check failed");
    assert!(
        raw.starts_with("HTTP/1.1 200 OK"),
        "expected HTTP/1.1 200, got: {raw}"
    );
}

#[tokio::test]
async fn test_h2_rejected_when_disabled() {
    install_tls_provider();

    // Server only offers http/1.1 in ALPN — no h2.
    let mut config = tls_server_config();
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::Http1_1];
    let port = spawn_server_with_tls_config(config).await;

    // Client only offers h2 — handshake should fail with NoApplicationProtocol.
    let roots = load_ca_roots();
    let mut client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec()];

    let result = tls_negotiated_alpn(port, client_config).await;
    assert!(
        result.is_err(),
        "expected TLS handshake to fail when h2 is not in server ALPN, got: {result:?}"
    );
}

#[tokio::test]
async fn test_h2_health_check_over_tls() {
    install_tls_provider();

    // Server offers both protocols with h2 preferred (h2 first in ALPN).
    let mut config = tls_server_config();
    config.transport.http.versions = vec![HttpVersion::Http1, HttpVersion::Http2];
    config.transport.tls.alpn_protocols = vec![AlpnProtocol::H2, AlpnProtocol::Http1_1];
    let port = spawn_server_with_tls_config(config).await;

    // Client also offers both; server-preferred h2 should win.
    let roots = load_ca_roots();
    let mut client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let negotiated = tls_negotiated_alpn(port, client_config)
        .await
        .expect("TLS connect should succeed");
    assert_eq!(
        negotiated.as_deref(),
        Some(b"h2".as_slice()),
        "expected h2 to be negotiated when server prefers it"
    );
}

// ── build_tls_server_config unit tests ─────────────────────────────

#[test]
fn test_build_tls_server_config_for_tls_mode() {
    install_tls_provider();
    let config = tls_server_config();
    let sc = build_tls_server_config(&config);
    assert!(sc.is_ok(), "TLS mode config build failed: {:?}", sc.err());
}

#[test]
fn test_build_tls_server_config_for_mtls_mode() {
    install_tls_provider();
    let config = mtls_server_config();
    let sc = build_tls_server_config(&config);
    assert!(sc.is_ok(), "mTLS mode config build failed: {:?}", sc.err());
}
