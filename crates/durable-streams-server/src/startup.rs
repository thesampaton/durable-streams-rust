//! Startup preflight, phase-aware diagnostics, and typed error mapping.
//!
//! The server startup sequence is divided into explicit phases. Each phase
//! produces a typed error that preserves the underlying cause chain and
//! identifies the failing phase so operators can act on the first failure
//! without guessing which stage broke.

use crate::config::{Config, ConfigLoadError, ConfigValidationError, TlsVersion, TransportMode};
use rustls::RootCertStore;
use rustls::server::WebPkiClientVerifier;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

// ── Startup phases ─────────────────────────────────────────────────

/// Discrete phases of the server startup sequence.
///
/// Each phase maps to a single logical step. When a phase fails, the
/// error identifies which phase produced the failure so operators can
/// diagnose without guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPhase {
    /// Loading and merging configuration sources.
    LoadConfig,
    /// Validating the merged configuration.
    ValidateConfig,
    /// Resolving the transport mode (HTTP / TLS / mTLS).
    ResolveTransport,
    /// Checking TLS file presence and readability.
    CheckTlsFiles,
    /// Building the rustls `ServerConfig`.
    BuildTlsContext,
    /// Binding the TCP listener.
    BindListener,
    /// Running the server after binding.
    StartServer,
}

impl fmt::Display for StartupPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LoadConfig => "load_config",
            Self::ValidateConfig => "validate_config",
            Self::ResolveTransport => "resolve_transport",
            Self::CheckTlsFiles => "check_tls_files",
            Self::BuildTlsContext => "build_tls_context",
            Self::BindListener => "bind_listener",
            Self::StartServer => "start_server",
        })
    }
}

// ── Startup errors ─────────────────────────────────────────────────

/// Top-level startup error carrying phase context and a typed cause.
#[derive(Debug, Error)]
#[error("[{phase}] {kind}")]
pub struct StartupError {
    /// The phase that failed.
    pub phase: StartupPhase,
    /// The typed cause.
    pub kind: StartupErrorKind,
}

impl StartupError {
    #[must_use]
    pub fn new(phase: StartupPhase, kind: StartupErrorKind) -> Self {
        Self { phase, kind }
    }
}

/// Typed cause attached to a [`StartupError`].
#[derive(Debug, Error)]
pub enum StartupErrorKind {
    /// Configuration file could not be loaded or parsed.
    #[error("config load failed: {0}")]
    ConfigLoad(#[from] ConfigLoadError),

    /// Merged configuration failed validation.
    #[error("config validation failed: {0}")]
    ConfigValidation(#[from] ConfigValidationError),

    /// A required TLS file is missing from the filesystem.
    #[error("TLS file not found: {path}")]
    TlsFileNotFound { path: String },

    /// A TLS file exists but is not a regular file (e.g. directory, symlink to dir).
    #[error("TLS path is not a regular file: {path}")]
    TlsFileNotRegular { path: String },

    /// A TLS file exists but could not be read (permissions, I/O).
    #[error("TLS file is not readable: {path}: {reason}")]
    TlsFileNotReadable { path: String, reason: String },

    /// The rustls `ServerConfig` could not be built from the provided PEM files.
    #[error("failed to build TLS context: {0}")]
    TlsContext(String),

    /// The TCP listener could not bind to the configured address.
    #[error("failed to bind {addr}: {source}")]
    Bind { addr: SocketAddr, source: io::Error },

    /// A runtime error after the server began accepting connections.
    #[error("server error: {0}")]
    Runtime(String),
}

// ── TLS file preflight ─────────────────────────────────────────────

/// Classification of a single TLS file check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsFileStatus {
    /// File exists, is a regular file, and is readable.
    Ok,
    /// File does not exist on disk.
    NotFound,
    /// Path exists but is not a regular file.
    NotRegular,
    /// File exists but cannot be read.
    NotReadable(String),
}

/// Check whether a single path refers to a readable regular file.
#[must_use]
pub fn check_tls_file(path: &str) -> TlsFileStatus {
    let p = Path::new(path);
    let metadata = match std::fs::metadata(p) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return TlsFileStatus::NotFound,
        Err(e) => return TlsFileStatus::NotReadable(e.to_string()),
    };
    if !metadata.is_file() {
        return TlsFileStatus::NotRegular;
    }
    // Attempt to open for reading to verify permissions.
    if let Err(e) = std::fs::File::open(p) {
        return TlsFileStatus::NotReadable(e.to_string());
    }
    TlsFileStatus::Ok
}

/// Run preflight checks on all configured TLS file paths.
///
/// Returns `Ok(())` if all present paths pass, or the first error found.
///
/// # Errors
///
/// Returns a [`StartupError`] with phase [`StartupPhase::CheckTlsFiles`] when
/// a configured TLS file is missing, not a regular file, or not readable.
pub fn preflight_tls_files(config: &Config) -> Result<(), StartupError> {
    let files: Vec<(&str, &str)> = [
        ("cert_path", config.transport.tls.cert_path.as_deref()),
        ("key_path", config.transport.tls.key_path.as_deref()),
        (
            "client_ca_path",
            config.transport.tls.client_ca_path.as_deref(),
        ),
    ]
    .into_iter()
    .filter_map(|(label, path)| path.map(|p| (label, p)))
    .collect();

    for (_label, path) in &files {
        match check_tls_file(path) {
            TlsFileStatus::Ok => {}
            TlsFileStatus::NotFound => {
                return Err(StartupError::new(
                    StartupPhase::CheckTlsFiles,
                    StartupErrorKind::TlsFileNotFound {
                        path: (*path).to_string(),
                    },
                ));
            }
            TlsFileStatus::NotRegular => {
                return Err(StartupError::new(
                    StartupPhase::CheckTlsFiles,
                    StartupErrorKind::TlsFileNotRegular {
                        path: (*path).to_string(),
                    },
                ));
            }
            TlsFileStatus::NotReadable(reason) => {
                return Err(StartupError::new(
                    StartupPhase::CheckTlsFiles,
                    StartupErrorKind::TlsFileNotReadable {
                        path: (*path).to_string(),
                        reason,
                    },
                ));
            }
        }
    }
    Ok(())
}

/// Bind a TCP listener and classify failures under the bind phase.
///
/// # Errors
///
/// Returns a [`StartupError`] with phase [`StartupPhase::BindListener`] when
/// the address cannot be bound or non-blocking mode cannot be enabled.
pub fn bind_tcp_listener(addr: SocketAddr) -> Result<std::net::TcpListener, StartupError> {
    let listener =
        std::net::TcpListener::bind(addr).map_err(|source| StartupError::bind(addr, source))?;
    listener
        .set_nonblocking(true)
        .map_err(|source| StartupError::bind(addr, source))?;
    Ok(listener)
}

// ── TLS server config builder ──────────────────────────────────────

/// Build a [`rustls::ServerConfig`] from the resolved [`Config`].
///
/// Handles:
/// - TLS protocol version selection (1.2, 1.3, or both)
/// - ALPN protocol negotiation
/// - Server certificate and private key loading
/// - Client certificate verification for mTLS mode
///
/// # Errors
///
/// Returns a [`StartupError`] in the [`StartupPhase::BuildTlsContext`] phase
/// when certificate/key loading or verifier construction fails.
///
/// # Panics
///
/// Panics if `cert_path` or `key_path` (or `client_ca_path` for mTLS) are
/// `None`. Callers must run [`Config::validate`] before calling this function.
pub fn build_tls_server_config(config: &Config) -> Result<rustls::ServerConfig, StartupError> {
    let cert_path = config
        .transport
        .tls
        .cert_path
        .as_deref()
        .expect("cert_path validated present before build_tls_server_config");
    let key_path = config
        .transport
        .tls
        .key_path
        .as_deref()
        .expect("key_path validated present before build_tls_server_config");

    // ── TLS version selection ──────────────────────────────────────
    let versions = tls_protocol_versions(
        config.transport.tls.min_version,
        config.transport.tls.max_version,
    );

    // ── Certificate chain and private key ──────────────────────────
    let certs = load_pem_certs(cert_path)?;
    let key = load_pem_private_key(key_path)?;

    // ── Build ServerConfig with or without client verification ─────
    let mut server_config = match config.transport.mode {
        TransportMode::Mtls => {
            let client_ca_path =
                config.transport.tls.client_ca_path.as_deref().expect(
                    "client_ca_path validated present for mTLS before build_tls_server_config",
                );
            let client_roots = load_root_store(client_ca_path)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
                .build()
                .map_err(|e| {
                    StartupError::tls_context(format!("failed to build client cert verifier: {e}"))
                })?;
            rustls::ServerConfig::builder_with_protocol_versions(&versions)
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| {
                    StartupError::tls_context(format!("failed to build mTLS server config: {e}"))
                })?
        }
        _ => rustls::ServerConfig::builder_with_protocol_versions(&versions)
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                StartupError::tls_context(format!("failed to build TLS server config: {e}"))
            })?,
    };

    // ── ALPN protocols ─────────────────────────────────────────────
    server_config.alpn_protocols = config
        .transport
        .tls
        .alpn_protocols
        .iter()
        .map(|a| a.as_str().as_bytes().to_vec())
        .collect();

    Ok(server_config)
}

/// Map config TLS version range to rustls protocol versions.
fn tls_protocol_versions(
    min: TlsVersion,
    max: TlsVersion,
) -> Vec<&'static rustls::SupportedProtocolVersion> {
    let mut versions = Vec::with_capacity(2);
    if min <= TlsVersion::V1_2 && max >= TlsVersion::V1_2 {
        versions.push(&rustls::version::TLS12);
    }
    if min <= TlsVersion::V1_3 && max >= TlsVersion::V1_3 {
        versions.push(&rustls::version::TLS13);
    }
    versions
}

/// Load PEM certificate chain from a file path.
fn load_pem_certs(
    path: &str,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, StartupError> {
    let data = std::fs::read(path).map_err(|e| {
        StartupError::tls_context(format!("failed to read cert file '{path}': {e}"))
    })?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut data.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            StartupError::tls_context(format!("failed to parse PEM certs from '{path}': {e}"))
        })?;
    if certs.is_empty() {
        return Err(StartupError::tls_context(format!(
            "no certificates found in '{path}'"
        )));
    }
    Ok(certs)
}

/// Load a PEM private key from a file path.
fn load_pem_private_key(
    path: &str,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, StartupError> {
    let data = std::fs::read(path)
        .map_err(|e| StartupError::tls_context(format!("failed to read key file '{path}': {e}")))?;
    rustls_pemfile::private_key(&mut data.as_slice())
        .map_err(|e| {
            StartupError::tls_context(format!("failed to parse PEM key from '{path}': {e}"))
        })?
        .ok_or_else(|| StartupError::tls_context(format!("no private key found in '{path}'")))
}

/// Load a root certificate store from a PEM CA bundle.
fn load_root_store(path: &str) -> Result<RootCertStore, StartupError> {
    let data = std::fs::read(path)
        .map_err(|e| StartupError::tls_context(format!("failed to read CA file '{path}': {e}")))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut data.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            StartupError::tls_context(format!("failed to parse PEM CA certs from '{path}': {e}"))
        })?;
    if certs.is_empty() {
        return Err(StartupError::tls_context(format!(
            "no CA certificates found in '{path}'"
        )));
    }
    let mut store = RootCertStore::empty();
    for cert in certs {
        store.add(cert).map_err(|e| {
            StartupError::tls_context(format!("failed to add CA cert to trust store: {e}"))
        })?;
    }
    Ok(store)
}

// ── Structured startup logging ─────────────────────────────────────

/// Log the startup phase transition at `INFO` level.
pub fn log_phase(phase: StartupPhase) {
    tracing::info!(startup_phase = %phase, "entering startup phase");
}

/// Log transport diagnostics after config validation succeeds.
pub fn log_transport_summary(config: &Config) {
    let transport = config.transport.mode.as_str();
    let versions: Vec<&str> = config
        .transport
        .http
        .versions
        .iter()
        .map(|v| v.as_str())
        .collect();

    tracing::info!(
        transport.mode = transport,
        http.versions = ?versions,
        "transport resolved"
    );

    if config.transport.mode.uses_tls() {
        let alpn: Vec<&str> = config
            .transport
            .tls
            .alpn_protocols
            .iter()
            .map(|a| a.as_str())
            .collect();
        tracing::info!(
            tls.min_version = config.transport.tls.min_version.as_str(),
            tls.max_version = config.transport.tls.max_version.as_str(),
            tls.alpn = ?alpn,
            tls.has_client_ca = config.transport.tls.client_ca_path.is_some(),
            "TLS configuration"
        );
    }

    tracing::info!(
        proxy.enabled = config.proxy.enabled,
        proxy.forwarded_headers = ?config.proxy.forwarded_headers,
        proxy.trusted_proxy_count = config.proxy.trusted_proxies.len(),
        proxy.identity_mode = ?config.proxy.identity.mode,
        "proxy trust state"
    );
}

/// Log a startup failure at `ERROR` level with phase context.
pub fn log_startup_failure(error: &StartupError) {
    tracing::error!(
        startup_phase = %error.phase,
        error = %error.kind,
        "startup failed"
    );
}

// ── Helper constructors ────────────────────────────────────────────

impl StartupError {
    #[must_use]
    pub fn config_load(source: ConfigLoadError) -> Self {
        Self::new(StartupPhase::LoadConfig, source.into())
    }

    #[must_use]
    pub fn config_validation(source: ConfigValidationError) -> Self {
        Self::new(StartupPhase::ValidateConfig, source.into())
    }

    pub fn tls_context(message: impl Into<String>) -> Self {
        Self::new(
            StartupPhase::BuildTlsContext,
            StartupErrorKind::TlsContext(message.into()),
        )
    }

    #[must_use]
    pub fn bind(addr: SocketAddr, source: io::Error) -> Self {
        Self::new(
            StartupPhase::BindListener,
            StartupErrorKind::Bind { addr, source },
        )
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::new(
            StartupPhase::StartServer,
            StartupErrorKind::Runtime(message.into()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn check_tls_file_ok() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("cert.pem");
        fs::write(&file, b"not-a-real-cert").unwrap();
        assert_eq!(check_tls_file(file.to_str().unwrap()), TlsFileStatus::Ok);
    }

    #[test]
    fn check_tls_file_not_found() {
        assert_eq!(
            check_tls_file("/tmp/does-not-exist-12345.pem"),
            TlsFileStatus::NotFound
        );
    }

    #[test]
    fn check_tls_file_not_regular() {
        // A directory is not a regular file.
        let dir = TempDir::new().unwrap();
        assert_eq!(
            check_tls_file(dir.path().to_str().unwrap()),
            TlsFileStatus::NotRegular
        );
    }

    #[test]
    fn preflight_passes_for_http_mode() {
        let config = Config::default(); // transport.mode = Http, no TLS paths
        assert!(preflight_tls_files(&config).is_ok());
    }

    #[test]
    fn preflight_fails_for_missing_cert() {
        let mut config = Config::default();
        config.transport.tls.cert_path = Some("/tmp/ds-nonexistent-cert-12345.pem".to_string());
        let err = preflight_tls_files(&config).unwrap_err();
        assert_eq!(err.phase, StartupPhase::CheckTlsFiles);
        assert!(
            matches!(&err.kind, StartupErrorKind::TlsFileNotFound { path }
                if path.contains("nonexistent"))
        );
    }

    #[test]
    fn preflight_fails_for_directory_as_cert() {
        let dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.transport.tls.cert_path = Some(dir.path().to_str().unwrap().to_string());
        let err = preflight_tls_files(&config).unwrap_err();
        assert_eq!(err.phase, StartupPhase::CheckTlsFiles);
        assert!(matches!(
            &err.kind,
            StartupErrorKind::TlsFileNotRegular { .. }
        ));
    }

    #[test]
    fn startup_error_display_includes_phase() {
        let err = StartupError::new(
            StartupPhase::CheckTlsFiles,
            StartupErrorKind::TlsFileNotFound {
                path: "/etc/ssl/missing.pem".to_string(),
            },
        );
        let msg = err.to_string();
        assert!(msg.contains("check_tls_files"), "got: {msg}");
        assert!(msg.contains("missing.pem"), "got: {msg}");
    }

    #[test]
    fn startup_error_preserves_config_validation_cause() {
        let validation_err = ConfigValidationError::MaxMemoryBytesTooSmall;
        let err = StartupError::config_validation(validation_err);
        assert_eq!(err.phase, StartupPhase::ValidateConfig);
        let msg = err.to_string();
        assert!(msg.contains("validate_config"), "got: {msg}");
        assert!(msg.contains("max_memory_bytes"), "got: {msg}");
    }

    #[test]
    fn startup_phase_display() {
        assert_eq!(StartupPhase::LoadConfig.to_string(), "load_config");
        assert_eq!(StartupPhase::ValidateConfig.to_string(), "validate_config");
        assert_eq!(
            StartupPhase::ResolveTransport.to_string(),
            "resolve_transport"
        );
        assert_eq!(StartupPhase::CheckTlsFiles.to_string(), "check_tls_files");
        assert_eq!(
            StartupPhase::BuildTlsContext.to_string(),
            "build_tls_context"
        );
        assert_eq!(StartupPhase::BindListener.to_string(), "bind_listener");
        assert_eq!(StartupPhase::StartServer.to_string(), "start_server");
    }

    #[test]
    fn bind_tcp_listener_returns_bind_phase_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let err = bind_tcp_listener(addr).unwrap_err();
        assert_eq!(err.phase, StartupPhase::BindListener);
        assert!(matches!(&err.kind, StartupErrorKind::Bind { addr: bound, .. } if *bound == addr));
    }
}
