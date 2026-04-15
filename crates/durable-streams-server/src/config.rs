//! Configuration loading and runtime settings for the server crate.
//!
//! [`Config`] is the resolved runtime view. Use [`Config::from_sources`] for the
//! layered TOML plus environment-variable flow used by the binary, or
//! [`Config::from_env`] when tests only need the `DS_*` override surface.

use crate::router::DEFAULT_STREAM_BASE_PATH;
use axum::http::{HeaderName, HeaderValue};
use figment::{
    Figment,
    providers::{Format, Toml},
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

/// Storage backend selection for the server runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageMode {
    /// In-memory backend.
    Memory,
    /// File backend without fsync/fdatasync on every append.
    #[serde(alias = "fast")]
    FileFast,
    /// File backend with fsync/fdatasync on every append.
    #[serde(alias = "file", alias = "durable")]
    FileDurable,
    /// ACID backend using sharded redb databases.
    #[serde(alias = "redb")]
    Acid,
}

/// Redb storage backend used by the [`StorageMode::Acid`] mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AcidBackend {
    /// File-backed redb (default). Data persists across restarts.
    File,
    /// In-memory redb. Provides ACID transactions without disk I/O; all data
    /// is lost on shutdown.
    #[serde(alias = "memory", alias = "inmemory")]
    InMemory,
}

impl AcidBackend {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::InMemory => "in-memory",
        }
    }
}

impl fmt::Display for StorageMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for TransportMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for HttpVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for AlpnProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl StorageMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::FileFast => "file-fast",
            Self::FileDurable => "file-durable",
            Self::Acid => "acid",
        }
    }

    #[must_use]
    pub fn uses_file_backend(self) -> bool {
        matches!(self, Self::FileFast | Self::FileDurable)
    }

    #[must_use]
    pub fn sync_on_append(self) -> bool {
        matches!(self, Self::FileDurable)
    }
}

/// Transport mode selected for the listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransportMode {
    /// Plain HTTP transport.
    Http,
    /// Server-side TLS.
    Tls,
    /// Mutual TLS.
    Mtls,
}

impl TransportMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Tls => "tls",
            Self::Mtls => "mtls",
        }
    }

    #[must_use]
    pub fn uses_tls(self) -> bool {
        matches!(self, Self::Tls | Self::Mtls)
    }
}

/// Operator-facing HTTP protocol versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpVersion {
    /// HTTP/1.1
    #[serde(
        rename = "http1",
        alias = "1.1",
        alias = "http1.1",
        alias = "http/1.1",
        alias = "h1"
    )]
    Http1,
    /// HTTP/2
    #[serde(rename = "http2", alias = "2", alias = "h2")]
    Http2,
}

impl HttpVersion {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
        }
    }
}

/// TLS protocol versions accepted by the config layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TlsVersion {
    /// TLS 1.2
    #[serde(rename = "1.2", alias = "tls1.2", alias = "tls-1.2")]
    V1_2,
    /// TLS 1.3
    #[serde(rename = "1.3", alias = "tls1.3", alias = "tls-1.3")]
    V1_3,
}

impl TlsVersion {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1_2 => "1.2",
            Self::V1_3 => "1.3",
        }
    }
}

/// ALPN protocols used when TLS is enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlpnProtocol {
    /// HTTP/1.1 ALPN identifier.
    #[serde(rename = "http/1.1", alias = "http1", alias = "h1")]
    Http1_1,
    /// HTTP/2 ALPN identifier.
    #[serde(rename = "h2", alias = "http2")]
    H2,
}

impl AlpnProtocol {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http1_1 => "http/1.1",
            Self::H2 => "h2",
        }
    }
}

/// Forwarded-header family trusted from a proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForwardedHeadersMode {
    /// Do not trust any proxy headers.
    #[serde(rename = "none")]
    None,
    /// Trust the `X-Forwarded-*` header family.
    #[serde(rename = "x-forwarded", alias = "xforwarded")]
    XForwarded,
    /// Trust RFC 7239 `Forwarded`.
    #[serde(rename = "forwarded")]
    Forwarded,
}

/// Proxy-origin identity handoff strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProxyIdentityMode {
    /// No proxy identity handoff.
    None,
    /// Trust a single HTTP header from the proxy.
    Header,
}

/// Typed profile selection for config loading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentProfile {
    Default,
    Dev,
    Prod,
    ProdTls,
    ProdMtls,
    Named(String),
}

impl DeploymentProfile {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Default => "default",
            Self::Dev => "dev",
            Self::Prod => "prod",
            Self::ProdTls => "prod-tls",
            Self::ProdMtls => "prod-mtls",
            Self::Named(name) => name.as_str(),
        }
    }
}

impl From<&str> for DeploymentProfile {
    fn from(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "default" => Self::Default,
            "dev" => Self::Dev,
            "prod" => Self::Prod,
            "prod-tls" => Self::ProdTls,
            "prod-mtls" => Self::ProdMtls,
            other => Self::Named(other.to_string()),
        }
    }
}

impl From<String> for DeploymentProfile {
    fn from(raw: String) -> Self {
        Self::from(raw.as_str())
    }
}

/// Server configuration resolved after all layering and defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Config {
    /// Listener settings.
    pub server: ServerConfig,
    /// Stream and storage size limits.
    pub limits: LimitsConfig,
    /// HTTP protocol surface configuration.
    pub http: HttpConfig,
    /// Persistence backend configuration.
    pub storage: StorageConfig,
    /// Transport and connection behaviour.
    pub transport: TransportConfig,
    /// Reverse-proxy trust and identity handoff.
    pub proxy: ProxyConfig,
    /// Logging and tracing defaults.
    pub observability: ObservabilityConfig,
}

/// Listener settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServerConfig {
    /// Socket address to bind, e.g. `0.0.0.0:4437`.
    pub bind_address: String,
}

/// Limits enforced by the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LimitsConfig {
    /// Maximum total in-process payload bytes across all streams.
    pub max_memory_bytes: u64,
    /// Maximum payload bytes retained for any single stream.
    pub max_stream_bytes: u64,
    /// Maximum byte length of a stream name.
    pub max_stream_name_bytes: usize,
    /// Maximum number of `/`-separated segments in a stream name.
    pub max_stream_name_segments: usize,
}

/// HTTP surface configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HttpConfig {
    /// CORS allowlist as `"*"` or a comma-separated origin list.
    pub cors_origins: String,
    /// Mount path for the protocol HTTP surface.
    pub stream_base_path: String,
}

/// Persistence configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StorageConfig {
    /// Selected persistence backend.
    pub mode: StorageMode,
    /// Root directory for file-backed and acid-backed storage.
    pub data_dir: String,
    /// Number of shards used by the acid/redb backend.
    pub acid_shard_count: usize,
    /// Redb backend selection for the acid storage mode.
    pub acid_backend: AcidBackend,
}

/// Transport configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportConfig {
    /// HTTP / TLS / mTLS transport mode.
    pub mode: TransportMode,
    /// HTTP protocol version settings.
    pub http: TransportHttpConfig,
    /// TLS-related settings.
    pub tls: TransportTlsConfig,
    /// Connection behaviour shared by live-read endpoints.
    pub connection: TransportConnectionConfig,
}

/// HTTP version settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportHttpConfig {
    /// Enabled HTTP protocol versions.
    pub versions: Vec<HttpVersion>,
}

/// TLS-related settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportTlsConfig {
    /// Optional server certificate path in PEM format.
    pub cert_path: Option<String>,
    /// Optional server private key path in PEM or PKCS#8 format.
    pub key_path: Option<String>,
    /// Optional client CA bundle for mTLS validation.
    pub client_ca_path: Option<String>,
    /// Minimum accepted TLS protocol version.
    pub min_version: TlsVersion,
    /// Maximum accepted TLS protocol version.
    pub max_version: TlsVersion,
    /// Negotiated ALPN protocols when TLS is enabled.
    pub alpn_protocols: Vec<AlpnProtocol>,
}

impl TransportTlsConfig {
    #[must_use]
    pub fn has_server_credentials(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }
}

/// Connection-level settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransportConnectionConfig {
    /// Long-poll timeout used by `GET ?live=long-poll`.
    pub long_poll_timeout_secs: u64,
    /// SSE reconnect interval in seconds (`0` disables forced reconnects).
    pub sse_reconnect_interval_secs: u64,
}

/// Reverse-proxy trust model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProxyConfig {
    /// Whether proxy header trust is enabled.
    pub enabled: bool,
    /// Header family trusted from the proxy.
    pub forwarded_headers: ForwardedHeadersMode,
    /// Trusted proxy IPs or CIDRs.
    pub trusted_proxies: Vec<String>,
    /// Proxy-origin identity settings.
    pub identity: ProxyIdentityConfig,
}

/// Proxy-origin identity settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProxyIdentityConfig {
    /// Identity handoff mode.
    pub mode: ProxyIdentityMode,
    /// Trusted header carrying the identity.
    pub header_name: Option<String>,
    /// Whether the proxy identity handoff requires TLS on the outer hop.
    pub require_tls: bool,
}

/// Logging and tracing defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservabilityConfig {
    /// Default tracing filter used when `RUST_LOG` is not explicitly set.
    pub rust_log: String,
}

#[derive(Debug, Clone)]
pub struct ConfigLoadOptions {
    /// Directory containing `default.toml`, `<profile>.toml`, and `local.toml`.
    pub config_dir: PathBuf,
    /// Named profile loaded after `default.toml`, for example `dev` or `prod-tls`.
    pub profile: DeploymentProfile,
    /// Optional extra TOML file merged after the standard config files.
    pub config_override: Option<PathBuf>,
}

impl Default for ConfigLoadOptions {
    fn default() -> Self {
        Self {
            config_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config"),
            profile: DeploymentProfile::Default,
            config_override: None,
        }
    }
}

/// Errors raised while loading config sources.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigLoadError {
    #[error("config override file not found: '{path}'")]
    OverrideFileNotFound { path: PathBuf },
    #[error("failed to parse TOML config: {message}")]
    TomlParse { message: String },
    #[error("invalid {input_source} value for {key}: '{value}' ({reason})")]
    InvalidValue {
        input_source: &'static str,
        key: &'static str,
        value: String,
        reason: String,
    },
}

/// Typed validation errors raised before startup.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigValidationError {
    #[error("server.bind_address is invalid: '{value}' ({reason})")]
    InvalidBindAddress { value: String, reason: String },
    #[error("http.stream_base_path is invalid: '{value}' ({reason})")]
    InvalidStreamBasePath { value: String, reason: String },
    #[error("http.cors_origins contains an empty origin entry")]
    EmptyCorsOrigin,
    #[error("http.cors_origins entry is invalid: '{value}'")]
    InvalidCorsOrigin { value: String },
    #[error("limits.max_memory_bytes must be at least 1")]
    MaxMemoryBytesTooSmall,
    #[error("limits.max_stream_bytes must be at least 1")]
    MaxStreamBytesTooSmall,
    #[error("limits.max_stream_name_bytes must be at least 1")]
    MaxStreamNameBytesTooSmall,
    #[error("limits.max_stream_name_segments must be at least 1")]
    MaxStreamNameSegmentsTooSmall,
    #[error("storage.data_dir must be a non-empty path when storage.mode is '{mode}'")]
    EmptyStorageDataDir { mode: StorageMode },
    #[error(
        "storage.acid_shard_count must be a power of two in 1..=256 when storage.mode is 'acid'"
    )]
    InvalidAcidShardCount,
    #[error("transport.connection.long_poll_timeout_secs must be at least 1")]
    LongPollTimeoutTooSmall,
    #[error("transport.http.versions must include at least one version")]
    EmptyHttpVersions,
    #[error("transport.mode='http' does not support transport.http.versions containing http2")]
    HttpModeDoesNotSupportHttp2,
    #[error("transport.tls.min_version must be less than or equal to transport.tls.max_version")]
    InvalidTlsVersionRange,
    #[error("transport.mode='{mode}' requires transport.tls.{field}")]
    MissingTlsField {
        mode: TransportMode,
        field: &'static str,
    },
    #[error("transport.mode='http' cannot be combined with transport.tls.{field}")]
    HttpModeDisallowsTlsField { field: &'static str },
    #[error("transport.mode='tls' cannot be combined with transport.tls.client_ca_path")]
    ClientCaRequiresMtls,
    #[error("transport.tls.{field} must be a non-empty path when set")]
    EmptyPath { field: &'static str },
    #[error(
        "transport.http.versions includes '{version}', but transport.tls.alpn_protocols is missing '{alpn}'"
    )]
    MissingAlpnProtocol {
        version: HttpVersion,
        alpn: AlpnProtocol,
    },
    #[error(
        "transport.tls.alpn_protocols includes '{alpn}', but transport.http.versions does not enable the matching HTTP version"
    )]
    UnexpectedAlpnProtocol { alpn: AlpnProtocol },
    #[error(
        "proxy.enabled=true requires proxy.forwarded_headers to be set to 'x-forwarded' or 'forwarded'"
    )]
    ProxyEnabledRequiresForwardedHeaders,
    #[error("proxy.enabled=true requires at least one entry in proxy.trusted_proxies")]
    ProxyEnabledRequiresTrustedProxies,
    #[error("proxy.enabled=false cannot be combined with proxy.trusted_proxies")]
    ProxyDisabledDisallowsTrustedProxies,
    #[error("proxy.enabled=false cannot be combined with proxy.forwarded_headers='{mode:?}'")]
    ProxyDisabledDisallowsForwardedHeaders { mode: ForwardedHeadersMode },
    #[error("proxy.enabled=false cannot be combined with proxy.identity.mode='{mode:?}'")]
    ProxyDisabledDisallowsIdentityMode { mode: ProxyIdentityMode },
    #[error("proxy.enabled=false cannot be combined with proxy.identity.header_name")]
    ProxyDisabledDisallowsIdentityHeader,
    #[error("proxy.trusted_proxies entry is invalid: '{value}'")]
    InvalidTrustedProxy { value: String },
    #[error("proxy.identity.mode='header' requires proxy.identity.header_name")]
    HeaderIdentityRequiresHeaderName,
    #[error("proxy.identity.mode='header' requires transport.mode='mtls'")]
    HeaderIdentityRequiresMtls,
    #[error("proxy.identity.mode='none' cannot be combined with proxy.identity.header_name")]
    IdentityHeaderRequiresHeaderMode,
    #[error("proxy.identity.header_name is invalid: '{value}'")]
    InvalidIdentityHeaderName { value: String },
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ConfigPatch {
    server: ServerConfigPatch,
    limits: LimitsConfigPatch,
    http: HttpConfigPatch,
    storage: StorageConfigPatch,
    transport: TransportConfigPatch,
    proxy: ProxyConfigPatch,
    observability: ObservabilityConfigPatch,
    tls: LegacyTlsPatch,
    log: LegacyLogPatch,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ServerConfigPatch {
    bind_address: Option<String>,
    port: Option<u16>,
    long_poll_timeout_secs: Option<u64>,
    sse_reconnect_interval_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
#[allow(clippy::struct_field_names)]
struct LimitsConfigPatch {
    max_memory_bytes: Option<u64>,
    max_stream_bytes: Option<u64>,
    max_stream_name_bytes: Option<usize>,
    max_stream_name_segments: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct HttpConfigPatch {
    cors_origins: Option<String>,
    stream_base_path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct StorageConfigPatch {
    mode: Option<StorageMode>,
    data_dir: Option<String>,
    acid_shard_count: Option<usize>,
    acid_backend: Option<AcidBackend>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct TransportConfigPatch {
    mode: Option<TransportMode>,
    http: TransportHttpConfigPatch,
    tls: TransportTlsConfigPatch,
    connection: TransportConnectionConfigPatch,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct TransportHttpConfigPatch {
    versions: Option<Vec<HttpVersion>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct TransportTlsConfigPatch {
    cert_path: Option<String>,
    key_path: Option<String>,
    client_ca_path: Option<String>,
    min_version: Option<TlsVersion>,
    max_version: Option<TlsVersion>,
    alpn_protocols: Option<Vec<AlpnProtocol>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct TransportConnectionConfigPatch {
    long_poll_timeout_secs: Option<u64>,
    sse_reconnect_interval_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ProxyConfigPatch {
    enabled: Option<bool>,
    forwarded_headers: Option<ForwardedHeadersMode>,
    trusted_proxies: Option<Vec<String>>,
    identity: ProxyIdentityConfigPatch,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ProxyIdentityConfigPatch {
    mode: Option<ProxyIdentityMode>,
    header_name: Option<String>,
    require_tls: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ObservabilityConfigPatch {
    rust_log: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct LegacyTlsPatch {
    cert_path: Option<String>,
    key_path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct LegacyLogPatch {
    rust_log: Option<String>,
}

#[derive(Debug, Default)]
struct MergeContext {
    explicit_transport_mode: bool,
    legacy_tls_seen: bool,
}

impl Config {
    /// Load configuration from `DS_*` environment variables with sensible defaults.
    ///
    /// Used by tests and as a simple entry point when TOML layering is not needed.
    ///
    /// # Errors
    ///
    /// Returns an error when any `DS_*` environment variable is present but invalid.
    pub fn from_env() -> Result<Self, ConfigLoadError> {
        let mut config = Self::default();
        let mut ctx = MergeContext::default();
        config.apply_env_overrides(&|key| env::var(key).ok(), &mut ctx)?;
        ctx.finalize(&mut config);
        Ok(config)
    }

    /// Load configuration from layered TOML files plus environment overrides.
    ///
    /// Order (later wins):
    /// 1. built-in defaults
    /// 2. built-in profile defaults
    /// 3. `config/default.toml` (if present)
    /// 4. `config/<profile>.toml` (if present)
    /// 5. `config/local.toml` (if present)
    /// 6. `--config <path>` override file (if provided)
    /// 7. `DS_*` env vars
    ///
    /// # Errors
    ///
    /// Returns an error when config files cannot be parsed or an explicit
    /// override file path does not exist/read.
    pub fn from_sources(options: &ConfigLoadOptions) -> Result<Self, ConfigLoadError> {
        let get = |key: &str| env::var(key).ok();
        Self::from_sources_with_lookup(options, &get)
    }

    fn from_sources_with_lookup(
        options: &ConfigLoadOptions,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigLoadError> {
        let mut config = Self::default();
        let mut ctx = MergeContext::default();

        if let Some(profile_patch) = built_in_profile_patch(&options.profile) {
            if profile_patch.transport.mode.is_some() {
                ctx.explicit_transport_mode = true;
            }
            config.apply_patch(profile_patch, &mut ctx);
        }

        let default_path = options.config_dir.join("default.toml");
        if default_path.is_file() {
            let patch = extract_toml_patch(&default_path)?;
            config.apply_patch(patch, &mut ctx);
        }

        let profile_path = options
            .config_dir
            .join(format!("{}.toml", options.profile.as_str()));
        if profile_path.is_file() {
            let patch = extract_toml_patch(&profile_path)?;
            config.apply_patch(patch, &mut ctx);
        }

        let local_path = options.config_dir.join("local.toml");
        if local_path.is_file() {
            let patch = extract_toml_patch(&local_path)?;
            config.apply_patch(patch, &mut ctx);
        }

        if let Some(override_path) = &options.config_override {
            if !override_path.is_file() {
                return Err(ConfigLoadError::OverrideFileNotFound {
                    path: override_path.clone(),
                });
            }
            let patch = extract_toml_patch(override_path)?;
            config.apply_patch(patch, &mut ctx);
        }

        config.apply_env_overrides(get, &mut ctx)?;
        ctx.finalize(&mut config);
        Ok(config)
    }

    fn apply_patch(&mut self, patch: ConfigPatch, ctx: &mut MergeContext) {
        if let Some(bind_address) = patch.server.bind_address {
            self.server.bind_address = bind_address;
        } else if let Some(port) = patch.server.port {
            self.server.bind_address = format!("0.0.0.0:{port}");
        }

        if let Some(max_memory_bytes) = patch.limits.max_memory_bytes {
            self.limits.max_memory_bytes = max_memory_bytes;
        }
        if let Some(max_stream_bytes) = patch.limits.max_stream_bytes {
            self.limits.max_stream_bytes = max_stream_bytes;
        }
        if let Some(max_stream_name_bytes) = patch.limits.max_stream_name_bytes {
            self.limits.max_stream_name_bytes = max_stream_name_bytes;
        }
        if let Some(max_stream_name_segments) = patch.limits.max_stream_name_segments {
            self.limits.max_stream_name_segments = max_stream_name_segments;
        }

        if let Some(cors_origins) = patch.http.cors_origins {
            self.http.cors_origins = cors_origins;
        }
        if let Some(stream_base_path) = patch.http.stream_base_path {
            self.http.stream_base_path = stream_base_path;
        }

        if let Some(mode) = patch.storage.mode {
            self.storage.mode = mode;
        }
        if let Some(data_dir) = patch.storage.data_dir {
            self.storage.data_dir = data_dir;
        }
        if let Some(acid_shard_count) = patch.storage.acid_shard_count {
            self.storage.acid_shard_count = acid_shard_count;
        }
        if let Some(acid_backend) = patch.storage.acid_backend {
            self.storage.acid_backend = acid_backend;
        }

        if let Some(mode) = patch.transport.mode {
            self.transport.mode = mode;
            ctx.explicit_transport_mode = true;
        }
        if let Some(versions) = patch.transport.http.versions {
            self.transport.http.versions = versions;
            self.transport.tls.alpn_protocols =
                default_alpn_protocols(&self.transport.http.versions);
        }

        let legacy_tls_cert_path = patch.tls.cert_path;
        let legacy_tls_key_path = patch.tls.key_path;
        let saw_legacy_tls = legacy_tls_cert_path.is_some() || legacy_tls_key_path.is_some();
        let tls_cert_path = patch.transport.tls.cert_path.or(legacy_tls_cert_path);
        let tls_key_path = patch.transport.tls.key_path.or(legacy_tls_key_path);
        if tls_cert_path.is_some() || tls_key_path.is_some() {
            ctx.legacy_tls_seen |= saw_legacy_tls;
        }
        if let Some(cert_path) = tls_cert_path {
            self.transport.tls.cert_path = Some(cert_path);
        }
        if let Some(key_path) = tls_key_path {
            self.transport.tls.key_path = Some(key_path);
        }
        if let Some(client_ca_path) = patch.transport.tls.client_ca_path {
            self.transport.tls.client_ca_path = Some(client_ca_path);
        }
        if let Some(min_version) = patch.transport.tls.min_version {
            self.transport.tls.min_version = min_version;
        }
        if let Some(max_version) = patch.transport.tls.max_version {
            self.transport.tls.max_version = max_version;
        }
        if let Some(alpn_protocols) = patch.transport.tls.alpn_protocols {
            self.transport.tls.alpn_protocols = alpn_protocols;
        }

        let long_poll_timeout_secs = patch
            .transport
            .connection
            .long_poll_timeout_secs
            .or(patch.server.long_poll_timeout_secs);
        if let Some(long_poll_timeout_secs) = long_poll_timeout_secs {
            self.transport.connection.long_poll_timeout_secs = long_poll_timeout_secs;
        }

        let sse_reconnect_interval_secs = patch
            .transport
            .connection
            .sse_reconnect_interval_secs
            .or(patch.server.sse_reconnect_interval_secs);
        if let Some(sse_reconnect_interval_secs) = sse_reconnect_interval_secs {
            self.transport.connection.sse_reconnect_interval_secs = sse_reconnect_interval_secs;
        }

        if let Some(enabled) = patch.proxy.enabled {
            self.proxy.enabled = enabled;
        }
        if let Some(forwarded_headers) = patch.proxy.forwarded_headers {
            self.proxy.forwarded_headers = forwarded_headers;
        }
        if let Some(trusted_proxies) = patch.proxy.trusted_proxies {
            self.proxy.trusted_proxies = trusted_proxies;
        }
        if let Some(mode) = patch.proxy.identity.mode {
            self.proxy.identity.mode = mode;
        }
        if let Some(header_name) = patch.proxy.identity.header_name {
            self.proxy.identity.header_name = Some(header_name);
        }
        if let Some(require_tls) = patch.proxy.identity.require_tls {
            self.proxy.identity.require_tls = require_tls;
        }

        let rust_log = patch.observability.rust_log.or(patch.log.rust_log);
        if let Some(rust_log) = rust_log {
            self.observability.rust_log = rust_log;
        }
    }

    /// Apply `DS_*` environment variable overrides on top of current config.
    fn apply_env_overrides(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
        ctx: &mut MergeContext,
    ) -> Result<(), ConfigLoadError> {
        if let Some(bind_address) = get("DS_SERVER__BIND_ADDRESS") {
            self.server.bind_address = bind_address;
        } else if let Some(port) = parse_env::<u16>(get, "DS_SERVER__PORT")? {
            self.server.bind_address = format!("0.0.0.0:{port}");
        }

        if let Some(long_poll_timeout_secs) =
            parse_env::<u64>(get, "DS_TRANSPORT__CONNECTION__LONG_POLL_TIMEOUT_SECS")?
                .or(parse_env::<u64>(get, "DS_SERVER__LONG_POLL_TIMEOUT_SECS")?)
        {
            self.transport.connection.long_poll_timeout_secs = long_poll_timeout_secs;
        }

        if let Some(sse_reconnect_interval_secs) =
            parse_env::<u64>(get, "DS_TRANSPORT__CONNECTION__SSE_RECONNECT_INTERVAL_SECS")?.or(
                parse_env::<u64>(get, "DS_SERVER__SSE_RECONNECT_INTERVAL_SECS")?,
            )
        {
            self.transport.connection.sse_reconnect_interval_secs = sse_reconnect_interval_secs;
        }

        if let Some(max_memory_bytes) = parse_env::<u64>(get, "DS_LIMITS__MAX_MEMORY_BYTES")? {
            self.limits.max_memory_bytes = max_memory_bytes;
        }
        if let Some(max_stream_bytes) = parse_env::<u64>(get, "DS_LIMITS__MAX_STREAM_BYTES")? {
            self.limits.max_stream_bytes = max_stream_bytes;
        }
        if let Some(max_stream_name_bytes) =
            parse_env::<usize>(get, "DS_LIMITS__MAX_STREAM_NAME_BYTES")?
        {
            self.limits.max_stream_name_bytes = max_stream_name_bytes;
        }
        if let Some(max_stream_name_segments) =
            parse_env::<usize>(get, "DS_LIMITS__MAX_STREAM_NAME_SEGMENTS")?
        {
            self.limits.max_stream_name_segments = max_stream_name_segments;
        }

        if let Some(cors_origins) = get("DS_HTTP__CORS_ORIGINS") {
            self.http.cors_origins = cors_origins;
        }
        if let Some(stream_base_path) = get("DS_HTTP__STREAM_BASE_PATH") {
            self.http.stream_base_path = stream_base_path;
        }

        if let Some(storage_mode) = parse_env_with(get, "DS_STORAGE__MODE", parse_storage_mode_env)?
        {
            self.storage.mode = storage_mode;
        }
        if let Some(data_dir) = get("DS_STORAGE__DATA_DIR") {
            self.storage.data_dir = data_dir;
        }
        if let Some(acid_shard_count) = parse_env::<usize>(get, "DS_STORAGE__ACID_SHARD_COUNT")? {
            self.storage.acid_shard_count = acid_shard_count;
        }
        if let Some(acid_backend) =
            parse_env_with(get, "DS_STORAGE__ACID_BACKEND", parse_acid_backend_env)?
        {
            self.storage.acid_backend = acid_backend;
        }

        if let Some(mode) = parse_env_with(get, "DS_TRANSPORT__MODE", parse_transport_mode_env)? {
            self.transport.mode = mode;
            ctx.explicit_transport_mode = true;
        }
        if let Some(versions) =
            parse_env_list_with(get, "DS_TRANSPORT__HTTP__VERSIONS", parse_http_version_env)?
        {
            self.transport.http.versions = versions;
            self.transport.tls.alpn_protocols =
                default_alpn_protocols(&self.transport.http.versions);
        }

        let tls_cert_path =
            get("DS_TRANSPORT__TLS__CERT_PATH").or_else(|| get("DS_TLS__CERT_PATH"));
        let tls_key_path = get("DS_TRANSPORT__TLS__KEY_PATH").or_else(|| get("DS_TLS__KEY_PATH"));
        if get("DS_TLS__CERT_PATH").is_some() || get("DS_TLS__KEY_PATH").is_some() {
            ctx.legacy_tls_seen = true;
        }
        if let Some(cert_path) = tls_cert_path {
            self.transport.tls.cert_path = Some(cert_path);
        }
        if let Some(key_path) = tls_key_path {
            self.transport.tls.key_path = Some(key_path);
        }
        if let Some(client_ca_path) = get("DS_TRANSPORT__TLS__CLIENT_CA_PATH") {
            self.transport.tls.client_ca_path = Some(client_ca_path);
        }
        if let Some(min_version) =
            parse_env_with(get, "DS_TRANSPORT__TLS__MIN_VERSION", parse_tls_version_env)?
        {
            self.transport.tls.min_version = min_version;
        }
        if let Some(max_version) =
            parse_env_with(get, "DS_TRANSPORT__TLS__MAX_VERSION", parse_tls_version_env)?
        {
            self.transport.tls.max_version = max_version;
        }
        if let Some(alpn_protocols) = parse_env_list_with(
            get,
            "DS_TRANSPORT__TLS__ALPN_PROTOCOLS",
            parse_alpn_protocol_env,
        )? {
            self.transport.tls.alpn_protocols = alpn_protocols;
        }

        if let Some(enabled) = parse_env::<bool>(get, "DS_PROXY__ENABLED")? {
            self.proxy.enabled = enabled;
        }
        if let Some(forwarded_headers) = parse_env_with(
            get,
            "DS_PROXY__FORWARDED_HEADERS",
            parse_forwarded_headers_mode_env,
        )? {
            self.proxy.forwarded_headers = forwarded_headers;
        }
        if let Some(trusted_proxies) = parse_env_csv_strings(get, "DS_PROXY__TRUSTED_PROXIES")? {
            self.proxy.trusted_proxies = trusted_proxies;
        }
        if let Some(mode) = parse_env_with(
            get,
            "DS_PROXY__IDENTITY__MODE",
            parse_proxy_identity_mode_env,
        )? {
            self.proxy.identity.mode = mode;
        }
        if let Some(header_name) = get("DS_PROXY__IDENTITY__HEADER_NAME") {
            self.proxy.identity.header_name = Some(header_name);
        }
        if let Some(require_tls) = parse_env::<bool>(get, "DS_PROXY__IDENTITY__REQUIRE_TLS")? {
            self.proxy.identity.require_tls = require_tls;
        }

        if let Some(rust_log) =
            get("DS_OBSERVABILITY__RUST_LOG").or_else(|| get("DS_LOG__RUST_LOG"))
        {
            self.observability.rust_log = rust_log;
        }

        Ok(())
    }

    /// Validate configuration invariants before server startup.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when config is internally inconsistent.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        validate_socket_addr(&self.server.bind_address)?;
        validate_cors_origins(&self.http.cors_origins)?;
        validate_stream_base_path(&self.http.stream_base_path)?;

        if self.limits.max_memory_bytes == 0 {
            return Err(ConfigValidationError::MaxMemoryBytesTooSmall);
        }
        if self.limits.max_stream_bytes == 0 {
            return Err(ConfigValidationError::MaxStreamBytesTooSmall);
        }
        if self.limits.max_stream_name_bytes == 0 {
            return Err(ConfigValidationError::MaxStreamNameBytesTooSmall);
        }
        if self.limits.max_stream_name_segments == 0 {
            return Err(ConfigValidationError::MaxStreamNameSegmentsTooSmall);
        }

        if self.storage.data_dir.trim().is_empty() {
            return Err(ConfigValidationError::EmptyStorageDataDir {
                mode: self.storage.mode,
            });
        }
        if self.storage.mode == StorageMode::Acid
            && !valid_acid_shard_count(self.storage.acid_shard_count)
        {
            return Err(ConfigValidationError::InvalidAcidShardCount);
        }

        if self.transport.connection.long_poll_timeout_secs == 0 {
            return Err(ConfigValidationError::LongPollTimeoutTooSmall);
        }

        if self.transport.http.versions.is_empty() {
            return Err(ConfigValidationError::EmptyHttpVersions);
        }
        if self.transport.mode == TransportMode::Http
            && self.transport.http.versions.contains(&HttpVersion::Http2)
        {
            return Err(ConfigValidationError::HttpModeDoesNotSupportHttp2);
        }
        if self.transport.tls.min_version > self.transport.tls.max_version {
            return Err(ConfigValidationError::InvalidTlsVersionRange);
        }

        for (field, value) in [
            ("cert_path", self.transport.tls.cert_path.as_deref()),
            ("key_path", self.transport.tls.key_path.as_deref()),
            (
                "client_ca_path",
                self.transport.tls.client_ca_path.as_deref(),
            ),
        ] {
            if matches!(value, Some(path) if path.trim().is_empty()) {
                return Err(ConfigValidationError::EmptyPath { field });
            }
        }

        match self.transport.mode {
            TransportMode::Http => {
                if self.transport.tls.cert_path.is_some() {
                    return Err(ConfigValidationError::HttpModeDisallowsTlsField {
                        field: "cert_path",
                    });
                }
                if self.transport.tls.key_path.is_some() {
                    return Err(ConfigValidationError::HttpModeDisallowsTlsField {
                        field: "key_path",
                    });
                }
                if self.transport.tls.client_ca_path.is_some() {
                    return Err(ConfigValidationError::HttpModeDisallowsTlsField {
                        field: "client_ca_path",
                    });
                }
            }
            TransportMode::Tls => {
                if self.transport.tls.cert_path.is_none() {
                    return Err(ConfigValidationError::MissingTlsField {
                        mode: self.transport.mode,
                        field: "cert_path",
                    });
                }
                if self.transport.tls.key_path.is_none() {
                    return Err(ConfigValidationError::MissingTlsField {
                        mode: self.transport.mode,
                        field: "key_path",
                    });
                }
                if self.transport.tls.client_ca_path.is_some() {
                    return Err(ConfigValidationError::ClientCaRequiresMtls);
                }
            }
            TransportMode::Mtls => {
                if self.transport.tls.cert_path.is_none() {
                    return Err(ConfigValidationError::MissingTlsField {
                        mode: self.transport.mode,
                        field: "cert_path",
                    });
                }
                if self.transport.tls.key_path.is_none() {
                    return Err(ConfigValidationError::MissingTlsField {
                        mode: self.transport.mode,
                        field: "key_path",
                    });
                }
                if self.transport.tls.client_ca_path.is_none() {
                    return Err(ConfigValidationError::MissingTlsField {
                        mode: self.transport.mode,
                        field: "client_ca_path",
                    });
                }
            }
        }

        let expected_alpn = default_alpn_protocols(&self.transport.http.versions);
        for (version, alpn) in expected_alpn.iter().map(|alpn| {
            let version = match alpn {
                AlpnProtocol::Http1_1 => HttpVersion::Http1,
                AlpnProtocol::H2 => HttpVersion::Http2,
            };
            (version, *alpn)
        }) {
            if !self.transport.tls.alpn_protocols.contains(&alpn) {
                return Err(ConfigValidationError::MissingAlpnProtocol { version, alpn });
            }
        }
        for alpn in &self.transport.tls.alpn_protocols {
            let expected_version = match alpn {
                AlpnProtocol::Http1_1 => HttpVersion::Http1,
                AlpnProtocol::H2 => HttpVersion::Http2,
            };
            if !self.transport.http.versions.contains(&expected_version) {
                return Err(ConfigValidationError::UnexpectedAlpnProtocol { alpn: *alpn });
            }
        }

        validate_proxy(self)?;
        Ok(())
    }

    /// True when direct TLS termination is enabled on this server.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.transport.mode.uses_tls() && self.transport.tls.has_server_credentials()
    }

    /// Parsed bind address used by the runtime.
    ///
    /// # Errors
    ///
    /// Returns the same validation error that [`Config::validate`] would emit
    /// for an invalid bind address.
    pub fn bind_socket_addr(&self) -> Result<SocketAddr, ConfigValidationError> {
        validate_socket_addr(&self.server.bind_address)
    }

    /// Long-poll timeout as a typed [`Duration`].
    #[must_use]
    pub fn long_poll_timeout(&self) -> Duration {
        Duration::from_secs(self.transport.connection.long_poll_timeout_secs)
    }

    /// Render the effective merged configuration as pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when the effective config cannot be serialized.
    pub fn render_effective_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl Default for Config {
    fn default() -> Self {
        let versions = vec![HttpVersion::Http1];
        Self {
            server: ServerConfig {
                bind_address: "0.0.0.0:4437".to_string(),
            },
            limits: LimitsConfig {
                max_memory_bytes: 100 * 1024 * 1024,
                max_stream_bytes: 10 * 1024 * 1024,
                max_stream_name_bytes: 1024,
                max_stream_name_segments: 8,
            },
            http: HttpConfig {
                cors_origins: "*".to_string(),
                stream_base_path: DEFAULT_STREAM_BASE_PATH.to_string(),
            },
            storage: StorageConfig {
                mode: StorageMode::Memory,
                data_dir: "./data/streams".to_string(),
                acid_shard_count: 16,
                acid_backend: AcidBackend::File,
            },
            transport: TransportConfig {
                mode: TransportMode::Http,
                http: TransportHttpConfig {
                    versions: versions.clone(),
                },
                tls: TransportTlsConfig {
                    cert_path: None,
                    key_path: None,
                    client_ca_path: None,
                    min_version: TlsVersion::V1_3,
                    max_version: TlsVersion::V1_3,
                    alpn_protocols: default_alpn_protocols(&versions),
                },
                connection: TransportConnectionConfig {
                    long_poll_timeout_secs: 30,
                    sse_reconnect_interval_secs: 60,
                },
            },
            proxy: ProxyConfig {
                enabled: false,
                forwarded_headers: ForwardedHeadersMode::None,
                trusted_proxies: Vec::new(),
                identity: ProxyIdentityConfig {
                    mode: ProxyIdentityMode::None,
                    header_name: None,
                    require_tls: true,
                },
            },
            observability: ObservabilityConfig {
                rust_log: "info".to_string(),
            },
        }
    }
}

/// Typed wrapper for long-poll timeout, injected via axum `Extension`.
#[derive(Debug, Clone, Copy)]
pub struct LongPollTimeout(pub Duration);

/// Typed wrapper for SSE reconnect interval in seconds (0 = disabled).
///
/// Matches Caddy's `sse_reconnect_interval`. Injected via axum `Extension`.
#[derive(Debug, Clone, Copy)]
pub struct SseReconnectInterval(pub u64);

fn built_in_profile_patch(profile: &DeploymentProfile) -> Option<ConfigPatch> {
    match profile {
        DeploymentProfile::Default => None,
        DeploymentProfile::Dev => Some(ConfigPatch {
            server: ServerConfigPatch {
                bind_address: Some("127.0.0.1:4437".to_string()),
                ..ServerConfigPatch::default()
            },
            observability: ObservabilityConfigPatch {
                rust_log: Some("debug".to_string()),
            },
            ..ConfigPatch::default()
        }),
        DeploymentProfile::Prod => Some(ConfigPatch {
            limits: LimitsConfigPatch {
                max_memory_bytes: Some(512 * 1024 * 1024),
                max_stream_bytes: Some(256 * 1024 * 1024),
                ..LimitsConfigPatch::default()
            },
            storage: StorageConfigPatch {
                mode: Some(StorageMode::FileDurable),
                data_dir: Some("/var/lib/durable-streams".to_string()),
                acid_shard_count: Some(16),
                ..StorageConfigPatch::default()
            },
            ..ConfigPatch::default()
        }),
        DeploymentProfile::ProdTls => Some(ConfigPatch {
            limits: LimitsConfigPatch {
                max_memory_bytes: Some(512 * 1024 * 1024),
                max_stream_bytes: Some(256 * 1024 * 1024),
                ..LimitsConfigPatch::default()
            },
            storage: StorageConfigPatch {
                mode: Some(StorageMode::FileDurable),
                data_dir: Some("/var/lib/durable-streams".to_string()),
                acid_shard_count: Some(16),
                ..StorageConfigPatch::default()
            },
            transport: TransportConfigPatch {
                mode: Some(TransportMode::Tls),
                http: TransportHttpConfigPatch {
                    versions: Some(vec![HttpVersion::Http1, HttpVersion::Http2]),
                },
                ..TransportConfigPatch::default()
            },
            ..ConfigPatch::default()
        }),
        DeploymentProfile::ProdMtls => Some(ConfigPatch {
            limits: LimitsConfigPatch {
                max_memory_bytes: Some(512 * 1024 * 1024),
                max_stream_bytes: Some(256 * 1024 * 1024),
                ..LimitsConfigPatch::default()
            },
            storage: StorageConfigPatch {
                mode: Some(StorageMode::FileDurable),
                data_dir: Some("/var/lib/durable-streams".to_string()),
                acid_shard_count: Some(16),
                ..StorageConfigPatch::default()
            },
            transport: TransportConfigPatch {
                mode: Some(TransportMode::Mtls),
                http: TransportHttpConfigPatch {
                    versions: Some(vec![HttpVersion::Http1, HttpVersion::Http2]),
                },
                ..TransportConfigPatch::default()
            },
            ..ConfigPatch::default()
        }),
        DeploymentProfile::Named(_) => None,
    }
}

fn extract_toml_patch(path: &Path) -> Result<ConfigPatch, ConfigLoadError> {
    Figment::from(Toml::file(path))
        .extract()
        .map_err(|error| ConfigLoadError::TomlParse {
            message: error.to_string(),
        })
}

fn parse_env<T>(
    get: &impl Fn(&str) -> Option<String>,
    key: &'static str,
) -> Result<Option<T>, ConfigLoadError>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    get(key)
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|error| ConfigLoadError::InvalidValue {
                    input_source: "environment",
                    key,
                    value,
                    reason: error.to_string(),
                })
        })
        .transpose()
}

fn parse_env_with<T>(
    get: &impl Fn(&str) -> Option<String>,
    key: &'static str,
    parser: impl Fn(&str) -> Option<T>,
) -> Result<Option<T>, ConfigLoadError> {
    get(key)
        .map(|value| {
            parser(&value).ok_or_else(|| ConfigLoadError::InvalidValue {
                input_source: "environment",
                key,
                value,
                reason: "unrecognized value".to_string(),
            })
        })
        .transpose()
}

fn parse_env_list_with<T>(
    get: &impl Fn(&str) -> Option<String>,
    key: &'static str,
    parser: impl Fn(&str) -> Option<T>,
) -> Result<Option<Vec<T>>, ConfigLoadError> {
    get(key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(|item| {
                    parser(item).ok_or_else(|| ConfigLoadError::InvalidValue {
                        input_source: "environment",
                        key,
                        value: value.clone(),
                        reason: format!("unrecognized list item '{item}'"),
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()
}

fn parse_env_csv_strings(
    get: &impl Fn(&str) -> Option<String>,
    key: &'static str,
) -> Result<Option<Vec<String>>, ConfigLoadError> {
    get(key)
        .map(|value| {
            if value.trim().is_empty() {
                return Ok(Vec::new());
            }
            Ok(value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect())
        })
        .transpose()
}

impl MergeContext {
    fn finalize(self, config: &mut Config) {
        if !self.explicit_transport_mode
            && self.legacy_tls_seen
            && config.transport.tls.has_server_credentials()
        {
            config.transport.mode = TransportMode::Tls;
        }
    }
}

fn parse_storage_mode_env(raw: &str) -> Option<StorageMode> {
    match raw.to_ascii_lowercase().as_str() {
        "memory" => Some(StorageMode::Memory),
        "file" | "file-durable" | "durable" => Some(StorageMode::FileDurable),
        "file-fast" | "fast" => Some(StorageMode::FileFast),
        "acid" | "redb" => Some(StorageMode::Acid),
        _ => None,
    }
}

fn parse_acid_backend_env(raw: &str) -> Option<AcidBackend> {
    match raw.to_ascii_lowercase().as_str() {
        "file" => Some(AcidBackend::File),
        "memory" | "in-memory" | "inmemory" => Some(AcidBackend::InMemory),
        _ => None,
    }
}

fn parse_transport_mode_env(raw: &str) -> Option<TransportMode> {
    match raw.to_ascii_lowercase().as_str() {
        "http" => Some(TransportMode::Http),
        "tls" => Some(TransportMode::Tls),
        "mtls" => Some(TransportMode::Mtls),
        _ => None,
    }
}

fn parse_http_version_env(raw: &str) -> Option<HttpVersion> {
    match raw.to_ascii_lowercase().as_str() {
        "http1" | "http1.1" | "http/1.1" | "1.1" | "h1" => Some(HttpVersion::Http1),
        "http2" | "2" | "h2" => Some(HttpVersion::Http2),
        _ => None,
    }
}

fn parse_tls_version_env(raw: &str) -> Option<TlsVersion> {
    match raw.to_ascii_lowercase().as_str() {
        "1.2" | "tls1.2" | "tls-1.2" => Some(TlsVersion::V1_2),
        "1.3" | "tls1.3" | "tls-1.3" => Some(TlsVersion::V1_3),
        _ => None,
    }
}

fn parse_alpn_protocol_env(raw: &str) -> Option<AlpnProtocol> {
    match raw.to_ascii_lowercase().as_str() {
        "http/1.1" | "http1" | "h1" => Some(AlpnProtocol::Http1_1),
        "h2" | "http2" => Some(AlpnProtocol::H2),
        _ => None,
    }
}

fn parse_forwarded_headers_mode_env(raw: &str) -> Option<ForwardedHeadersMode> {
    match raw.to_ascii_lowercase().as_str() {
        "none" => Some(ForwardedHeadersMode::None),
        "x-forwarded" | "xforwarded" => Some(ForwardedHeadersMode::XForwarded),
        "forwarded" => Some(ForwardedHeadersMode::Forwarded),
        _ => None,
    }
}

fn parse_proxy_identity_mode_env(raw: &str) -> Option<ProxyIdentityMode> {
    match raw.to_ascii_lowercase().as_str() {
        "none" => Some(ProxyIdentityMode::None),
        "header" => Some(ProxyIdentityMode::Header),
        _ => None,
    }
}

fn default_alpn_protocols(versions: &[HttpVersion]) -> Vec<AlpnProtocol> {
    let mut protocols = Vec::new();
    if versions.contains(&HttpVersion::Http1) {
        protocols.push(AlpnProtocol::Http1_1);
    }
    if versions.contains(&HttpVersion::Http2) {
        protocols.push(AlpnProtocol::H2);
    }
    protocols
}

fn validate_socket_addr(raw: &str) -> Result<SocketAddr, ConfigValidationError> {
    raw.parse::<SocketAddr>()
        .map_err(|error| ConfigValidationError::InvalidBindAddress {
            value: raw.to_string(),
            reason: error.to_string(),
        })
}

fn validate_cors_origins(origins: &str) -> Result<(), ConfigValidationError> {
    if origins == "*" {
        return Ok(());
    }

    let mut parsed_any = false;
    for origin in origins.split(',').map(str::trim) {
        if origin.is_empty() {
            return Err(ConfigValidationError::EmptyCorsOrigin);
        }
        HeaderValue::from_str(origin).map_err(|_| ConfigValidationError::InvalidCorsOrigin {
            value: origin.to_string(),
        })?;
        parsed_any = true;
    }

    if !parsed_any {
        return Err(ConfigValidationError::EmptyCorsOrigin);
    }

    Ok(())
}

fn validate_stream_base_path(raw: &str) -> Result<(), ConfigValidationError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ConfigValidationError::InvalidStreamBasePath {
            value: raw.to_string(),
            reason: "must be a non-empty absolute path".to_string(),
        });
    }
    if !trimmed.starts_with('/') {
        return Err(ConfigValidationError::InvalidStreamBasePath {
            value: raw.to_string(),
            reason: "must start with '/'".to_string(),
        });
    }

    if trimmed != "/" && trimmed.ends_with('/') {
        return Err(ConfigValidationError::InvalidStreamBasePath {
            value: raw.to_string(),
            reason: "must not end with '/' unless the path is '/'".to_string(),
        });
    }

    Ok(())
}

fn valid_acid_shard_count(value: usize) -> bool {
    (1..=256).contains(&value) && value.is_power_of_two()
}

fn validate_proxy(config: &Config) -> Result<(), ConfigValidationError> {
    let proxy = &config.proxy;
    if !proxy.enabled {
        if !proxy.trusted_proxies.is_empty() {
            return Err(ConfigValidationError::ProxyDisabledDisallowsTrustedProxies);
        }
        if proxy.forwarded_headers != ForwardedHeadersMode::None {
            return Err(
                ConfigValidationError::ProxyDisabledDisallowsForwardedHeaders {
                    mode: proxy.forwarded_headers,
                },
            );
        }
        if proxy.identity.mode != ProxyIdentityMode::None {
            return Err(ConfigValidationError::ProxyDisabledDisallowsIdentityMode {
                mode: proxy.identity.mode,
            });
        }
        if proxy.identity.header_name.is_some() {
            return Err(ConfigValidationError::ProxyDisabledDisallowsIdentityHeader);
        }
        return Ok(());
    }

    if proxy.forwarded_headers == ForwardedHeadersMode::None {
        return Err(ConfigValidationError::ProxyEnabledRequiresForwardedHeaders);
    }
    if proxy.trusted_proxies.is_empty() {
        return Err(ConfigValidationError::ProxyEnabledRequiresTrustedProxies);
    }
    for value in &proxy.trusted_proxies {
        if !valid_ip_or_cidr(value) {
            return Err(ConfigValidationError::InvalidTrustedProxy {
                value: value.clone(),
            });
        }
    }

    match proxy.identity.mode {
        ProxyIdentityMode::None => {
            if proxy.identity.header_name.is_some() {
                return Err(ConfigValidationError::IdentityHeaderRequiresHeaderMode);
            }
        }
        ProxyIdentityMode::Header => {
            if config.transport.mode != TransportMode::Mtls {
                return Err(ConfigValidationError::HeaderIdentityRequiresMtls);
            }
            let Some(header_name) = proxy.identity.header_name.as_deref() else {
                return Err(ConfigValidationError::HeaderIdentityRequiresHeaderName);
            };
            HeaderName::from_bytes(header_name.as_bytes()).map_err(|_| {
                ConfigValidationError::InvalidIdentityHeaderName {
                    value: header_name.to_string(),
                }
            })?;
        }
    }

    Ok(())
}

fn valid_ip_or_cidr(raw: &str) -> bool {
    if raw.parse::<IpAddr>().is_ok() {
        return true;
    }

    let Some((address, prefix)) = raw.split_once('/') else {
        return false;
    };
    let Ok(address) = address.parse::<IpAddr>() else {
        return false;
    };
    let Ok(prefix) = prefix.parse::<u8>() else {
        return false;
    };

    match address {
        IpAddr::V4(_) => prefix <= 32,
        IpAddr::V6(_) => prefix <= 128,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |key: &str| map.get(key).cloned()
    }

    fn temp_config_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("ds-config-tests-{}-{}", std::process::id(), id));
        fs::create_dir_all(&path).expect("create temp config dir");
        path
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind_address, "0.0.0.0:4437");
        assert_eq!(config.limits.max_memory_bytes, 100 * 1024 * 1024);
        assert_eq!(config.limits.max_stream_bytes, 10 * 1024 * 1024);
        assert_eq!(config.http.cors_origins, "*");
        assert_eq!(config.transport.connection.long_poll_timeout_secs, 30);
        assert_eq!(config.transport.connection.sse_reconnect_interval_secs, 60);
        assert_eq!(config.http.stream_base_path, DEFAULT_STREAM_BASE_PATH);
        assert_eq!(config.storage.mode, StorageMode::Memory);
        assert_eq!(config.storage.data_dir, "./data/streams");
        assert_eq!(config.storage.acid_shard_count, 16);
        assert_eq!(config.storage.acid_backend, AcidBackend::File);
        assert_eq!(config.transport.mode, TransportMode::Http);
        assert_eq!(config.transport.http.versions, vec![HttpVersion::Http1]);
        assert_eq!(config.transport.tls.cert_path, None);
        assert_eq!(config.transport.tls.key_path, None);
        assert_eq!(config.observability.rust_log, "info");
    }

    #[test]
    fn test_from_env_uses_defaults_when_no_ds_vars() {
        let config = Config::from_env().expect("config from env");
        assert_eq!(config.server.bind_address, "0.0.0.0:4437");
        assert_eq!(config.storage.mode, StorageMode::Memory);
        assert_eq!(config.observability.rust_log, "info");
    }

    #[test]
    fn test_env_overrides_parse_new_and_legacy_keys() {
        let options = ConfigLoadOptions::default();
        let env = lookup(&[
            ("DS_SERVER__PORT", "8080"),
            ("DS_LIMITS__MAX_MEMORY_BYTES", "200000000"),
            ("DS_LIMITS__MAX_STREAM_BYTES", "20000000"),
            ("DS_HTTP__CORS_ORIGINS", "https://example.com"),
            ("DS_TRANSPORT__CONNECTION__LONG_POLL_TIMEOUT_SECS", "5"),
            ("DS_SERVER__SSE_RECONNECT_INTERVAL_SECS", "120"),
            ("DS_HTTP__STREAM_BASE_PATH", "/streams"),
            ("DS_STORAGE__MODE", "file-fast"),
            ("DS_STORAGE__DATA_DIR", "/tmp/ds-store"),
            ("DS_STORAGE__ACID_SHARD_COUNT", "32"),
            ("DS_TRANSPORT__MODE", "tls"),
            ("DS_TLS__CERT_PATH", "/tmp/cert.pem"),
            ("DS_TRANSPORT__TLS__KEY_PATH", "/tmp/key.pem"),
            ("DS_TRANSPORT__HTTP__VERSIONS", "http1,http2"),
            ("DS_OBSERVABILITY__RUST_LOG", "debug"),
        ]);
        let config = Config::from_sources_with_lookup(&options, &env).expect("config from env");

        assert_eq!(config.server.bind_address, "0.0.0.0:8080");
        assert_eq!(config.limits.max_memory_bytes, 200_000_000);
        assert_eq!(config.limits.max_stream_bytes, 20_000_000);
        assert_eq!(config.http.cors_origins, "https://example.com");
        assert_eq!(config.transport.connection.long_poll_timeout_secs, 5);
        assert_eq!(config.transport.connection.sse_reconnect_interval_secs, 120);
        assert_eq!(config.http.stream_base_path, "/streams");
        assert_eq!(config.storage.mode, StorageMode::FileFast);
        assert_eq!(config.storage.data_dir, "/tmp/ds-store");
        assert_eq!(config.storage.acid_shard_count, 32);
        assert_eq!(config.transport.mode, TransportMode::Tls);
        assert_eq!(
            config.transport.http.versions,
            vec![HttpVersion::Http1, HttpVersion::Http2]
        );
        assert_eq!(
            config.transport.tls.alpn_protocols,
            vec![AlpnProtocol::Http1_1, AlpnProtocol::H2]
        );
        assert_eq!(
            config.transport.tls.cert_path.as_deref(),
            Some("/tmp/cert.pem")
        );
        assert_eq!(
            config.transport.tls.key_path.as_deref(),
            Some("/tmp/key.pem")
        );
        assert_eq!(config.observability.rust_log, "debug");
    }

    #[test]
    fn test_invalid_env_override_returns_typed_error() {
        let err = Config::from_sources_with_lookup(
            &ConfigLoadOptions::default(),
            &lookup(&[("DS_TRANSPORT__TLS__MIN_VERSION", "tls1.0")]),
        )
        .expect_err("expected invalid env override");

        assert_eq!(
            err,
            ConfigLoadError::InvalidValue {
                input_source: "environment",
                key: "DS_TRANSPORT__TLS__MIN_VERSION",
                value: "tls1.0".to_string(),
                reason: "unrecognized value".to_string(),
            }
        );
    }

    #[test]
    fn test_built_in_profile_defaults_apply_cleanly() {
        // Use an empty fixture dir so only the built-in profile patch and
        // code defaults are tested — no coupling to the shipped config/ files.
        let config_dir = temp_config_dir();
        let config = Config::from_sources_with_lookup(
            &ConfigLoadOptions {
                config_dir,
                profile: DeploymentProfile::ProdTls,
                config_override: None,
            },
            &lookup(&[]),
        )
        .expect("config");

        assert_eq!(config.storage.mode, StorageMode::FileDurable);
        assert_eq!(config.storage.data_dir, "/var/lib/durable-streams");
        assert_eq!(config.transport.mode, TransportMode::Tls);
        assert_eq!(
            config.transport.http.versions,
            vec![HttpVersion::Http1, HttpVersion::Http2]
        );
        assert_eq!(
            config.transport.tls.alpn_protocols,
            vec![AlpnProtocol::Http1_1, AlpnProtocol::H2]
        );
    }

    #[test]
    fn test_sources_layer_default_profile_local_override_and_env() {
        let config_dir = temp_config_dir();
        fs::write(
            config_dir.join("default.toml"),
            r#"
[server]
bind_address = "0.0.0.0:4437"

[http]
stream_base_path = "/v1/stream"

[storage]
mode = "memory"

[transport.connection]
long_poll_timeout_secs = 30

[observability]
rust_log = "warn"
"#,
        )
        .expect("write default config");
        fs::write(
            config_dir.join("dev.toml"),
            r#"
[server]
bind_address = "127.0.0.1:7777"

[http]
stream_base_path = "/streams"

[storage]
mode = "file-fast"
data_dir = "/tmp/dev-store"
"#,
        )
        .expect("write profile config");
        fs::write(
            config_dir.join("local.toml"),
            r#"
[server]
bind_address = "127.0.0.1:8888"
"#,
        )
        .expect("write local config");

        let config = Config::from_sources_with_lookup(
            &ConfigLoadOptions {
                config_dir,
                profile: DeploymentProfile::Dev,
                config_override: None,
            },
            &lookup(&[
                ("DS_SERVER__BIND_ADDRESS", "127.0.0.1:9999"),
                ("DS_OBSERVABILITY__RUST_LOG", "debug"),
            ]),
        )
        .expect("config from sources");

        assert_eq!(config.server.bind_address, "127.0.0.1:9999");
        assert_eq!(config.http.stream_base_path, "/streams");
        assert_eq!(config.storage.mode, StorageMode::FileFast);
        assert_eq!(config.storage.data_dir, "/tmp/dev-store");
        assert_eq!(config.observability.rust_log, "debug");
    }

    #[test]
    fn test_legacy_tls_fields_infer_tls_mode_when_mode_not_set() {
        let config_dir = temp_config_dir();
        fs::write(
            config_dir.join("default.toml"),
            r#"
[tls]
cert_path = "/tmp/cert.pem"
key_path = "/tmp/key.pem"
"#,
        )
        .expect("write config");

        let config = Config::from_sources_with_lookup(
            &ConfigLoadOptions {
                config_dir,
                ..ConfigLoadOptions::default()
            },
            &lookup(&[]),
        )
        .expect("config from sources");

        assert_eq!(config.transport.mode, TransportMode::Tls);
        assert_eq!(
            config.transport.tls.cert_path.as_deref(),
            Some("/tmp/cert.pem")
        );
        assert_eq!(
            config.transport.tls.key_path.as_deref(),
            Some("/tmp/key.pem")
        );
    }

    #[test]
    fn test_render_effective_json_contains_nested_sections() {
        let rendered = Config::default()
            .render_effective_json()
            .expect("render effective config");
        assert!(rendered.contains("\"transport\""));
        assert!(rendered.contains("\"observability\""));
        assert!(rendered.contains("\"proxy\""));
    }

    #[test]
    fn test_validate_accepts_valid_config_matrix() {
        let valid_configs = [
            Config::default(),
            Config {
                transport: TransportConfig {
                    mode: TransportMode::Tls,
                    http: TransportHttpConfig {
                        versions: vec![HttpVersion::Http1, HttpVersion::Http2],
                    },
                    tls: TransportTlsConfig {
                        cert_path: Some("/tmp/cert.pem".to_string()),
                        key_path: Some("/tmp/key.pem".to_string()),
                        client_ca_path: None,
                        min_version: TlsVersion::V1_2,
                        max_version: TlsVersion::V1_3,
                        alpn_protocols: vec![AlpnProtocol::Http1_1, AlpnProtocol::H2],
                    },
                    connection: TransportConnectionConfig {
                        long_poll_timeout_secs: 30,
                        sse_reconnect_interval_secs: 60,
                    },
                },
                ..Config::default()
            },
            Config {
                transport: TransportConfig {
                    mode: TransportMode::Mtls,
                    http: TransportHttpConfig {
                        versions: vec![HttpVersion::Http1],
                    },
                    tls: TransportTlsConfig {
                        cert_path: Some("/tmp/cert.pem".to_string()),
                        key_path: Some("/tmp/key.pem".to_string()),
                        client_ca_path: Some("/tmp/ca.pem".to_string()),
                        min_version: TlsVersion::V1_2,
                        max_version: TlsVersion::V1_3,
                        alpn_protocols: vec![AlpnProtocol::Http1_1],
                    },
                    connection: TransportConnectionConfig {
                        long_poll_timeout_secs: 30,
                        sse_reconnect_interval_secs: 60,
                    },
                },
                proxy: ProxyConfig {
                    enabled: true,
                    forwarded_headers: ForwardedHeadersMode::XForwarded,
                    trusted_proxies: vec!["127.0.0.1/32".to_string()],
                    identity: ProxyIdentityConfig {
                        mode: ProxyIdentityMode::Header,
                        header_name: Some("x-client-identity".to_string()),
                        require_tls: true,
                    },
                },
                ..Config::default()
            },
        ];

        for config in valid_configs {
            assert!(
                config.validate().is_ok(),
                "config should validate: {config:?}"
            );
        }
    }

    #[test]
    fn test_validate_rejects_invalid_config_matrix() {
        let invalid_cases = [
            (
                Config {
                    transport: TransportConfig {
                        mode: TransportMode::Http,
                        tls: TransportTlsConfig {
                            cert_path: Some("/tmp/cert.pem".to_string()),
                            ..Config::default().transport.tls
                        },
                        ..Config::default().transport
                    },
                    ..Config::default()
                },
                ConfigValidationError::HttpModeDisallowsTlsField { field: "cert_path" },
            ),
            (
                Config {
                    transport: TransportConfig {
                        mode: TransportMode::Tls,
                        tls: TransportTlsConfig {
                            cert_path: Some("/tmp/cert.pem".to_string()),
                            key_path: None,
                            ..Config::default().transport.tls
                        },
                        ..Config::default().transport
                    },
                    ..Config::default()
                },
                ConfigValidationError::MissingTlsField {
                    mode: TransportMode::Tls,
                    field: "key_path",
                },
            ),
            (
                Config {
                    transport: TransportConfig {
                        mode: TransportMode::Http,
                        http: TransportHttpConfig {
                            versions: vec![HttpVersion::Http1, HttpVersion::Http2],
                        },
                        tls: TransportTlsConfig {
                            alpn_protocols: vec![AlpnProtocol::Http1_1, AlpnProtocol::H2],
                            ..Config::default().transport.tls
                        },
                        ..Config::default().transport
                    },
                    ..Config::default()
                },
                ConfigValidationError::HttpModeDoesNotSupportHttp2,
            ),
            (
                Config {
                    transport: TransportConfig {
                        mode: TransportMode::Tls,
                        tls: TransportTlsConfig {
                            cert_path: Some("/tmp/cert.pem".to_string()),
                            key_path: Some("/tmp/key.pem".to_string()),
                            min_version: TlsVersion::V1_3,
                            max_version: TlsVersion::V1_2,
                            alpn_protocols: vec![AlpnProtocol::Http1_1],
                            ..Config::default().transport.tls
                        },
                        ..Config::default().transport
                    },
                    ..Config::default()
                },
                ConfigValidationError::InvalidTlsVersionRange,
            ),
            (
                Config {
                    proxy: ProxyConfig {
                        enabled: true,
                        forwarded_headers: ForwardedHeadersMode::None,
                        trusted_proxies: vec!["127.0.0.1".to_string()],
                        ..Config::default().proxy
                    },
                    ..Config::default()
                },
                ConfigValidationError::ProxyEnabledRequiresForwardedHeaders,
            ),
            (
                Config {
                    proxy: ProxyConfig {
                        enabled: true,
                        forwarded_headers: ForwardedHeadersMode::Forwarded,
                        trusted_proxies: vec!["not-a-cidr".to_string()],
                        ..Config::default().proxy
                    },
                    ..Config::default()
                },
                ConfigValidationError::InvalidTrustedProxy {
                    value: "not-a-cidr".to_string(),
                },
            ),
            (
                Config {
                    transport: TransportConfig {
                        mode: TransportMode::Tls,
                        tls: TransportTlsConfig {
                            cert_path: Some("/tmp/cert.pem".to_string()),
                            key_path: Some("/tmp/key.pem".to_string()),
                            alpn_protocols: vec![AlpnProtocol::Http1_1],
                            ..Config::default().transport.tls
                        },
                        ..Config::default().transport
                    },
                    proxy: ProxyConfig {
                        enabled: true,
                        forwarded_headers: ForwardedHeadersMode::XForwarded,
                        trusted_proxies: vec!["127.0.0.1".to_string()],
                        identity: ProxyIdentityConfig {
                            mode: ProxyIdentityMode::Header,
                            header_name: Some("x-client-identity".to_string()),
                            require_tls: true,
                        },
                    },
                    ..Config::default()
                },
                ConfigValidationError::HeaderIdentityRequiresMtls,
            ),
            (
                Config {
                    transport: TransportConfig {
                        mode: TransportMode::Mtls,
                        tls: TransportTlsConfig {
                            cert_path: Some("/tmp/cert.pem".to_string()),
                            key_path: Some("/tmp/key.pem".to_string()),
                            client_ca_path: Some("/tmp/ca.pem".to_string()),
                            alpn_protocols: vec![AlpnProtocol::Http1_1],
                            ..Config::default().transport.tls
                        },
                        ..Config::default().transport
                    },
                    proxy: ProxyConfig {
                        enabled: true,
                        forwarded_headers: ForwardedHeadersMode::XForwarded,
                        trusted_proxies: vec!["127.0.0.1".to_string()],
                        identity: ProxyIdentityConfig {
                            mode: ProxyIdentityMode::Header,
                            header_name: None,
                            require_tls: true,
                        },
                    },
                    ..Config::default()
                },
                ConfigValidationError::HeaderIdentityRequiresHeaderName,
            ),
        ];

        for (config, expected) in invalid_cases {
            assert_eq!(config.validate().expect_err("config should fail"), expected);
        }
    }
}
