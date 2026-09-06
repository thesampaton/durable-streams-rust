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

/// Declares a string-valued enum plus its `as_str`, [`fmt::Display`], and an
/// env-parser function from a single `Variant => "canonical" | "alias"`  table.
///
/// This keeps the canonical name, serde aliases, and env-parser aliases in one
/// place so they cannot drift apart. Additional `#[derive(...)]` attributes on
/// the enum (e.g. for `PartialOrd`) stack with the macro-generated derives.
macro_rules! string_enum {
    (
        $(#[$enum_attr:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$variant_attr:meta])*
                $variant:ident => $canonical:literal $(| $alias:literal)*
            ),+ $(,)?
        }
        parse_fn $parse_fn:ident;
    ) => {
        $(#[$enum_attr])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        $vis enum $name {
            $(
                $(#[$variant_attr])*
                #[serde(rename = $canonical $(, alias = $alias)*)]
                $variant,
            )+
        }

        impl $name {
            /// Canonical string form — matches the primary serde name.
            #[must_use]
            pub fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $canonical, )+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        fn $parse_fn(raw: &str) -> Option<$name> {
            match raw.to_ascii_lowercase().as_str() {
                $( $canonical $(| $alias)* => Some($name::$variant), )+
                _ => None,
            }
        }
    };
}

string_enum! {
    /// Storage backend selection for the server runtime.
    ///
    /// This is the main operator-facing choice when deciding how the server should
    /// persist streams.
    ///
    /// The `file-*` modes use the simple append-log implementation from
    /// [`crate::storage::file::FileStorage`]. The `acid` mode uses
    /// [`crate::storage::acid::AcidStorage`], which stores data in redb databases
    /// instead of per-stream log files.
    pub enum StorageMode {
        /// In-memory backend.
        ///
        /// Best suited to tests, demos, and ephemeral development environments.
        Memory => "memory",
        /// File logs with journaled append commits and fewer lower-level syncs.
        ///
        /// Append/replacement transactions sync their journal, data, and metadata
        /// in both file modes. This mode omits extra syncs on lower-level writes,
        /// including initial stream data. See [`crate::storage::file`].
        FileFast => "file-fast" | "fast",
        /// File logs with journaled commits and additional write syncing.
        ///
        /// Uses the same layout and append journal as [`Self::FileFast`], with
        /// extra `fsync`/`fdatasync` calls on lower-level writes.
        FileDurable => "file-durable" | "file" | "durable",
        /// Transactional redb-backed storage.
        ///
        /// Choose this when you want stronger transactional durability for
        /// metadata and message updates than the plain file-log modes provide.
        /// The concrete redb persistence medium is controlled separately by
        /// [`AcidBackend`].
        Acid => "acid" | "redb",
    }
    parse_fn parse_storage_mode_env;
}

impl StorageMode {
    /// Whether this mode selects the append-log backend, excluding file-backed ACID storage.
    #[must_use]
    pub fn uses_file_backend(self) -> bool {
        matches!(self, Self::FileFast | Self::FileDurable)
    }

    /// Whether the append-log backend should sync writes before acknowledging them.
    #[must_use]
    pub fn sync_on_append(self) -> bool {
        matches!(self, Self::FileDurable)
    }
}

string_enum! {
    /// Redb persistence medium used by [`StorageMode::Acid`].
    ///
    /// This only applies when `storage.mode = "acid"`. It does not affect the
    /// separate `file-fast` / `file-durable` storage family.
    pub enum AcidBackend {
        /// File-backed redb (default).
        ///
        /// Persists shard databases under `storage.data_dir` and survives restarts.
        /// Choose this when you want the acid/redb backend with durable local
        /// storage.
        File => "file",
        /// In-memory redb.
        ///
        /// Keeps the redb databases in memory only. This preserves the
        /// transactional behavior of the acid backend but drops all state on
        /// shutdown, which can be useful for tests or benchmarking.
        InMemory => "in-memory" | "memory" | "inmemory",
    }
    parse_fn parse_acid_backend_env;
}

string_enum! {
    /// Transport mode selected for the listener.
    pub enum TransportMode {
        /// Plain HTTP transport.
        Http => "http",
        /// Server-side TLS.
        Tls => "tls",
        /// Mutual TLS.
        Mtls => "mtls",
    }
    parse_fn parse_transport_mode_env;
}

impl TransportMode {
    /// Whether the listener requires a TLS context, including mutual TLS.
    #[must_use]
    pub fn uses_tls(self) -> bool {
        matches!(self, Self::Tls | Self::Mtls)
    }
}

string_enum! {
    /// Operator-facing HTTP protocol versions.
    pub enum HttpVersion {
        /// HTTP/1.1
        Http1 => "http1" | "1.1" | "http1.1" | "http/1.1" | "h1",
        /// HTTP/2
        Http2 => "http2" | "2" | "h2",
    }
    parse_fn parse_http_version_env;
}

string_enum! {
    /// TLS protocol versions accepted by the config layer.
    #[derive(PartialOrd, Ord)]
    pub enum TlsVersion {
        /// TLS 1.2
        V1_2 => "1.2" | "tls1.2" | "tls-1.2",
        /// TLS 1.3
        V1_3 => "1.3" | "tls1.3" | "tls-1.3",
    }
    parse_fn parse_tls_version_env;
}

string_enum! {
    /// ALPN protocols used when TLS is enabled.
    pub enum AlpnProtocol {
        /// HTTP/1.1 ALPN identifier.
        Http1_1 => "http/1.1" | "http1" | "h1",
        /// HTTP/2 ALPN identifier.
        H2 => "h2" | "http2",
    }
    parse_fn parse_alpn_protocol_env;
}

string_enum! {
    /// Forwarded-header family trusted from a proxy.
    pub enum ForwardedHeadersMode {
        /// Do not trust any proxy headers.
        None => "none",
        /// Trust the `X-Forwarded-*` header family.
        XForwarded => "x-forwarded" | "xforwarded",
        /// Trust RFC 7239 `Forwarded`.
        Forwarded => "forwarded",
    }
    parse_fn parse_forwarded_headers_mode_env;
}

string_enum! {
    /// Proxy-origin identity handoff strategy.
    pub enum ProxyIdentityMode {
        /// No proxy identity handoff.
        None => "none",
        /// Trust a single HTTP header from the proxy.
        Header => "header",
    }
    parse_fn parse_proxy_identity_mode_env;
}

/// Typed profile selection for config loading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentProfile {
    /// Minimal HTTP profile with the built-in defaults.
    Default,
    /// Loopback development profile.
    Dev,
    /// Production profile for externally terminated TLS.
    Prod,
    /// Production profile with server-side TLS termination.
    ProdTls,
    /// Production profile requiring client TLS certificates.
    ProdMtls,
    /// Custom profile name selecting an additional TOML file.
    Named(String),
}

impl DeploymentProfile {
    /// Profile name used to select the corresponding TOML file.
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
#[non_exhaustive]
pub struct Config {
    /// Listener settings.
    pub server: ServerConfig,
    /// Stream and storage size limits.
    pub limits: LimitsConfig,
    /// HTTP protocol surface configuration.
    pub http: HttpConfig,
    /// Optional operator/admin HTTP surface configuration.
    pub admin: AdminConfig,
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
#[non_exhaustive]
pub struct ServerConfig {
    /// Socket address to bind, e.g. `0.0.0.0:4437`.
    pub bind_address: String,
}

/// Limits enforced by the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct LimitsConfig {
    /// Maximum total in-process payload bytes across all streams.
    pub max_memory_bytes: u64,
    /// Maximum payload bytes retained for any single stream.
    pub max_stream_bytes: u64,
    /// Maximum bytes buffered from one HTTP request body, including JSON framing.
    pub max_request_body_bytes: usize,
    /// Maximum byte length of a stream name.
    pub max_stream_name_bytes: usize,
    /// Maximum number of `/`-separated segments in a stream name.
    pub max_stream_name_segments: usize,
}

/// HTTP surface configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct HttpConfig {
    /// CORS allowlist as `"*"` or a comma-separated origin list.
    pub cors_origins: String,
    /// Mount path for the protocol HTTP surface.
    pub stream_base_path: String,
    /// When `true`, wildcard CORS (`"*"`) is accepted without warnings or
    /// profile-level validation errors. Defaults to `false`.
    pub allow_wildcard_cors: bool,
    /// Permit HTTP webhook targets on localhost for development only.
    pub allow_insecure_webhooks: bool,
}

/// Optional operator/admin HTTP surface configuration.
///
/// Admin routes are disabled by default and are intended for trusted networks,
/// reverse proxies, or external access-control layers. The server deliberately
/// does not own authentication or authorization policy for this surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AdminConfig {
    /// Whether to mount the admin router.
    pub enabled: bool,
    /// Mount path for operator-focused admin routes, e.g. `/admin`.
    pub base_path: String,
}

/// Persistence configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct TransportHttpConfig {
    /// Enabled HTTP protocol versions.
    pub versions: Vec<HttpVersion>,
}

/// TLS-related settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
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
    /// Whether both certificate and private-key paths are set; does not check their contents.
    #[must_use]
    pub fn has_server_credentials(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }
}

/// Connection-level settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TransportConnectionConfig {
    /// Long-poll timeout used by `GET ?live=long-poll`.
    pub long_poll_timeout_secs: u64,
    /// SSE reconnect interval in seconds (`0` disables forced reconnects).
    pub sse_reconnect_interval_secs: u64,
}

/// Reverse-proxy trust model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct ObservabilityConfig {
    /// Default tracing filter used when `RUST_LOG` is not explicitly set.
    pub rust_log: String,
}

/// Select configuration files and the profile used by [`Config::from_sources`].
#[derive(Debug, Clone)]
#[non_exhaustive]
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
#[non_exhaustive]
pub enum ConfigLoadError {
    #[error("config override file not found: '{path}'")]
    /// An explicitly requested override file does not exist.
    OverrideFileNotFound {
        /// Requested override file path.
        path: PathBuf,
    },
    #[error("failed to parse TOML config: {message}")]
    /// A configuration source could not be decoded as the expected TOML structure.
    TomlParse {
        /// Parser diagnostic from the configuration provider.
        message: String,
    },
    #[error("invalid {input_source} value for {key}: '{value}' ({reason})")]
    /// A source value could not be converted to the expected setting type.
    InvalidValue {
        /// Source category, such as an environment override.
        input_source: &'static str,
        /// Configuration key that failed conversion.
        key: &'static str,
        /// Unconverted input value.
        value: String,
        /// Explanation of the failed conversion.
        reason: String,
    },
}

/// Typed validation errors raised before startup.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConfigValidationError {
    #[error("server.bind_address is invalid: '{value}' ({reason})")]
    /// The listener address is not a valid socket address.
    InvalidBindAddress {
        /// Configured value that failed validation.
        value: String,
        /// Explanation of the validation failure.
        reason: String,
    },
    #[error("http.stream_base_path is invalid: '{value}' ({reason})")]
    /// The protocol mount path violates route constraints.
    InvalidStreamBasePath {
        /// Configured value that failed validation.
        value: String,
        /// Explanation of the validation failure.
        reason: String,
    },
    #[error("admin.base_path is invalid: '{value}' ({reason})")]
    /// The admin mount path violates route constraints.
    InvalidAdminBasePath {
        /// Configured value that failed validation.
        value: String,
        /// Explanation of the validation failure.
        reason: String,
    },
    #[error("admin.base_path must not overlap http.stream_base_path")]
    /// Enabled admin routes overlap the protocol mount path.
    AdminBasePathConflictsWithStreamBasePath,
    #[error("http.cors_origins contains an empty origin entry")]
    /// The CORS list includes an empty entry.
    EmptyCorsOrigin,
    #[error("http.cors_origins entry is invalid: '{value}'")]
    /// A CORS entry is not an accepted origin.
    InvalidCorsOrigin {
        /// Configured value that failed validation.
        value: String,
    },
    #[error("limits.max_memory_bytes must be at least 1")]
    /// The total payload byte limit is zero.
    MaxMemoryBytesTooSmall,
    #[error("limits.max_stream_bytes must be at least 1")]
    /// The per-stream payload byte limit is zero.
    MaxStreamBytesTooSmall,
    #[error("limits.max_stream_name_bytes must be at least 1")]
    /// The stream-name byte limit is zero.
    MaxStreamNameBytesTooSmall,
    #[error("limits.max_stream_name_segments must be at least 1")]
    /// The stream-name segment limit is zero.
    MaxStreamNameSegmentsTooSmall,
    #[error("storage.data_dir must be a non-empty path when storage.mode is '{mode}'")]
    /// A persistence mode requires a non-empty data directory.
    EmptyStorageDataDir {
        /// Configured mode involved in the conflict or requirement.
        mode: StorageMode,
    },
    #[error(
        "storage.acid_shard_count must be a power of two in 1..=256 when storage.mode is 'acid'"
    )]
    /// ACID shard count must be a power of two between 1 and 256.
    InvalidAcidShardCount,
    #[error("transport.connection.long_poll_timeout_secs must be at least 1")]
    /// The long-poll timeout is zero.
    LongPollTimeoutTooSmall,
    #[error("transport.http.versions must include at least one version")]
    /// No HTTP protocol version is enabled.
    EmptyHttpVersions,
    #[error("transport.mode='http' does not support transport.http.versions containing http2")]
    /// HTTP/2 was requested for the plain HTTP listener.
    HttpModeDoesNotSupportHttp2,
    #[error("transport.tls.min_version must be less than or equal to transport.tls.max_version")]
    /// The minimum TLS version exceeds the maximum.
    InvalidTlsVersionRange,
    #[error("transport.mode='{mode}' requires transport.tls.{field}")]
    /// The selected TLS mode lacks a required credential path.
    MissingTlsField {
        /// Configured mode involved in the conflict or requirement.
        mode: TransportMode,
        /// TLS field requiring correction.
        field: &'static str,
    },
    #[error("transport.mode='http' cannot be combined with transport.tls.{field}")]
    /// A TLS credential path was supplied for plain HTTP.
    HttpModeDisallowsTlsField {
        /// TLS field requiring correction.
        field: &'static str,
    },
    #[error("transport.mode='tls' cannot be combined with transport.tls.client_ca_path")]
    /// A client CA was supplied for TLS without client authentication.
    ClientCaRequiresMtls,
    #[error("transport.tls.{field} must be a non-empty path when set")]
    /// A configured TLS file path is empty.
    EmptyPath {
        /// TLS field requiring correction.
        field: &'static str,
    },
    #[error(
        "transport.http.versions includes '{version}', but transport.tls.alpn_protocols is missing '{alpn}'"
    )]
    /// An enabled HTTP version is absent from TLS ALPN negotiation.
    MissingAlpnProtocol {
        /// Enabled HTTP version.
        version: HttpVersion,
        /// ALPN identifier involved in the mismatch.
        alpn: AlpnProtocol,
    },
    #[error(
        "transport.tls.alpn_protocols includes '{alpn}', but transport.http.versions does not enable the matching HTTP version"
    )]
    /// TLS ALPN advertises an HTTP version that is not enabled.
    UnexpectedAlpnProtocol {
        /// ALPN identifier involved in the mismatch.
        alpn: AlpnProtocol,
    },
    #[error(
        "proxy.enabled=true requires proxy.forwarded_headers to be set to 'x-forwarded' or 'forwarded'"
    )]
    /// Proxy trust is enabled without a forwarded-header family.
    ProxyEnabledRequiresForwardedHeaders,
    #[error("proxy.enabled=true requires at least one entry in proxy.trusted_proxies")]
    /// Proxy trust is enabled without any trusted peer addresses.
    ProxyEnabledRequiresTrustedProxies,
    #[error("proxy.enabled=false cannot be combined with proxy.trusted_proxies")]
    /// Trusted peers were configured while proxy trust is disabled.
    ProxyDisabledDisallowsTrustedProxies,
    #[error("proxy.enabled=false cannot be combined with proxy.forwarded_headers='{mode:?}'")]
    /// Forwarded headers were enabled while proxy trust is disabled.
    ProxyDisabledDisallowsForwardedHeaders {
        /// Configured mode involved in the conflict or requirement.
        mode: ForwardedHeadersMode,
    },
    #[error("proxy.enabled=false cannot be combined with proxy.identity.mode='{mode:?}'")]
    /// Proxy identity handoff was enabled while proxy trust is disabled.
    ProxyDisabledDisallowsIdentityMode {
        /// Configured mode involved in the conflict or requirement.
        mode: ProxyIdentityMode,
    },
    #[error("proxy.enabled=false cannot be combined with proxy.identity.header_name")]
    /// An identity header was set while proxy trust is disabled.
    ProxyDisabledDisallowsIdentityHeader,
    #[error("proxy.trusted_proxies entry is invalid: '{value}'")]
    /// A trusted proxy entry is not an accepted IP address or CIDR range.
    InvalidTrustedProxy {
        /// Configured value that failed validation.
        value: String,
    },
    #[error("proxy.identity.mode='header' requires proxy.identity.header_name")]
    /// Header-based identity handoff lacks a header name.
    HeaderIdentityRequiresHeaderName,
    #[error("proxy.identity.mode='header' requires transport.mode='mtls'")]
    /// Header-based identity handoff requires mutual TLS.
    HeaderIdentityRequiresMtls,
    #[error("proxy.identity.mode='none' cannot be combined with proxy.identity.header_name")]
    /// An identity header name was set without header-based identity handoff.
    IdentityHeaderRequiresHeaderMode,
    #[error("proxy.identity.header_name is invalid: '{value}'")]
    /// The identity header name is not a valid HTTP header name.
    InvalidIdentityHeaderName {
        /// Configured value that failed validation.
        value: String,
    },
    #[error(
        "http.cors_origins='*' is not allowed for the '{profile}' deployment profile; \
         set http.allow_wildcard_cors=true to override, or specify explicit origins"
    )]
    /// A production profile uses wildcard CORS without an explicit override.
    WildcardCorsOriginsProd {
        /// Deployment profile that requires an explicit CORS policy.
        profile: String,
    },
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ConfigPatch {
    server: ServerConfigPatch,
    limits: LimitsConfigPatch,
    http: HttpConfigPatch,
    admin: AdminConfigPatch,
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
    max_request_body_bytes: Option<usize>,
    max_stream_name_bytes: Option<usize>,
    max_stream_name_segments: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct HttpConfigPatch {
    cors_origins: Option<String>,
    stream_base_path: Option<String>,
    allow_wildcard_cors: Option<bool>,
    allow_insecure_webhooks: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct AdminConfigPatch {
    enabled: Option<bool>,
    base_path: Option<String>,
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

/// Apply one `parse_env::<T>`-style env override: if the key is set, parse it
/// and assign the target field. Returns from the enclosing function via `?` if
/// parsing fails.
macro_rules! env_parse_into {
    ($self:expr, $get:expr, $key:literal => $($field:ident).+ : $ty:ty) => {
        if let Some(__value) = parse_env::<$ty>($get, $key)? {
            $self . $($field).+ = __value;
        }
    };
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
        self.apply_server_patch(&patch.server);
        self.apply_limits_patch(&patch.limits);
        self.apply_http_patch(&patch.http);
        self.apply_admin_patch(&patch.admin);
        self.apply_storage_patch(&patch.storage);
        self.apply_transport_patch(&patch.transport, &patch.tls, &patch.server, ctx);
        self.apply_proxy_patch(&patch.proxy);

        let rust_log = patch.observability.rust_log.or(patch.log.rust_log);
        if let Some(rust_log) = rust_log {
            self.observability.rust_log = rust_log;
        }
    }

    fn apply_server_patch(&mut self, patch: &ServerConfigPatch) {
        if let Some(bind_address) = &patch.bind_address {
            self.server.bind_address.clone_from(bind_address);
        } else if let Some(port) = patch.port {
            self.server.bind_address = format!("0.0.0.0:{port}");
        }
    }

    fn apply_limits_patch(&mut self, patch: &LimitsConfigPatch) {
        if let Some(max_memory_bytes) = patch.max_memory_bytes {
            self.limits.max_memory_bytes = max_memory_bytes;
        }
        if let Some(max_stream_bytes) = patch.max_stream_bytes {
            self.limits.max_stream_bytes = max_stream_bytes;
        }
        if let Some(limit) = patch.max_request_body_bytes {
            self.limits.max_request_body_bytes = limit;
        }
        if let Some(max_stream_name_bytes) = patch.max_stream_name_bytes {
            self.limits.max_stream_name_bytes = max_stream_name_bytes;
        }
        if let Some(max_stream_name_segments) = patch.max_stream_name_segments {
            self.limits.max_stream_name_segments = max_stream_name_segments;
        }
    }

    fn apply_http_patch(&mut self, patch: &HttpConfigPatch) {
        if let Some(value) = patch.allow_insecure_webhooks {
            self.http.allow_insecure_webhooks = value;
        }
        if let Some(cors_origins) = &patch.cors_origins {
            self.http.cors_origins.clone_from(cors_origins);
        }
        if let Some(stream_base_path) = &patch.stream_base_path {
            self.http.stream_base_path.clone_from(stream_base_path);
        }
        if let Some(allow_wildcard_cors) = patch.allow_wildcard_cors {
            self.http.allow_wildcard_cors = allow_wildcard_cors;
        }
    }

    fn apply_admin_patch(&mut self, patch: &AdminConfigPatch) {
        if let Some(enabled) = patch.enabled {
            self.admin.enabled = enabled;
        }
        if let Some(base_path) = &patch.base_path {
            self.admin.base_path.clone_from(base_path);
        }
    }

    fn apply_storage_patch(&mut self, patch: &StorageConfigPatch) {
        if let Some(mode) = patch.mode {
            self.storage.mode = mode;
        }
        if let Some(data_dir) = &patch.data_dir {
            self.storage.data_dir.clone_from(data_dir);
        }
        if let Some(acid_shard_count) = patch.acid_shard_count {
            self.storage.acid_shard_count = acid_shard_count;
        }
        if let Some(acid_backend) = patch.acid_backend {
            self.storage.acid_backend = acid_backend;
        }
    }

    fn apply_transport_patch(
        &mut self,
        patch: &TransportConfigPatch,
        legacy_tls: &LegacyTlsPatch,
        server_patch: &ServerConfigPatch,
        ctx: &mut MergeContext,
    ) {
        if let Some(mode) = patch.mode {
            self.transport.mode = mode;
            ctx.explicit_transport_mode = true;
        }
        if let Some(versions) = &patch.http.versions {
            self.transport.http.versions.clone_from(versions);
            self.transport.tls.alpn_protocols =
                default_alpn_protocols(&self.transport.http.versions);
        }

        let legacy_tls_cert_path = &legacy_tls.cert_path;
        let legacy_tls_key_path = &legacy_tls.key_path;
        let saw_legacy_tls = legacy_tls_cert_path.is_some() || legacy_tls_key_path.is_some();
        let tls_cert_path = patch
            .tls
            .cert_path
            .as_ref()
            .or(legacy_tls_cert_path.as_ref());
        let tls_key_path = patch.tls.key_path.as_ref().or(legacy_tls_key_path.as_ref());
        if tls_cert_path.is_some() || tls_key_path.is_some() {
            ctx.legacy_tls_seen |= saw_legacy_tls;
        }
        if let Some(cert_path) = tls_cert_path {
            self.transport.tls.cert_path = Some(cert_path.clone());
        }
        if let Some(key_path) = tls_key_path {
            self.transport.tls.key_path = Some(key_path.clone());
        }
        if let Some(client_ca_path) = &patch.tls.client_ca_path {
            self.transport.tls.client_ca_path = Some(client_ca_path.clone());
        }
        if let Some(min_version) = patch.tls.min_version {
            self.transport.tls.min_version = min_version;
        }
        if let Some(max_version) = patch.tls.max_version {
            self.transport.tls.max_version = max_version;
        }
        if let Some(alpn_protocols) = &patch.tls.alpn_protocols {
            self.transport.tls.alpn_protocols.clone_from(alpn_protocols);
        }

        let long_poll_timeout_secs = patch
            .connection
            .long_poll_timeout_secs
            .or(server_patch.long_poll_timeout_secs);
        if let Some(long_poll_timeout_secs) = long_poll_timeout_secs {
            self.transport.connection.long_poll_timeout_secs = long_poll_timeout_secs;
        }

        let sse_reconnect_interval_secs = patch
            .connection
            .sse_reconnect_interval_secs
            .or(server_patch.sse_reconnect_interval_secs);
        if let Some(sse_reconnect_interval_secs) = sse_reconnect_interval_secs {
            self.transport.connection.sse_reconnect_interval_secs = sse_reconnect_interval_secs;
        }
    }

    fn apply_proxy_patch(&mut self, patch: &ProxyConfigPatch) {
        if let Some(enabled) = patch.enabled {
            self.proxy.enabled = enabled;
        }
        if let Some(forwarded_headers) = patch.forwarded_headers {
            self.proxy.forwarded_headers = forwarded_headers;
        }
        if let Some(trusted_proxies) = &patch.trusted_proxies {
            self.proxy.trusted_proxies.clone_from(trusted_proxies);
        }
        if let Some(mode) = patch.identity.mode {
            self.proxy.identity.mode = mode;
        }
        if let Some(header_name) = &patch.identity.header_name {
            self.proxy.identity.header_name = Some(header_name.clone());
        }
        if let Some(require_tls) = patch.identity.require_tls {
            self.proxy.identity.require_tls = require_tls;
        }
    }

    /// Apply `DS_*` environment variable overrides on top of current config.
    fn apply_env_overrides(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
        ctx: &mut MergeContext,
    ) -> Result<(), ConfigLoadError> {
        self.apply_server_env(get)?;
        self.apply_limits_env(get)?;
        self.apply_http_env(get)?;
        self.apply_admin_env(get)?;
        self.apply_storage_env(get)?;
        self.apply_transport_env(get, ctx)?;
        self.apply_proxy_env(get)?;

        if let Some(rust_log) =
            get("DS_OBSERVABILITY__RUST_LOG").or_else(|| get("DS_LOG__RUST_LOG"))
        {
            self.observability.rust_log = rust_log;
        }

        Ok(())
    }

    fn apply_server_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
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

        Ok(())
    }

    fn apply_limits_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigLoadError> {
        env_parse_into!(self, get, "DS_LIMITS__MAX_MEMORY_BYTES" => limits.max_memory_bytes : u64);
        env_parse_into!(self, get, "DS_LIMITS__MAX_STREAM_BYTES" => limits.max_stream_bytes : u64);
        env_parse_into!(self, get, "DS_LIMITS__MAX_REQUEST_BODY_BYTES" => limits.max_request_body_bytes : usize);
        env_parse_into!(self, get, "DS_LIMITS__MAX_STREAM_NAME_BYTES" => limits.max_stream_name_bytes : usize);
        env_parse_into!(self, get, "DS_LIMITS__MAX_STREAM_NAME_SEGMENTS" => limits.max_stream_name_segments : usize);
        Ok(())
    }

    fn apply_http_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigLoadError> {
        if let Some(cors_origins) = get("DS_HTTP__CORS_ORIGINS") {
            self.http.cors_origins = cors_origins;
        }
        if let Some(stream_base_path) = get("DS_HTTP__STREAM_BASE_PATH") {
            self.http.stream_base_path = stream_base_path;
        }
        env_parse_into!(self, get, "DS_HTTP__ALLOW_INSECURE_WEBHOOKS" => http.allow_insecure_webhooks : bool);
        env_parse_into!(self, get, "DS_HTTP__ALLOW_WILDCARD_CORS" => http.allow_wildcard_cors : bool);
        Ok(())
    }

    fn apply_admin_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigLoadError> {
        env_parse_into!(self, get, "DS_ADMIN__ENABLED" => admin.enabled : bool);
        if let Some(base_path) = get("DS_ADMIN__BASE_PATH") {
            self.admin.base_path = base_path;
        }
        Ok(())
    }

    fn apply_storage_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigLoadError> {
        if let Some(storage_mode) = parse_env_with(get, "DS_STORAGE__MODE", parse_storage_mode_env)?
        {
            self.storage.mode = storage_mode;
        }
        if let Some(data_dir) = get("DS_STORAGE__DATA_DIR") {
            self.storage.data_dir = data_dir;
        }
        env_parse_into!(self, get, "DS_STORAGE__ACID_SHARD_COUNT" => storage.acid_shard_count : usize);
        if let Some(acid_backend) =
            parse_env_with(get, "DS_STORAGE__ACID_BACKEND", parse_acid_backend_env)?
        {
            self.storage.acid_backend = acid_backend;
        }
        Ok(())
    }

    fn apply_transport_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
        ctx: &mut MergeContext,
    ) -> Result<(), ConfigLoadError> {
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
        Ok(())
    }

    fn apply_proxy_env(
        &mut self,
        get: &impl Fn(&str) -> Option<String>,
    ) -> Result<(), ConfigLoadError> {
        env_parse_into!(self, get, "DS_PROXY__ENABLED" => proxy.enabled : bool);
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
        env_parse_into!(self, get, "DS_PROXY__IDENTITY__REQUIRE_TLS" => proxy.identity.require_tls : bool);
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
        validate_admin_base_path(&self.admin.base_path)?;
        if self.admin.enabled {
            validate_admin_path_separation(&self.http.stream_base_path, &self.admin.base_path)?;
        }
        self.validate_limits()?;
        self.validate_storage()?;
        self.validate_transport()?;
        validate_proxy(self)?;
        Ok(())
    }

    pub(crate) fn validate_router(&self) -> Result<(), ConfigValidationError> {
        validate_cors_origins(&self.http.cors_origins)?;
        validate_stream_base_path(&self.http.stream_base_path)?;
        if self.admin.enabled {
            validate_admin_base_path(&self.admin.base_path)?;
            validate_admin_path_separation(&self.http.stream_base_path, &self.admin.base_path)?;
        }
        if self.limits.max_stream_name_bytes == 0 {
            return Err(ConfigValidationError::MaxStreamNameBytesTooSmall);
        }
        if self.limits.max_stream_name_segments == 0 {
            return Err(ConfigValidationError::MaxStreamNameSegmentsTooSmall);
        }
        if self.transport.connection.long_poll_timeout_secs == 0 {
            return Err(ConfigValidationError::LongPollTimeoutTooSmall);
        }
        validate_proxy(self)
    }

    fn validate_limits(&self) -> Result<(), ConfigValidationError> {
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
        Ok(())
    }

    fn validate_storage(&self) -> Result<(), ConfigValidationError> {
        if self.storage.mode != StorageMode::Memory && self.storage.data_dir.trim().is_empty() {
            return Err(ConfigValidationError::EmptyStorageDataDir {
                mode: self.storage.mode,
            });
        }
        if self.storage.mode == StorageMode::Acid
            && !valid_acid_shard_count(self.storage.acid_shard_count)
        {
            return Err(ConfigValidationError::InvalidAcidShardCount);
        }
        Ok(())
    }

    fn validate_transport(&self) -> Result<(), ConfigValidationError> {
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

        self.validate_transport_mode_tls()?;
        self.validate_alpn_protocols()?;
        Ok(())
    }

    fn validate_transport_mode_tls(&self) -> Result<(), ConfigValidationError> {
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
        Ok(())
    }

    fn validate_alpn_protocols(&self) -> Result<(), ConfigValidationError> {
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
        Ok(())
    }

    /// Deployment-profile-specific validation run after [`Config::validate`].
    ///
    /// Production profiles (`prod`, `prod-tls`, `prod-mtls`) reject wildcard
    /// CORS unless the operator has explicitly set `http.allow_wildcard_cors`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigValidationError::WildcardCorsOriginsProd`] when a
    /// production profile is active with `cors_origins = "*"` and the escape
    /// hatch is not set.
    pub fn validate_profile(
        &self,
        profile: &DeploymentProfile,
    ) -> Result<(), ConfigValidationError> {
        let is_prod = matches!(
            profile,
            DeploymentProfile::Prod | DeploymentProfile::ProdTls | DeploymentProfile::ProdMtls
        );

        if is_prod && self.http.cors_origins == "*" && !self.http.allow_wildcard_cors {
            return Err(ConfigValidationError::WildcardCorsOriginsProd {
                profile: profile.as_str().to_string(),
            });
        }

        Ok(())
    }

    /// Non-fatal advisories about the current configuration.
    ///
    /// Returns human-readable warning strings that should be logged at startup
    /// but do not block the server from starting. Currently checks for wildcard
    /// CORS without an explicit opt-in via `http.allow_wildcard_cors`.
    #[must_use]
    pub fn warnings(&self) -> Vec<String> {
        let mut w = Vec::new();
        if self.http.cors_origins == "*" && !self.http.allow_wildcard_cors {
            w.push(
                "http.cors_origins is set to '*' (allows all origins); \
                 consider restricting for production use"
                    .to_string(),
            );
        }
        if self.admin.enabled {
            match self.bind_socket_addr() {
                Ok(addr) if !addr.ip().is_loopback() => w.push(format!(
                    "admin routes are enabled on non-loopback bind address {addr}; \
                     place them behind a trusted network, reverse proxy, or external access-control layer"
                )),
                _ => {}
            }
        }
        w
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
                max_request_body_bytes: 10 * 1024 * 1024,
                max_stream_name_bytes: 1024,
                max_stream_name_segments: 8,
            },
            http: HttpConfig {
                cors_origins: "*".to_string(),
                stream_base_path: DEFAULT_STREAM_BASE_PATH.to_string(),
                allow_wildcard_cors: false,
                allow_insecure_webhooks: false,
            },
            admin: AdminConfig {
                enabled: false,
                base_path: "/admin".to_string(),
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

/// Shared defaults applied to every `prod*` profile: stricter limits and
/// file-durable on-disk storage.
fn prod_base_patch() -> ConfigPatch {
    ConfigPatch {
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
    }
}

/// Prod base extended with a TLS-family transport (TLS or mTLS) serving both
/// HTTP/1.1 and HTTP/2.
fn prod_tls_patch(mode: TransportMode) -> ConfigPatch {
    ConfigPatch {
        transport: TransportConfigPatch {
            mode: Some(mode),
            http: TransportHttpConfigPatch {
                versions: Some(vec![HttpVersion::Http1, HttpVersion::Http2]),
            },
            ..TransportConfigPatch::default()
        },
        ..prod_base_patch()
    }
}

fn built_in_profile_patch(profile: &DeploymentProfile) -> Option<ConfigPatch> {
    match profile {
        DeploymentProfile::Default | DeploymentProfile::Named(_) => None,
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
        DeploymentProfile::Prod => Some(prod_base_patch()),
        DeploymentProfile::ProdTls => Some(prod_tls_patch(TransportMode::Tls)),
        DeploymentProfile::ProdMtls => Some(prod_tls_patch(TransportMode::Mtls)),
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

fn default_alpn_protocols(versions: &[HttpVersion]) -> Vec<AlpnProtocol> {
    let mut protocols = Vec::new();
    // h2 first: conventional preference order for TLS ALPN negotiation.
    if versions.contains(&HttpVersion::Http2) {
        protocols.push(AlpnProtocol::H2);
    }
    if versions.contains(&HttpVersion::Http1) {
        protocols.push(AlpnProtocol::Http1_1);
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
    if raw.chars().any(char::is_whitespace) || raw.contains(['{', '}', ':', '*', '?', '#']) {
        return Err(ConfigValidationError::InvalidStreamBasePath {
            value: raw.into(),
            reason: "must be a literal URL path without whitespace, parameters, query, or fragment"
                .into(),
        });
    }
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

fn validate_admin_base_path(raw: &str) -> Result<(), ConfigValidationError> {
    if raw.chars().any(char::is_whitespace) || raw.contains(['{', '}', ':', '*', '?', '#']) {
        return Err(ConfigValidationError::InvalidAdminBasePath {
            value: raw.to_string(),
            reason: "must be a literal path without whitespace, parameters, query, or fragment"
                .to_string(),
        });
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ConfigValidationError::InvalidAdminBasePath {
            value: raw.to_string(),
            reason: "must be a non-empty absolute path".to_string(),
        });
    }
    if !trimmed.starts_with('/') {
        return Err(ConfigValidationError::InvalidAdminBasePath {
            value: raw.to_string(),
            reason: "must start with '/'".to_string(),
        });
    }
    if trimmed == "/" {
        return Err(ConfigValidationError::InvalidAdminBasePath {
            value: raw.to_string(),
            reason: "must not be '/'".to_string(),
        });
    }
    if trimmed.ends_with('/') {
        return Err(ConfigValidationError::InvalidAdminBasePath {
            value: raw.to_string(),
            reason: "must not end with '/'".to_string(),
        });
    }
    Ok(())
}

fn validate_admin_path_separation(
    stream_base_path: &str,
    admin_base_path: &str,
) -> Result<(), ConfigValidationError> {
    if paths_overlap(stream_base_path, admin_base_path) {
        return Err(ConfigValidationError::AdminBasePathConflictsWithStreamBasePath);
    }
    Ok(())
}

fn paths_overlap(left: &str, right: &str) -> bool {
    left == "/"
        || right == "/"
        || left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
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
        assert!(!config.admin.enabled);
        assert_eq!(config.admin.base_path, "/admin");
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
            vec![AlpnProtocol::H2, AlpnProtocol::Http1_1]
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
            vec![AlpnProtocol::H2, AlpnProtocol::Http1_1]
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
    fn test_disabled_admin_allows_overlapping_stream_mounts() {
        for path in ["/admin", "/admin/events", "/"] {
            let mut config = Config::default();
            config.http.stream_base_path = path.to_string();
            assert!(config.validate().is_ok(), "disabled admin reserves {path}");
        }
    }

    #[test]
    fn test_enabled_admin_rejects_overlapping_stream_mounts() {
        for path in ["/admin", "/admin/events", "/"] {
            let mut config = Config::default();
            config.admin.enabled = true;
            config.http.stream_base_path = path.to_string();
            assert_eq!(
                config.validate(),
                Err(ConfigValidationError::AdminBasePathConflictsWithStreamBasePath),
                "overlapping mount: {path}"
            );
        }

        let mut config = Config::default();
        config.admin.enabled = true;
        config.admin.base_path = "/v1/stream/admin".to_string();
        assert_eq!(
            config.validate(),
            Err(ConfigValidationError::AdminBasePathConflictsWithStreamBasePath)
        );
        config.admin.base_path = "/v1/streams-admin".to_string();
        assert!(
            config.validate().is_ok(),
            "shared text prefix is not overlap"
        );
    }

    #[test]
    fn test_admin_base_path_rejects_nonliteral_or_whitespace_paths() {
        for path in [
            " /admin",
            "/admin ",
            "/{*path}",
            "/{tenant}",
            "/:admin",
            "/*admin",
        ] {
            let mut config = Config::default();
            config.admin.enabled = true;
            config.admin.base_path = path.to_string();
            assert!(
                matches!(
                    config.validate(),
                    Err(ConfigValidationError::InvalidAdminBasePath { .. })
                ),
                "invalid admin mount: {path}"
            );
        }
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

    fn assert_invalid_configs(
        invalid_cases: impl IntoIterator<Item = (Config, ConfigValidationError)>,
    ) {
        for (config, expected) in invalid_cases {
            assert_eq!(config.validate().expect_err("config should fail"), expected);
        }
    }

    #[test]
    fn test_validate_rejects_http_transport_tls_misconfigurations() {
        assert_invalid_configs([
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
        ]);
    }

    #[test]
    fn test_validate_rejects_invalid_tls_ranges_and_proxy_headers() {
        assert_invalid_configs([
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
        ]);
    }

    #[test]
    fn test_validate_rejects_invalid_proxy_identity_requirements() {
        assert_invalid_configs([
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
        ]);
    }

    // ── Wildcard CORS warning / profile validation ─────────────────

    #[test]
    fn test_wildcard_cors_emits_warning() {
        let config = Config::default();
        assert_eq!(config.http.cors_origins, "*");
        let warnings = config.warnings();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("cors_origins"));
    }

    #[test]
    fn test_allow_wildcard_cors_suppresses_warning() {
        let config = Config {
            http: HttpConfig {
                allow_wildcard_cors: true,
                ..Config::default().http
            },
            ..Config::default()
        };
        assert!(config.warnings().is_empty());
    }

    #[test]
    fn test_explicit_origins_no_warning() {
        let config = Config {
            http: HttpConfig {
                cors_origins: "https://example.com".to_string(),
                ..Config::default().http
            },
            ..Config::default()
        };
        assert!(config.warnings().is_empty());
    }

    #[test]
    fn test_validate_profile_rejects_wildcard_cors_for_prod_profiles() {
        let config = Config::default();
        for profile in [
            DeploymentProfile::Prod,
            DeploymentProfile::ProdTls,
            DeploymentProfile::ProdMtls,
        ] {
            let expected = ConfigValidationError::WildcardCorsOriginsProd {
                profile: profile.as_str().to_string(),
            };
            assert_eq!(
                config.validate_profile(&profile).expect_err("should fail"),
                expected,
            );
        }
    }

    #[test]
    fn test_validate_profile_allows_wildcard_cors_for_non_prod_profiles() {
        let config = Config::default();
        for profile in [
            DeploymentProfile::Default,
            DeploymentProfile::Dev,
            DeploymentProfile::Named("staging".to_string()),
        ] {
            assert!(
                config.validate_profile(&profile).is_ok(),
                "non-prod profile {profile:?} should pass"
            );
        }
    }

    #[test]
    fn test_validate_profile_allows_wildcard_cors_with_escape_hatch() {
        let config = Config {
            http: HttpConfig {
                allow_wildcard_cors: true,
                ..Config::default().http
            },
            ..Config::default()
        };
        assert!(config.validate_profile(&DeploymentProfile::Prod).is_ok());
        assert!(config.validate_profile(&DeploymentProfile::ProdTls).is_ok());
        assert!(
            config
                .validate_profile(&DeploymentProfile::ProdMtls)
                .is_ok()
        );
    }

    #[test]
    fn test_memory_mode_allows_empty_data_dir() {
        let config = Config {
            storage: StorageConfig {
                data_dir: String::new(),
                ..Config::default().storage
            },
            ..Config::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_allow_wildcard_cors_env_override() {
        let config = Config::from_sources_with_lookup(
            &ConfigLoadOptions::default(),
            &lookup(&[("DS_HTTP__ALLOW_WILDCARD_CORS", "true")]),
        )
        .expect("config from env");
        assert!(config.http.allow_wildcard_cors);
        assert!(config.warnings().is_empty());
        assert!(config.validate_profile(&DeploymentProfile::Prod).is_ok());
    }
}
