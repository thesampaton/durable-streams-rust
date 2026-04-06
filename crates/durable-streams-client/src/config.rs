//! Configuration types and layered loading for the client crate.
//!
//! [`ClientConfig`] is the resolved runtime view. [`ClientConfigLoader`] is the
//! operational helper that merges TOML files and environment-variable overrides.

use crate::auth::AuthConfig;
use crate::error::Error;
use crate::model::RetryOptions;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use url::Url;

/// Resolved client configuration used to construct a [`crate::Client`].
#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    /// Base server URL, for example `http://127.0.0.1:4437`.
    pub base_url: Url,
    /// Authentication mode applied to outgoing requests.
    pub auth: AuthConfig,
    /// Transport-level timeouts, headers, and proxy settings.
    pub transport: TransportConfig,
    /// Retry behavior for transient operations.
    pub retry: RetryOptions,
    /// Request defaults merged into per-call options.
    pub defaults: DefaultsConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_url: Url::parse("http://127.0.0.1:8080").expect("static URL"),
            auth: AuthConfig::None,
            transport: TransportConfig::default(),
            retry: RetryOptions {
                max_retries: 3,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(2),
                backoff_multiplier: 2.0,
            },
            defaults: DefaultsConfig::default(),
        }
    }
}

impl ClientConfig {
    /// Validate auth, transport, and retry settings before client construction.
    pub fn validate(&self) -> Result<(), Error> {
        self.auth.validate()?;
        crate::retry::RetryPolicy::validate(self.retry)?;
        self.transport.validate()?;
        Ok(())
    }
}

/// Default request-level options assembled from configuration.
///
/// These values are merged into individual requests unless the call site
/// overrides them explicitly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct DefaultsConfig {
    pub default_content_type: Option<String>,
    pub headers: HashMap<String, String>,
    pub query: HashMap<String, String>,
}

/// Transport-level configuration for the underlying HTTP client.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct TransportConfig {
    /// TCP/TLS connect timeout.
    pub connect_timeout: Duration,
    /// Whole-request timeout.
    pub request_timeout: Duration,
    /// User-Agent header sent with requests.
    pub user_agent: String,
    /// Optional proxy URL passed through to `reqwest`.
    pub proxy_url: Option<String>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            user_agent: "durable-streams-client/0.1.0".to_string(),
            proxy_url: None,
        }
    }
}

impl TransportConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.connect_timeout.is_zero() {
            return Err(Error::invalid_argument(
                "transport connect_timeout must be greater than zero",
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(Error::invalid_argument(
                "transport request_timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// File-and-environment configuration loader.
///
/// Load order:
///
/// 1. `config/default.toml`
/// 2. `config/<profile>.toml`
/// 3. `config/local.toml`
/// 4. `config_override` if set
/// 5. environment variables with `env_prefix`
#[derive(Debug, Clone)]
pub struct ClientConfigLoader {
    /// Directory containing layered TOML config files.
    pub config_dir: PathBuf,
    /// Profile loaded after `default.toml`.
    pub profile: String,
    /// Optional extra TOML file merged last before env overrides.
    pub config_override: Option<PathBuf>,
    /// Environment variable prefix, for example `DURABLE_STREAMS_CLIENT__`.
    pub env_prefix: String,
}

impl Default for ClientConfigLoader {
    fn default() -> Self {
        Self {
            config_dir: PathBuf::from("config"),
            profile: "default".to_string(),
            config_override: None,
            env_prefix: "DURABLE_STREAMS_CLIENT__".to_string(),
        }
    }
}

impl ClientConfigLoader {
    /// Load configuration from files plus environment-variable overrides.
    pub fn load(&self) -> Result<ClientConfig, ClientConfigLoaderError> {
        self.load_with_lookup(&|key| env::var(key).ok())
    }

    /// Load configuration from the default search path plus one explicit override file.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<ClientConfig, ClientConfigLoaderError> {
        let mut loader = Self::default();
        loader.config_override = Some(path.as_ref().to_path_buf());
        loader.load()
    }

    fn load_with_lookup(
        &self,
        get_env: &impl Fn(&str) -> Option<String>,
    ) -> Result<ClientConfig, ClientConfigLoaderError> {
        let mut file_config = FileConfig::default();

        self.merge_if_exists(self.config_dir.join("default.toml"), &mut file_config)?;
        let profile_path = self
            .config_dir
            .join(format!("{}.toml", self.profile.trim()));
        self.merge_if_exists(profile_path, &mut file_config)?;
        self.merge_if_exists(self.config_dir.join("local.toml"), &mut file_config)?;

        if let Some(path) = &self.config_override {
            if !path.is_file() {
                return Err(ClientConfigLoaderError::Config(format!(
                    "config override file not found: '{}'",
                    path.display()
                )));
            }
            self.merge_file(path, &mut file_config)?;
        }

        apply_env_overrides(&self.env_prefix, get_env, &mut file_config)?;

        let config = file_config.try_into_config()?;
        config.validate().map_err(ClientConfigLoaderError::Error)?;
        Ok(config)
    }

    fn merge_if_exists(
        &self,
        path: PathBuf,
        into: &mut FileConfig,
    ) -> Result<(), ClientConfigLoaderError> {
        if path.is_file() {
            self.merge_file(path, into)?;
        }
        Ok(())
    }

    fn merge_file(
        &self,
        path: impl AsRef<Path>,
        into: &mut FileConfig,
    ) -> Result<(), ClientConfigLoaderError> {
        let raw = fs::read_to_string(path.as_ref())?;
        let parsed: FileConfig = toml::from_str(&raw)?;
        into.merge(parsed);
        Ok(())
    }
}

/// Errors returned while assembling [`ClientConfig`] from operational sources.
#[derive(Debug)]
pub enum ClientConfigLoaderError {
    Io(std::io::Error),
    Toml(toml::de::Error),
    Config(String),
    Error(Error),
}

impl fmt::Display for ClientConfigLoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Toml(error) => write!(f, "{error}"),
            Self::Config(message) => f.write_str(message),
            Self::Error(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClientConfigLoaderError {}

impl From<std::io::Error> for ClientConfigLoaderError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<toml::de::Error> for ClientConfigLoaderError {
    fn from(value: toml::de::Error) -> Self {
        Self::Toml(value)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FileConfig {
    client: ClientFileConfig,
    auth: AuthFileConfig,
    transport: TransportFileConfig,
    retry: RetryFileConfig,
    defaults: DefaultsConfig,
}

impl FileConfig {
    fn merge(&mut self, other: Self) {
        self.client.merge(other.client);
        self.auth.merge(other.auth);
        self.transport.merge(other.transport);
        self.retry.merge(other.retry);
        if other.defaults.default_content_type.is_some() {
            self.defaults.default_content_type = other.defaults.default_content_type;
        }
        self.defaults.headers.extend(other.defaults.headers);
        self.defaults.query.extend(other.defaults.query);
    }

    fn try_into_config(self) -> Result<ClientConfig, ClientConfigLoaderError> {
        let mut config = ClientConfig::default();

        if let Some(base_url) = self.client.base_url {
            config.base_url = Url::parse(&base_url)
                .map_err(Error::from)
                .map_err(ClientConfigLoaderError::Error)?;
        }

        config.auth = self.auth.into_auth()?;
        config.transport = self.transport.into_transport(config.transport);
        config.retry = self.retry.into_retry(config.retry)?;
        config.defaults = self.defaults;

        Ok(config)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ClientFileConfig {
    base_url: Option<String>,
}

impl ClientFileConfig {
    fn merge(&mut self, other: Self) {
        if other.base_url.is_some() {
            self.base_url = other.base_url;
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct AuthFileConfig {
    r#type: Option<String>,
    bearer_token: Option<String>,
    username: Option<String>,
    password: Option<String>,
    header_name: Option<String>,
    header_value: Option<String>,
}

impl AuthFileConfig {
    fn merge(&mut self, other: Self) {
        if other.r#type.is_some() {
            self.r#type = other.r#type;
        }
        if other.bearer_token.is_some() {
            self.bearer_token = other.bearer_token;
        }
        if other.username.is_some() {
            self.username = other.username;
        }
        if other.password.is_some() {
            self.password = other.password;
        }
        if other.header_name.is_some() {
            self.header_name = other.header_name;
        }
        if other.header_value.is_some() {
            self.header_value = other.header_value;
        }
    }

    fn into_auth(self) -> Result<AuthConfig, ClientConfigLoaderError> {
        let auth = match self.r#type.as_deref().unwrap_or("none") {
            "none" => AuthConfig::None,
            "bearer" => AuthConfig::Bearer {
                token: self.bearer_token.ok_or_else(|| {
                    ClientConfigLoaderError::Config("auth bearer_token is required".to_string())
                })?,
            },
            "basic" => AuthConfig::Basic {
                username: self.username.ok_or_else(|| {
                    ClientConfigLoaderError::Config("auth username is required".to_string())
                })?,
                password: self.password.ok_or_else(|| {
                    ClientConfigLoaderError::Config("auth password is required".to_string())
                })?,
            },
            "header" => AuthConfig::Header {
                name: self.header_name.ok_or_else(|| {
                    ClientConfigLoaderError::Config("auth header_name is required".to_string())
                })?,
                value: self.header_value.ok_or_else(|| {
                    ClientConfigLoaderError::Config("auth header_value is required".to_string())
                })?,
            },
            other => {
                return Err(ClientConfigLoaderError::Config(format!(
                    "unsupported auth.type value: '{other}'"
                )));
            }
        };

        Ok(auth)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct TransportFileConfig {
    connect_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    user_agent: Option<String>,
    proxy_url: Option<String>,
}

impl TransportFileConfig {
    fn merge(&mut self, other: Self) {
        if other.connect_timeout_ms.is_some() {
            self.connect_timeout_ms = other.connect_timeout_ms;
        }
        if other.request_timeout_ms.is_some() {
            self.request_timeout_ms = other.request_timeout_ms;
        }
        if other.user_agent.is_some() {
            self.user_agent = other.user_agent;
        }
        if other.proxy_url.is_some() {
            self.proxy_url = other.proxy_url;
        }
    }

    fn into_transport(self, mut current: TransportConfig) -> TransportConfig {
        if let Some(timeout) = self.connect_timeout_ms {
            current.connect_timeout = Duration::from_millis(timeout);
        }
        if let Some(timeout) = self.request_timeout_ms {
            current.request_timeout = Duration::from_millis(timeout);
        }
        if let Some(user_agent) = self.user_agent {
            current.user_agent = user_agent;
        }
        if self.proxy_url.is_some() {
            current.proxy_url = self.proxy_url;
        }
        current
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RetryFileConfig {
    max_retries: Option<u32>,
    initial_backoff_ms: Option<u64>,
    max_backoff_ms: Option<u64>,
    backoff_multiplier: Option<f64>,
}

impl RetryFileConfig {
    fn merge(&mut self, other: Self) {
        if other.max_retries.is_some() {
            self.max_retries = other.max_retries;
        }
        if other.initial_backoff_ms.is_some() {
            self.initial_backoff_ms = other.initial_backoff_ms;
        }
        if other.max_backoff_ms.is_some() {
            self.max_backoff_ms = other.max_backoff_ms;
        }
        if other.backoff_multiplier.is_some() {
            self.backoff_multiplier = other.backoff_multiplier;
        }
    }

    fn into_retry(
        self,
        mut current: RetryOptions,
    ) -> Result<RetryOptions, ClientConfigLoaderError> {
        if let Some(max_retries) = self.max_retries {
            current.max_retries = max_retries;
        }
        if let Some(initial) = self.initial_backoff_ms {
            current.initial_backoff = Duration::from_millis(initial);
        }
        if let Some(max) = self.max_backoff_ms {
            current.max_backoff = Duration::from_millis(max);
        }
        if let Some(multiplier) = self.backoff_multiplier {
            current.backoff_multiplier = multiplier;
        }
        crate::retry::RetryPolicy::validate(current).map_err(ClientConfigLoaderError::Error)?;
        Ok(current)
    }
}

fn apply_env_overrides(
    prefix: &str,
    get_env: &impl Fn(&str) -> Option<String>,
    config: &mut FileConfig,
) -> Result<(), ClientConfigLoaderError> {
    if let Some(base_url) = get_env(&format!("{prefix}CLIENT__BASE_URL")) {
        config.client.base_url = Some(base_url);
    }
    if let Some(kind) = get_env(&format!("{prefix}AUTH__TYPE")) {
        config.auth.r#type = Some(kind);
    }
    if let Some(token) = get_env(&format!("{prefix}AUTH__BEARER_TOKEN")) {
        config.auth.bearer_token = Some(token);
    }
    if let Some(username) = get_env(&format!("{prefix}AUTH__USERNAME")) {
        config.auth.username = Some(username);
    }
    if let Some(password) = get_env(&format!("{prefix}AUTH__PASSWORD")) {
        config.auth.password = Some(password);
    }
    if let Some(name) = get_env(&format!("{prefix}AUTH__HEADER_NAME")) {
        config.auth.header_name = Some(name);
    }
    if let Some(value) = get_env(&format!("{prefix}AUTH__HEADER_VALUE")) {
        config.auth.header_value = Some(value);
    }
    if let Some(ms) = get_env(&format!("{prefix}TRANSPORT__CONNECT_TIMEOUT_MS")) {
        config.transport.connect_timeout_ms =
            Some(parse_u64("TRANSPORT__CONNECT_TIMEOUT_MS", &ms)?);
    }
    if let Some(ms) = get_env(&format!("{prefix}TRANSPORT__REQUEST_TIMEOUT_MS")) {
        config.transport.request_timeout_ms =
            Some(parse_u64("TRANSPORT__REQUEST_TIMEOUT_MS", &ms)?);
    }
    if let Some(user_agent) = get_env(&format!("{prefix}TRANSPORT__USER_AGENT")) {
        config.transport.user_agent = Some(user_agent);
    }
    if let Some(proxy_url) = get_env(&format!("{prefix}TRANSPORT__PROXY_URL")) {
        config.transport.proxy_url = Some(proxy_url);
    }
    if let Some(max_retries) = get_env(&format!("{prefix}RETRY__MAX_RETRIES")) {
        config.retry.max_retries = Some(parse_u32("RETRY__MAX_RETRIES", &max_retries)?);
    }
    if let Some(ms) = get_env(&format!("{prefix}RETRY__INITIAL_BACKOFF_MS")) {
        config.retry.initial_backoff_ms = Some(parse_u64("RETRY__INITIAL_BACKOFF_MS", &ms)?);
    }
    if let Some(ms) = get_env(&format!("{prefix}RETRY__MAX_BACKOFF_MS")) {
        config.retry.max_backoff_ms = Some(parse_u64("RETRY__MAX_BACKOFF_MS", &ms)?);
    }
    if let Some(multiplier) = get_env(&format!("{prefix}RETRY__BACKOFF_MULTIPLIER")) {
        config.retry.backoff_multiplier = Some(multiplier.parse::<f64>().map_err(|_| {
            ClientConfigLoaderError::Config(
                "invalid RETRY__BACKOFF_MULTIPLIER environment override".to_string(),
            )
        })?);
    }
    if let Some(content_type) = get_env(&format!("{prefix}DEFAULTS__DEFAULT_CONTENT_TYPE")) {
        config.defaults.default_content_type = Some(content_type);
    }
    if let Some(headers_json) = get_env(&format!("{prefix}DEFAULTS__HEADERS_JSON")) {
        config.defaults.headers = serde_json::from_str(&headers_json).map_err(|error| {
            ClientConfigLoaderError::Config(format!(
                "invalid DEFAULTS__HEADERS_JSON environment override: {error}"
            ))
        })?;
    }
    if let Some(query_json) = get_env(&format!("{prefix}DEFAULTS__QUERY_JSON")) {
        config.defaults.query = serde_json::from_str(&query_json).map_err(|error| {
            ClientConfigLoaderError::Config(format!(
                "invalid DEFAULTS__QUERY_JSON environment override: {error}"
            ))
        })?;
    }
    Ok(())
}

fn parse_u32(name: &str, value: &str) -> Result<u32, ClientConfigLoaderError> {
    value.parse::<u32>().map_err(|_| {
        ClientConfigLoaderError::Config(format!("invalid {name} environment override"))
    })
}

fn parse_u64(name: &str, value: &str) -> Result<u64, ClientConfigLoaderError> {
    value.parse::<u64>().map_err(|_| {
        ClientConfigLoaderError::Config(format!("invalid {name} environment override"))
    })
}

#[cfg(test)]
mod tests {
    use super::ClientConfigLoader;

    #[test]
    fn env_override_wins() {
        let loader = ClientConfigLoader::default();
        let config = loader
            .load_with_lookup(&|key| match key {
                "DURABLE_STREAMS_CLIENT__CLIENT__BASE_URL" => {
                    Some("http://example.test".to_string())
                }
                "DURABLE_STREAMS_CLIENT__AUTH__TYPE" => Some("bearer".to_string()),
                "DURABLE_STREAMS_CLIENT__AUTH__BEARER_TOKEN" => Some("secret".to_string()),
                _ => None,
            })
            .expect("config loads");

        assert_eq!(config.base_url.as_str(), "http://example.test/");
        assert!(matches!(config.auth, crate::AuthConfig::Bearer { .. }));
    }
}
