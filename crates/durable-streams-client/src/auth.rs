use serde::{Deserialize, Serialize};
use std::fmt;

/// Authentication configuration applied to outgoing requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// No authentication.
    #[default]
    None,
    /// Bearer token authentication.
    Bearer { token: String },
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// Static header authentication for gateways or custom proxies.
    Header { name: String, value: String },
}

impl AuthConfig {
    pub(crate) fn apply(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            Self::None => builder,
            Self::Bearer { token } => builder.bearer_auth(token),
            Self::Basic { username, password } => builder.basic_auth(username, Some(password)),
            Self::Header { name, value } => builder.header(name, value),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        match self {
            Self::None => Ok(()),
            Self::Bearer { token } if token.trim().is_empty() => Err(
                crate::Error::invalid_argument("auth bearer token must not be empty"),
            ),
            Self::Basic { username, .. } if username.trim().is_empty() => Err(
                crate::Error::invalid_argument("auth basic username must not be empty"),
            ),
            Self::Header { name, .. } if name.trim().is_empty() => Err(
                crate::Error::invalid_argument("auth header name must not be empty"),
            ),
            Self::Header { value, .. } if value.trim().is_empty() => Err(
                crate::Error::invalid_argument("auth header value must not be empty"),
            ),
            _ => Ok(()),
        }
    }
}

impl fmt::Display for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Bearer { .. } => f.write_str("bearer"),
            Self::Basic { .. } => f.write_str("basic"),
            Self::Header { name, .. } => write!(f, "header({name})"),
        }
    }
}
