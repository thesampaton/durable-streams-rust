use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ApiError, ApiResult};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(super) enum DeliveryType {
    Webhook,
    PullWake,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Webhook {
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Configuration {
    #[serde(rename = "type")]
    pub kind: DeliveryType,
    pub pattern: Option<String>,
    #[serde(default)]
    pub streams: BTreeSet<String>,
    pub webhook: Option<Webhook>,
    pub wake_stream: Option<String>,
    #[serde(default = "default_lease")]
    pub lease_ttl_ms: i64,
    pub description: Option<String>,
}

fn default_lease() -> i64 {
    30_000
}

impl Configuration {
    pub fn validate(&self) -> ApiResult<()> {
        if self.pattern.is_none() && self.streams.is_empty() {
            return Err(ApiError::bad(
                "INVALID_CONFIG",
                "pattern or streams is required",
            ));
        }
        if !(1_000..=600_000).contains(&self.lease_ttl_ms) {
            return Err(ApiError::bad(
                "INVALID_CONFIG",
                "lease_ttl_ms must be between 1000 and 600000",
            ));
        }
        if let Some(pattern) = &self.pattern {
            validate_path(pattern)?;
            if pattern
                .split('/')
                .any(|s| s.contains('*') && s != "*" && s != "**")
            {
                return Err(ApiError::bad(
                    "INVALID_CONFIG",
                    "wildcards must occupy a complete path segment",
                ));
            }
        }
        for path in &self.streams {
            validate_path(path)?;
        }
        match self.kind {
            DeliveryType::Webhook if self.webhook.is_none() || self.wake_stream.is_some() => {
                return Err(ApiError::bad(
                    "INVALID_CONFIG",
                    "webhook delivery requires only webhook.url",
                ));
            }
            DeliveryType::PullWake if self.wake_stream.is_none() || self.webhook.is_some() => {
                return Err(ApiError::bad(
                    "INVALID_CONFIG",
                    "pull-wake delivery requires only wake_stream",
                ));
            }
            _ => {}
        }
        if let Some(path) = &self.wake_stream {
            validate_path(path)?;
            if self.matches(path) || self.streams.contains(path) {
                return Err(ApiError::bad(
                    "INVALID_CONFIG",
                    "a subscription cannot consume its own wake stream",
                ));
            }
        }
        Ok(())
    }

    pub fn matches(&self, path: &str) -> bool {
        self.pattern.as_ref().is_some_and(|p| glob_matches(p, path))
    }
}

pub(super) fn validate_path(path: &str) -> ApiResult<()> {
    if path.is_empty()
        || path.len() > 1024
        || path.split('/').count() > 64
        || crate::protocol::stream_name::invalid_segment(path).is_some()
        || crate::protocol::stream_name::is_reserved(path)
        || path.chars().any(char::is_control)
    {
        return Err(ApiError::bad(
            "INVALID_PATH",
            "expected an application stream-root-relative path",
        ));
    }
    Ok(())
}

// Dynamic programming keeps repeated ** segments bounded instead of exponential.
pub(super) fn glob_matches(pattern: &str, path: &str) -> bool {
    let path: Vec<_> = path.split('/').collect();
    let mut matches = vec![false; path.len() + 1];
    matches[0] = true;
    for segment in pattern.split('/') {
        if segment == "**" {
            for i in 1..=path.len() {
                matches[i] |= matches[i - 1];
            }
        } else {
            for i in (1..=path.len()).rev() {
                matches[i] = matches[i - 1] && (segment == "*" || segment == path[i - 1]);
            }
            matches[0] = false;
        }
    }
    matches[path.len()]
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Link {
    pub explicit: bool,
    pub acked_offset: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Snapshot {
    pub path: String,
    pub link_type: String,
    pub acked_offset: String,
    pub tail_offset: String,
    pub has_pending: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Wake {
    pub id: String,
    pub generation: u64,
    pub streams: Vec<Snapshot>,
    pub token: Option<String>,
    pub holder: Option<String>,
    pub lease_until: Option<i64>,
    pub delivered: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Subscription {
    pub config: Configuration,
    pub config_hash: String,
    pub links: BTreeMap<String, Link>,
    pub created_at: String,
    pub origin: String,
    pub generation: u64,
    pub wake: Option<Wake>,
    pub deleted: bool,
    pub next_attempt_at: i64,
    pub attempts: u32,
    pub failed: bool,
}

impl Subscription {
    pub fn snapshots(&self, tails: &BTreeMap<String, String>) -> Vec<Snapshot> {
        self.links
            .iter()
            .map(|(path, link)| {
                let tail = tails.get(path).unwrap_or(&link.acked_offset);
                Snapshot {
                    path: path.clone(),
                    link_type: if link.explicit { "explicit" } else { "glob" }.into(),
                    acked_offset: link.acked_offset.clone(),
                    tail_offset: tail.clone(),
                    has_pending: tail > &link.acked_offset,
                }
            })
            .collect()
    }

    pub fn response(&self, id: &str, kid: &str, base_path: &str) -> Value {
        let webhook = self.config.webhook.as_ref().map(|w| {
            json!({
                "url": w.url, "signing": { "alg": "ed25519", "kid": kid,
                "jwks_url": format!("{}{base_path}/__ds/jwks.json", self.origin) }
            })
        });
        json!({
            "id": id, "subscription_id": id, "type": self.config.kind,
            "pattern": self.config.pattern, "streams": self.links.iter().map(|(path, link)| json!({
                "path": path, "link_type": if link.explicit { "explicit" } else { "glob" },
                "acked_offset": link.acked_offset,
            })).collect::<Vec<_>>(), "webhook": webhook, "wake_stream": self.config.wake_stream,
            "lease_ttl_ms": self.config.lease_ttl_ms, "created_at": self.created_at,
            "status": if self.failed { "failed" } else { "active" }, "description": self.config.description,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Database {
    pub version: u32,
    pub signing_key: Vec<u8>,
    pub subscriptions: BTreeMap<String, Subscription>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_glob_segment_and_recursive_matching() {
        assert!(glob_matches("events/*", "events/a"));
        assert!(!glob_matches("events/*", "events/a/b"));
        assert!(glob_matches("events/**", "events"));
        assert!(glob_matches("events/**/done", "events/a/b/done"));
        assert!(!glob_matches("events/**/done", "events/a/b/other"));
        assert!(glob_matches("**/**/done", "done"));
    }
}
