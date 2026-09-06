//! Durable subscription control state, generation fencing, and delivery scheduling.
//!
//! State is persisted separately from application streams. Each mutation is
//! saved before becoming visible or causing an external delivery. Stream tails
//! are reconciled periodically, so acknowledged appends survive process crashes
//! even when the process stops before issuing the corresponding wake.

mod api;
mod crypto;
mod delivery;
mod model;
mod worker;

pub(crate) use worker::run;

use crate::{
    config::Config,
    execution::{Execution, ExecutionError, JobError},
    protocol::offset::Offset,
    storage::Storage,
};
use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
};
use model::{Database, DeliveryType, Link, Subscription, Wake};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    extra: Value,
}
type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    fn bad(code: &'static str, message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message: message.into(),
            extra: Value::Null,
        }
    }
    fn internal(message: &str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "UNAVAILABLE",
            message: message.into(),
            extra: Value::Null,
        }
    }
    fn fenced() -> Self {
        Self::conflict("FENCED", "wake or lease is no longer current")
    }
    fn conflict(code: &'static str, message: &str) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
            extra: Value::Null,
        }
    }
    fn missing() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: "subscription not found".into(),
            extra: Value::Null,
        }
    }
    fn json(_: serde_json::Error) -> Self {
        Self::bad("INVALID_JSON", "invalid JSON request")
    }
}
impl From<crate::protocol::error::Error> for ApiError {
    fn from(error: crate::protocol::error::Error) -> Self {
        tracing::warn!(%error, "subscription storage operation failed");
        Self::internal("subscription storage operation failed")
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut error = json!({"code":self.code, "message":self.message});
        if let (Some(dst), Some(extra)) = (error.as_object_mut(), self.extra.as_object()) {
            dst.extend(extra.clone());
        }
        let mut response = (self.status, Json(json!({"error":error}))).into_response();
        if self.status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert("retry-after", axum::http::HeaderValue::from_static("1"));
        }
        response
    }
}

pub(crate) struct Service {
    storage: Arc<dyn Storage>,
    execution: Arc<Execution>,
    database: Mutex<Database>,
    base_path: String,
    allow_local: bool,
}

pub(crate) fn initialize(
    storage: Arc<dyn Storage>,
    config: &Config,
    execution: Arc<Execution>,
) -> Result<Arc<Service>, crate::router::ServerError> {
    let database = Service::load_database(storage.as_ref())?;
    let service = Service {
        storage,
        execution,
        database: Mutex::new(database),
        base_path: config
            .http
            .stream_base_path
            .trim_end_matches('/')
            .to_string(),
        allow_local: config.http.allow_insecure_webhooks,
    };
    Ok(Arc::new(service))
}

pub(crate) fn routes(service: Arc<Service>) -> Router {
    Router::new()
        .route("/__ds/{*control}", any(api::control))
        .route("/__ds", any(|| async { ApiError::missing() }))
        .with_state(service)
}

impl Service {
    fn load_database(storage: &dyn Storage) -> Result<Database, crate::router::ServerError> {
        let db = match storage.load_subscription_state()? {
            Some(bytes) => {
                let db: Database = serde_json::from_slice(&bytes).map_err(|_| {
                    crate::router::ServerError::Initialization(
                        "invalid persisted subscription state".into(),
                    )
                })?;
                if db.version != 1 {
                    return Err(crate::router::ServerError::Initialization(
                        "unsupported subscription state version".into(),
                    ));
                }
                crypto::jwk(&db.signing_key)
                    .map_err(|e| crate::router::ServerError::Initialization(e.message))?;
                db
            }
            None => Database {
                version: 1,
                signing_key: crypto::new_key()
                    .map_err(|e| crate::router::ServerError::Initialization(e.message))?,
                subscriptions: BTreeMap::new(),
            },
        };
        storage.save_subscription_state(
            &serde_json::to_vec(&db)
                .map_err(|e| crate::router::ServerError::Initialization(e.to_string()))?,
        )?;
        Ok(db)
    }

    fn save(&self, current: &mut Database, next: Database) -> ApiResult<()> {
        let bytes = serde_json::to_vec(&next).map_err(ApiError::json)?;
        let old = serde_json::to_vec(current).map_err(ApiError::json)?;
        if old != bytes {
            self.storage.save_subscription_state(&bytes)?;
        }
        *current = next;
        Ok(())
    }

    fn lock_database(&self) -> std::sync::MutexGuard<'_, Database> {
        self.database
            .lock()
            .expect("subscription database lock poisoned")
    }

    fn tails(&self) -> ApiResult<BTreeMap<String, String>> {
        Ok(self
            .storage
            .list_streams()?
            .into_iter()
            .filter(|(n, _)| !n.starts_with("__ds/"))
            .map(|(name, meta)| (name, meta.next_offset.to_string()))
            .collect())
    }
}

fn refresh(db: &mut Database, tails: &BTreeMap<String, String>, now: i64) {
    for sub in db.subscriptions.values_mut().filter(|s| !s.deleted) {
        for path in tails.keys() {
            if sub.config.matches(path) {
                sub.links.entry(path.clone()).or_insert_with(|| Link {
                    explicit: false,
                    acked_offset: Offset::new(0, 0).to_string(),
                });
            }
        }
        if sub
            .wake
            .as_ref()
            .is_some_and(|w| w.lease_until.is_some_and(|until| until <= now) && !sub.failed)
        {
            sub.wake = None;
            sub.next_attempt_at = now;
        }
    }
}

fn issue_wake(
    sub: &mut Subscription,
    id: &str,
    key: &[u8],
    tails: &BTreeMap<String, String>,
    now: i64,
) -> ApiResult<()> {
    let streams = sub.snapshots(tails);
    sub.generation = sub
        .generation
        .checked_add(1)
        .ok_or_else(|| ApiError::internal("generation exhausted"))?;
    let wake_id = crypto::random_id()?;
    let webhook = sub.config.kind == DeliveryType::Webhook;
    let token = if webhook {
        Some(crypto::token(
            key,
            &crypto::Claims {
                subscription: id.into(),
                generation: sub.generation,
                wake_id: wake_id.clone(),
            },
        )?)
    } else {
        None
    };
    sub.wake = Some(Wake {
        id: wake_id,
        generation: sub.generation,
        streams,
        token,
        holder: None,
        lease_until: webhook.then_some(now + sub.config.lease_ttl_ms),
        delivered: false,
    });
    sub.attempts = 0;
    sub.failed = false;
    sub.next_attempt_at = now;
    Ok(())
}

fn pending(sub: &Subscription, tails: &BTreeMap<String, String>) -> bool {
    sub.snapshots(tails).iter().any(|s| s.has_pending)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    stream: String,
    offset: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Completion {
    wake_id: String,
    generation: u64,
    #[serde(default)]
    acks: Vec<Ack>,
    #[serde(default)]
    done: bool,
}

impl JobError for ApiError {
    fn is_server_error(&self) -> bool {
        self.status.is_server_error()
    }
    fn from_execution(error: ExecutionError) -> Self {
        let mut response = Self::internal("subscription execution unavailable");
        if matches!(error, ExecutionError::Failed) {
            response.status = StatusCode::INTERNAL_SERVER_ERROR;
        }
        response
    }
}
