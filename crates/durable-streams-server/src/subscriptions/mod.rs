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

use crate::{config::Config, protocol::offset::Offset, storage::Storage};
use axum::{
    Json, Router,
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
};
use chrono::Utc;
use model::{Database, DeliveryType, Link, Subscription, Wake};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
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
        (self.status, Json(json!({"error":error}))).into_response()
    }
}

struct Service<S> {
    storage: Arc<S>,
    database: Mutex<Option<Database>>,
    base_path: String,
    allow_local: bool,
}

pub(crate) fn routes<S: Storage + 'static>(
    storage: Arc<S>,
    config: &Config,
    shutdown: CancellationToken,
) -> Router {
    let service = Arc::new(Service {
        storage,
        database: Mutex::new(None),
        base_path: config
            .http
            .stream_base_path
            .trim_end_matches('/')
            .to_string(),
        allow_local: config.http.allow_insecure_webhooks,
    });
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::spawn(run(Arc::downgrade(&service), shutdown));
    }
    Router::new()
        .route("/__ds/{*control}", any(api::control::<S>))
        .route("/__ds", any(|| async { ApiError::missing() }))
        .with_state(service)
}

impl<S: Storage> Service<S> {
    fn load(&self, database: &mut Option<Database>) -> ApiResult<()> {
        if database.is_some() {
            return Ok(());
        }
        let db = match self.storage.load_subscription_state()? {
            Some(bytes) => {
                let db: Database = serde_json::from_slice(&bytes)
                    .map_err(|_| ApiError::internal("invalid persisted subscription state"))?;
                if db.version != 1 {
                    return Err(ApiError::internal("unsupported subscription state version"));
                }
                crypto::jwk(&db.signing_key)?;
                db
            }
            None => Database {
                version: 1,
                signing_key: crypto::new_key()?,
                subscriptions: BTreeMap::new(),
            },
        };
        self.storage
            .save_subscription_state(&serde_json::to_vec(&db).map_err(ApiError::json)?)?;
        *database = Some(db);
        Ok(())
    }

    fn save(&self, current: &mut Option<Database>, next: Database) -> ApiResult<()> {
        let bytes = serde_json::to_vec(&next).map_err(ApiError::json)?;
        let old = current
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(ApiError::json)?;
        if old.as_deref() != Some(bytes.as_slice()) {
            self.storage.save_subscription_state(&bytes)?;
        }
        *current = Some(next);
        Ok(())
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

struct Job {
    id: String,
    generation: u64,
    url: String,
    body: Vec<u8>,
    signature: String,
}

async fn run<S: Storage + 'static>(weak: Weak<Service<S>>, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    let mut jobs: tokio::task::JoinSet<(String, u64, ApiResult<bool>)> =
        tokio::task::JoinSet::new();
    let mut active = BTreeSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                if let Ok((id, generation, result)) = result {
                    active.remove(&(id.clone(), generation));
                    let Some(service) = weak.upgrade() else { break; };
                    if let Err(error) = finish(&service, &id, generation, result).await { tracing::warn!(code = error.code, "could not persist webhook result"); }
                }
            },
            _ = interval.tick() => {
                let Some(service) = weak.upgrade() else { break; };
                match tick(&service, &active).await {
                    Ok(work) => for job in work {
                        active.insert((job.id.clone(), job.generation));
                        let allow_local = service.allow_local;
                        jobs.spawn(async move {
                            let result = delivery::deliver(&job.url, allow_local, job.body, job.signature).await;
                            (job.id, job.generation, result)
                        });
                    },
                    Err(error) => tracing::warn!(code = error.code, "subscription reconciliation failed"),
                }
            },
        }
    }
    jobs.abort_all();
}

async fn tick<S: Storage>(
    service: &Service<S>,
    active: &BTreeSet<(String, u64)>,
) -> ApiResult<Vec<Job>> {
    let mut guard = service.database.lock().await;
    // A backend that has never hosted subscriptions need not persist a signing
    // key or implement control storage merely to serve ordinary stream traffic.
    if guard.is_none() && service.storage.load_subscription_state()?.is_none() {
        return Ok(Vec::new());
    }
    service.load(&mut guard)?;
    let mut db = guard.as_ref().expect("database loaded").clone();
    // Avoid scanning application streams when no subscriptions need reconciliation.
    if db.subscriptions.values().all(|s| s.deleted) {
        return Ok(Vec::new());
    }
    let tails = service.tails()?;
    let now = Utc::now().timestamp_millis();
    refresh(&mut db, &tails, now);
    let mut jobs = Vec::new();
    let mut pull = Vec::new();
    let jwk = crypto::jwk(&db.signing_key)?;
    for (id, sub) in &mut db.subscriptions {
        if sub.deleted {
            continue;
        }
        if sub.wake.is_none() && pending(sub, &tails) {
            issue_wake(sub, id, &db.signing_key, &tails, now)?;
        }
        let Some(wake) = &mut sub.wake else {
            continue;
        };
        if wake.delivered
            || sub.next_attempt_at > now
            || active.contains(&(id.clone(), wake.generation))
        {
            continue;
        }
        if sub.config.kind == DeliveryType::PullWake {
            if let Some(path) = wake.streams.iter().find(|s| s.has_pending).map(|s| &s.path) {
                pull.push((id.clone(), wake.generation, sub.config.wake_stream.clone().expect("validated wake stream"), json!({"type":"wake", "subscription_id":id, "stream":path, "generation":wake.generation, "ts":now})));
            }
        } else if active.len() + jobs.len() < 16 {
            wake.lease_until = Some(now + sub.config.lease_ttl_ms);
            sub.failed = false;
            let body = serde_json::to_vec(&json!({"subscription_id":id, "wake_id":wake.id, "generation":wake.generation, "streams":wake.streams,
                "callback_url":format!("{}{}/__ds/subscriptions/{id}/callback", sub.origin, service.base_path), "callback_token":wake.token})).map_err(ApiError::json)?;
            let timestamp = Utc::now().timestamp();
            let mut signed = format!("{timestamp}.").into_bytes();
            signed.extend_from_slice(&body);
            let signature = format!(
                "t={timestamp},kid={},ed25519={}",
                jwk["kid"].as_str().expect("JWK kid"),
                crypto::signature(&db.signing_key, &signed)?
            );
            jobs.push(Job {
                id: id.clone(),
                generation: wake.generation,
                url: sub
                    .config
                    .webhook
                    .as_ref()
                    .expect("validated webhook")
                    .url
                    .clone(),
                body,
                signature,
            });
            // Persist a retry deadline before sending: restart must not produce a tight retry loop.
            sub.next_attempt_at = now + 6_000;
        }
    }
    service.save(&mut guard, db.clone())?;
    // At-least-once wake publication: a crash between append and marking delivered
    // may repeat a generation; subscription-level claims still fence workers.
    for (id, generation, stream, event) in pull {
        let bytes = Bytes::from(serde_json::to_vec(&event).map_err(ApiError::json)?);
        let result = service.storage.append(&stream, bytes, "application/json");
        if let Some(sub) = db.subscriptions.get_mut(&id) {
            if result.is_ok() {
                if let Some(wake) = &mut sub.wake
                    && wake.generation == generation
                {
                    wake.delivered = true;
                }
                sub.failed = false;
            } else {
                sub.failed = true;
                sub.next_attempt_at = now + 1_000;
            }
        }
    }
    service.save(&mut guard, db)?;
    Ok(jobs)
}

async fn finish<S: Storage>(
    service: &Service<S>,
    id: &str,
    generation: u64,
    result: ApiResult<bool>,
) -> ApiResult<()> {
    let mut guard = service.database.lock().await;
    service.load(&mut guard)?;
    let mut db = guard.as_ref().expect("database loaded").clone();
    let Some(sub) = db.subscriptions.get_mut(id).filter(|s| !s.deleted) else {
        return Ok(());
    };
    let Some(wake) = &mut sub.wake else {
        return Ok(());
    };
    if wake.generation != generation
        || wake
            .lease_until
            .is_some_and(|until| until <= Utc::now().timestamp_millis())
    {
        return Ok(());
    }
    let now = Utc::now().timestamp_millis();
    if let Ok(done) = result {
        wake.delivered = true;
        sub.failed = false;
        sub.attempts = 0;
        if done {
            for stream in &wake.streams {
                if let Some(link) = sub.links.get_mut(&stream.path) {
                    link.acked_offset = link.acked_offset.clone().max(stream.tail_offset.clone());
                }
            }
            sub.wake = None;
            sub.next_attempt_at = now;
        }
    } else {
        sub.failed = true;
        let delay = 1_000_i64
            .saturating_mul(1_i64 << sub.attempts.min(6))
            .min(60_000);
        sub.attempts = sub.attempts.saturating_add(1);
        let jitter = crypto::retry_jitter()?;
        sub.next_attempt_at = now + delay + delay * jitter / 1_000;
    }
    service.save(&mut guard, db)
}
