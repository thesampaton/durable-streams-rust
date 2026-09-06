//! Async delivery scheduling around bounded, serialized persistence jobs.

use super::model::DeliveryType;
use super::{ApiError, ApiResult, Service, crypto, delivery, issue_wake, pending, refresh};
use crate::execution::ExecutionError;
use axum::body::Bytes;
use chrono::Utc;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const MAX_DELIVERIES: usize = 16;

#[cfg(test)]
mod tests;
type DeliveryKey = (String, u64);

struct DeliveryOutcome {
    key: DeliveryKey,
    result: ApiResult<bool>,
}

pub(crate) async fn run(weak: Weak<Service>, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut jobs = tokio::task::JoinSet::new();
    let mut active = BTreeSet::new();
    let mut completions = BTreeMap::new();
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            result = jobs.join_next(), if !jobs.is_empty() => {
                match result {
                    Some(Ok(DeliveryOutcome { key, result })) => {
                        completions.insert(key, Arc::new(result));
                    }
                    Some(Err(error)) => {
                        tracing::error!(%error, "subscription delivery task failed");
                        shutdown.cancel();
                        break;
                    }
                    None => {}
                }
            }
            _ = interval.tick() => {
                let Some(service) = weak.upgrade() else { break; };
                if let Err(error) = persist_completions(&service, &mut completions, &mut active).await {
                    if matches!(error, ExecutionError::Busy) { continue; }
                    break;
                }
                let execution = service.execution.clone();
                let running = active.clone();
                let transaction = service.clone();
                match execution.run("subscription reconcile", move || tick(&transaction, &running)).await {
                    Ok(Ok(work)) => {
                        if shutdown.is_cancelled() { break; }
                        for job in work {
                            let key = (job.id, job.generation);
                            active.insert(key.clone());
                            let allow_local = service.allow_local;
                            jobs.spawn(async move {
                                let result = delivery::deliver(&job.url, allow_local, job.body, job.signature).await;
                                DeliveryOutcome { key, result }
                            });
                        }
                    },
                    Ok(Err(_)) | Err(ExecutionError::Busy) => {}, // Recorded by the boundary; retry on a later tick.
                    Err(_) => break,
                }
            }
        }
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
}

async fn persist_completions(
    service: &Arc<Service>,
    completions: &mut BTreeMap<DeliveryKey, Arc<ApiResult<bool>>>,
    active: &mut BTreeSet<DeliveryKey>,
) -> Result<(), ExecutionError> {
    // Pending results still occupy delivery capacity, even when storage is full.
    for key in completions.keys().cloned().collect::<Vec<_>>() {
        let result = completions[&key].clone();
        let transaction = service.clone();
        let (id, generation) = key.clone();
        match service
            .execution
            .run("subscription completion", move || {
                finish(&transaction, &id, generation, &result)
            })
            .await?
        {
            Ok(()) => {
                completions.remove(&key);
                active.remove(&key);
            }
            Err(_) => break, // Keep the result through a failed save; avoid a tight retry loop.
        }
    }
    Ok(())
}

struct Job {
    id: String,
    generation: u64,
    url: String,
    body: Vec<u8>,
    signature: String,
}

fn tick(service: &Service, active: &BTreeSet<(String, u64)>) -> ApiResult<Vec<Job>> {
    let mut guard = service.lock_database();
    let mut db = guard.clone();
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
        } else if active.len() + jobs.len() < MAX_DELIVERIES {
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
        let result = service
            .storage
            .append(&stream, bytes, "application/json")
            .map(|result| result.start_offset);
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

fn finish(service: &Service, id: &str, generation: u64, result: &ApiResult<bool>) -> ApiResult<()> {
    let mut guard = service.lock_database();
    let mut db = guard.clone();
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
        if *done {
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
