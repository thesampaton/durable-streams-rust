use super::model::{Configuration, Database, DeliveryType, Link, Subscription};
use super::{
    ApiError, ApiResult, Completion, Service, crypto, delivery, issue_wake, model, pending, refresh,
};
use crate::{
    middleware::proxy_trust::ProxyTrustResult, protocol::offset::Offset, storage::Storage,
};
use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Copy)]
struct CompletionContext<'a> {
    id: &'a str,
    key: &'a [u8],
    headers: &'a HeaderMap,
    body: &'a [u8],
    action: &'a str,
    tails: &'a BTreeMap<String, String>,
    now: i64,
}

fn complete(sub: &mut Subscription, context: CompletionContext<'_>) -> ApiResult<Response> {
    let CompletionContext {
        id,
        key,
        headers,
        body,
        action,
        tails,
        now,
    } = context;
    let completion: Completion = serde_json::from_slice(body).map_err(ApiError::json)?;
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .ok_or_else(ApiError::fenced)?;
    let claims = crypto::verify(key, token)?;
    let wake = sub.wake.as_ref().ok_or_else(ApiError::fenced)?;
    if claims.subscription != id
        || claims.generation != wake.generation
        || claims.wake_id != wake.id
        || completion.wake_id != wake.id
        || completion.generation != wake.generation
        || wake.lease_until.is_none_or(|until| until <= now)
        || wake.token.as_deref() != Some(token)
        || (action == "callback") != (sub.config.kind == DeliveryType::Webhook)
    {
        return Err(ApiError::fenced());
    }
    // Validate the whole batch before applying any cursor update.
    for ack in &completion.acks {
        let link = sub
            .links
            .get(&ack.stream)
            .ok_or_else(|| ApiError::bad("INVALID_ACK", "stream is not linked"))?;
        let offset = ack
            .offset
            .parse::<Offset>()
            .map_err(|_| ApiError::bad("INVALID_ACK", "invalid offset"))?;
        if offset.is_start()
            || offset.is_now()
            || ack.offset < link.acked_offset
            || tails.get(&ack.stream).is_none_or(|tail| &ack.offset > tail)
        {
            return Err(ApiError::bad(
                "INVALID_ACK",
                "acknowledgement must not regress or exceed the stream tail",
            ));
        }
    }
    if action != "release" {
        for ack in completion.acks {
            if let Some(link) = sub.links.get_mut(&ack.stream) {
                link.acked_offset = link.acked_offset.clone().max(ack.offset);
            }
        }
    }
    if completion.done || action == "release" {
        sub.wake = None;
        sub.failed = false;
        sub.next_attempt_at = now;
    } else if let Some(wake) = &mut sub.wake {
        wake.lease_until = Some(now + sub.config.lease_ttl_ms);
    }
    if action == "release" {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Ok(
            Json(json!({"ok":true, "next_wake":completion.done && pending(sub, tails)}))
                .into_response(),
        )
    }
}

pub(super) async fn control<S: Storage + 'static>(
    State(service): State<Arc<Service<S>>>,
    Path(control): Path<String>,
    Extension(origin): Extension<ProxyTrustResult>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let mut configuration = validated_configuration(&method, &body, service.allow_local).await?;
    let mut guard = service.database.lock().await;
    service.load(&mut guard)?;
    let mut db = guard.as_ref().expect("database loaded").clone();
    let jwk = crypto::jwk(&db.signing_key)?;
    if control == "jwks.json" && method == Method::GET {
        return Ok((
            [
                ("content-type", "application/jwk-set+json"),
                ("cache-control", "public, max-age=300"),
            ],
            Json(json!({"keys":[jwk]})),
        )
            .into_response());
    }
    let mut parts = control.splitn(3, '/');
    if parts.next() != Some("subscriptions") {
        return Err(ApiError::missing());
    }
    let id = parts
        .next()
        .filter(|id| {
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
        .ok_or_else(ApiError::missing)?;
    let action = parts.next().unwrap_or("");
    let tails = service.tails()?;
    let now = Utc::now().timestamp_millis();
    refresh(&mut db, &tails, now);
    let response = if method == Method::PUT && action.is_empty() {
        create_subscription(
            &service,
            &mut db,
            id,
            configuration.take().expect("PUT configuration parsed"),
            &origin,
            &tails,
        )?
    } else {
        let sub = db
            .subscriptions
            .get_mut(id)
            .filter(|s| !s.deleted)
            .ok_or_else(ApiError::missing)?;
        match (method, action) {
            (Method::GET, "") => Json(sub.response(
                id,
                jwk["kid"].as_str().expect("JWK kid"),
                &service.base_path,
            ))
            .into_response(),
            (Method::DELETE, "") => {
                sub.deleted = true;
                sub.wake = None;
                StatusCode::NO_CONTENT.into_response()
            }
            (Method::POST, "streams") => add_streams(sub, &body, &tails)?,
            (Method::DELETE, path) if path.starts_with("streams/") => {
                let path = &path[8..];
                model::validate_path(path)?;
                if sub.config.matches(path) {
                    if let Some(link) = sub.links.get_mut(path) {
                        link.explicit = false;
                    }
                } else {
                    sub.links.remove(path);
                }
                StatusCode::NO_CONTENT.into_response()
            }
            (Method::POST, "claim") if sub.config.kind == DeliveryType::PullWake => {
                claim_subscription(sub, id, &db.signing_key, &body, &tails, now)?
            }
            (Method::POST, "callback" | "ack" | "release") => complete(
                sub,
                CompletionContext {
                    id,
                    key: &db.signing_key,
                    headers: &headers,
                    body: &body,
                    action,
                    tails: &tails,
                    now,
                },
            )?,
            _ => return Err(ApiError::missing()),
        }
    };
    service.save(&mut guard, db)?;
    Ok(response)
}

fn claim_subscription(
    sub: &mut Subscription,
    id: &str,
    key: &[u8],
    body: &[u8],
    tails: &BTreeMap<String, String>,
    now: i64,
) -> ApiResult<Response> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Claim {
        worker: String,
    }
    let claim: Claim = serde_json::from_slice(body).map_err(ApiError::json)?;
    if claim.worker.trim().is_empty() || claim.worker.len() > 256 {
        return Err(ApiError::bad(
            "INVALID_WORKER",
            "worker must be a non-empty name",
        ));
    }
    if let Some(wake) = &sub.wake
        && let Some(holder) = &wake.holder
    {
        let mut error =
            ApiError::conflict("ALREADY_CLAIMED", "another worker holds this subscription");
        error.extra = json!({"current_holder":holder, "generation":wake.generation});
        return Err(error);
    }
    if sub.wake.is_none() {
        if !pending(sub, tails) {
            return Err(ApiError::conflict(
                "NO_PENDING_WORK",
                "subscription has no pending work",
            ));
        }
        issue_wake(sub, id, key, tails, now)?;
    }
    sub.failed = false;
    let streams = sub.snapshots(tails);
    let wake = sub.wake.as_mut().expect("wake issued");
    let token = crypto::token(
        key,
        &crypto::Claims {
            subscription: id.into(),
            generation: wake.generation,
            wake_id: wake.id.clone(),
        },
    )?;
    wake.token = Some(token.clone());
    wake.holder = Some(claim.worker);
    wake.lease_until = Some(now + sub.config.lease_ttl_ms);
    wake.delivered = true;
    Ok(Json(json!({"wake_id":wake.id, "generation":wake.generation, "token":token, "streams":streams, "lease_ttl_ms":sub.config.lease_ttl_ms})).into_response())
}

fn add_streams(
    sub: &mut Subscription,
    body: &[u8],
    tails: &BTreeMap<String, String>,
) -> ApiResult<Response> {
    #[derive(Deserialize)]
    struct Membership {
        streams: BTreeSet<String>,
    }
    let membership: Membership = serde_json::from_slice(body).map_err(ApiError::json)?;
    for path in membership.streams {
        model::validate_path(&path)?;
        if sub.config.wake_stream.as_ref() == Some(&path) {
            return Err(ApiError::bad("INVALID_PATH", "cannot link the wake stream"));
        }
        sub.links
            .entry(path.clone())
            .and_modify(|l| l.explicit = true)
            .or_insert_with(|| Link {
                explicit: true,
                acked_offset: tails
                    .get(&path)
                    .cloned()
                    .unwrap_or_else(|| Offset::new(0, 0).to_string()),
            });
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

fn create_subscription<S: Storage>(
    service: &Service<S>,
    db: &mut Database,
    id: &str,
    config: Configuration,
    origin: &ProxyTrustResult,
    tails: &BTreeMap<String, String>,
) -> ApiResult<Response> {
    let now = Utc::now().timestamp_millis();
    let jwk = crypto::jwk(&db.signing_key)?;
    let hash = crypto::hash(&serde_json::to_vec(&config).map_err(ApiError::json)?);
    let response = if let Some(existing) = db.subscriptions.get(id) {
        if existing.deleted || existing.config_hash != hash {
            return Err(ApiError::conflict(
                "CONFIG_MISMATCH",
                "subscription ID already has a different configuration",
            ));
        }
        (
            StatusCode::OK,
            Json(existing.response(
                id,
                jwk["kid"].as_str().expect("JWK kid"),
                &service.base_path,
            )),
        )
            .into_response()
    } else {
        if let Some(wake_stream) = &config.wake_stream {
            let meta = service.storage.head(wake_stream).map_err(|_| {
                ApiError::bad(
                    "INVALID_WAKE_STREAM",
                    "wake stream must be created explicitly",
                )
            })?;
            if meta.config.content_type != "application/json" || meta.closed {
                return Err(ApiError::bad(
                    "INVALID_WAKE_STREAM",
                    "wake stream must be open application/json",
                ));
            }
        }
        let mut links = BTreeMap::new();
        for (path, tail) in tails {
            if config.matches(path) {
                links.insert(
                    path.clone(),
                    Link {
                        explicit: false,
                        acked_offset: tail.clone(),
                    },
                );
            }
        }
        for path in &config.streams {
            links.insert(
                path.clone(),
                Link {
                    explicit: true,
                    acked_offset: tails
                        .get(path)
                        .cloned()
                        .unwrap_or_else(|| Offset::new(0, 0).to_string()),
                },
            );
        }
        let authority = origin
            .authority
            .as_deref()
            .ok_or_else(|| ApiError::bad("INVALID_ORIGIN", "request host required"))?;
        let sub = Subscription {
            config,
            config_hash: hash,
            links,
            created_at: Utc::now().to_rfc3339(),
            origin: format!("{}://{authority}", origin.scheme),
            generation: 0,
            wake: None,
            deleted: false,
            next_attempt_at: now,
            attempts: 0,
            failed: false,
        };
        let response = (
            StatusCode::CREATED,
            Json(sub.response(
                id,
                jwk["kid"].as_str().expect("JWK kid"),
                &service.base_path,
            )),
        )
            .into_response();
        db.subscriptions.insert(id.into(), sub);
        response
    };
    Ok(response)
}

async fn validated_configuration(
    method: &Method,
    body: &[u8],
    allow_local: bool,
) -> ApiResult<Option<Configuration>> {
    let mut configuration = if *method == Method::PUT {
        Some(serde_json::from_slice::<Configuration>(body).map_err(ApiError::json)?)
    } else {
        None
    };
    if let Some(config) = &mut configuration {
        config.validate()?;
        if let Some(webhook) = &mut config.webhook {
            let (_, url) = delivery::webhook_client(&webhook.url, allow_local).await?;
            webhook.url = url.to_string();
        }
    }
    Ok(configuration)
}
