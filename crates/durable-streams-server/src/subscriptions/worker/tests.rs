use super::*;
use crate::middleware::proxy_trust::ProxyTrustResult;
use crate::protocol::error::Error;
use crate::storage::Storage;
use crate::subscriptions::{api, initialize};
use crate::{
    Config, InMemoryStorage, execution::Execution, router::StorageLease, storage::StreamOptions,
};
use axum::{
    Extension,
    extract::{Path, State},
    http::{HeaderMap, Method},
};
use std::sync::mpsc;

async fn subscriptions() -> Arc<Service> {
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new(4096, 4096));
    storage
        .create_stream_with_data(
            "events/one",
            StreamOptions::new("text/plain"),
            vec![Bytes::from_static(b"event")],
            false,
        )
        .unwrap();
    let execution = Execution::new(
        1,
        CancellationToken::new(),
        Arc::new(StorageLease::acquire(&storage).unwrap()),
    );
    execution.start(tokio::runtime::Handle::current());
    let mut config = Config::default();
    config.http.allow_insecure_webhooks = true;
    let service = initialize(storage, &config, execution).unwrap();
    for n in 0..=MAX_DELIVERIES {
        let origin = ProxyTrustResult {
            peer_ip: None,
            trusted: false,
            scheme: "http".into(),
            authority: Some("example.test".into()),
            client_address: None,
        };
        let response = api::control(State(service.clone()), Path(format!("subscriptions/sub-{n:02}")),
            Extension(origin), Method::PUT, HeaderMap::new(), Bytes::from(serde_json::to_vec(&json!({
                "type":"webhook", "pattern":"events/**", "webhook":{"url":"http://127.0.0.1:9/hook"}, "lease_ttl_ms":30000
            })).unwrap())).await.unwrap();
        assert_eq!(response.status(), 201);
    }
    service
        .storage
        .append("events/one", Bytes::from_static(b"new"), "text/plain")
        .unwrap();
    service
}

#[tokio::test]
async fn pending_completions_retain_delivery_capacity_through_storage_saturation() {
    let service = subscriptions().await;
    let transaction = service.clone();
    let jobs = service
        .execution
        .run("test reconcile", move || {
            tick(&transaction, &BTreeSet::new())
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(jobs.len(), MAX_DELIVERIES);
    let mut completions: BTreeMap<_, _> = jobs
        .into_iter()
        .map(|job| ((job.id, job.generation), Arc::new(Ok(true))))
        .collect();
    let mut active = completions.keys().cloned().collect::<BTreeSet<_>>();
    let execution = service.execution.clone();
    let (release, wait) = mpsc::channel();
    let (started, observed) = tokio::sync::oneshot::channel();
    let holder = tokio::spawn(async move {
        execution
            .run("pause", move || {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok::<_, Error>(())
            })
            .await
            .unwrap()
            .unwrap();
    });
    observed.await.unwrap();
    assert!(matches!(
        persist_completions(&service, &mut completions, &mut active).await,
        Err(ExecutionError::Busy)
    ));
    assert_eq!(completions.len(), MAX_DELIVERIES);
    assert_eq!(active.len(), MAX_DELIVERIES);
    release.send(()).unwrap();
    holder.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match persist_completions(&service, &mut completions, &mut active).await {
                Ok(()) => break,
                Err(ExecutionError::Busy) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected execution failure: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(completions.is_empty() && active.is_empty());
    let saved: super::super::model::Database =
        serde_json::from_slice(&service.storage.load_subscription_state().unwrap().unwrap())
            .unwrap();
    assert_eq!(
        saved
            .subscriptions
            .values()
            .filter(|sub| sub.wake.is_none())
            .count(),
        MAX_DELIVERIES
    );
    // The seventeenth subscription has not consumed a delivery slot yet.
    assert!(saved.subscriptions["sub-16"].wake.is_some());
    service.execution.close();
    service.execution.drain().await;
}
