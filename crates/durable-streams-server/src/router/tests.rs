use super::*;
use crate::protocol::error::Error;
use std::sync::mpsc;
use std::time::Duration;

fn owner(storage: Arc<dyn Storage>) -> RunningServer {
    Server::new(
        crate::StreamService::new(storage),
        &Config::default(),
        RouterOptions::default(),
    )
    .unwrap()
    .start()
    .unwrap()
}

async fn pause_job(server: &RunningServer) -> mpsc::Sender<()> {
    let execution = server.state.service.execution.clone();
    let (release, wait) = mpsc::channel();
    let (started, observed) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(async move {
        execution
            .run("lifecycle test", move || {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok::<_, Error>(())
            })
            .await
    });
    observed.await.unwrap();
    caller.abort();
    let _ = caller.await;
    release
}

#[tokio::test]
async fn worker_join_error_does_not_skip_storage_drain() {
    let running = owner(Arc::new(crate::InMemoryStorage::new(1024, 1024)));
    let release = pause_job(&running).await;
    running.state.worker.lock().await.as_ref().unwrap().abort();
    tokio::task::yield_now().await;
    let mut drain = Box::pin(running.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut drain)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert!(drain.await.is_err());
}

#[tokio::test]
async fn cancelled_shutdown_can_be_retried_while_accepted_job_finishes() {
    let running = owner(Arc::new(crate::InMemoryStorage::new(1024, 1024)));
    let release = pause_job(&running).await;
    let mut abandoned = Box::pin(running.shutdown());
    assert!(futures_util::poll!(&mut abandoned).is_pending());
    drop(abandoned);
    let mut replacement = Box::pin(running.shutdown());
    assert!(futures_util::poll!(&mut replacement).is_pending());
    release.send(()).unwrap();
    replacement.await.unwrap();
}

#[tokio::test]
async fn detached_job_retains_storage_lease_after_final_owner_is_dropped() {
    let storage: Arc<dyn Storage> = Arc::new(crate::InMemoryStorage::new(1024, 1024));
    let running = owner(storage.clone());
    let release = pause_job(&running).await;
    drop(running);
    assert!(matches!(
        Server::new(
            crate::StreamService::new(storage.clone()),
            &Config::default(),
            RouterOptions::default()
        ),
        Err(ServerError::StorageAlreadyOwned)
    ));
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Server::new(
                crate::StreamService::new(storage.clone()),
                &Config::default(),
                RouterOptions::default(),
            ) {
                Ok(replacement) => {
                    drop(replacement);
                    break;
                }
                Err(ServerError::StorageAlreadyOwned) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected restart failure: {error}"),
            }
        }
    })
    .await
    .unwrap();
}
