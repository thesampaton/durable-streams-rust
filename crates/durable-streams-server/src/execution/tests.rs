use super::*;
use std::io::Write;
use std::sync::{Barrier, mpsc};
use std::time::Duration;

fn execution(capacity: usize) -> Arc<Execution> {
    let storage: Arc<dyn crate::Storage> = Arc::new(crate::InMemoryStorage::new(1024, 1024));
    let lease = Arc::new(StorageLease::acquire(&storage).unwrap());
    let execution = Execution::new(capacity, CancellationToken::new(), lease);
    execution.start(Handle::current());
    execution
}

#[test]
fn queued_and_disconnected_jobs_keep_capacity_and_drain_ownership() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let execution = execution(2);
        let completed = Arc::new(AtomicUsize::new(0));
        let (release, wait) = mpsc::channel();
        let (started, observed) = oneshot::channel();
        let count = completed.clone();
        let first = execution
            .submit("first", move || {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            })
            .unwrap();
        observed.await.unwrap();
        let count = completed.clone();
        let queued = execution
            .submit("queued", move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            })
            .unwrap();
        drop(queued);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(execution.admitted.load(Ordering::Relaxed), 2);
        assert_eq!(execution.running.load(Ordering::Relaxed), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        assert!(matches!(
            execution.submit("busy", || Ok::<_, Error>(())),
            Err(ExecutionError::Busy)
        ));
        execution.close();
        let mut drain = Box::pin(execution.drain());
        assert!(futures_util::poll!(&mut drain).is_pending());
        assert!(matches!(
            execution.submit("closed", || Ok::<_, Error>(())),
            Err(ExecutionError::Closed)
        ));
        release.send(()).unwrap();
        drain.await;
        first.await.unwrap().unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(execution.admitted.load(Ordering::Relaxed), 0);
        assert_eq!(execution.running.load(Ordering::Relaxed), 0);
        assert_eq!(execution.slots.available_permits(), 2);
    });
}

#[tokio::test]
async fn racing_admission_is_either_rejected_or_included_in_drain() {
    for _ in 0..32 {
        let execution = execution(16);
        let barrier = Arc::new(Barrier::new(2));
        let completed = Arc::new(AtomicUsize::new(0));
        let submitter = execution.clone();
        let rendezvous = barrier.clone();
        let count = completed.clone();
        // Submission uses the captured serving runtime even on a different thread.
        let submissions = std::thread::spawn(move || {
            rendezvous.wait();
            (0..16)
                .filter_map(|_| {
                    let count = count.clone();
                    submitter
                        .submit("race", move || {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok::<_, Error>(())
                        })
                        .ok()
                })
                .collect::<Vec<_>>()
        });
        barrier.wait();
        execution.close();
        let replies = submissions.join().unwrap();
        execution.drain().await;
        assert_eq!(completed.load(Ordering::SeqCst), replies.len());
        for reply in replies {
            reply.await.unwrap().unwrap().unwrap();
        }
    }
}

#[derive(Clone)]
struct Log(Arc<Mutex<Vec<u8>>>);

impl Write for Log {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn failures_remain_observable_after_the_reply_is_dropped() {
    for panic in [false, true] {
        let execution = execution(1);
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let log = Log(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || log.clone())
            .finish();
        let (release, wait) = mpsc::channel();
        let (started, observed) = oneshot::channel();
        let reply = tracing::subscriber::with_default(subscriber, || {
            execution
                .submit("detached", move || {
                    started.send(()).unwrap();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                    assert!(!panic, "injected storage panic");
                    Err::<(), _>(Error::Storage("injected storage error".into()))
                })
                .unwrap()
        });
        observed.await.unwrap();
        drop(reply);
        if !panic {
            execution.close();
        }
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), execution.drain())
            .await
            .unwrap();
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(
            output.contains(if panic {
                "storage job panicked"
            } else {
                "storage job failed"
            }),
            "{output}"
        );
        assert!(output.contains("caller_disconnected=true"), "{output}");
        assert_eq!(execution.slots.available_permits(), 1);
        assert!(matches!(
            execution.submit("after failure", || Ok::<_, Error>(())),
            Err(ExecutionError::Closed)
        ));
        assert_eq!(execution.shutdown.is_cancelled(), panic);
    }
}
