//! Runnable design sketch, deliberately separate from the production server.
//! See docs/design/blocking-execution-boundary.md at the workspace root.

use bytes::Bytes;
use durable_streams_server::storage::StreamOptions;
use durable_streams_server::{FileStorage, StreamService};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

#[derive(Debug, thiserror::Error)]
enum AdmissionError {
    #[error("all storage slots are occupied")]
    Busy,
    #[error("storage execution is shutting down")]
    Closed,
}

struct Boundary {
    accepting: Mutex<bool>,
    slots: Arc<Semaphore>,
    tasks: TaskTracker,
}

impl Boundary {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            accepting: Mutex::new(true),
            slots: Arc::new(Semaphore::new(capacity.get())),
            tasks: TaskTracker::new(),
        }
    }

    // Call within Tokio. Registration and close share a short lock, never an I/O lock.
    fn submit<T: Send + 'static>(
        &self,
        operation: impl FnOnce() -> T + Send + 'static,
    ) -> Result<JoinHandle<T>, AdmissionError> {
        let accepting = self.accepting.lock().expect("admission lock poisoned");
        if !*accepting {
            return Err(AdmissionError::Closed);
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| AdmissionError::Busy)?;
        let task = self.tasks.spawn_blocking(move || {
            // The job owns capacity, including time queued in Tokio's blocking pool.
            let _permit = permit;
            operation()
        });
        drop(accepting);
        Ok(task)
    }

    fn close(&self) {
        let mut accepting = self.accepting.lock().expect("admission lock poisoned");
        *accepting = false;
        // TaskTracker::close alone does not prevent new task registration.
        self.tasks.close();
    }

    async fn shutdown(&self) {
        self.close();
        self.tasks.wait().await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let storage = Arc::new(FileStorage::new(root.path().to_owned(), 1024, 1024)?);
    let streams = StreamService::new(storage);
    streams.create("demo", StreamOptions::new("text/plain"), vec![])?;
    let boundary = Boundary::new(NonZeroUsize::new(2).expect("two is nonzero"));
    let mut releases = Vec::new();
    let mut replies = Vec::new();
    for body in [b"a", b"b"] {
        let (release, wait) = mpsc::channel();
        let (started, observed) = oneshot::channel();
        let streams = streams.clone();
        replies.push(boundary.submit(move || {
            let _ = started.send(());
            wait.recv().expect("demo must release its paused job");
            streams.append("demo", Bytes::from_static(body), "text/plain")
        })?);
        releases.push(release);
        observed.await?;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    println!("The single async worker ran a timer while two blocking jobs were paused.");
    assert!(matches!(boundary.submit(|| ()), Err(AdmissionError::Busy)));
    println!("A third job was rejected: both storage slots are occupied.");
    drop(replies.pop()); // Simulate a disconnected HTTP caller.
    assert!(matches!(boundary.submit(|| ()), Err(AdmissionError::Busy)));
    println!("Dropping a reply did not free a slot or cancel its accepted write.");
    boundary.close();
    let mut drain = Box::pin(boundary.shutdown());
    assert!(futures_util::poll!(&mut drain).is_pending());
    assert!(matches!(
        boundary.submit(|| ()),
        Err(AdmissionError::Closed)
    ));
    println!("Shutdown closed admission and remains pending until both jobs finish.");
    for release in releases {
        release.send(())?;
    }
    drain.await;
    for reply in replies {
        reply.await??;
    }
    let meta = streams.head("demo")?;
    assert_eq!(meta.message_count, 2);
    println!(
        "Shutdown completed: both file-backed appends are visible, including the disconnected one."
    );
    Ok(())
}
