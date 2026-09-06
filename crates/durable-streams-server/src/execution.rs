//! Admission and completion ownership for synchronous server operations.

#[cfg(test)]
mod tests;

use crate::protocol::{error::Error, offset::Offset};
use crate::router::StorageLease;
use crate::{storage::ReadResult, streams::StreamService};
use std::fmt::Debug;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tokio::{
    runtime::Handle,
    sync::{Semaphore, oneshot},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExecutionError {
    #[error("all storage execution slots are occupied")]
    Busy,
    #[error("storage execution is shutting down")]
    Closed,
    #[error("storage job did not complete normally; outcome may be uncertain")]
    Failed,
}

impl ExecutionError {
    pub(crate) fn into_domain(self) -> Error {
        match self {
            Self::Busy | Self::Closed => {
                Error::storage_unavailable("execution", "admission", self.to_string())
            }
            Self::Failed => Error::Storage(self.to_string()),
        }
    }
}

/// Classify outcomes without merging the stream and subscription error contracts.
pub(crate) trait JobError: Debug + Send + 'static {
    fn is_server_error(&self) -> bool;
    fn from_execution(error: ExecutionError) -> Self;
}

impl JobError for Error {
    fn from_execution(error: ExecutionError) -> Self {
        error.into_domain()
    }
    fn is_server_error(&self) -> bool {
        self.status_code().is_server_error()
    }
}

type Completion<T, E> = Result<Result<T, E>, ExecutionError>;

pub(crate) struct Execution {
    accepting: Mutex<bool>,
    slots: Arc<Semaphore>,
    tasks: TaskTracker,
    runtime: OnceLock<Handle>,
    shutdown: CancellationToken,
    admitted: AtomicUsize,
    running: AtomicUsize,
    // Admitted closures retain this lease even if every router has been dropped.
    _lease: Arc<StorageLease>,
}

impl Execution {
    pub(crate) fn new(
        capacity: usize,
        shutdown: CancellationToken,
        lease: Arc<StorageLease>,
    ) -> Arc<Self> {
        Arc::new(Self {
            accepting: Mutex::new(true),
            slots: Arc::new(Semaphore::new(capacity)),
            tasks: TaskTracker::new(),
            runtime: OnceLock::new(),
            shutdown,
            admitted: AtomicUsize::new(0),
            running: AtomicUsize::new(0),
            _lease: lease,
        })
    }

    pub(crate) fn start(&self, runtime: Handle) {
        self.runtime
            .set(runtime)
            .expect("server execution starts once");
    }

    pub(crate) async fn run<T, E>(
        self: &Arc<Self>,
        operation: &'static str,
        work: impl FnOnce() -> Result<T, E> + Send + 'static,
    ) -> Completion<T, E>
    where
        T: Send + 'static,
        E: JobError,
    {
        self.submit(operation, work)?
            .await
            .map_err(|_| ExecutionError::Failed)?
    }

    fn submit<T, E>(
        self: &Arc<Self>,
        operation: &'static str,
        work: impl FnOnce() -> Result<T, E> + Send + 'static,
    ) -> Result<oneshot::Receiver<Completion<T, E>>, ExecutionError>
    where
        T: Send + 'static,
        E: JobError,
    {
        // Registration and close share this short lock; neither waits for storage.
        let accepting = self.accepting.lock().expect("admission lock poisoned");
        if !*accepting || self.shutdown.is_cancelled() {
            tracing::debug!(operation, reason = "closed", "storage admission rejected");
            return Err(ExecutionError::Closed);
        }
        let runtime = self.runtime.get().ok_or(ExecutionError::Closed)?;
        let permit = self.slots.clone().try_acquire_owned().map_err(|_| {
            tracing::debug!(operation, reason = "busy", "storage admission rejected");
            ExecutionError::Busy
        })?;
        let queued = Instant::now();
        let admitted = self.admitted.fetch_add(1, Ordering::Relaxed) + 1;
        let span = tracing::Span::current();
        let job = Job {
            execution: self.clone(),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            operation,
            started: false,
            completed: false,
        };
        let (reply, receive) = oneshot::channel();
        tracing::debug!(operation, admitted, "storage job admitted");
        self.tasks.spawn_blocking_on(
            move || {
                let _permit = permit;
                let mut job = job;
                let dispatch = job.dispatch.clone();
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let _span = span.enter();
                job.started = true;
                let running = job.execution.running.fetch_add(1, Ordering::Relaxed) + 1;
                let started = Instant::now();
                tracing::debug!(
                    operation,
                    running,
                    queue_wait_us = queued.elapsed().as_micros(),
                    "storage job started"
                );
                let result = if let Ok(result) = catch_unwind(AssertUnwindSafe(work)) {
                    if let Err(error) = &result {
                        if error.is_server_error() {
                            tracing::warn!(
                                operation,
                                ?error,
                                caller_disconnected = reply.is_closed(),
                                "storage job failed"
                            );
                        } else {
                            tracing::debug!(operation, ?error, "storage operation rejected");
                        }
                    }
                    Ok(result)
                } else {
                    tracing::error!(
                        operation,
                        caller_disconnected = reply.is_closed(),
                        "storage job panicked; outcome may be uncertain"
                    );
                    job.execution.close();
                    job.execution.shutdown.cancel();
                    Err(ExecutionError::Failed)
                };
                let caller_disconnected = reply.send(result).is_err();
                job.completed = true;
                tracing::debug!(
                    operation,
                    caller_disconnected,
                    duration_us = started.elapsed().as_micros(),
                    "storage job completed"
                );
            },
            runtime,
        );
        drop(accepting);
        Ok(receive)
    }

    pub(crate) fn close(&self) {
        let mut accepting = self.accepting.lock().expect("admission lock poisoned");
        *accepting = false;
        self.tasks.close();
    }

    pub(crate) async fn drain(&self) {
        self.tasks.wait().await;
    }
}

struct Job {
    execution: Arc<Execution>,
    dispatch: tracing::Dispatch,
    operation: &'static str,
    started: bool,
    completed: bool,
}

impl Drop for Job {
    fn drop(&mut self) {
        let _dispatch = tracing::dispatcher::set_default(&self.dispatch);
        if self.started {
            self.execution.running.fetch_sub(1, Ordering::Relaxed);
        }
        let admitted = self.execution.admitted.fetch_sub(1, Ordering::Relaxed) - 1;
        let running = self.execution.running.load(Ordering::Relaxed);
        tracing::debug!(
            operation = self.operation,
            admitted,
            running,
            "storage job released"
        );
        if !self.completed {
            tracing::error!(
                operation = self.operation,
                "storage job abandoned before completion"
            );
        }
    }
}

/// Async execution for server-owned callers of the synchronous stream service.
pub(crate) struct AsyncStreams {
    pub(crate) service: Arc<StreamService>,
    pub(crate) execution: Arc<Execution>,
}

impl AsyncStreams {
    pub(crate) async fn run<T, E>(
        &self,
        operation: &'static str,
        work: impl FnOnce(&StreamService) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: JobError,
    {
        let service = self.service.clone();
        self.execution
            .run(operation, move || work(&service))
            .await
            .map_err(E::from_execution)?
    }

    pub(crate) async fn head(&self, name: &str) -> Result<crate::streams::StreamMetadata, Error> {
        let name = name.to_owned();
        self.run("head", move |service| service.head(&name)).await
    }

    pub(crate) async fn read(&self, name: &str, offset: &Offset) -> Result<ReadResult, Error> {
        let name = name.to_owned();
        let offset = offset.clone();
        self.run("read", move |service| service.read(&name, &offset))
            .await
    }

    pub(crate) async fn subscribe_and_read(
        &self,
        name: &str,
        offset: &Offset,
    ) -> Result<(tokio::sync::broadcast::Receiver<()>, ReadResult), Error> {
        let name = name.to_owned();
        let offset = offset.clone();
        self.run("subscribe and read", move |service| {
            let receiver = service
                .subscribe(&name)?
                .ok_or_else(|| Error::NotFound(name.clone()))?;
            Ok((receiver, service.read(&name, &offset)?))
        })
        .await
    }
}
