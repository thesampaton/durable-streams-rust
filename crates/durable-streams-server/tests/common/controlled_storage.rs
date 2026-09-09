//! Deterministic operation pauses around real file persistence.
use bytes::Bytes;
use durable_streams_server::protocol::offset::Offset as StorageOffset;
use durable_streams_server::protocol::{error::Result, offset::Offset, producer::ProducerHeaders};
use durable_streams_server::storage::{
    CreateStreamResult, CreateWithDataResult, ProducerAppendResult, ReadResult, StreamOptions,
};
use durable_streams_server::streams::StreamMetadata;
use durable_streams_server::{FileStorage, Storage};
use std::{
    collections::BTreeMap,
    sync::{Mutex, mpsc},
    time::Duration,
};
use tokio::sync::{broadcast, oneshot};

struct Pause {
    started: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

pub struct ControlledStorage {
    pub inner: FileStorage,
    pub mute_notifications: std::sync::atomic::AtomicBool,
    notifications: broadcast::Sender<()>,
    pauses: Mutex<BTreeMap<&'static str, Pause>>,
}

impl ControlledStorage {
    pub fn new(path: &std::path::Path) -> Self {
        Self {
            mute_notifications: std::sync::atomic::AtomicBool::new(false),
            notifications: broadcast::channel(16).0,
            inner: FileStorage::new(path.to_owned(), 1024 * 1024, 1024 * 1024).unwrap(),
            pauses: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn arm(&self, operation: &'static str) -> (oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (started, observed) = oneshot::channel();
        let (release, wait) = mpsc::channel();
        assert!(
            self.pauses
                .lock()
                .unwrap()
                .insert(
                    operation,
                    Pause {
                        started,
                        release: wait
                    }
                )
                .is_none()
        );
        (observed, release)
    }
    fn pause(&self, operation: &'static str) {
        let pause = self.pauses.lock().unwrap().remove(operation);
        if let Some(pause) = pause {
            let _ = pause.started.send(());
            // A failing assertion drops the release sender and unblocks the test backend.
            let _ = pause.release.recv_timeout(Duration::from_secs(10));
        }
    }
}

impl Storage for ControlledStorage {
    fn create_stream(&self, name: &str, config: StreamOptions) -> Result<CreateStreamResult> {
        self.inner.create_stream(name, config)
    }

    fn append(
        &self,
        name: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<durable_streams_server::storage::AppendResult> {
        self.pause("append");
        self.inner.append(name, data, content_type)
    }

    fn append_batch(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        seq: Option<&str>,
        close: bool,
    ) -> Result<durable_streams_server::storage::AppendResult> {
        self.pause("append");
        self.inner
            .append_batch(name, messages, content_type, seq, close)
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        let result = self.inner.read(name, from_offset);
        self.pause("read");
        result
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.inner.delete(name)
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        self.inner.head(name)
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        self.inner.close_stream(name)
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        self.pause("append");
        self.inner
            .append_with_producer(name, messages, content_type, producer, should_close, seq)
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamOptions,
        messages: Vec<Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        self.inner
            .create_stream_with_data(name, config, messages, should_close)
    }

    fn load_subscription_state(&self) -> Result<Option<Vec<u8>>> {
        self.inner.load_subscription_state()
    }
    fn save_subscription_state(&self, state: &[u8]) -> Result<()> {
        let result = self.inner.save_subscription_state(state);
        self.pause("subscription save");
        result
    }
    fn create_fork_with_options(
        &self,
        name: &str,
        source: &str,
        offset: Option<&Offset>,
        config: StreamOptions,
        options: durable_streams_server::storage::ForkOptions,
    ) -> Result<CreateStreamResult> {
        self.inner
            .create_fork_with_options(name, source, offset, config, options)
    }

    fn exists(&self, name: &str) -> Result<bool> {
        self.inner.exists(name)
    }

    fn replace_stream(
        &self,
        name: &str,
        config: StreamOptions,
        messages: Vec<Bytes>,
        closed: bool,
    ) -> Result<durable_streams_server::storage::AppendResult> {
        self.inner.replace_stream(name, config, messages, closed)
    }

    fn subscribe(&self, name: &str) -> Result<Option<broadcast::Receiver<()>>> {
        if self
            .mute_notifications
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(Some(self.notifications.subscribe()));
        }
        self.inner.subscribe(name)
    }

    fn cleanup_expired_streams(&self) -> usize {
        self.inner.cleanup_expired_streams()
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        self.pause("list");
        self.inner.list_streams()
    }

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&StorageOffset>,
        config: StreamOptions,
    ) -> Result<CreateStreamResult> {
        self.inner
            .create_fork(name, source_name, fork_offset, config)
    }
}
