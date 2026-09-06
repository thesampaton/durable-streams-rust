use durable_streams_server::Storage;
use durable_streams_server::config::AcidBackend;
use durable_streams_server::protocol::error::Result;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::offset::Offset as StorageOffset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{
    CreateStreamResult, CreateWithDataResult, ProducerAppendResult, ReadResult, StreamOptions,
    acid::AcidStorage, file::FileStorage, memory::InMemoryStorage,
};
use durable_streams_server::streams::StreamMetadata;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

static STORAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
pub enum StorageTestBackend {
    Memory,
    File,
    Acid,
    AcidInMemory,
}

impl StorageTestBackend {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
            Self::Acid => "acid",
            Self::AcidInMemory => "acid-in-memory",
        }
    }
}

pub enum TestStorage {
    Memory(InMemoryStorage),
    File(FileStorage),
    Acid(AcidStorage),
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TestStorage>();
};

impl Storage for TestStorage {
    fn create_stream(&self, name: &str, config: StreamOptions) -> Result<CreateStreamResult> {
        self.as_storage().create_stream(name, config)
    }

    fn append(
        &self,
        name: &str,
        data: bytes::Bytes,
        content_type: &str,
    ) -> Result<durable_streams_server::storage::AppendResult> {
        self.as_storage().append(name, data, content_type)
    }

    fn append_batch(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        seq: Option<&str>,
        close: bool,
    ) -> Result<durable_streams_server::storage::AppendResult> {
        self.as_storage()
            .append_batch(name, messages, content_type, seq, close)
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        self.as_storage().read(name, from_offset)
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.as_storage().delete(name)
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        self.as_storage().head(name)
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        self.as_storage().close_stream(name)
    }

    fn append_with_producer(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        producer: &ProducerHeaders,
        should_close: bool,
        seq: Option<&str>,
    ) -> Result<ProducerAppendResult> {
        self.as_storage().append_with_producer(
            name,
            messages,
            content_type,
            producer,
            should_close,
            seq,
        )
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamOptions,
        messages: Vec<bytes::Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        self.as_storage()
            .create_stream_with_data(name, config, messages, should_close)
    }

    fn replace_stream(
        &self,
        name: &str,
        config: StreamOptions,
        messages: Vec<bytes::Bytes>,
        closed: bool,
    ) -> Result<durable_streams_server::storage::AppendResult> {
        self.as_storage()
            .replace_stream(name, config, messages, closed)
    }

    fn subscribe(&self, name: &str) -> Result<Option<broadcast::Receiver<()>>> {
        self.as_storage().subscribe(name)
    }

    fn cleanup_expired_streams(&self) -> usize {
        self.as_storage().cleanup_expired_streams()
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        self.as_storage().list_streams()
    }

    fn create_fork_with_options(
        &self,
        name: &str,
        source_name: &str,
        offset: Option<&Offset>,
        config: StreamOptions,
        options: durable_streams_server::storage::ForkOptions,
    ) -> Result<CreateStreamResult> {
        self.as_storage()
            .create_fork_with_options(name, source_name, offset, config, options)
    }
    fn load_subscription_state(&self) -> Result<Option<Vec<u8>>> {
        self.as_storage().load_subscription_state()
    }
    fn save_subscription_state(&self, state: &[u8]) -> Result<()> {
        self.as_storage().save_subscription_state(state)
    }
    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&StorageOffset>,
        config: StreamOptions,
    ) -> Result<CreateStreamResult> {
        self.as_storage()
            .create_fork(name, source_name, fork_offset, config)
    }
}

impl TestStorage {
    fn as_storage(&self) -> &dyn Storage {
        match self {
            Self::Memory(inner) => inner,
            Self::File(inner) => inner,
            Self::Acid(inner) => inner,
        }
    }

    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        match self {
            Self::Memory(inner) => inner.total_bytes(),
            Self::File(inner) => inner.total_bytes(),
            Self::Acid(inner) => inner.total_bytes(),
        }
    }
}

pub struct TestStorageHandle {
    pub storage: TestStorage,
    _storage_dir: Option<PathBuf>,
}

#[must_use]
pub fn create_test_storage(backend: StorageTestBackend) -> TestStorageHandle {
    create_test_storage_with_limits(backend, 1024 * 1024, 100 * 1024)
}

#[must_use]
pub fn create_test_storage_with_limits(
    backend: StorageTestBackend,
    max_total_bytes: u64,
    max_stream_bytes: u64,
) -> TestStorageHandle {
    match backend {
        StorageTestBackend::Memory => TestStorageHandle {
            storage: TestStorage::Memory(InMemoryStorage::new(max_total_bytes, max_stream_bytes)),
            _storage_dir: None,
        },
        StorageTestBackend::File => {
            let storage_dir = unique_storage_dir("file");
            let storage = FileStorage::new(&storage_dir, max_total_bytes, max_stream_bytes)
                .expect("failed to initialize test file storage");
            TestStorageHandle {
                storage: TestStorage::File(storage),
                _storage_dir: Some(storage_dir),
            }
        }
        StorageTestBackend::Acid => {
            let storage_dir = unique_storage_dir("acid");
            let storage = AcidStorage::new(
                &storage_dir,
                16,
                max_total_bytes,
                max_stream_bytes,
                AcidBackend::File,
            )
            .expect("failed to initialize test acid storage");
            TestStorageHandle {
                storage: TestStorage::Acid(storage),
                _storage_dir: Some(storage_dir),
            }
        }
        StorageTestBackend::AcidInMemory => {
            let storage_dir = unique_storage_dir("acid-mem");
            let storage = AcidStorage::new(
                &storage_dir,
                16,
                max_total_bytes,
                max_stream_bytes,
                AcidBackend::InMemory,
            )
            .expect("failed to initialize test acid in-memory storage");
            TestStorageHandle {
                storage: TestStorage::Acid(storage),
                _storage_dir: Some(storage_dir),
            }
        }
    }
}

fn unique_storage_dir(prefix: &str) -> PathBuf {
    let seq = STORAGE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    std::env::temp_dir().join(format!("ds-{prefix}-storage-test-{pid}-{ts}-{seq}"))
}
