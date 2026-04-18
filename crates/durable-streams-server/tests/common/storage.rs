use durable_streams_server::config::AcidBackend;
use durable_streams_server::protocol::error::Result;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::offset::Offset as StorageOffset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{
    CreateStreamResult, CreateWithDataResult, ProducerAppendResult, ReadResult, Storage,
    StreamConfig, StreamMetadata, acid::AcidStorage, file::FileStorage, memory::InMemoryStorage,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

static STORAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
pub enum StorageTestBackend {
    Memory,
    FileDurable,
    Acid,
    AcidInMemory,
}

impl StorageTestBackend {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::FileDurable => "file-durable",
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
    fn create_stream(&self, name: &str, config: StreamConfig) -> Result<CreateStreamResult> {
        match self {
            Self::Memory(inner) => inner.create_stream(name, config),
            Self::File(inner) => inner.create_stream(name, config),
            Self::Acid(inner) => inner.create_stream(name, config),
        }
    }

    fn append(&self, name: &str, data: bytes::Bytes, content_type: &str) -> Result<Offset> {
        match self {
            Self::Memory(inner) => inner.append(name, data, content_type),
            Self::File(inner) => inner.append(name, data, content_type),
            Self::Acid(inner) => inner.append(name, data, content_type),
        }
    }

    fn batch_append(
        &self,
        name: &str,
        messages: Vec<bytes::Bytes>,
        content_type: &str,
        seq: Option<&str>,
    ) -> Result<Offset> {
        match self {
            Self::Memory(inner) => inner.batch_append(name, messages, content_type, seq),
            Self::File(inner) => inner.batch_append(name, messages, content_type, seq),
            Self::Acid(inner) => inner.batch_append(name, messages, content_type, seq),
        }
    }

    fn read(&self, name: &str, from_offset: &Offset) -> Result<ReadResult> {
        match self {
            Self::Memory(inner) => inner.read(name, from_offset),
            Self::File(inner) => inner.read(name, from_offset),
            Self::Acid(inner) => inner.read(name, from_offset),
        }
    }

    fn delete(&self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.delete(name),
            Self::File(inner) => inner.delete(name),
            Self::Acid(inner) => inner.delete(name),
        }
    }

    fn head(&self, name: &str) -> Result<StreamMetadata> {
        match self {
            Self::Memory(inner) => inner.head(name),
            Self::File(inner) => inner.head(name),
            Self::Acid(inner) => inner.head(name),
        }
    }

    fn close_stream(&self, name: &str) -> Result<()> {
        match self {
            Self::Memory(inner) => inner.close_stream(name),
            Self::File(inner) => inner.close_stream(name),
            Self::Acid(inner) => inner.close_stream(name),
        }
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
        match self {
            Self::Memory(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
            Self::File(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
            Self::Acid(inner) => inner.append_with_producer(
                name,
                messages,
                content_type,
                producer,
                should_close,
                seq,
            ),
        }
    }

    fn create_stream_with_data(
        &self,
        name: &str,
        config: StreamConfig,
        messages: Vec<bytes::Bytes>,
        should_close: bool,
    ) -> Result<CreateWithDataResult> {
        match self {
            Self::Memory(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
            Self::File(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
            Self::Acid(inner) => {
                inner.create_stream_with_data(name, config, messages, should_close)
            }
        }
    }

    fn exists(&self, name: &str) -> bool {
        match self {
            Self::Memory(inner) => inner.exists(name),
            Self::File(inner) => inner.exists(name),
            Self::Acid(inner) => inner.exists(name),
        }
    }

    fn subscribe(&self, name: &str) -> Option<broadcast::Receiver<()>> {
        match self {
            Self::Memory(inner) => inner.subscribe(name),
            Self::File(inner) => inner.subscribe(name),
            Self::Acid(inner) => inner.subscribe(name),
        }
    }

    fn cleanup_expired_streams(&self) -> usize {
        match self {
            Self::Memory(inner) => inner.cleanup_expired_streams(),
            Self::File(inner) => inner.cleanup_expired_streams(),
            Self::Acid(inner) => inner.cleanup_expired_streams(),
        }
    }

    fn list_streams(&self) -> Result<Vec<(String, StreamMetadata)>> {
        match self {
            Self::Memory(inner) => inner.list_streams(),
            Self::File(inner) => inner.list_streams(),
            Self::Acid(inner) => inner.list_streams(),
        }
    }

    fn create_fork(
        &self,
        name: &str,
        source_name: &str,
        fork_offset: Option<&StorageOffset>,
        config: StreamConfig,
    ) -> Result<CreateStreamResult> {
        match self {
            Self::Memory(inner) => inner.create_fork(name, source_name, fork_offset, config),
            Self::File(inner) => inner.create_fork(name, source_name, fork_offset, config),
            Self::Acid(inner) => inner.create_fork(name, source_name, fork_offset, config),
        }
    }
}

impl TestStorage {
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
        StorageTestBackend::FileDurable => {
            let storage_dir = unique_storage_dir("file");
            let storage = FileStorage::new(&storage_dir, max_total_bytes, max_stream_bytes, true)
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
