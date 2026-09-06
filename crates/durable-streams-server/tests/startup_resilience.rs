//! Integration coverage for startup resilience.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

//! Startup resilience and initialization tests.
//!
//! These validate that storage backends handle various startup scenarios
//! correctly: many streams, corrupted files, extra non-stream entries,
//! and the `/readyz` endpoint for readiness signaling.

mod common;

use bytes::Bytes;
use common::{read_problem, spawn_test_server_with_readyz, test_client};
use durable_streams_server::config::AcidBackend;
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::storage::acid::AcidStorage;
use durable_streams_server::storage::file::FileStorage;
use durable_streams_server::storage::{Storage, StreamConfig};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

fn unique_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("ds-startup-test-{prefix}-{pid}-{seq}"))
}

fn new_file_storage(root: &Path) -> FileStorage {
    FileStorage::new(root, 10 * 1024 * 1024, 1024 * 1024, true)
        .expect("storage init should succeed")
}

fn new_acid(root: &Path) -> AcidStorage {
    AcidStorage::new(root, 4, 10 * 1024 * 1024, 1024 * 1024, AcidBackend::File)
        .expect("acid storage init should succeed")
}

fn plain_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

// ── Durable backend abstraction for shared startup tests ────────────

#[derive(Debug, Clone, Copy)]
enum DurableBackend {
    File,
    Acid,
}

impl DurableBackend {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Acid => "acid",
        }
    }
}

/// Open (or create) a durable storage instance at the given root.
fn open_durable(backend: DurableBackend, root: &Path) -> Box<dyn Storage> {
    match backend {
        DurableBackend::File => Box::new(new_file_storage(root)),
        DurableBackend::Acid => Box::new(new_acid(root)),
    }
}

// ---------------------------------------------------------------------------
// 1. Durable storage startup with many streams (file + acid)
// ---------------------------------------------------------------------------

backend_tests! {
    type DurableBackend;
    file => DurableBackend::File,
    acid => DurableBackend::Acid;

    #[test]
    fn durable_startup_with_100_streams() {
        let root = unique_dir(&format!("many-{}", BACKEND.as_str()));
        let expected_total;
        {
            let s = open_durable(BACKEND, &root);
            for i in 0..100 {
                let name = format!("stream-{i:03}");
                s.create_stream(&name, plain_config()).unwrap();
                s.append(&name, Bytes::from(format!("data-{i}")), "text/plain")
                    .unwrap();
            }
            let meta = s.list_streams().unwrap();
            expected_total = meta.iter().map(|(_, m)| m.total_bytes).sum::<u64>();
            assert!(expected_total > 0);
        }

        let restored = open_durable(BACKEND, &root);
        let meta = restored.list_streams().unwrap();
        let restored_total = meta.iter().map(|(_, m)| m.total_bytes).sum::<u64>();
        assert_eq!(
            restored_total, expected_total,
            "total_bytes should match after restoring 100 streams"
        );

        for i in 0..100 {
            let name = format!("stream-{i:03}");
            let read = restored.read(&name, &Offset::start()).unwrap();
            assert_eq!(
                read.messages.len(),
                1,
                "stream {name} should have 1 message"
            );
            assert_eq!(read.messages[0], Bytes::from(format!("data-{i}")));
        }
    }

    // ---------------------------------------------------------------------------
    // 2. Durable storage starts clean on empty directory (file + acid)
    // ---------------------------------------------------------------------------

    #[test]
    fn durable_starts_clean_on_empty_directory() {
        let root = unique_dir(&format!("empty-{}", BACKEND.as_str()));
        let s = open_durable(BACKEND, &root);

        let meta = s.list_streams().unwrap();
        assert!(meta.is_empty());

        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("hello"), "text/plain").unwrap();

        let read = s.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 1);
    }
}

// ---------------------------------------------------------------------------
// 3. AcidStorage startup with truncated .redb file
// ---------------------------------------------------------------------------

#[test]
fn acid_truncated_redb_returns_clear_error() {
    let root = unique_dir("truncated");
    {
        let s = new_acid(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain").unwrap();
    }

    let shard_path = root.join("acid").join("shard_00.redb");
    assert!(shard_path.exists());
    fs::write(&shard_path, [0xFF; 10]).unwrap();

    let result = AcidStorage::new(&root, 4, 10 * 1024 * 1024, 1024 * 1024, AcidBackend::File);
    assert!(result.is_err(), "truncated shard should fail startup");
    match result {
        Err(Error::Storage(msg)) => {
            assert!(!msg.is_empty(), "error message should be descriptive");
        }
        _ => panic!("expected Error::Storage"),
    }
}

// ---------------------------------------------------------------------------
// 4. Extra non-stream files in storage directory
// ---------------------------------------------------------------------------

#[test]
fn file_storage_ignores_extra_files_in_root() {
    let root = unique_dir("extra-files");
    {
        let s = new_file_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain").unwrap();
    }

    // Add various non-stream items
    fs::write(root.join("random.txt"), "not a stream").unwrap();
    fs::write(root.join(".hidden"), "hidden file").unwrap();
    fs::write(root.join("README.md"), "readme").unwrap();

    let restored = new_file_storage(&root);
    assert!(restored.exists("s"), "real stream should be loaded");

    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("data"));
}

#[test]
fn file_storage_ignores_non_directory_entries() {
    let root = unique_dir("non-dir");
    {
        let s = new_file_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain").unwrap();
    }

    // Create a regular file that looks like it could be a stream directory
    fs::write(root.join("fake-stream-dir"), "not a directory").unwrap();

    let restored = new_file_storage(&root);
    assert!(restored.exists("s"));
    // No panic or error — the fake file is silently skipped
}

// ---------------------------------------------------------------------------
// 5. layout.json version/policy mismatch
// ---------------------------------------------------------------------------

#[test]
fn acid_layout_format_version_mismatch() {
    let root = unique_dir("layout-ver");
    {
        let _s = new_acid(&root);
    }

    let layout_path = root.join("acid").join("layout.json");
    let raw = fs::read_to_string(&layout_path).unwrap();
    let updated = raw.replace("\"format_version\": 1", "\"format_version\": 99");
    assert_ne!(raw, updated);
    fs::write(&layout_path, updated).unwrap();

    let result = AcidStorage::new(&root, 4, 10 * 1024 * 1024, 1024 * 1024, AcidBackend::File);
    assert!(result.is_err());
    match result {
        Err(Error::Storage(msg)) => {
            assert!(
                msg.contains("format_version"),
                "error should mention format_version: {msg}"
            );
        }
        _ => panic!("expected Error::Storage"),
    }
}

#[test]
fn acid_layout_hash_policy_mismatch() {
    let root = unique_dir("layout-hash");
    {
        let _s = new_acid(&root);
    }

    let layout_path = root.join("acid").join("layout.json");
    let raw = fs::read_to_string(&layout_path).unwrap();
    let updated = raw.replace("seahash-v1", "murmurhash-v3");
    assert_ne!(raw, updated);
    fs::write(&layout_path, updated).unwrap();

    let result = AcidStorage::new(&root, 4, 10 * 1024 * 1024, 1024 * 1024, AcidBackend::File);
    assert!(result.is_err());
    match result {
        Err(Error::Storage(msg)) => {
            assert!(
                msg.contains("hash_policy"),
                "error should mention hash_policy: {msg}"
            );
        }
        _ => panic!("expected Error::Storage"),
    }
}

// ---------------------------------------------------------------------------
// 6. /readyz endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn readyz_returns_503_when_not_ready() {
    let (base_url, _port, _ready) = spawn_test_server_with_readyz().await;
    let client = test_client();

    let resp = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        503,
        "readyz should return 503 when not ready"
    );
    let problem = read_problem(resp).await;
    assert_eq!(problem.problem_type, "/errors/unavailable");
    assert_eq!(problem.title, "Service Unavailable");
    assert_eq!(problem.status, 503);
    assert_eq!(problem.code, "UNAVAILABLE");
    assert_eq!(problem.instance.as_deref(), Some("/readyz"));
}

#[tokio::test]
async fn readyz_returns_200_when_ready() {
    let (base_url, _port, ready) = spawn_test_server_with_readyz().await;
    let client = test_client();

    // Flip the ready flag
    ready.store(true, std::sync::atomic::Ordering::Release);

    let resp = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "readyz should return 200 when ready"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(body, "ready");
}

#[tokio::test]
async fn healthz_always_returns_200_regardless_of_ready() {
    let (base_url, _port, _ready) = spawn_test_server_with_readyz().await;
    let client = test_client();

    // Ready flag is false, but healthz should still return 200
    let resp = client
        .get(format!("{base_url}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "healthz should always return 200"
    );
}

// ---------------------------------------------------------------------------
// 7. FileStorage with stream directories missing data.log
// ---------------------------------------------------------------------------

#[test]
fn file_storage_handles_stream_dir_without_data_log() {
    let root = unique_dir("no-datalog");
    {
        let s = new_file_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain").unwrap();
    }

    // Remove just the data.log but keep meta.json
    let encoded = base64::Engine::encode(&base64::prelude::BASE64_URL_SAFE_NO_PAD, "s".as_bytes());
    let data_log = root.join(&encoded).join("data.log");
    fs::remove_file(&data_log).unwrap();

    // Should still start (data.log is recreated when opened)
    let restored = new_file_storage(&root);
    // Stream exists but has no messages (data was lost)
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 0);
}
