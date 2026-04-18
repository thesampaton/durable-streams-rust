use super::*;
use base64::Engine;
use bytes::Bytes;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

fn test_storage_dir() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("ds-file-storage-test-{stamp}-{pid}-{seq}"))
}

fn test_storage() -> FileStorage {
    FileStorage::new(test_storage_dir(), 1024 * 1024, 100 * 1024, false)
        .expect("file storage should initialize")
}

#[test]
fn test_delete_removes_files() {
    let storage = test_storage();
    let config = StreamConfig::new("text/plain".to_string());
    storage.create_stream("test", config).unwrap();
    storage
        .append("test", Bytes::from("data"), "text/plain")
        .unwrap();

    let dir = storage.stream_dir_for_name("test").unwrap();
    assert!(dir.exists(), "stream directory should exist before delete");

    storage.delete("test").unwrap();
    assert!(
        !dir.exists(),
        "stream directory should be removed after delete"
    );
}

// Restore-from-disk and closed-stream durability tests live in
// tests/crash_recovery.rs and the storage_backend_contract suite.

#[test]
fn test_partial_record_truncation_on_recovery() {
    let root = test_storage_dir();
    let config = StreamConfig::new("text/plain".to_string());

    {
        let storage = FileStorage::new(root.clone(), 1024 * 1024, 100 * 1024, false).unwrap();
        storage.create_stream("s", config.clone()).unwrap();
        storage
            .append("s", Bytes::from("good"), "text/plain")
            .unwrap();
    }

    let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode("s".as_bytes());
    let log_path = root.join(&encoded).join("data.log");
    let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
    f.write_all(&[0xFF, 0xFF]).unwrap();
    drop(f);

    let restored = FileStorage::new(root, 1024 * 1024, 100 * 1024, false).unwrap();
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("good"));
}
