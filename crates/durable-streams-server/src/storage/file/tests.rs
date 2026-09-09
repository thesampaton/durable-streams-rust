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
    FileStorage::new(test_storage_dir(), 1024 * 1024, 100 * 1024)
        .expect("file storage should initialize")
}

#[test]
fn test_delete_removes_files() {
    let storage = test_storage();
    let config = crate::storage::StreamOptions::new("text/plain".to_string());
    storage.create_stream("test", config).unwrap();
    storage
        .append("test", Bytes::from("data"), "text/plain")
        .map(|result| result.start_offset)
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
    let config = crate::storage::StreamOptions::new("text/plain".to_string());

    {
        let storage = FileStorage::new(root.clone(), 1024 * 1024, 100 * 1024).unwrap();
        storage.create_stream("s", config.clone()).unwrap();
        storage
            .append("s", Bytes::from("good"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode("s".as_bytes());
    let log_path = root.join(&encoded).join("data.log");
    let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
    f.write_all(&[0xFF, 0xFF]).unwrap();
    drop(f);

    let restored = FileStorage::new(root, 1024 * 1024, 100 * 1024).unwrap();
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("good"));
}

#[test]
fn initial_stream_and_fork_data_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let storage = FileStorage::new(root.path(), 1024, 1024).unwrap();
    let created = storage
        .create_stream_with_data(
            "source",
            crate::storage::StreamOptions::new("text/plain"),
            vec![Bytes::from_static(b"source")],
            false,
        )
        .unwrap();
    let options = crate::storage::ForkOptions {
        initial_body: Bytes::from_static(b"fork"),
        ..crate::storage::ForkOptions::default()
    };
    storage
        .create_fork_with_options(
            "fork",
            "source",
            None,
            crate::storage::StreamOptions::new("text/plain").with_closed(true),
            options,
        )
        .unwrap();
    drop(storage);

    let reopened = FileStorage::new(root.path(), 1024, 1024).unwrap();
    let source = reopened.read("source", &Offset::start()).unwrap();
    assert_eq!(source.messages, vec![Bytes::from_static(b"source")]);
    assert_eq!(source.next_offset, created.next_offset);
    let fork = reopened.read("fork", &Offset::start()).unwrap();
    assert_eq!(
        fork.messages,
        vec![Bytes::from_static(b"source"), Bytes::from_static(b"fork")]
    );
    assert!(fork.closed);
    assert_eq!(reopened.total_bytes(), 10);
}

#[cfg(unix)]
#[test]
fn failed_initial_sync_releases_payload_capacity() {
    let root = tempfile::tempdir().unwrap();
    let storage = FileStorage::new(root.path(), 8, 8).unwrap();
    let config = crate::storage::StreamOptions::new("text/plain")
        .resolve(Utc::now())
        .unwrap();
    // A device accepts the record write but rejects sync_data, isolating the
    // failure between reserving/writing initial data and publishing metadata.
    let file = OpenOptions::new().write(true).open("/dev/null").unwrap();
    let mut entry = StreamEntry::new(config, file, root.path().to_owned());
    assert!(
        storage
            .append_initial_records("s", &mut entry, &[Bytes::from_static(b"12345678")])
            .is_err()
    );
    assert_eq!(storage.total_bytes(), 0);
    assert!(!storage.exists("s").unwrap());
    storage
        .create_stream_with_data(
            "s",
            crate::storage::StreamOptions::new("text/plain"),
            vec![Bytes::from_static(b"12345678")],
            false,
        )
        .unwrap();
    assert_eq!(storage.total_bytes(), 8);
}
