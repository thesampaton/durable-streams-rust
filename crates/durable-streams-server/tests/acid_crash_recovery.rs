//! White-box crash-recovery tests for [`AcidStorage`].
//!
//! These test reopen-after-shutdown behavior, shard configuration validation,
//! and data durability across restarts.

use bytes::Bytes;
use durable_streams_server::config::AcidBackend;
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::storage::acid::AcidStorage;
use durable_streams_server::storage::{Storage, StreamConfig};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

fn unique_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("ds-acid-crash-test-{prefix}-{pid}-{seq}"))
}

fn new_acid(root: &Path) -> AcidStorage {
    AcidStorage::new(root, 4, 1024 * 1024, 100 * 1024, AcidBackend::File)
        .expect("acid storage init should succeed")
}

fn plain_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

// ---------------------------------------------------------------------------
// 1. Reopen after clean shutdown
// ---------------------------------------------------------------------------

#[test]
fn reopen_after_clean_shutdown_preserves_data() {
    let root = unique_dir("reopen");
    {
        let s = new_acid(&root);
        s.create_stream("s1", plain_config()).unwrap();
        s.append("s1", Bytes::from("msg-a"), "text/plain").unwrap();
        s.append("s1", Bytes::from("msg-b"), "text/plain").unwrap();

        s.create_stream("s2", plain_config()).unwrap();
        s.append("s2", Bytes::from("msg-c"), "text/plain").unwrap();
    }

    let restored = new_acid(&root);

    let read1 = restored.read("s1", &Offset::start()).unwrap();
    assert_eq!(read1.messages.len(), 2);
    assert_eq!(read1.messages[0], Bytes::from("msg-a"));
    assert_eq!(read1.messages[1], Bytes::from("msg-b"));

    let read2 = restored.read("s2", &Offset::start()).unwrap();
    assert_eq!(read2.messages.len(), 1);
    assert_eq!(read2.messages[0], Bytes::from("msg-c"));
}

#[test]
fn reopen_preserves_closed_state() {
    let root = unique_dir("reopen-closed");
    {
        let s = new_acid(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain").unwrap();
        s.close_stream("s").unwrap();
    }

    let restored = new_acid(&root);
    let meta = restored.head("s").unwrap();
    assert!(meta.closed, "closed state should survive restart");
    assert_eq!(meta.message_count, 1);

    assert!(matches!(
        restored.append("s", Bytes::from("more"), "text/plain"),
        Err(Error::StreamClosed)
    ));
}

#[test]
fn reopen_preserves_total_bytes() {
    let root = unique_dir("reopen-bytes");
    let expected;
    {
        let s = new_acid(&root);
        s.create_stream("s1", plain_config()).unwrap();
        s.append("s1", Bytes::from("hello"), "text/plain").unwrap(); // 5
        s.create_stream("s2", plain_config()).unwrap();
        s.append("s2", Bytes::from("world!"), "text/plain").unwrap(); // 6
        expected = s.total_bytes();
        assert_eq!(expected, 11);
    }

    let restored = new_acid(&root);
    assert_eq!(
        restored.total_bytes(),
        expected,
        "total_bytes should be restored"
    );
}

// ---------------------------------------------------------------------------
// 2. Idempotent recovery across multiple restarts
// ---------------------------------------------------------------------------

#[test]
fn idempotent_recovery_acid() {
    let root = unique_dir("idempotent");
    {
        let s = new_acid(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("one"), "text/plain").unwrap();
        s.append("s", Bytes::from("two"), "text/plain").unwrap();
    }

    for i in 0..5 {
        let restored = new_acid(&root);
        let read = restored.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 2, "restart {i}: should have 2 messages");
        assert_eq!(read.messages[0], Bytes::from("one"));
        assert_eq!(read.messages[1], Bytes::from("two"));
    }
}

// ---------------------------------------------------------------------------
// 3. Shard count mismatch rejection
// ---------------------------------------------------------------------------

#[test]
fn shard_count_mismatch_rejected() {
    let root = unique_dir("shard-mismatch");
    {
        // Create with 4 shards
        let _s = new_acid(&root);
    }

    // Try to reopen with 8 shards -- should fail
    let result = AcidStorage::new(&root, 8, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(
        result.is_err(),
        "reopening with different shard count should fail"
    );
    match result {
        Err(Error::Storage(msg)) => {
            assert!(
                msg.contains("shard_count"),
                "error should mention shard_count mismatch: {msg}"
            );
        }
        _ => panic!("expected Error::Storage with shard_count message"),
    }
}

#[test]
fn shard_count_same_succeeds() {
    let root = unique_dir("shard-same");
    {
        let _s = new_acid(&root);
    }

    // Reopen with same shard count -- should succeed
    let result = AcidStorage::new(&root, 4, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(result.is_ok(), "reopening with same shard count should work");
}

// ---------------------------------------------------------------------------
// 4. Corrupted layout.json
// ---------------------------------------------------------------------------

#[test]
fn corrupted_layout_json_fails_gracefully() {
    let root = unique_dir("bad-layout");
    {
        let _s = new_acid(&root);
    }

    let layout_path = root.join("acid").join("layout.json");
    fs::write(&layout_path, "not json {{{").unwrap();

    let result = AcidStorage::new(&root, 4, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(result.is_err(), "corrupted layout.json should cause error");
}

// ---------------------------------------------------------------------------
// 5. Recovery with many streams across shards
// ---------------------------------------------------------------------------

#[test]
fn recovery_with_many_streams_across_shards() {
    let root = unique_dir("many-shards");
    {
        let s = new_acid(&root);
        for i in 0..50 {
            let name = format!("stream-{i}");
            s.create_stream(&name, plain_config()).unwrap();
            s.append(&name, Bytes::from(format!("data-{i}")), "text/plain")
                .unwrap();
        }
    }

    let restored = new_acid(&root);
    for i in 0..50 {
        let name = format!("stream-{i}");
        let read = restored.read(&name, &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 1, "stream {name} should have 1 message");
        assert_eq!(read.messages[0], Bytes::from(format!("data-{i}")));
    }
}

// ---------------------------------------------------------------------------
// 6. New appends after recovery
// ---------------------------------------------------------------------------

#[test]
fn appends_after_recovery_maintain_offset_monotonicity() {
    let root = unique_dir("append-after");
    let offset_before;
    {
        let s = new_acid(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("a"), "text/plain").unwrap();
        offset_before = s.append("s", Bytes::from("b"), "text/plain").unwrap();
    }

    let restored = new_acid(&root);
    let new_offset = restored
        .append("s", Bytes::from("c"), "text/plain")
        .unwrap();

    assert!(
        new_offset > offset_before,
        "post-recovery offset {new_offset:?} should exceed pre-crash offset {offset_before:?}"
    );

    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 3);
    assert_eq!(read.messages[2], Bytes::from("c"));
}

// ---------------------------------------------------------------------------
// 7. Truncated .redb file
// ---------------------------------------------------------------------------

#[test]
fn truncated_redb_file_returns_error() {
    let root = unique_dir("truncated-redb");
    {
        let _s = new_acid(&root);
    }

    // Find and truncate a shard file
    let acid_dir = root.join("acid");
    let shard_path = acid_dir.join("shard_00.redb");
    assert!(shard_path.exists(), "shard file should exist");
    fs::write(&shard_path, [0xFF; 10]).unwrap();

    let result = AcidStorage::new(&root, 4, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(
        result.is_err(),
        "truncated redb file should cause storage init error"
    );
}
