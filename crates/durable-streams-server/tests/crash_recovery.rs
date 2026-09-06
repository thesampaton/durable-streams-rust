//! Integration coverage for crash recovery.

#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

//! White-box crash-recovery tests for [`FileStorage`].
//!
//! These simulate mid-write crashes by directly manipulating the on-disk
//! data.log and meta.json files, then reopening a fresh `FileStorage`
//! instance to verify recovery semantics.

use base64::Engine;
use bytes::Bytes;
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::file::FileStorage;
use durable_streams_server::storage::{ProducerAppendResult, Storage, StreamOptions};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

fn unique_dir(prefix: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("ds-crash-test-{prefix}-{pid}-{seq}"))
}

fn new_storage(root: &Path) -> FileStorage {
    FileStorage::new(root, 1024 * 1024, 100 * 1024, true).expect("storage init should succeed")
}

fn stream_dir(root: &Path, name: &str) -> PathBuf {
    let encoded = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(name.as_bytes());
    root.join(encoded)
}

fn data_log_path(root: &Path, name: &str) -> PathBuf {
    stream_dir(root, name).join("data.log")
}

fn meta_json_path(root: &Path, name: &str) -> PathBuf {
    stream_dir(root, name).join("meta.json")
}

fn plain_config() -> StreamOptions {
    StreamOptions::new("text/plain".to_string())
}

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
    ProducerHeaders {
        id: id.to_string(),
        epoch,
        seq,
    }
}

// ---------------------------------------------------------------------------
// 1. Partial header truncation
// ---------------------------------------------------------------------------

#[test]
fn partial_header_truncation_1_byte() {
    let root = unique_dir("hdr1");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("good"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Append 1 byte of a record header (less than the 4-byte header)
    let log = data_log_path(&root, "s");
    let mut f = OpenOptions::new().append(true).open(&log).unwrap();
    f.write_all(&[0xFF]).unwrap();
    drop(f);

    let restored = new_storage(&root);
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("good"));
}

#[test]
fn partial_header_truncation_2_bytes() {
    let root = unique_dir("hdr2");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("good"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    let log = data_log_path(&root, "s");
    let mut f = OpenOptions::new().append(true).open(&log).unwrap();
    f.write_all(&[0xFF, 0xFF]).unwrap();
    drop(f);

    let restored = new_storage(&root);
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("good"));
}

#[test]
fn partial_header_truncation_3_bytes() {
    let root = unique_dir("hdr3");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("good"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    let log = data_log_path(&root, "s");
    let mut f = OpenOptions::new().append(true).open(&log).unwrap();
    f.write_all(&[0xFF, 0xFF, 0xFF]).unwrap();
    drop(f);

    let restored = new_storage(&root);
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("good"));
}

// ---------------------------------------------------------------------------
// 2. Partial payload truncation
// ---------------------------------------------------------------------------

#[test]
fn partial_payload_truncation() {
    let root = unique_dir("payload");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("first"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        s.append("s", Bytes::from("second"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Append a valid 4-byte header claiming 100 bytes, then only 50 bytes of payload
    let log = data_log_path(&root, "s");
    let mut f = OpenOptions::new().append(true).open(&log).unwrap();
    let len: u32 = 100;
    f.write_all(&len.to_le_bytes()).unwrap();
    f.write_all(&[0xAB; 50]).unwrap();
    drop(f);

    let restored = new_storage(&root);
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 2, "partial record should be truncated");
    assert_eq!(read.messages[0], Bytes::from("first"));
    assert_eq!(read.messages[1], Bytes::from("second"));
}

#[test]
fn partial_payload_truncation_zero_extra_bytes() {
    // Header present but zero payload bytes written
    let root = unique_dir("payload0");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("ok"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    let log = data_log_path(&root, "s");
    let mut f = OpenOptions::new().append(true).open(&log).unwrap();
    let len: u32 = 64;
    f.write_all(&len.to_le_bytes()).unwrap();
    // No payload bytes at all
    drop(f);

    let restored = new_storage(&root);
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("ok"));
}

// ---------------------------------------------------------------------------
// 3. meta.json vs data.log divergence
// ---------------------------------------------------------------------------

#[test]
fn meta_json_claims_closed_but_log_has_more_data() {
    let root = unique_dir("meta-closed");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("a"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        s.append("s", Bytes::from("b"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Manually set closed=true in meta.json
    let meta_path = meta_json_path(&root, "s");
    let raw = fs::read_to_string(&meta_path).unwrap();
    let updated = raw.replace("\"closed\":false", "\"closed\":true");
    assert_ne!(raw, updated, "should have replaced closed flag");
    fs::write(&meta_path, updated).unwrap();

    let restored = new_storage(&root);
    let meta = restored.head("s").unwrap();
    // meta.json said closed, so closed should be true on recovery
    assert!(meta.closed, "closed flag from meta.json should be honored");
    // But all data from the log should still be present
    assert_eq!(
        meta.message_count, 2,
        "both messages should be recovered from log"
    );

    // Verify data integrity
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 2);
    assert_eq!(read.messages[0], Bytes::from("a"));
    assert_eq!(read.messages[1], Bytes::from("b"));
    assert!(read.closed, "read should report stream as closed");
}

#[test]
fn meta_json_stale_producer_state_after_crash() {
    let root = unique_dir("meta-producer");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        // First producer append
        s.append_with_producer(
            "s",
            vec![Bytes::from("msg1")],
            "text/plain",
            &producer("p1", 1, 0),
            false,
            None,
        )
        .unwrap();
    }

    // Corrupt meta.json by removing producers
    let meta_path = meta_json_path(&root, "s");
    let raw = fs::read_to_string(&meta_path).unwrap();
    let mut parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
    parsed["producers"] = serde_json::json!({});
    fs::write(&meta_path, serde_json::to_string_pretty(&parsed).unwrap()).unwrap();

    let restored = new_storage(&root);

    // Data should still be present
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 1);
    assert_eq!(read.messages[0], Bytes::from("msg1"));

    // But producer state is lost -- duplicate detection is broken
    // This documents the known limitation: producer epoch=1/seq=0 should be
    // a duplicate but without persisted state the server will accept it.
    let result = restored.append_with_producer(
        "s",
        vec![Bytes::from("msg1-dup")],
        "text/plain",
        &producer("p1", 1, 0),
        false,
        None,
    );
    // Without producer state, this will be accepted instead of being a duplicate
    assert!(
        matches!(result, Ok(ProducerAppendResult::Accepted { .. })),
        "lost producer state means duplicate detection is broken after crash"
    );
}

// ---------------------------------------------------------------------------
// 4. Zero-length data.log with valid meta.json
// ---------------------------------------------------------------------------

#[test]
fn zero_length_data_log_with_valid_meta() {
    let root = unique_dir("zerolog");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        // Don't append any data, just create the stream
    }

    // Verify the stream exists and has zero messages
    let restored = new_storage(&root);
    let meta = restored.head("s").unwrap();
    assert_eq!(meta.message_count, 0);
    assert_eq!(meta.total_bytes, 0);

    let read = restored.read("s", &Offset::start()).unwrap();
    assert!(read.messages.is_empty());
    assert!(read.at_tail);
}

#[test]
fn zero_length_data_log_after_truncation() {
    let root = unique_dir("zerologtrunc");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Truncate the data.log to zero bytes (simulate total data loss)
    let log = data_log_path(&root, "s");
    fs::write(&log, b"").unwrap();

    let restored = new_storage(&root);
    let meta = restored.head("s").unwrap();
    assert_eq!(
        meta.message_count, 0,
        "truncated log should have zero messages"
    );
    assert_eq!(meta.total_bytes, 0);
}

// ---------------------------------------------------------------------------
// 5. Corrupted meta.json
// ---------------------------------------------------------------------------

#[test]
fn corrupted_meta_json_invalid_json() {
    let root = unique_dir("badjson");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Write garbage to meta.json
    let meta_path = meta_json_path(&root, "s");
    fs::write(&meta_path, "not valid json {{{").unwrap();

    // Opening storage should fail because meta.json cannot be parsed
    let result = FileStorage::new(&root, 1024 * 1024, 100 * 1024, true);
    assert!(
        result.is_err(),
        "corrupted meta.json should cause storage init error"
    );
    match result {
        Err(Error::Storage(msg)) => {
            assert!(msg.contains("parse"), "error should mention parsing: {msg}");
        }
        _ => panic!("expected Error::Storage with parse message"),
    }
}

#[test]
fn missing_meta_json_skips_directory() {
    let root = unique_dir("nometa");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("data"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Delete meta.json but leave data.log
    let meta_path = meta_json_path(&root, "s");
    fs::remove_file(&meta_path).unwrap();

    let restored = new_storage(&root);
    // Stream should not exist (no meta.json to load from)
    assert!(
        !restored.exists("s").unwrap(),
        "stream without meta.json should not be loaded"
    );
}

// ---------------------------------------------------------------------------
// 6. Idempotent recovery
// ---------------------------------------------------------------------------

#[test]
fn idempotent_recovery_multiple_restarts() {
    let root = unique_dir("idempotent");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("event-1"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        s.append("s", Bytes::from("event-2"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        s.append("s", Bytes::from("event-3"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Restart 5 times and verify data is identical each time
    for i in 0..5 {
        let restored = new_storage(&root);
        let read = restored.read("s", &Offset::start()).unwrap();
        assert_eq!(
            read.messages.len(),
            3,
            "restart {i}: should have 3 messages"
        );
        assert_eq!(read.messages[0], Bytes::from("event-1"));
        assert_eq!(read.messages[1], Bytes::from("event-2"));
        assert_eq!(read.messages[2], Bytes::from("event-3"));

        let meta = restored.head("s").unwrap();
        assert_eq!(meta.total_bytes, 21); // 7 + 7 + 7
        assert_eq!(meta.message_count, 3);
    }
}

#[test]
fn recovery_preserves_offsets_and_allows_new_appends() {
    let root = unique_dir("offsets");
    let offset_before;
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("a"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        offset_before = s
            .append("s", Bytes::from("b"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    let restored = new_storage(&root);

    // Offset after recovery should allow continued appends
    let new_offset = restored
        .append("s", Bytes::from("c"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();
    assert!(
        new_offset > offset_before,
        "new offset {new_offset:?} should be greater than pre-crash offset {offset_before:?}"
    );

    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 3);
    assert_eq!(read.messages[2], Bytes::from("c"));
}

// ---------------------------------------------------------------------------
// 7. Recovery with multiple streams
// ---------------------------------------------------------------------------

#[test]
fn recovery_with_many_streams() {
    let root = unique_dir("many");
    {
        let s = new_storage(&root);
        for i in 0..20 {
            let name = format!("stream-{i}");
            s.create_stream(&name, plain_config()).unwrap();
            s.append(&name, Bytes::from(format!("data-{i}")), "text/plain")
                .map(|result| result.start_offset)
                .unwrap();
        }
    }

    let restored = new_storage(&root);
    for i in 0..20 {
        let name = format!("stream-{i}");
        let read = restored.read(&name, &Offset::start()).unwrap();
        assert_eq!(
            read.messages.len(),
            1,
            "stream {name} should have 1 message"
        );
        assert_eq!(read.messages[0], Bytes::from(format!("data-{i}")));
    }

    let total = restored.total_bytes();
    assert!(
        total > 0,
        "total_bytes should be restored across all streams"
    );
}

// ---------------------------------------------------------------------------
// 8. Recovery of total_bytes accounting
// ---------------------------------------------------------------------------

#[test]
fn total_bytes_restored_accurately() {
    let root = unique_dir("totalbytes");
    let expected_total;
    {
        let s = new_storage(&root);
        s.create_stream("s1", plain_config()).unwrap();
        s.append("s1", Bytes::from("hello"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap(); // 5 bytes
        s.create_stream("s2", plain_config()).unwrap();
        s.append("s2", Bytes::from("world!"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap(); // 6 bytes
        expected_total = s.total_bytes();
        assert_eq!(expected_total, 11);
    }

    let restored = new_storage(&root);
    assert_eq!(
        restored.total_bytes(),
        expected_total,
        "total_bytes should match pre-crash value"
    );
}

// ---------------------------------------------------------------------------
// 9. Partial record after multiple valid records
// ---------------------------------------------------------------------------

#[test]
fn partial_record_mid_batch_recovery() {
    let root = unique_dir("midbatch");
    {
        let s = new_storage(&root);
        s.create_stream("s", plain_config()).unwrap();
        s.append("s", Bytes::from("one"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        s.append("s", Bytes::from("two"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
        s.append("s", Bytes::from("three"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    // Append a partial record after the three valid ones
    let log = data_log_path(&root, "s");
    let mut f = OpenOptions::new().append(true).open(&log).unwrap();
    let len: u32 = 200;
    f.write_all(&len.to_le_bytes()).unwrap();
    f.write_all(&[0xDE; 10]).unwrap(); // Only 10 of 200 bytes
    drop(f);

    let restored = new_storage(&root);
    let read = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(
        read.messages.len(),
        3,
        "partial record at end should be truncated"
    );
    assert_eq!(read.messages[0], Bytes::from("one"));
    assert_eq!(read.messages[1], Bytes::from("two"));
    assert_eq!(read.messages[2], Bytes::from("three"));

    // Can still append after recovery
    restored
        .append("s", Bytes::from("four"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();
    let read2 = restored.read("s", &Offset::start()).unwrap();
    assert_eq!(read2.messages.len(), 4);
}
