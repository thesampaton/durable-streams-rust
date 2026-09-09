use super::*;
use crate::protocol::error::Error;
use crate::protocol::offset::Offset;
use crate::storage::{CreateStreamResult, ForkInfo, Storage, StreamState};
use bytes::Bytes;
use chrono::Duration;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

fn test_storage_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("ds-acid-storage-test-{stamp}-{pid}-{seq}"))
}

fn test_storage() -> AcidStorage {
    AcidStorage::new(
        test_storage_dir(),
        16,
        1024 * 1024,
        100 * 1024,
        AcidBackend::File,
    )
    .expect("acid storage should initialize")
}

fn names_on_different_shards(storage: &AcidStorage) -> (String, usize, String, usize) {
    let source = "source-0".to_string();
    let source_idx = storage.shard_index(&source);

    for i in 1..512 {
        let fork = format!("fork-{i}");
        let fork_idx = storage.shard_index(&fork);
        if fork_idx != source_idx {
            return (source, source_idx, fork, fork_idx);
        }
    }

    panic!("failed to find stream names on different shards");
}

// Restore-from-disk and producer-state durability tests live in
// tests/acid_crash_recovery.rs and the storage_backend_contract suite.

#[test]
fn test_shard_routing_same_stream_is_stable() {
    let storage = test_storage();
    let a = storage.shard_index("same-stream");
    let b = storage.shard_index("same-stream");
    assert_eq!(a, b);
}

#[test]
fn test_shard_distribution_uses_multiple_shards() {
    let storage = test_storage();
    let mut seen = std::collections::HashSet::new();
    for i in 0..256 {
        seen.insert(storage.shard_index(&format!("stream-{i}")));
    }
    assert!(seen.len() > 1);
}

#[test]
fn test_startup_purges_expired_streams() {
    let root = test_storage_dir();
    {
        let storage =
            AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
        let expires = Utc::now() + Duration::milliseconds(200);
        let cfg =
            crate::storage::StreamOptions::new("text/plain".to_string()).with_expires_at(expires);
        storage.create_stream("expiring", cfg).unwrap();
        storage
            .append("expiring", Bytes::from("x"), "text/plain")
            .map(|result| result.start_offset)
            .unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(250));

    let restored = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
    assert!(!restored.exists("expiring").unwrap());
    assert!(matches!(
        restored.read("expiring", &Offset::start()),
        Err(Error::NotFound(_) | Error::StreamExpired)
    ));
}

#[test]
fn test_global_cap_strict_under_concurrency() {
    let storage =
        Arc::new(AcidStorage::new(test_storage_dir(), 16, 120, 120, AcidBackend::File).unwrap());
    let shard_count = (0..8)
        .map(|i| storage.shard_index(&format!("s-{i}")))
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert!(
        shard_count > 1,
        "test streams must span multiple shards to validate cross-shard cap behavior"
    );

    for i in 0..8 {
        storage
            .create_stream(
                &format!("s-{i}"),
                crate::storage::StreamOptions::new("text/plain".to_string()),
            )
            .unwrap();
    }

    let mut handles = Vec::new();
    for i in 0..8 {
        let storage = Arc::clone(&storage);
        handles.push(thread::spawn(move || {
            storage
                .append(&format!("s-{i}"), Bytes::from(vec![0_u8; 40]), "text/plain")
                .map(|result| result.start_offset)
        }));
    }

    for h in handles {
        let _ = h.join().unwrap();
    }

    assert!(storage.total_bytes() <= 120);
}

#[test]
fn test_layout_manifest_mismatch_fails_fast() {
    let root = test_storage_dir();
    let first = AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(first.is_ok());

    let mismatch = AcidStorage::new(root, 8, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(matches!(mismatch, Err(Error::Storage(_))));
}

#[test]
fn test_create_fork_routes_to_source_shard_when_names_hash_apart() {
    let storage = test_storage();
    let (source, source_idx, fork, fork_hash_idx) = names_on_different_shards(&storage);

    storage
        .create_stream(
            &source,
            crate::storage::StreamOptions::new("text/plain".to_string()),
        )
        .unwrap();
    storage
        .append(&source, Bytes::from("root"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();
    assert_eq!(
        storage.find_stream_shard_index(&source).unwrap(),
        Some(source_idx)
    );
    assert_ne!(
        source_idx, fork_hash_idx,
        "test inputs must naturally hash to different shards"
    );

    let created = storage
        .create_fork(
            &fork,
            &source,
            None,
            crate::storage::StreamOptions::new("text/plain".to_string()),
        )
        .unwrap();
    assert_eq!(created, CreateStreamResult::Created);
    assert_eq!(
        storage.find_stream_shard_index(&fork).unwrap(),
        Some(source_idx)
    );

    let fork_read = storage.read(&fork, &Offset::start()).unwrap();
    assert_eq!(fork_read.messages, vec![Bytes::from("root")]);
}

#[test]
fn test_reopen_rejects_legacy_cross_shard_fork_lineage() {
    let root = test_storage_dir();
    let storage =
        AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
    let (source, source_idx, fork, fork_idx) = names_on_different_shards(&storage);
    assert_ne!(source_idx, fork_idx);

    storage
        .create_stream(
            &source,
            crate::storage::StreamOptions::new("text/plain".to_string()),
        )
        .unwrap();

    let fork_meta = StoredStreamMeta {
        config: crate::storage::StreamOptions::new("text/plain")
            .resolve(Utc::now())
            .unwrap(),
        closed: false,
        next_read_seq: 0,
        next_byte_offset: 0,
        total_bytes: 0,
        created_at: Utc::now(),
        updated_at: None,
        last_seq: None,
        producers: HashMap::new(),
        fork_info: Some(ForkInfo {
            sub_offset: 0,
            source_name: source.clone(),
            fork_offset: Offset::start(),
        }),
        ref_count: 0,
        state: StreamState::Active,
    };

    let shard = &storage.shards[fork_idx];
    let txn = AcidStorage::begin_write_txn(&shard.db).unwrap();
    let mut streams = txn.open_table(STREAMS).unwrap();
    AcidStorage::write_stream_meta(&mut streams, &fork, &fork_meta).unwrap();
    drop(streams);
    txn.commit().unwrap();
    drop(storage);

    let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(matches!(
        reopened,
        Err(Error::Storage(message))
        if message.contains("legacy cross-shard fork lineage requires migration")
    ));
}

#[test]
fn test_layout_manifest_invalid_json_fails_fast() {
    let root = test_storage_dir();
    let acid_dir = root.join("acid");
    fs::create_dir_all(&acid_dir).unwrap();
    fs::write(acid_dir.join("layout.json"), b"{invalid-json").unwrap();

    let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(matches!(reopened, Err(Error::Storage(_))));
}

#[test]
fn test_layout_manifest_hash_policy_mismatch_fails_fast() {
    let root = test_storage_dir();
    let storage =
        AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
    drop(storage);

    let layout_path = root.join("acid").join("layout.json");
    let mut layout: serde_json::Value =
        serde_json::from_slice(&fs::read(&layout_path).unwrap()).unwrap();
    layout["hash_policy"] = serde_json::Value::String("tampered-hash-policy".to_string());
    fs::write(layout_path, serde_json::to_vec_pretty(&layout).unwrap()).unwrap();

    let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(matches!(reopened, Err(Error::Storage(_))));
}

#[test]
fn test_corrupted_stream_metadata_fails_fast_on_startup() {
    let root = test_storage_dir();
    let storage =
        AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
    storage
        .create_stream(
            "s",
            crate::storage::StreamOptions::new("text/plain".to_string()),
        )
        .unwrap();
    storage
        .append("s", Bytes::from("payload"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();

    let shard_idx = storage.shard_index("s");
    let txn = AcidStorage::begin_write_txn(&storage.shards[shard_idx].db).unwrap();
    let mut streams = txn.open_table(STREAMS).unwrap();
    let corrupt = b"{not-json".to_vec();
    streams.insert("s", corrupt.as_slice()).unwrap();
    drop(streams);
    txn.commit().unwrap();
    drop(storage);

    let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(matches!(reopened, Err(Error::Storage(_))));
}

#[test]
fn test_tampered_shard_file_fails_fast_on_startup() {
    let root = test_storage_dir();
    let storage =
        AcidStorage::new(root.clone(), 16, 1024 * 1024, 100 * 1024, AcidBackend::File).unwrap();
    storage
        .create_stream(
            "s",
            crate::storage::StreamOptions::new("text/plain".to_string()),
        )
        .unwrap();
    storage
        .append("s", Bytes::from("payload"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();
    let shard_idx = storage.shard_index("s");
    drop(storage);

    let shard_path = root
        .join("acid")
        .join(format!("shard_{shard_idx:02x}.redb"));
    fs::write(&shard_path, b"not-a-valid-redb-file").unwrap();

    let reopened = AcidStorage::new(root, 16, 1024 * 1024, 100 * 1024, AcidBackend::File);
    assert!(matches!(reopened, Err(Error::Storage(_))));
}

#[test]
fn test_in_memory_backend_create_append_read() {
    let storage = AcidStorage::new(
        test_storage_dir(),
        4,
        1024 * 1024,
        100 * 1024,
        AcidBackend::InMemory,
    )
    .expect("in-memory acid storage should initialize");

    let cfg = crate::storage::StreamOptions::new("text/plain".to_string());
    storage.create_stream("s", cfg).unwrap();
    storage
        .append("s", Bytes::from("hello"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();
    storage
        .append("s", Bytes::from("world"), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();

    let read = storage.read("s", &Offset::start()).unwrap();
    assert_eq!(read.messages.len(), 2);
    assert_eq!(read.messages[0], Bytes::from("hello"));
    assert_eq!(read.messages[1], Bytes::from("world"));

    let meta = storage.head("s").unwrap();
    assert_eq!(meta.message_count, 2);
    assert_eq!(meta.total_bytes, 10);
}

#[test]
fn test_in_memory_backend_global_cap() {
    let storage = AcidStorage::new(test_storage_dir(), 4, 50, 50, AcidBackend::InMemory)
        .expect("in-memory acid storage should initialize");

    let cfg = crate::storage::StreamOptions::new("text/plain".to_string());
    storage.create_stream("s", cfg).unwrap();
    storage
        .append("s", Bytes::from(vec![0_u8; 40]), "text/plain")
        .map(|result| result.start_offset)
        .unwrap();

    let result = storage
        .append("s", Bytes::from(vec![0_u8; 20]), "text/plain")
        .map(|result| result.start_offset);
    assert!(result.is_err());
    assert_eq!(storage.total_bytes(), 40);
}

/// Issue #8: messages, tail offsets, and closure must come from one snapshot,
/// including fork reads that renew TTL while another writer is appending.
#[test]
fn test_concurrent_reads_keep_messages_and_tail_in_one_snapshot() {
    use std::sync::Barrier;

    for forked in [false, true] {
        for ttl in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let storage = Arc::new(
                AcidStorage::new(
                    dir.path(),
                    4,
                    1024 * 1024,
                    100 * 1024,
                    AcidBackend::InMemory,
                )
                .unwrap(),
            );
            let config = if ttl {
                crate::storage::StreamOptions::new("text/plain").with_ttl(60)
            } else {
                crate::storage::StreamOptions::new("text/plain")
            };
            if forked {
                storage.create_stream("source", config.clone()).unwrap();
                storage
                    .append("source", Bytes::from_static(b"s"), "text/plain")
                    .map(|result| result.start_offset)
                    .unwrap();
                storage
                    .create_fork("stream", "source", None, config)
                    .unwrap();
                // The source can keep growing, but its later data is not inherited.
                storage
                    .append("source", Bytes::from_static(b"hidden"), "text/plain")
                    .map(|result| result.start_offset)
                    .unwrap();
                storage.delete("source").unwrap();
            } else {
                storage.create_stream("stream", config).unwrap();
            }
            let barrier = Arc::new(Barrier::new(2));
            let writer_storage = Arc::clone(&storage);
            let writer_barrier = Arc::clone(&barrier);
            let writer = thread::spawn(move || {
                writer_barrier.wait();
                for _ in 0..400 {
                    writer_storage
                        .append("stream", Bytes::from_static(b"x"), "text/plain")
                        .map(|result| result.start_offset)
                        .unwrap();
                }
                writer_storage.close_stream("stream").unwrap();
            });
            let mut mismatches = Vec::new();
            barrier.wait();
            for _ in 0..400 {
                let result = storage.read("stream", &Offset::start()).unwrap();
                let (seq, bytes) = result.next_offset.parse_components().unwrap();
                if seq != result.messages.len() as u64
                    || bytes != result.messages.iter().map(|m| m.len() as u64).sum::<u64>()
                {
                    mismatches.push((seq, bytes, result.messages.len()));
                }
            }
            writer.join().unwrap();
            assert!(
                mismatches.is_empty(),
                "forked={forked}, ttl={ttl}: {mismatches:?}"
            );
            let result = storage.read("stream", &Offset::start()).unwrap();
            assert!(result.closed);
            assert_eq!(result.messages.len(), 400 + usize::from(forked));
        }
    }
}
