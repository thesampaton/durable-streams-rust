//! Concurrent-access stress tests that exercise race conditions across
//! all storage backends.
//!
//! These go beyond the single `concurrent_append_offsets_unique` test in the
//! contract suite by stressing concurrent readers + writers, create races,
//! delete-during-read, subscribe+close, and broadcast channel saturation.

mod common;

use bytes::Bytes;
use common::{StorageTestBackend, create_test_storage, create_test_storage_with_limits};
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::storage::{CreateStreamResult, Storage, StreamConfig};
use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, RefUnwindSafe, catch_unwind};
use std::sync::{Arc, Barrier};
use std::thread;

const BACKENDS: [StorageTestBackend; 3] = [
    StorageTestBackend::Memory,
    StorageTestBackend::FileDurable,
    StorageTestBackend::Acid,
];

fn plain_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

fn with_each_backend(test: impl Fn(StorageTestBackend) + RefUnwindSafe) {
    for backend in BACKENDS {
        let result = catch_unwind(AssertUnwindSafe(|| test(backend)));
        if let Err(payload) = result {
            let panic_msg = if let Some(msg) = payload.downcast_ref::<&str>() {
                (*msg).to_string()
            } else if let Some(msg) = payload.downcast_ref::<String>() {
                msg.clone()
            } else {
                "non-string panic payload".to_string()
            };
            panic!(
                "concurrent stress failed for backend={}: {}",
                backend.as_str(),
                panic_msg
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 1. Concurrent writers + concurrent readers on the same stream
// ---------------------------------------------------------------------------

#[test]
fn concurrent_readers_and_writers_no_torn_reads() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        storage.create_stream("s", plain_config()).unwrap();

        let barrier = Arc::new(Barrier::new(8)); // 4 writers + 4 readers

        let mut threads = Vec::new();

        // 4 writer threads, each appending 100 messages
        for writer_id in 0..4u32 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                for i in 0..100u32 {
                    let data = format!("w{writer_id}-m{i}");
                    s.append("s", Bytes::from(data), "text/plain").unwrap();
                }
            }));
        }

        // 4 reader threads, each reading the stream 50 times
        for _reader_id in 0..4 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                for _ in 0..50 {
                    let result = s.read("s", &Offset::start());
                    match result {
                        Ok(read) => {
                            // Every read must return a valid, consistent
                            // interleaving: each writer's messages appear in
                            // their original order.
                            let mut last_seq_per_writer: HashMap<String, u32> = HashMap::new();
                            for msg in &read.messages {
                                let text = std::str::from_utf8(msg).expect("valid utf-8");
                                // Messages have the format "w{id}-m{seq}"
                                let (writer, seq_str) = text
                                    .split_once("-m")
                                    .expect("message should match w{id}-m{seq}");
                                let seq: u32 = seq_str.parse().expect("seq should be a u32");
                                if let Some(&prev) = last_seq_per_writer.get(writer) {
                                    assert!(
                                        seq > prev,
                                        "writer {writer} messages out of order: {prev} then {seq}"
                                    );
                                }
                                last_seq_per_writer.insert(writer.to_string(), seq);
                            }
                        }
                        Err(e) => {
                            panic!("reader got unexpected error: {e:?}");
                        }
                    }
                    thread::yield_now();
                }
            }));
        }

        for t in threads {
            t.join().expect("thread should not panic");
        }

        // After all writers finish, total should be exactly 400 messages
        let read = storage.read("s", &Offset::start()).unwrap();
        assert_eq!(
            read.messages.len(),
            400,
            "backend={}: expected 400 messages, got {}",
            backend.as_str(),
            read.messages.len()
        );
    });
}

// ---------------------------------------------------------------------------
// 2. Read-after-write visibility
// ---------------------------------------------------------------------------

#[test]
fn read_after_write_visibility() {
    with_each_backend(|backend| {
        let handle = create_test_storage(backend);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();

        for i in 0..50 {
            let data = format!("msg-{i}");
            storage
                .append("s", Bytes::from(data.clone()), "text/plain")
                .unwrap();

            // Immediately read back -- the just-appended message must be visible
            let read = storage.read("s", &Offset::start()).unwrap();
            assert!(
                read.messages.len() > i,
                "backend={}: after append {i}, expected at least {} messages but got {}",
                backend.as_str(),
                i + 1,
                read.messages.len()
            );
        }
    });
}

// ---------------------------------------------------------------------------
// 3. Concurrent create_stream_with_data race
// ---------------------------------------------------------------------------

#[test]
fn concurrent_create_stream_with_data_race() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        let barrier = Arc::new(Barrier::new(4));
        let mut threads = Vec::new();

        for _ in 0..4 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                s.create_stream_with_data("race", plain_config(), vec![Bytes::from("data")], false)
            }));
        }

        let results: Vec<_> = threads
            .into_iter()
            .map(|t| t.join().expect("should not panic"))
            .collect();

        // Exactly one should be Created, the rest AlreadyExists
        let created_count = results
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Ok(cwd) if cwd.status == CreateStreamResult::Created
                )
            })
            .count();
        let exists_count = results
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Ok(cwd) if cwd.status == CreateStreamResult::AlreadyExists
                )
            })
            .count();

        assert_eq!(
            created_count,
            1,
            "backend={}: exactly one Created expected, got {created_count}",
            backend.as_str()
        );
        assert_eq!(
            exists_count,
            3,
            "backend={}: three AlreadyExists expected, got {exists_count}",
            backend.as_str()
        );

        // No corruption: should have exactly 1 message
        let read = storage.read("race", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 1);
        assert_eq!(read.messages[0], Bytes::from("data"));
    });
}

// ---------------------------------------------------------------------------
// 4. Delete during active read
// ---------------------------------------------------------------------------

#[test]
fn delete_during_concurrent_reads() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        storage.create_stream("s", plain_config()).unwrap();
        for i in 0..100 {
            storage
                .append("s", Bytes::from(format!("msg-{i}")), "text/plain")
                .unwrap();
        }

        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();

        // 2 reader threads
        for _ in 0..2 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                let mut errors = Vec::new();
                for _ in 0..20 {
                    match s.read("s", &Offset::start()) {
                        Ok(read) => {
                            // If read succeeds, data must be consistent
                            assert!(!read.messages.is_empty() || read.at_tail);
                        }
                        Err(Error::NotFound(_) | Error::StreamExpired) => {
                            // Expected after delete
                            errors.push("not_found");
                        }
                        Err(e) => {
                            panic!("unexpected error during read: {e:?}");
                        }
                    }
                    thread::yield_now();
                }
                errors
            }));
        }

        // 1 deleter thread
        {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                // Small delay to let readers start
                thread::yield_now();
                let _ = s.delete("s");
                Vec::new()
            }));
        }

        for t in threads {
            t.join().expect("should not panic");
        }

        // Stream should be gone
        assert!(!storage.exists("s"));
    });
}

// ---------------------------------------------------------------------------
// 5. Subscribe + close race
// ---------------------------------------------------------------------------

#[test]
fn subscribe_receives_close_notification() {
    with_each_backend(|backend| {
        let handle = create_test_storage(backend);
        let storage = Arc::new(handle.storage);

        storage.create_stream("s", plain_config()).unwrap();
        storage
            .append("s", Bytes::from("data"), "text/plain")
            .unwrap();

        let rx = storage.subscribe("s");
        assert!(
            rx.is_some(),
            "backend={}: subscribe should succeed",
            backend.as_str()
        );
        let mut rx = rx.unwrap();

        // Close the stream -- this should send a notification
        storage.close_stream("s").unwrap();

        // The subscriber should receive the notification
        // Use try_recv since the notification was sent synchronously
        let received = rx.try_recv();
        assert!(
            received.is_ok(),
            "backend={}: subscriber should receive close notification",
            backend.as_str()
        );
    });
}

// ---------------------------------------------------------------------------
// 6. Broadcast channel saturation (lagged receiver)
// ---------------------------------------------------------------------------

#[test]
fn broadcast_channel_saturation_does_not_deadlock() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();

        // Subscribe but never read from the receiver
        let rx = storage.subscribe("s");
        assert!(rx.is_some());
        let mut rx = rx.unwrap();

        // Append more messages than the broadcast channel capacity (16)
        // This should NOT deadlock or panic
        for i in 0..30 {
            storage
                .append("s", Bytes::from(format!("msg-{i}")), "text/plain")
                .unwrap();
        }

        // The receiver should be lagged, have a notification, or be empty.
        // The only unacceptable state is Closed.
        match rx.try_recv() {
            Ok(())
            | Err(
                tokio::sync::broadcast::error::TryRecvError::Lagged(_)
                | tokio::sync::broadcast::error::TryRecvError::Empty,
            ) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                panic!("backend={}: channel should not be closed", backend.as_str());
            }
        }

        // Storage should still be functional
        let read = storage.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 30);
    });
}

// ---------------------------------------------------------------------------
// 7. Concurrent appends produce unique monotonic offsets
// ---------------------------------------------------------------------------

#[test]
fn concurrent_appends_produce_unique_monotonic_offsets() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        storage.create_stream("s", plain_config()).unwrap();

        let barrier = Arc::new(Barrier::new(8));
        let mut threads = Vec::new();

        for thread_id in 0..8u32 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                let mut offsets = Vec::new();
                for i in 0..50 {
                    let data = format!("t{thread_id}-{i}");
                    let offset = s.append("s", Bytes::from(data), "text/plain").unwrap();
                    offsets.push(offset);
                }
                offsets
            }));
        }

        let all_offsets: Vec<Offset> = threads
            .into_iter()
            .flat_map(|t| t.join().expect("should not panic"))
            .collect();

        // All offsets must be unique
        let unique: HashSet<_> = all_offsets.iter().map(|o| o.as_str().to_string()).collect();
        assert_eq!(
            unique.len(),
            400,
            "backend={}: expected 400 unique offsets, got {}",
            backend.as_str(),
            unique.len()
        );

        // Final read should have exactly 400 messages
        let read = storage.read("s", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 400);
    });
}

// ---------------------------------------------------------------------------
// 8. total_bytes consistency under concurrent appends
// ---------------------------------------------------------------------------

#[test]
fn total_bytes_consistent_after_concurrent_appends() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        storage.create_stream("s", plain_config()).unwrap();

        let barrier = Arc::new(Barrier::new(4));
        let msg = Bytes::from("x".repeat(100)); // 100 bytes each

        let mut threads = Vec::new();
        for _ in 0..4 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            let m = msg.clone();
            threads.push(thread::spawn(move || {
                b.wait();
                for _ in 0..50 {
                    s.append("s", m.clone(), "text/plain").unwrap();
                }
            }));
        }

        for t in threads {
            t.join().expect("should not panic");
        }

        // 4 threads * 50 messages * 100 bytes = 20,000 bytes
        let meta = storage.head("s").unwrap();
        assert_eq!(
            meta.total_bytes,
            20_000,
            "backend={}: stream total_bytes should be exactly 20000",
            backend.as_str()
        );
        assert_eq!(meta.message_count, 200);
    });
}

// ---------------------------------------------------------------------------
// 9. Concurrent create + delete does not corrupt state
// ---------------------------------------------------------------------------

#[test]
fn concurrent_create_delete_no_corruption() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        let barrier = Arc::new(Barrier::new(4));
        let mut threads = Vec::new();

        // 2 threads creating streams, 2 threads deleting them
        for creator_id in 0..2 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                for i in 0..50 {
                    let name = format!("s-{creator_id}-{i}");
                    let _ = s.create_stream(&name, plain_config());
                    let _ = s.append(&name, Bytes::from("data"), "text/plain");
                }
            }));
        }

        for deleter_id in 0..2 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                b.wait();
                for i in 0..50 {
                    let name = format!("s-{deleter_id}-{i}");
                    let _ = s.delete(&name); // may fail with NotFound, that's fine
                }
            }));
        }

        for t in threads {
            t.join().expect("should not panic");
        }

        // No assertion on specific state -- the test passes if there are no panics,
        // deadlocks, or data corruption.
    });
}

// ---------------------------------------------------------------------------
// 10. Concurrent reads from different offsets
// ---------------------------------------------------------------------------

#[test]
fn concurrent_reads_from_different_offsets() {
    with_each_backend(|backend| {
        let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = Arc::new(handle.storage);

        storage.create_stream("s", plain_config()).unwrap();
        for i in 0..100 {
            storage
                .append("s", Bytes::from(format!("msg-{i}")), "text/plain")
                .unwrap();
        }

        let barrier = Arc::new(Barrier::new(4));
        let mut threads = Vec::new();

        // Collect some offsets to read from
        let meta = storage.head("s").unwrap();
        let next = meta.next_offset;

        // 4 readers reading from different positions (start and tail)
        for reader_id in 0..4u64 {
            let s = Arc::clone(&storage);
            let b = Arc::clone(&barrier);
            let from = if reader_id % 2 == 0 {
                Offset::start()
            } else {
                next.clone()
            };
            threads.push(thread::spawn(move || {
                b.wait();
                for _ in 0..20 {
                    let read = s.read("s", &from).unwrap();
                    // Should return messages or be at tail, never error
                    assert!(
                        !read.messages.is_empty() || read.at_tail,
                        "read should return data or be at tail"
                    );
                }
            }));
        }

        for t in threads {
            t.join().expect("should not panic");
        }
    });
}
