//! Resource management and cleanup tests.
//!
//! These validate that expired streams are reclaimed, producer state TTL works,
//! memory accounting is consistent after failures, and the new proactive
//! `cleanup_expired_streams` method works correctly.

mod common;

use bytes::Bytes;
use chrono::Utc;
use common::{create_test_storage, create_test_storage_with_limits};
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{Storage, StreamConfig};

fn plain_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
    ProducerHeaders {
        id: id.to_string(),
        epoch,
        seq,
    }
}

storage_backend_tests! {
    // ---------------------------------------------------------------------------
    // 1. Expired stream cleanup on access
    // ---------------------------------------------------------------------------

    #[test]
    fn expired_stream_returns_not_found_on_read() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);
        storage.create_stream("s", config).unwrap();
        storage
            .append("s", Bytes::from("data"), "text/plain")
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2500));

        assert!(matches!(
            storage.read("s", &Offset::start()),
            Err(Error::StreamExpired)
        ));
    }

    #[test]
    fn expired_stream_returns_not_found_on_head() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);
        storage.create_stream("s", config).unwrap();
        storage
            .append("s", Bytes::from("data"), "text/plain")
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2500));

        assert!(matches!(storage.head("s"), Err(Error::StreamExpired)));
    }

    #[test]
    fn expired_stream_returns_error_on_append() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);
        storage.create_stream("s", config).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2500));

        assert!(matches!(
            storage.append("s", Bytes::from("late"), "text/plain"),
            Err(Error::StreamExpired)
        ));
    }

    // ---------------------------------------------------------------------------
    // 2. Expired stream re-creation
    // ---------------------------------------------------------------------------

    #[test]
    fn expired_stream_can_be_recreated() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);
        storage.create_stream("s", config).unwrap();
        storage
            .append("s", Bytes::from("old-data"), "text/plain")
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2500));

        // Recreate with a fresh config (no expiry)
        let fresh_config = plain_config();
        let result = storage.create_stream("s", fresh_config);
        assert!(result.is_ok(), "expected recreate to succeed, got {result:?}");

        // Old data should be gone
        let read = storage.read("s", &Offset::start()).unwrap();
        assert!(read.messages.is_empty());
    }

    // ---------------------------------------------------------------------------
    // 3. Proactive cleanup_expired_streams
    // ---------------------------------------------------------------------------

    #[test]
    fn cleanup_expired_streams_removes_expired() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);

        storage.create_stream("exp-1", config.clone()).unwrap();
        storage
            .append("exp-1", Bytes::from("data1"), "text/plain")
            .unwrap();
        storage.create_stream("exp-2", config).unwrap();
        storage
            .append("exp-2", Bytes::from("data2"), "text/plain")
            .unwrap();

        // Also create a non-expiring stream
        storage.create_stream("keep", plain_config()).unwrap();
        storage
            .append("keep", Bytes::from("keep-data"), "text/plain")
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2500));

        let removed = storage.cleanup_expired_streams();
        assert_eq!(removed, 2);

        // Expired streams should be gone
        assert!(!storage.exists("exp-1"));
        assert!(!storage.exists("exp-2"));

        // Non-expiring stream should still be there
        assert!(storage.exists("keep"));
        let read = storage.read("keep", &Offset::start()).unwrap();
        assert_eq!(read.messages.len(), 1);
    }

    #[test]
    fn cleanup_expired_streams_returns_zero_when_none_expired() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();
        storage
            .append("s", Bytes::from("data"), "text/plain")
            .unwrap();

        let removed = storage.cleanup_expired_streams();
        assert_eq!(removed, 0);

        // Stream should still exist
        assert!(storage.exists("s"));
    }

    #[test]
    fn cleanup_expired_streams_reclaims_bytes() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);

        storage.create_stream("exp", config).unwrap();
        storage
            .append("exp", Bytes::from("x".repeat(100)), "text/plain")
            .unwrap();

        let before = handle.storage.total_bytes();
        assert_eq!(before, 100);

        std::thread::sleep(std::time::Duration::from_millis(2500));

        storage.cleanup_expired_streams();

        let after = handle.storage.total_bytes();
        assert_eq!(after, 0);
    }

    // ---------------------------------------------------------------------------
    // 4. Memory limit accounting after failed batch
    // ---------------------------------------------------------------------------

    #[test]
    fn memory_limit_rollback_on_failed_append() {
        let handle = create_test_storage_with_limits(BACKEND, 100, 50);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();
        storage
            .append("s", Bytes::from(vec![0u8; 40]), "text/plain")
            .unwrap();

        let before = handle.storage.total_bytes();
        assert_eq!(before, 40);

        // This should fail (40 + 20 > 50 per-stream limit)
        let result = storage.append("s", Bytes::from(vec![0u8; 20]), "text/plain");
        assert!(result.is_err());

        // total_bytes should be unchanged
        let after = handle.storage.total_bytes();
        assert_eq!(after, before);
    }

    #[test]
    fn global_memory_limit_rollback_on_failed_append() {
        let handle = create_test_storage_with_limits(BACKEND, 100, 80);
        let storage = &handle.storage;

        storage.create_stream("a", plain_config()).unwrap();
        storage.create_stream("b", plain_config()).unwrap();
        storage
            .append("a", Bytes::from(vec![0u8; 60]), "text/plain")
            .unwrap();

        // This should fail (60 + 50 > 100 global limit)
        let result = storage.append("b", Bytes::from(vec![0u8; 50]), "text/plain");
        assert!(result.is_err());

        // total_bytes should still be 60
        let after = handle.storage.total_bytes();
        assert_eq!(after, 60);
    }

    // ---------------------------------------------------------------------------
    // 5. Delete reclaims bytes
    // ---------------------------------------------------------------------------

    #[test]
    fn delete_reclaims_total_bytes() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();
        storage
            .append("s", Bytes::from("hello"), "text/plain")
            .unwrap();

        assert_eq!(handle.storage.total_bytes(), 5);

        storage.delete("s").unwrap();

        assert_eq!(handle.storage.total_bytes(), 0);
    }

    #[test]
    fn delete_allows_reuse_of_capacity() {
        let handle = create_test_storage_with_limits(BACKEND, 100, 80);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();
        storage
            .append("s", Bytes::from(vec![0u8; 80]), "text/plain")
            .unwrap();

        // Global is at 80, can't add more
        storage.create_stream("s2", plain_config()).unwrap();
        let result = storage.append("s2", Bytes::from(vec![0u8; 30]), "text/plain");
        assert!(result.is_err());

        // Delete first stream, freeing capacity
        storage.delete("s").unwrap();

        // Now should succeed
        let result = storage.append("s2", Bytes::from(vec![0u8; 30]), "text/plain");
        assert!(result.is_ok(), "expected append to succeed, got {result:?}");
    }

    // ---------------------------------------------------------------------------
    // 6. Producer state cleanup (7-day TTL)
    // ---------------------------------------------------------------------------

    #[test]
    fn producer_append_still_works_after_many_operations() {
        // This test verifies that producer state management doesn't leak over
        // many sequential operations. While we can't directly test the 7-day TTL
        // without time manipulation, we verify the cleanup path runs correctly.
        let handle = create_test_storage_with_limits(BACKEND, 10 * 1024 * 1024, 10 * 1024 * 1024);
        let storage = &handle.storage;

        storage.create_stream("s", plain_config()).unwrap();

        // Use many different producers
        for i in 0..50 {
            let pid = format!("producer-{i}");
            let result = storage.append_with_producer(
                "s",
                vec![Bytes::from(format!("msg-{i}"))],
                "text/plain",
                &producer(&pid, 0, 0),
                false,
                None,
            );
            assert!(result.is_ok(), "producer {pid} should succeed, got {result:?}");
        }

        let meta = storage.head("s").unwrap();
        assert_eq!(meta.message_count, 50);
    }

    // ---------------------------------------------------------------------------
    // 7. Cleanup is idempotent
    // ---------------------------------------------------------------------------

    #[test]
    fn cleanup_expired_streams_is_idempotent() {
        let handle = create_test_storage(BACKEND);
        let storage = &handle.storage;

        let expires = Utc::now() + chrono::Duration::seconds(2);
        let config = plain_config().with_expires_at(expires);
        storage.create_stream("exp", config).unwrap();
        storage
            .append("exp", Bytes::from("data"), "text/plain")
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(2500));

        let first = storage.cleanup_expired_streams();
        assert_eq!(first, 1);

        // Second call should find nothing to clean
        let second = storage.cleanup_expired_streams();
        assert_eq!(second, 0);
    }
}
