mod common;

use bytes::Bytes;
use chrono::Utc;
use common::{create_test_storage, create_test_storage_with_limits};
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{
    CreateStreamResult, ProducerAppendResult, Storage, StreamConfig,
};
use std::sync::Arc;
use std::thread;

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerHeaders {
    ProducerHeaders {
        id: id.to_string(),
        epoch,
        seq,
    }
}

fn plain_text_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

storage_backend_tests! {
    mod core {
        use super::*;

        #[test]
        fn create_idempotent_and_config_mismatch() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            let cfg = plain_text_config();

            let created = storage.create_stream("s", cfg.clone()).unwrap();
            assert_eq!(created, CreateStreamResult::Created);

            let idempotent = storage.create_stream("s", cfg).unwrap();
            assert_eq!(idempotent, CreateStreamResult::AlreadyExists);

            assert!(matches!(
                storage.create_stream("s", StreamConfig::new("application/json".to_string())),
                Err(Error::ConfigMismatch)
            ));
        }

        #[test]
        fn append_read_and_offset_monotonicity() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            let o1 = storage.append("s", Bytes::from("a"), "text/plain").unwrap();
            let o2 = storage.append("s", Bytes::from("b"), "text/plain").unwrap();
            assert!(o1 < o2);

            let read = storage.read("s", &Offset::start()).unwrap();
            assert_eq!(read.messages, vec![Bytes::from("a"), Bytes::from("b")]);
            assert!(read.at_tail);
        }

        #[test]
        fn read_from_offset_and_sentinels() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            let o1 = storage
                .append("s", Bytes::from("m1"), "text/plain")
                .unwrap();
            let o2 = storage
                .append("s", Bytes::from("m2"), "text/plain")
                .unwrap();
            let _ = storage
                .append("s", Bytes::from("m3"), "text/plain")
                .unwrap();

            let from_o2 = storage.read("s", &o2).unwrap();
            assert_eq!(from_o2.messages, vec![Bytes::from("m2"), Bytes::from("m3")]);

            let from_o1 = storage.read("s", &o1).unwrap();
            assert_eq!(from_o1.messages.len(), 3);

            let from_now = storage.read("s", &Offset::now()).unwrap();
            assert!(from_now.messages.is_empty());
            assert!(from_now.at_tail);
        }

        #[test]
        fn close_and_content_type_rules() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            assert!(matches!(
                storage.append("s", Bytes::from("x"), "application/json"),
                Err(Error::ContentTypeMismatch { .. })
            ));

            storage
                .append("s", Bytes::from("ok"), "TEXT/PLAIN")
                .unwrap();
            storage.close_stream("s").unwrap();

            assert!(matches!(
                storage.append("s", Bytes::from("x"), "text/plain"),
                Err(Error::StreamClosed)
            ));

            let read = storage.read("s", &Offset::start()).unwrap();
            assert!(read.closed);
        }

        #[test]
        fn delete_and_exists() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();
            storage.append("s", Bytes::from("x"), "text/plain").unwrap();

            assert!(storage.exists("s"));
            storage.delete("s").unwrap();
            assert!(!storage.exists("s"));
            assert_eq!(storage.total_bytes(), 0);
            assert!(matches!(storage.delete("s"), Err(Error::NotFound(_))));
        }
    }

    mod limits_atomicity {
        use super::*;

        #[test]
        fn limits_and_not_found() {
            let handle = create_test_storage_with_limits(BACKEND, 100, 50);
            let storage = &handle.storage;
            let cfg = plain_text_config();
            storage.create_stream("a", cfg.clone()).unwrap();
            storage.create_stream("b", cfg).unwrap();

            storage
                .append("a", Bytes::from(vec![0_u8; 50]), "text/plain")
                .unwrap();
            assert!(matches!(
                storage.append("a", Bytes::from(vec![0_u8; 10]), "text/plain"),
                Err(Error::StreamSizeLimitExceeded)
            ));

            storage
                .append("b", Bytes::from(vec![0_u8; 40]), "text/plain")
                .unwrap();
            assert!(matches!(
                storage.append("b", Bytes::from(vec![0_u8; 20]), "text/plain"),
                Err(Error::MemoryLimitExceeded)
            ));

            assert!(matches!(
                storage.append("missing", Bytes::from("x"), "text/plain"),
                Err(Error::NotFound(_))
            ));
            assert!(matches!(
                storage.read("missing", &Offset::start()),
                Err(Error::NotFound(_))
            ));
            assert!(matches!(storage.head("missing"), Err(Error::NotFound(_))));
            assert!(matches!(
                storage.close_stream("missing"),
                Err(Error::NotFound(_))
            ));
        }

        #[test]
        fn create_with_data_atomicity_and_idempotency() {
            let small = create_test_storage_with_limits(BACKEND, 1024, 8);
            let cfg = plain_text_config();
            let oversized = vec![Bytes::from(vec![0_u8; 9])];
            let err = small
                .storage
                .create_stream_with_data("s", cfg.clone(), oversized, false);
            assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));
            assert!(!small.storage.exists("s"));

            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;

            let closed_cfg = plain_text_config().with_created_closed(true);
            let created = storage
                .create_stream_with_data("closed", closed_cfg, vec![], true)
                .unwrap();
            assert_eq!(created.status, CreateStreamResult::Created);
            assert!(created.closed);

            let r1 = storage
                .create_stream_with_data("idempotent", cfg.clone(), vec![Bytes::from("a")], false)
                .unwrap();
            assert_eq!(r1.status, CreateStreamResult::Created);

            let r2 = storage
                .create_stream_with_data("idempotent", cfg, vec![Bytes::from("b")], false)
                .unwrap();
            assert_eq!(r2.status, CreateStreamResult::AlreadyExists);

            let meta = storage.head("idempotent").unwrap();
            assert_eq!(meta.message_count, 1);
        }

        #[test]
        fn stream_seq_rollback_after_failed_commit() {
            let small = create_test_storage_with_limits(BACKEND, 1024, 8);
            small
                .storage
                .create_stream("s", plain_text_config())
                .unwrap();

            let oversized = vec![Bytes::from(vec![0_u8; 9])];
            let err = small
                .storage
                .batch_append("s", oversized.clone(), "text/plain", Some("s1"));
            assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));

            let retry =
                small
                    .storage
                    .batch_append("s", vec![Bytes::from("ok")], "text/plain", Some("s1"));
            assert!(retry.is_ok());

            let err = small.storage.append_with_producer(
                "s",
                oversized,
                "text/plain",
                &producer("p1", 0, 0),
                false,
                Some("s2"),
            );
            assert!(matches!(err, Err(Error::StreamSizeLimitExceeded)));

            let retry = small.storage.append_with_producer(
                "s",
                vec![Bytes::from("ok")],
                "text/plain",
                &producer("p1", 0, 0),
                false,
                Some("s2"),
            );
            assert!(matches!(retry, Ok(ProducerAppendResult::Accepted { .. })));
        }
    }

    mod producer {
        use super::*;

        #[test]
        fn duplicate_gap_fencing_and_epoch_reset_rules() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            let accepted = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m0")],
                    "text/plain",
                    &producer("p1", 0, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(accepted, ProducerAppendResult::Accepted { .. }));

            let duplicate = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m0")],
                    "text/plain",
                    &producer("p1", 0, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(duplicate, ProducerAppendResult::Duplicate { .. }));

            let gap = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m3")],
                    "text/plain",
                    &producer("p1", 0, 3),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(
                gap,
                Error::SequenceGap {
                    expected: 1,
                    actual: 3
                }
            ));

            let new_epoch = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("m")],
                    "text/plain",
                    &producer("p1", 2, 0),
                    false,
                    None,
                )
                .unwrap();
            assert!(matches!(new_epoch, ProducerAppendResult::Accepted { .. }));

            let stale_epoch = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("zombie")],
                    "text/plain",
                    &producer("p1", 1, 0),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(stale_epoch, Error::EpochFenced { .. }));

            let nonzero_after_bump = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("bad")],
                    "text/plain",
                    &producer("p1", 3, 2),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(nonzero_after_bump, Error::InvalidProducerState(_)));
        }

        #[test]
        fn multi_producer_independence_and_closed_precedence() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;
            storage.create_stream("s", plain_text_config()).unwrap();

            for producer_id in ["p1", "p2"] {
                let result = storage
                    .append_with_producer(
                        "s",
                        vec![Bytes::from(format!("{producer_id}-0"))],
                        "text/plain",
                        &producer(producer_id, 0, 0),
                        false,
                        None,
                    )
                    .unwrap();
                assert!(matches!(result, ProducerAppendResult::Accepted { .. }));
            }

            let closed = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("close")],
                    "text/plain",
                    &producer("p1", 0, 1),
                    true,
                    None,
                )
                .unwrap();
            assert!(matches!(
                closed,
                ProducerAppendResult::Accepted { closed: true, .. }
            ));

            let closed_precedence = storage
                .append_with_producer(
                    "s",
                    vec![Bytes::from("later")],
                    "text/plain",
                    &producer("p1", 0, 10),
                    false,
                    None,
                )
                .unwrap_err();
            assert!(matches!(closed_precedence, Error::StreamClosed));
        }
    }

    mod fork_lifecycle {
        use super::*;

        #[test]
        fn fork_idempotency_requires_matching_source_and_offset() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;

            storage
                .create_stream("source-a", plain_text_config())
                .unwrap();
            storage
                .append("source-a", Bytes::from("a1"), "text/plain")
                .unwrap();
            storage
                .create_stream("source-b", plain_text_config())
                .unwrap();
            storage
                .append("source-b", Bytes::from("b1"), "text/plain")
                .unwrap();

            let created = storage
                .create_fork("fork", "source-a", None, plain_text_config())
                .unwrap();
            assert_eq!(created, CreateStreamResult::Created);

            let idempotent = storage
                .create_fork("fork", "source-a", None, plain_text_config())
                .unwrap();
            assert_eq!(idempotent, CreateStreamResult::AlreadyExists);

            let wrong_source = storage
                .create_fork("fork", "source-b", None, plain_text_config())
                .unwrap_err();
            assert!(matches!(wrong_source, Error::ConfigMismatch));

            let different_offset = storage
                .create_fork(
                    "fork",
                    "source-a",
                    Some(&Offset::start()),
                    plain_text_config(),
                )
                .unwrap_err();
            assert!(matches!(different_offset, Error::ConfigMismatch));
        }

        #[test]
        fn expired_parent_with_descendants_becomes_tombstone_and_blocks_recreation() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;

            let expires_at = Utc::now() + chrono::Duration::milliseconds(300);
            let config = plain_text_config().with_expires_at(expires_at);
            storage.create_stream("source", config).unwrap();
            storage
                .append("source", Bytes::from("baseline"), "text/plain")
                .unwrap();

            let fork_created = storage
                .create_fork("fork", "source", None, plain_text_config().with_ttl(10))
                .unwrap();
            assert_eq!(fork_created, CreateStreamResult::Created);

            std::thread::sleep(std::time::Duration::from_millis(500));
            let removed = storage.cleanup_expired_streams();
            assert_eq!(removed, 1);

            let source_head = storage.head("source").unwrap_err();
            assert!(matches!(source_head, Error::StreamGone(_)));

            let recreate = storage
                .create_stream("source", plain_text_config())
                .unwrap_err();
            assert!(matches!(recreate, Error::StreamPathBlocked(_)));

            let fork_read = storage.read("fork", &Offset::start()).unwrap();
            assert_eq!(fork_read.messages, vec![Bytes::from("baseline")]);
        }

        #[test]
        fn deleting_last_descendant_cascades_cleanup() {
            let handle = create_test_storage(BACKEND);
            let storage = &handle.storage;

            storage
                .create_stream("source", plain_text_config())
                .unwrap();
            storage
                .append("source", Bytes::from("baseline"), "text/plain")
                .unwrap();
            storage
                .create_fork("fork", "source", None, plain_text_config())
                .unwrap();

            storage.delete("source").unwrap();
            assert!(matches!(storage.head("source"), Err(Error::StreamGone(_))));
            assert!(matches!(
                storage.create_stream("source", plain_text_config()),
                Err(Error::StreamPathBlocked(_))
            ));

            storage.delete("fork").unwrap();
            assert!(!storage.exists("fork"));

            let recreated = storage
                .create_stream("source", plain_text_config())
                .unwrap();
            assert_eq!(recreated, CreateStreamResult::Created);

            let source_read = storage.read("source", &Offset::start()).unwrap();
            assert!(source_read.messages.is_empty());
        }
    }

    mod concurrency {
        use super::*;

        #[test]
        fn concurrent_append_offsets_unique() {
            let handle = create_test_storage(BACKEND);
            let storage = Arc::new(handle.storage);
            storage.create_stream("s", plain_text_config()).unwrap();

            let mut handles = Vec::new();
            for worker in 0..4 {
                let storage = Arc::clone(&storage);
                handles.push(thread::spawn(move || {
                    let mut offsets = Vec::new();
                    for i in 0..20 {
                        let offset = storage
                            .append(
                                "s",
                                Bytes::from(format!("worker-{worker}-{i}")),
                                "text/plain",
                            )
                            .unwrap();
                        offsets.push(offset);
                    }
                    offsets
                }));
            }

            let mut all = Vec::new();
            for handle in handles {
                all.extend(handle.join().unwrap());
            }

            all.sort();
            for idx in 1..all.len() {
                assert_ne!(all[idx - 1], all[idx]);
                assert!(all[idx - 1] < all[idx]);
            }

            let meta = storage.head("s").unwrap();
            assert_eq!(meta.message_count, 80);
        }
    }
}

mod extended_contract {
    use super::*;
    storage_backend_tests! {
        mod extended_forks {
            use super::*;
            use durable_streams_server::storage::ForkOptions;

            /// PROTOCOL.md §4.2: partial fork creation is atomic and keeps writer state fresh.
            #[test]
            fn test_partial_fork_failure_has_no_visible_stream_or_reserved_bytes() {
                let handle = create_test_storage_with_limits(BACKEND, 1024, 5);
                let storage = &handle.storage;
                storage.create_stream_with_data("source", plain_text_config(), vec![Bytes::from("hello")], false).unwrap();
                let before = storage.total_bytes();
                let result = storage.create_fork_with_options("fork", "source", Some(&Offset::new(0,0)), plain_text_config(), ForkOptions {
                    sub_offset: 3, initial_body: Bytes::from("XYZ"), ..ForkOptions::default()
                });
                assert!(matches!(result, Err(Error::StreamSizeLimitExceeded)));
                assert!(!storage.exists("fork"));
                assert_eq!(storage.total_bytes(), before);
                storage.delete("source").unwrap();
                assert_eq!(storage.total_bytes(), 0);
            }

            /// PROTOCOL.md §4.2, §8: nested fork reads respect all ancestor bounds.
            #[test]
            fn test_nested_fork_before_parent_boundary_and_resume() {
                let handle = create_test_storage(BACKEND);
                let storage = &handle.storage;
                storage.create_stream("source", plain_text_config()).unwrap();
                storage.append("source", Bytes::from("a"), "text/plain").unwrap();
                let anchor = storage.head("source").unwrap().next_offset;
                storage.append("source", Bytes::from("b"), "text/plain").unwrap();
                storage.create_fork("parent", "source", None, plain_text_config()).unwrap();
                storage.append("parent", Bytes::from("c"), "text/plain").unwrap();
                storage.create_fork("child", "parent", Some(&anchor), plain_text_config()).unwrap();
                assert_eq!(storage.read("child", &Offset::start()).unwrap().messages, vec![Bytes::from("a")]);
                storage.append("child", Bytes::from("d"), "text/plain").unwrap();
                assert_eq!(storage.read("child", &anchor).unwrap().messages, vec![Bytes::from("d")]);
            }

            /// Subscription control data is private and independent of stream quotas/listing.
            #[test]
            fn test_subscription_snapshot_replaces_state_without_creating_streams() {
                let handle = create_test_storage(BACKEND);
                let storage = &handle.storage;
                assert!(storage.load_subscription_state().unwrap().is_none());
                storage.save_subscription_state(b"one").unwrap();
                storage.save_subscription_state(b"two").unwrap();
                assert_eq!(storage.load_subscription_state().unwrap(), Some(b"two".to_vec()));
                assert!(storage.list_streams().unwrap().is_empty());
            }
        }
    }
}
