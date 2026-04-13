//! Property-based tests for storage backends using proptest.
//!
//! These exercise random operation sequences against all backends to catch
//! edge cases that hand-written tests miss. Based on the property-based
//! testing recommendations in `IMPLEMENTATION_TESTING.md`.
#![allow(
    clippy::too_many_lines,
    clippy::option_if_let_else,
    clippy::redundant_else,
    clippy::cast_possible_truncation,
    clippy::missing_docs_in_private_items,
    clippy::doc_markdown
)]

mod common;

use bytes::Bytes;
use common::{StorageTestBackend, create_test_storage_with_limits};
use durable_streams_server::protocol::error::Error;
use durable_streams_server::protocol::offset::Offset;
use durable_streams_server::protocol::producer::ProducerHeaders;
use durable_streams_server::storage::{CreateStreamResult, Storage, StreamConfig};
use proptest::prelude::*;

const BACKENDS: [StorageTestBackend; 3] = [
    StorageTestBackend::Memory,
    StorageTestBackend::FileDurable,
    StorageTestBackend::Acid,
];

fn plain_config() -> StreamConfig {
    StreamConfig::new("text/plain".to_string())
}

// ---------------------------------------------------------------------------
// Operation enum for random sequences
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Create { stream_idx: usize },
    Append { stream_idx: usize, data: Vec<u8> },
    BatchAppend { stream_idx: usize, messages: Vec<Vec<u8>> },
    Read { stream_idx: usize },
    Close { stream_idx: usize },
    Delete { stream_idx: usize },
    Head { stream_idx: usize },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Create
        (0..4usize).prop_map(|stream_idx| Op::Create { stream_idx }),
        // Append with small payloads
        (0..4usize, prop::collection::vec(any::<u8>(), 0..200))
            .prop_map(|(stream_idx, data)| Op::Append { stream_idx, data }),
        // Batch append (1-5 messages, small payloads)
        (
            0..4usize,
            prop::collection::vec(prop::collection::vec(any::<u8>(), 1..100), 1..5)
        )
            .prop_map(|(stream_idx, messages)| Op::BatchAppend {
                stream_idx,
                messages,
            }),
        // Read
        (0..4usize).prop_map(|stream_idx| Op::Read { stream_idx }),
        // Close
        (0..4usize).prop_map(|stream_idx| Op::Close { stream_idx }),
        // Delete
        (0..4usize).prop_map(|stream_idx| Op::Delete { stream_idx }),
        // Head
        (0..4usize).prop_map(|stream_idx| Op::Head { stream_idx }),
    ]
}

// ---------------------------------------------------------------------------
// 1. Random operation sequences maintain invariants
// ---------------------------------------------------------------------------

fn run_random_ops(backend: StorageTestBackend, ops: Vec<Op>) {
    let handle = create_test_storage_with_limits(backend, 1024 * 1024, 512 * 1024);
    let storage = &handle.storage;
    let stream_names: Vec<String> = (0..4).map(|i| format!("s-{i}")).collect();

    // Track which streams are created and not deleted
    let mut created = [false; 4];
    let mut closed = [false; 4];
    let mut message_counts = [0u64; 4];

    for op in ops {
        let idx = match &op {
            Op::Create { stream_idx }
            | Op::Append { stream_idx, .. }
            | Op::BatchAppend { stream_idx, .. }
            | Op::Read { stream_idx }
            | Op::Close { stream_idx }
            | Op::Delete { stream_idx }
            | Op::Head { stream_idx } => *stream_idx,
        };
        let name = &stream_names[idx];

        match op {
            Op::Create { .. } => {
                match storage.create_stream(name, plain_config()) {
                    Ok(CreateStreamResult::Created) => {
                        created[idx] = true;
                        closed[idx] = false;
                        message_counts[idx] = 0;
                    }
                    Ok(CreateStreamResult::AlreadyExists) => {
                        assert!(created[idx], "AlreadyExists but not tracked as created");
                    }
                    Err(_) => {}
                }
            }
            Op::Append { data, .. } => {
                match storage.append(name, Bytes::from(data), "text/plain") {
                    Ok(offset) => {
                        assert!(created[idx], "append succeeded on uncreated stream");
                        assert!(!closed[idx], "append succeeded on closed stream");
                        // Offset should be concrete
                        assert!(
                            offset.parse_components().is_some(),
                            "append returned non-concrete offset"
                        );
                        message_counts[idx] += 1;
                    }
                    Err(Error::NotFound(_)) => {
                        assert!(!created[idx], "NotFound but stream was created");
                    }
                    Err(Error::StreamClosed) => {
                        assert!(closed[idx], "StreamClosed but not tracked as closed");
                    }
                    Err(Error::MemoryLimitExceeded | Error::StreamSizeLimitExceeded) => {}
                    Err(e) => panic!("unexpected append error: {e:?}"),
                }
            }
            Op::BatchAppend { messages, .. } => {
                let msgs: Vec<Bytes> = messages.into_iter().map(Bytes::from).collect();
                let count = msgs.len() as u64;
                match storage.batch_append(name, msgs, "text/plain", None) {
                    Ok(_) => {
                        assert!(created[idx]);
                        assert!(!closed[idx]);
                        message_counts[idx] += count;
                    }
                    Err(Error::NotFound(_)) => {
                        assert!(!created[idx]);
                    }
                    Err(Error::StreamClosed) => {
                        assert!(closed[idx]);
                    }
                    Err(Error::MemoryLimitExceeded | Error::StreamSizeLimitExceeded) => {}
                    Err(e) => panic!("unexpected batch_append error: {e:?}"),
                }
            }
            Op::Read { .. } => {
                match storage.read(name, &Offset::start()) {
                    Ok(read) => {
                        assert!(created[idx]);
                        // Messages should be <= our tracked count (could be
                        // less if stream was deleted and recreated)
                        assert!(
                            read.messages.len() as u64 <= message_counts[idx],
                            "read returned more messages than appended"
                        );
                        // Verify offset monotonicity in returned messages
                        // (implicitly checked by the storage returning them in order)
                    }
                    Err(Error::NotFound(_) | Error::StreamExpired) => {}
                    Err(e) => panic!("unexpected read error: {e:?}"),
                }
            }
            Op::Close { .. } => {
                match storage.close_stream(name) {
                    Ok(()) => {
                        assert!(created[idx]);
                        closed[idx] = true;
                    }
                    Err(Error::NotFound(_) | Error::StreamExpired) => {}
                    Err(e) => panic!("unexpected close error: {e:?}"),
                }
            }
            Op::Delete { .. } => {
                match storage.delete(name) {
                    Ok(()) => {
                        created[idx] = false;
                        closed[idx] = false;
                        message_counts[idx] = 0;
                    }
                    Err(Error::NotFound(_)) => {}
                    Err(e) => panic!("unexpected delete error: {e:?}"),
                }
            }
            Op::Head { .. } => {
                match storage.head(name) {
                    Ok(meta) => {
                        assert!(created[idx]);
                        assert_eq!(
                            meta.closed, closed[idx],
                            "head.closed doesn't match tracked state"
                        );
                        assert_eq!(
                            meta.message_count, message_counts[idx],
                            "head.message_count doesn't match tracked state"
                        );
                    }
                    Err(Error::NotFound(_) | Error::StreamExpired) => {}
                    Err(e) => panic!("unexpected head error: {e:?}"),
                }
            }
        }
    }

    // Final invariant: total_bytes should be non-negative (it's u64, so just
    // verify it's consistent with what we can observe)
    let total = handle.storage.total_bytes();
    let mut observable_bytes = 0u64;
    for (idx, name) in stream_names.iter().enumerate() {
        if created[idx] && let Ok(meta) = storage.head(name) {
            observable_bytes += meta.total_bytes;
        }
    }
    assert_eq!(
        total, observable_bytes,
        "total_bytes mismatch: tracked={total}, observable={observable_bytes}"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn random_ops_memory(ops in prop::collection::vec(op_strategy(), 1..80)) {
        run_random_ops(StorageTestBackend::Memory, ops);
    }

    #[test]
    fn random_ops_file(ops in prop::collection::vec(op_strategy(), 1..80)) {
        run_random_ops(StorageTestBackend::FileDurable, ops);
    }

    #[test]
    fn random_ops_acid(ops in prop::collection::vec(op_strategy(), 1..80)) {
        run_random_ops(StorageTestBackend::Acid, ops);
    }
}

// ---------------------------------------------------------------------------
// 2. Offset boundary tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn offset_new_never_panics(read_seq: u64, byte_offset: u64) {
        let offset = Offset::new(read_seq, byte_offset);
        // Should always produce a concrete offset with valid components
        let (rs, bo) = offset.parse_components().unwrap();
        prop_assert_eq!(rs, read_seq);
        prop_assert_eq!(bo, byte_offset);
    }

    #[test]
    fn offset_ordering_is_consistent(
        a_rs: u64, a_bo: u64,
        b_rs: u64, b_bo: u64
    ) {
        let a = Offset::new(a_rs, a_bo);
        let b = Offset::new(b_rs, b_bo);

        // Ordering should match tuple ordering
        let expected = (a_rs, a_bo).cmp(&(b_rs, b_bo));
        prop_assert_eq!(a.cmp(&b), expected);

        // Reflexive
        prop_assert_eq!(a.cmp(&a), std::cmp::Ordering::Equal);

        // Anti-symmetric
        if a.cmp(&b) == std::cmp::Ordering::Less {
            prop_assert_eq!(b.cmp(&a), std::cmp::Ordering::Greater);
        }
    }

    #[test]
    fn offset_equality_is_consistent_with_hash(
        a_rs: u64, a_bo: u64,
        b_rs: u64, b_bo: u64
    ) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = Offset::new(a_rs, a_bo);
        let b = Offset::new(b_rs, b_bo);

        if a == b {
            let mut ha = DefaultHasher::new();
            a.hash(&mut ha);
            let mut hb = DefaultHasher::new();
            b.hash(&mut hb);
            prop_assert_eq!(ha.finish(), hb.finish(), "equal offsets must have equal hashes");
        }
    }

    #[test]
    fn offset_roundtrip_through_string(read_seq: u64, byte_offset: u64) {
        let original = Offset::new(read_seq, byte_offset);
        let s = original.as_str();
        let parsed: Offset = s.parse().unwrap();
        prop_assert_eq!(original, parsed);
    }
}

// ---------------------------------------------------------------------------
// 3. Offset boundary values near u64::MAX
// ---------------------------------------------------------------------------

#[test]
fn offset_max_values() {
    let max = Offset::new(u64::MAX, u64::MAX);
    assert_eq!(max.as_str(), "ffffffffffffffff_ffffffffffffffff");
    let (rs, bo) = max.parse_components().unwrap();
    assert_eq!(rs, u64::MAX);
    assert_eq!(bo, u64::MAX);

    let near_max = Offset::new(u64::MAX - 1, u64::MAX - 1);
    assert!(near_max < max);

    // Verify ordering at boundary
    let a = Offset::new(u64::MAX, 0);
    let b = Offset::new(u64::MAX, 1);
    assert!(a < b);

    let c = Offset::new(u64::MAX - 1, u64::MAX);
    let d = Offset::new(u64::MAX, 0);
    assert!(c < d);
}

// ---------------------------------------------------------------------------
// 4. Producer state machine fuzzing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ProducerOp {
    /// Normal next-seq append
    NextSeq,
    /// Duplicate of last seq
    Duplicate,
    /// Bump epoch (reset to seq 0)
    BumpEpoch,
    /// Skip a sequence number (should cause SequenceGap)
    SkipSeq,
    /// Use an old epoch (should cause EpochFenced)
    OldEpoch,
}

fn producer_op_strategy() -> impl Strategy<Value = ProducerOp> {
    prop_oneof![
        3 => Just(ProducerOp::NextSeq),
        2 => Just(ProducerOp::Duplicate),
        1 => Just(ProducerOp::BumpEpoch),
        1 => Just(ProducerOp::SkipSeq),
        1 => Just(ProducerOp::OldEpoch),
    ]
}

fn run_producer_state_machine(backend: StorageTestBackend, ops: Vec<ProducerOp>) {
    let handle = create_test_storage_with_limits(backend, 10 * 1024 * 1024, 10 * 1024 * 1024);
    let storage = &handle.storage;

    storage.create_stream("s", plain_config()).unwrap();

    let mut current_epoch: u64 = 0;
    let mut current_seq: u64 = 0;
    let mut accepted_count: u64 = 0;
    let mut first_append = true;

    for op in ops {
        let (epoch, seq) = match op {
            ProducerOp::NextSeq => {
                if first_append {
                    (0, 0)
                } else {
                    (current_epoch, current_seq + 1)
                }
            }
            ProducerOp::Duplicate => {
                if first_append {
                    // Can't duplicate before first append; do a normal one
                    (0, 0)
                } else {
                    (current_epoch, current_seq)
                }
            }
            ProducerOp::BumpEpoch => (current_epoch + 1, 0),
            ProducerOp::SkipSeq => {
                if first_append {
                    (0, 5) // Skip from expected 0
                } else {
                    (current_epoch, current_seq + 5)
                }
            }
            ProducerOp::OldEpoch => {
                if current_epoch == 0 {
                    // Can't go below 0; skip this op
                    continue;
                } else {
                    (current_epoch - 1, 0)
                }
            }
        };

        let producer = ProducerHeaders {
            id: "p1".to_string(),
            epoch,
            seq,
        };

        let result = storage.append_with_producer(
            "s",
            vec![Bytes::from("x")],
            "text/plain",
            &producer,
            false,
            None,
        );

        match &op {
            ProducerOp::NextSeq => {
                // Should be accepted (or duplicate if it was the first and we replayed)
                match &result {
                    Ok(durable_streams_server::storage::ProducerAppendResult::Accepted { .. }) => {
                        current_epoch = epoch;
                        current_seq = seq;
                        accepted_count += 1;
                        first_append = false;
                    }
                    Ok(durable_streams_server::storage::ProducerAppendResult::Duplicate {
                        ..
                    }) => {
                        // Can happen if this is effectively a duplicate
                    }
                    Err(e) => panic!("NextSeq should succeed, got {e:?}"),
                }
            }
            ProducerOp::Duplicate => {
                if first_append {
                    // Was converted to a normal append
                    if let Ok(
                        durable_streams_server::storage::ProducerAppendResult::Accepted { .. },
                    ) = &result
                    {
                        current_epoch = epoch;
                        current_seq = seq;
                        accepted_count += 1;
                        first_append = false;
                    }
                } else {
                    // Should be duplicate
                    assert!(
                        matches!(
                            result,
                            Ok(durable_streams_server::storage::ProducerAppendResult::Duplicate {
                                ..
                            })
                        ),
                        "Duplicate should return Duplicate, got {result:?}"
                    );
                }
            }
            ProducerOp::BumpEpoch => {
                match &result {
                    Ok(durable_streams_server::storage::ProducerAppendResult::Accepted { .. }) => {
                        current_epoch = epoch;
                        current_seq = seq;
                        accepted_count += 1;
                        first_append = false;
                    }
                    Err(e) => panic!("BumpEpoch with seq=0 should succeed, got {e:?}"),
                    _ => panic!("BumpEpoch unexpected result: {result:?}"),
                }
            }
            ProducerOp::SkipSeq => {
                if first_append && seq == 5 {
                    // seq=5 when expected=0 → SequenceGap
                    assert!(
                        matches!(result, Err(Error::SequenceGap { .. })),
                        "SkipSeq should return SequenceGap, got {result:?}"
                    );
                } else if !first_append {
                    assert!(
                        matches!(result, Err(Error::SequenceGap { .. })),
                        "SkipSeq should return SequenceGap, got {result:?}"
                    );
                }
            }
            ProducerOp::OldEpoch => {
                assert!(
                    matches!(result, Err(Error::EpochFenced { .. })),
                    "OldEpoch should return EpochFenced, got {result:?}"
                );
            }
        }
    }

    // Verify final state
    let meta = storage.head("s").unwrap();
    assert_eq!(
        meta.message_count, accepted_count,
        "message_count should match accepted appends"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn producer_state_machine_memory(
        ops in prop::collection::vec(producer_op_strategy(), 1..40)
    ) {
        run_producer_state_machine(StorageTestBackend::Memory, ops);
    }

    #[test]
    fn producer_state_machine_file(
        ops in prop::collection::vec(producer_op_strategy(), 1..40)
    ) {
        run_producer_state_machine(StorageTestBackend::FileDurable, ops);
    }

    #[test]
    fn producer_state_machine_acid(
        ops in prop::collection::vec(producer_op_strategy(), 1..40)
    ) {
        run_producer_state_machine(StorageTestBackend::Acid, ops);
    }
}

// ---------------------------------------------------------------------------
// 5. Batch atomicity
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn batch_append_is_atomic(
        batch_sizes in prop::collection::vec(1..20usize, 1..10),
        msg_size in 1..500usize
    ) {
        for backend in BACKENDS {
            let handle = create_test_storage_with_limits(
                backend, 10 * 1024 * 1024, 10 * 1024 * 1024,
            );
            let storage = &handle.storage;

            storage.create_stream("s", plain_config()).unwrap();

            let mut total_messages = 0u64;
            for batch_size in &batch_sizes {
                let msgs: Vec<Bytes> = (0..*batch_size)
                    .map(|i| Bytes::from(vec![i as u8; msg_size]))
                    .collect();
                let count = msgs.len() as u64;

                match storage.batch_append("s", msgs, "text/plain", None) {
                    Ok(_) => {
                        total_messages += count;
                    }
                    Err(Error::MemoryLimitExceeded | Error::StreamSizeLimitExceeded) => {
                        // Batch rejected entirely; count unchanged
                    }
                    Err(e) => panic!("unexpected batch error on {}: {e:?}", backend.as_str()),
                }
            }

            // Verify atomicity: message count must equal sum of accepted batches
            let meta = storage.head("s").unwrap();
            prop_assert_eq!(
                meta.message_count,
                total_messages,
                "batch atomicity violated on backend={}",
                backend.as_str()
            );
        }
    }
}
