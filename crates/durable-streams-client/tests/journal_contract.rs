//! Contract-style tests for client-side JSON journal semantics.

mod common;

use common::{append_json_values, client, create_json_stream, spawn_test_server};
use durable_streams_client::{
    JournalDirection, JournalStreamIdentity, JsonJournal, ProducerJournalProgress, ReadReplica,
    ReadRequest,
};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::Write;
use tempfile::tempdir;

#[test]
fn journal_replay_rebuilds_the_same_local_state_after_restart() {
    let dir = tempdir().expect("tempdir");
    let journal_path = dir.path().join("orders.jsonl");
    let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");

    {
        let mut journal = JsonJournal::open(&journal_path, stream.clone()).expect("open journal");
        journal
            .append_values(
                JournalDirection::Inbound,
                vec![json!({"id": 1}), json!({"id": 2})],
                Some("2".to_string()),
                None,
            )
            .expect("append");
    }

    let reopened = JsonJournal::open(&journal_path, stream).expect("reopen journal");
    assert_eq!(
        reopened.values().cloned().collect::<Vec<_>>(),
        vec![json!({"id": 1}), json!({"id": 2})]
    );
    assert_eq!(reopened.resume_offset(), Some("2"));
}

#[test]
fn trailing_partial_line_is_ignored_without_losing_earlier_records() {
    let dir = tempdir().expect("tempdir");
    let journal_path = dir.path().join("orders.jsonl");
    let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");
    let mut journal = JsonJournal::open(&journal_path, stream.clone()).expect("open journal");
    journal
        .append_values(
            JournalDirection::Inbound,
            vec![json!({"id": 1})],
            Some("1".to_string()),
            None,
        )
        .expect("append");
    OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .expect("open raw file")
        .write_all(br#"{"version":1,"stream":{"path":"/orders""#)
        .expect("write partial line");

    let reopened = JsonJournal::open(&journal_path, stream).expect("reopen journal");
    assert_eq!(
        reopened.values().cloned().collect::<Vec<_>>(),
        vec![json!({"id": 1})]
    );
}

#[test]
fn producer_metadata_is_forward_compatible_with_v1_replay() {
    let dir = tempdir().expect("tempdir");
    let journal_path = dir.path().join("orders.jsonl");
    let stream = JournalStreamIdentity::new("/orders", "application/json").expect("stream");

    fs::write(
        &journal_path,
        concat!(
            "{\"version\":1,\"stream\":{\"path\":\"/orders\",\"content_type\":\"application/json\"},",
            "\"direction\":\"inbound\",\"local_seq\":0,\"batch_seq\":0,\"batch_index\":0,",
            "\"batch_len\":1,\"payload\":{\"id\":1},\"next_offset\":\"1\",",
            "\"observed_at\":\"2026-01-01T00:00:00Z\",\"persisted_at\":\"2026-01-01T00:00:00Z\"}\n"
        ),
    )
    .expect("seed journal");

    let mut journal = JsonJournal::open(&journal_path, stream.clone()).expect("open journal");
    assert_eq!(
        journal.values().cloned().collect::<Vec<_>>(),
        vec![json!({"id": 1})]
    );
    journal
        .append_values(
            JournalDirection::Outbound,
            vec![json!({"id": 2})],
            Some("2".to_string()),
            Some(&ProducerJournalProgress {
                producer_id: "producer-1".to_string(),
                epoch: 4,
                next_seq: 9,
                acked_server_offset: Some("2".to_string()),
                acked_local_seq: Some(1),
            }),
        )
        .expect("append outbound metadata");

    let reopened = JsonJournal::open(&journal_path, stream).expect("reopen journal");
    assert_eq!(reopened.records().len(), 2);
    assert_eq!(
        reopened.records()[1]
            .producer
            .as_ref()
            .expect("producer")
            .next_seq,
        9
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_uses_the_last_persisted_server_offset_and_fetches_only_gaps() {
    let base_url = spawn_test_server().await;
    let client = client(&base_url);
    let stream_path = "/orders";
    create_json_stream(&client, stream_path).await;
    append_json_values(&client, stream_path, &[json!({"id": 1}), json!({"id": 2})]).await;

    let dir = tempdir().expect("tempdir");
    let journal_path = dir.path().join("orders.jsonl");
    let stream = JournalStreamIdentity::new(stream_path, "application/json").expect("stream");
    let mut replica =
        ReadReplica::open(client.clone(), &journal_path, stream.clone()).expect("open replica");
    let first = replica
        .replicate(ReadRequest::default())
        .await
        .expect("initial replication");
    assert_eq!(first.appended, 2);

    append_json_values(&client, stream_path, &[json!({"id": 3})]).await;

    let mut restarted =
        ReadReplica::open(client.clone(), &journal_path, stream).expect("reopen replica");
    let second = restarted
        .replicate(ReadRequest::default())
        .await
        .expect("gap replication");

    assert_eq!(second.appended, 1);
    assert_eq!(
        restarted.values().cloned().collect::<Vec<_>>(),
        vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_replication_preserves_message_order_across_restart_and_catch_up() {
    let base_url = spawn_test_server().await;
    let client = client(&base_url);
    let stream_path = "/orders";
    create_json_stream(&client, stream_path).await;
    let first_offset =
        append_json_values(&client, stream_path, &[json!({"id": 1}), json!({"id": 2})]).await;

    let dir = tempdir().expect("tempdir");
    let journal_path = dir.path().join("orders.jsonl");
    let stream = JournalStreamIdentity::new(stream_path, "application/json").expect("stream");
    let mut replica =
        ReadReplica::open(client.clone(), &journal_path, stream.clone()).expect("open replica");
    replica
        .replicate(ReadRequest::default())
        .await
        .expect("initial replication");
    assert_eq!(replica.resume_offset(), first_offset.as_deref());

    append_json_values(&client, stream_path, &[json!({"id": 3}), json!({"id": 4})]).await;

    let mut restarted =
        ReadReplica::open(client.clone(), &journal_path, stream).expect("reopen replica");
    restarted
        .replicate(ReadRequest::default())
        .await
        .expect("catch-up replication");

    assert_eq!(
        restarted.values().cloned().collect::<Vec<_>>(),
        vec![
            json!({"id": 1}),
            json!({"id": 2}),
            json!({"id": 3}),
            json!({"id": 4}),
        ]
    );
}
