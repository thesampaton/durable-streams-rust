//! Smoke tests for the thin JSON/JSONL CLI surface.

mod common;

use common::{append_json_values, client, create_json_stream, spawn_test_server};
use durable_streams_client::{JournalStreamIdentity, JsonJournal};
use serde_json::json;
use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn cli_persist_normalizes_jsonl_into_the_library_journal_path() {
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("input.jsonl");
    let journal_path = dir.path().join("orders.jsonl");
    fs::write(&input_path, "{\"id\":1}\n{\"id\":2}\n").expect("write input");

    let output = Command::new(env!("CARGO_BIN_EXE_durable-streams-json"))
        .args([
            "persist",
            "--journal",
            journal_path.to_str().expect("journal path"),
            "--stream",
            "/orders",
            "--content-type",
            "application/json",
            "--input",
            input_path.to_str().expect("input path"),
        ])
        .output()
        .expect("run persist cli");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let journal = JsonJournal::open(
        &journal_path,
        JournalStreamIdentity::new("/orders", "application/json").expect("stream"),
    )
    .expect("open journal");
    assert_eq!(
        journal.values().cloned().collect::<Vec<_>>(),
        vec![json!({"id": 1}), json!({"id": 2})]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_replicate_ingests_server_messages_into_the_same_library_journal() {
    let base_url = spawn_test_server().await;
    let client = client(&base_url);
    create_json_stream(&client, "/orders").await;
    append_json_values(&client, "/orders", &[json!({"id": 1}), json!({"id": 2})]).await;

    let dir = tempdir().expect("tempdir");
    let journal_path = dir.path().join("orders.jsonl");
    let output = Command::new(env!("CARGO_BIN_EXE_durable-streams-json"))
        .args([
            "replicate",
            "--base-url",
            &base_url,
            "--journal",
            journal_path.to_str().expect("journal path"),
            "--stream",
            "/orders",
            "--content-type",
            "application/json",
        ])
        .output()
        .expect("run replicate cli");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let journal = JsonJournal::open(
        &journal_path,
        JournalStreamIdentity::new("/orders", "application/json").expect("stream"),
    )
    .expect("open journal");
    assert_eq!(
        journal.values().cloned().collect::<Vec<_>>(),
        vec![json!({"id": 1}), json!({"id": 2})]
    );
}
