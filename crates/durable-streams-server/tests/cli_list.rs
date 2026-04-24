mod common;

use bytes::Bytes;
use common::{spawn_test_server_with_config, test_client};
use durable_streams_server::{
    Config, Storage,
    storage::{StreamConfig, file::FileStorage},
    streams::StreamListEntry,
};
use std::process::{Command, Output};
use tempfile::TempDir;

fn server_bin() -> &'static str {
    env!("CARGO_BIN_EXE_durable-streams-server")
}

fn write_config(dir: &TempDir, body: &str) -> std::path::PathBuf {
    let path = dir.path().join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

fn run_list(config_path: &std::path::Path, extra_args: &[&str]) -> Output {
    let mut command = Command::new(server_bin());
    command.arg("--config").arg(config_path).arg("list");
    command.args(extra_args);
    command.output().expect("run durable-streams-server list")
}

/// Validates: `list` defaults to local operator inspection for file storage
/// and does not require a running HTTP server.
#[test]
fn test_cli_list_file_storage_local_by_default() {
    let temp = tempfile::tempdir().expect("tempdir");
    let data_dir = temp.path().join("streams");
    let storage =
        FileStorage::new(&data_dir, 1024 * 1024, 1024 * 1024, false).expect("create file storage");
    storage
        .create_stream(
            "local-file-stream",
            StreamConfig::new("text/plain".to_string()),
        )
        .expect("create stream");
    storage
        .append(
            "local-file-stream",
            Bytes::from_static(b"hello"),
            "text/plain",
        )
        .expect("append data");
    drop(storage);

    let config_path = write_config(
        &temp,
        &format!(
            r#"
[storage]
mode = "file-fast"
data_dir = "{}"
"#,
            data_dir.display()
        ),
    );

    let output = run_list(&config_path, &["--json"]);
    assert!(
        output.status.success(),
        "list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let entries: Vec<StreamListEntry> =
        serde_json::from_slice(&output.stdout).expect("list JSON should parse");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "local-file-stream");
    assert_eq!(entries[0].message_count, 1);
}

/// Validates: local `list` fails clearly for memory storage because there is no
/// durable out-of-process state to inspect.
#[test]
fn test_cli_list_memory_storage_fails_locally() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(
        &temp,
        r#"
[storage]
mode = "memory"
"#,
    );

    let output = run_list(&config_path, &[]);
    assert!(!output.status.success(), "memory list should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("storage.mode='memory'"), "{stderr}");
    assert!(stderr.contains("--url"), "{stderr}");
}

/// Validates: remote HTTP listing is retained only when the operator explicitly
/// passes an admin endpoint URL.
#[tokio::test]
async fn test_cli_list_remote_url_is_explicit() {
    let mut server_config = Config::default();
    server_config.admin.enabled = true;
    let (base_url, _port) = spawn_test_server_with_config(server_config).await;
    let client = test_client();

    let create = client
        .put(format!("{base_url}/v1/stream/remote-listed"))
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);

    let temp = tempfile::tempdir().expect("tempdir");
    let config_path = write_config(
        &temp,
        r#"
[storage]
mode = "memory"
"#,
    );

    let list_url = format!("{base_url}/admin/streams");
    let output = tokio::task::spawn_blocking(move || {
        run_list(&config_path, &["--json", "--url", &list_url])
    })
    .await
    .expect("list command task should complete");
    assert!(
        output.status.success(),
        "remote list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let entries: Vec<StreamListEntry> =
        serde_json::from_slice(&output.stdout).expect("list JSON should parse");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "remote-listed");
}
