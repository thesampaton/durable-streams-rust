mod common;

use common::{spawn_test_server_with_config, test_client};
use durable_streams_server::{Config, streams::StreamListEntry};

/// Validates: admin routes are absent unless explicitly enabled.
#[tokio::test]
async fn test_admin_streams_route_absent_by_default() {
    let (base_url, _port) = spawn_test_server_with_config(Config::default()).await;
    let client = test_client();

    let response = client
        .get(format!("{base_url}/admin/streams"))
        .send()
        .await
        .expect("admin request failed");

    assert_eq!(response.status(), 404);
}

/// Validates: opt-in admin router exposes operator stream listing separately
/// from the Durable Streams protocol base path.
#[tokio::test]
async fn test_admin_streams_route_present_when_enabled() {
    let mut config = Config::default();
    config.admin.enabled = true;
    config.admin.base_path = "/admin".to_string();

    let (base_url, _port) = spawn_test_server_with_config(config).await;
    let client = test_client();

    let create = client
        .put(format!("{base_url}/v1/stream/admin-listed"))
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .expect("create request failed");
    assert_eq!(create.status(), 201);

    let response = client
        .get(format!("{base_url}/admin/streams"))
        .send()
        .await
        .expect("admin list request failed");
    assert_eq!(response.status(), 200);

    let entries = response
        .json::<Vec<StreamListEntry>>()
        .await
        .expect("admin list response should parse");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "admin-listed");
    assert_eq!(entries[0].message_count, 1);

    let protocol_response = client
        .get(format!("{base_url}/v1/stream/"))
        .send()
        .await
        .expect("protocol root request failed");
    assert_ne!(protocol_response.status(), 200);
}
