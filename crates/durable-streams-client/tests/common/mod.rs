use durable_streams_client::{
    AppendRequest, Client, ClientConfig, CreateStreamRequest, RequestOptions,
};
use durable_streams_server::{Config, InMemoryStorage, build_router};
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn spawn_test_server() -> String {
    let storage = Arc::new(InMemoryStorage::new(1024 * 1024, 256 * 1024));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    let app = build_router(storage, &Config::default());

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    format!("http://127.0.0.1:{}/v1/stream/", addr.port())
}

pub fn client(base_url: &str) -> Client {
    let config = ClientConfig {
        base_url: url::Url::parse(base_url).expect("base url"),
        ..ClientConfig::default()
    };
    Client::new(config).expect("client config should be valid")
}

pub async fn create_json_stream(client: &Client, path: &str) {
    client
        .create_raw(
            path,
            &CreateStreamRequest {
                content_type: "application/json".to_string(),
                ttl_seconds: None,
                expires_at: None,
                closed: false,
                body: None,
                options: RequestOptions::default(),
            },
        )
        .await
        .expect("stream should be created");
}

pub async fn append_json_values(
    client: &Client,
    path: &str,
    values: &[serde_json::Value],
) -> Option<String> {
    client
        .append_raw(
            path,
            &AppendRequest {
                body: serde_json::to_vec(values)
                    .expect("values should serialize")
                    .into(),
                content_type: Some("application/json".to_string()),
                stream_seq: None,
                producer: None,
                options: RequestOptions::default(),
            },
        )
        .await
        .expect("append should succeed")
        .next_offset
}
