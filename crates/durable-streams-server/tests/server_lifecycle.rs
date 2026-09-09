//! Embedding ownership, initialization, and shutdown contracts.
#![allow(
    clippy::unwrap_used,
    reason = "test setup and assertions fail the test on error"
)]

use durable_streams_server::{
    Config, InMemoryStorage, RouterOptions, Server, ServerError, Storage, StreamService,
};
use std::sync::Arc;

fn storage() -> Arc<dyn Storage> {
    Arc::new(InMemoryStorage::new(1024 * 1024, 1024 * 1024))
}

#[test]
fn construct_before_runtime_then_start_and_await_shutdown() {
    let storage = storage();
    let server = Server::new(
        StreamService::new(storage.clone()),
        &Config::default(),
        RouterOptions::default(),
    )
    .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let running = server.start().unwrap();
        let router = running.router();
        let clone = running.clone();
        let (a, b) = tokio::join!(running.shutdown(), clone.shutdown());
        a.unwrap();
        b.unwrap();
        drop(router);
        drop(clone);
        drop(running);
        // Awaited shutdown releases the worker's ownership, so a new server can recover the storage.
        let replacement = Server::new(
            StreamService::new(storage),
            &Config::default(),
            RouterOptions::default(),
        )
        .unwrap()
        .start()
        .unwrap();
        replacement.shutdown().await.unwrap();
    });
}

#[test]
fn starting_without_runtime_is_a_typed_error() {
    let server = Server::new(
        StreamService::new(storage()),
        &Config::default(),
        RouterOptions::default(),
    )
    .unwrap();
    assert!(matches!(server.start(), Err(ServerError::RuntimeRequired)));
}

#[test]
fn independent_owners_of_same_storage_are_rejected() {
    let storage = storage();
    let owner = Server::new(
        StreamService::new(storage.clone()),
        &Config::default(),
        RouterOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        Server::new(
            StreamService::new(storage.clone()),
            &Config::default(),
            RouterOptions::default()
        ),
        Err(ServerError::StorageAlreadyOwned)
    ));
    drop(owner);
    assert!(
        Server::new(
            StreamService::new(storage),
            &Config::default(),
            RouterOptions::default()
        )
        .is_ok()
    );
}

#[test]
fn invalid_mount_is_an_error_but_unused_listener_settings_are_not_validated() {
    let mut config = Config::default();
    config.http.stream_base_path = "missing-leading-slash".into();
    assert!(matches!(
        Server::new(
            StreamService::new(storage()),
            &config,
            RouterOptions::default()
        ),
        Err(ServerError::Configuration(_))
    ));
    config.http.stream_base_path = "/streams".into();
    config.server.bind_address = "unused by embedding".into();
    assert!(
        Server::new(
            StreamService::new(storage()),
            &config,
            RouterOptions::default()
        )
        .is_ok()
    );
}

#[test]
fn corrupt_control_state_fails_before_routes_are_available() {
    let storage = storage();
    storage.save_subscription_state(b"invalid json").unwrap();
    assert!(matches!(
        Server::new(
            StreamService::new(storage.clone()),
            &Config::default(),
            RouterOptions::default()
        ),
        Err(ServerError::Initialization(_))
    ));
    // Failed initialization must release its owner lease.
    storage.save_subscription_state(b"null").unwrap();
    assert!(matches!(
        Server::new(
            StreamService::new(storage),
            &Config::default(),
            RouterOptions::default()
        ),
        Err(ServerError::Initialization(_))
    ));
}

#[tokio::test]
async fn route_groups_isolate_admin_middleware_and_share_state_between_listeners() {
    let mut config = Config::default();
    config.admin.enabled = true;
    let running = Server::new(
        StreamService::new(storage()),
        &config,
        RouterOptions::default(),
    )
    .unwrap()
    .start()
    .unwrap();
    let admin = running
        .admin_router()
        .route_layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                if request.headers().get("authorization").is_none() {
                    return axum::response::IntoResponse::into_response(
                        axum::http::StatusCode::UNAUTHORIZED,
                    );
                }
                next.run(request).await
            },
        ));
    let app = running
        .protocol_router()
        .merge(admin)
        .merge(running.probe_router());
    let stop = tokio_util::sync::CancellationToken::new();
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(format!("http://{}", listener.local_addr().unwrap()));
        let router = app.clone();
        let stop = stop.clone();
        listeners.push(tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
        }));
    }
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{}/admin/streams", addresses[0]))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .get(format!("{}/healthz", addresses[0]))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .put(format!("{}/v1/stream/shared", addresses[0]))
            .header("content-type", "text/plain")
            .body("hello")
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        client
            .get(format!("{}/v1/stream/shared?offset=-1", addresses[1]))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "hello"
    );
    let listing = client
        .get(format!("{}/admin/streams", addresses[1]))
        .header("authorization", "test")
        .send()
        .await
        .unwrap();
    assert_eq!(listing.status(), 200);
    assert!(listing.text().await.unwrap().contains("shared"));
    stop.cancel();
    for listener in listeners {
        listener.await.unwrap();
    }
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn literal_root_mount_builds_and_cancelled_start_is_rejected() {
    let mut config = Config::default();
    config.http.stream_base_path = "/".into();
    let running = Server::new(
        StreamService::new(storage()),
        &config,
        RouterOptions::default(),
    )
    .unwrap()
    .start()
    .unwrap();
    drop(running.router());
    running.shutdown().await.unwrap();
    for path in ["/{name}", "/:name", "/a?b", "/a b", "/a/*rest"] {
        config.http.stream_base_path = path.into();
        assert!(matches!(
            Server::new(
                StreamService::new(storage()),
                &config,
                RouterOptions::default()
            ),
            Err(ServerError::Configuration(_))
        ));
    }
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let server = Server::new(
        StreamService::new(storage()),
        &Config::default(),
        RouterOptions::default().with_shutdown(token),
    )
    .unwrap();
    assert!(matches!(
        server.start(),
        Err(ServerError::ShutdownRequested)
    ));
}
