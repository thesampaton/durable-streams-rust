# Durable Streams server

Run a standalone HTTP server or embed it in an `axum` application. The crate
supports live reads, forks, subscriptions, and memory, file, or transactional
`redb` storage.

For deployment profiles, configuration examples, and CLI commands, see the
[server README](https://docs.rs/crate/durable-streams-server/latest/source/README.md).

# Embedding

[`StreamService`] provides stream operations over an `Arc<dyn Storage>`.
[`Server`] validates HTTP settings and loads subscription state; [`Server::start`]
starts the worker and returns [`RunningServer`]. Construction can happen before
Tokio, but `start` requires a runtime.

```no_run
use durable_streams_server::{Config, InMemoryStorage, RouterOptions, Server, StreamService};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

# async fn example(shutdown: CancellationToken) -> Result<(), Box<dyn std::error::Error>> {
let storage = Arc::new(InMemoryStorage::new(1024 * 1024, 1024 * 1024));
let options = RouterOptions::default().with_shutdown(shutdown.clone());
let server = Server::new(StreamService::new(storage), &Config::default(), options)?;
let listener = tokio::net::TcpListener::bind("127.0.0.1:4437").await?;
let running = server.start()?;

// The caller cancels `shutdown` to release live reads and drain the listener.
let result = axum::serve(listener, running.router())
    .with_graceful_shutdown(shutdown.cancelled_owned())
    .await;
running.shutdown().await?;
result?;
# Ok(())
# }
```

Clone the running server or its routers for additional listeners. They share
one owner and worker; a second independent server using the same storage `Arc`
is rejected. Dropping the last handle/router cancels the worker. Explicit
`shutdown` also waits for delivery tasks to finish cancellation. HTTP listeners
remain caller-owned. Server shutdown cancels a child token, leaving unrelated
users of the supplied parent token unaffected.

Use [`RunningServer::protocol_router`], [`RunningServer::admin_router`], and
[`RunningServer::probe_router`] to apply different middleware before merging.
Admin routes are disabled by default and supply no authentication policy.
Embedded `/readyz` is opt-in through [`RouterOptions::with_readiness`]; the
standalone executable enables it.

# Configuration

Use [`Config::default`] for programmatic configuration, or [`Config::from_sources`]
with [`ConfigLoadOptions`] to load TOML and environment overrides. Loading does
not start a listener or worker. [`DeploymentProfile`] selects built-in defaults;
see the README for file precedence and deployment examples.

# Stream operations

```rust
use durable_streams_server::{InMemoryStorage, StreamService};
use durable_streams_server::storage::StreamOptions;
use bytes::Bytes;
use std::sync::Arc;
# fn example() -> Result<(), durable_streams_server::protocol::error::Error> {
let streams = StreamService::new(Arc::new(InMemoryStorage::new(1024 * 1024, 1024 * 1024)));
streams.create("events", StreamOptions::new("text/plain").with_ttl(60), vec![])?;
let result = streams.append_batch("events", vec![Bytes::from_static(b"done")], "text/plain", None, true)?;
assert!(result.closed);
// result.start_offset identifies this batch; result.next_offset resumes after it.
# Ok(())
# }
```

`StreamOptions` expresses caller intent; storage resolves it to `StreamConfig`
with normalized content type and an initialized expiration deadline. Choose one
`Expiry` policy or use `with_ttl` / `with_expires_at`, which replace one another.
Invalid TTL bounds fail before mutation. `with_closed(true)` creates a closed
stream, including its initial body.

The storage trait remains synchronous. File I/O and lock waits currently run
on the calling thread; embedders should account for that execution cost.

# API map

| Module | Purpose |
| --- | --- |
| [`config`] | Configuration, profiles, and validation |
| [`router`] | HTTP routes and server lifecycle |
| [`storage`] | Synchronous storage contract and backends |
| [`streams`] | Stream operations and metadata |
| [`startup`] | Listener/TLS preflight and startup errors |
| [`protocol`] | Protocol types and errors |
| [`transfer`] | Stream export/import |
