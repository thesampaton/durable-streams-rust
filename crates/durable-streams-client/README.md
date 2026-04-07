# durable-streams-client

Rust client library for Durable Streams.

This crate provides:

- stream-first ergonomic APIs for common operations
- explicit raw request and response models when you need protocol control
- first-class typed configuration
- layered TOML config loading
- environment-variable overrides
- explicit auth configuration
- async HTTP client built on `reqwest` and `tokio`
- idempotent producer support

## Main Types

The main entry points are:

- `Client` for top-level operations
- `StreamHandle` for stream-scoped usage
- `ClientBuilder` for fluent construction
- `ClientConfig` and `ClientConfigLoader` for config-first construction
- `raw` for protocol-shaped request/response types
- `IdempotentProducer` for producer fencing and sequence-aware writes

## Example

```no_run
use durable_streams_client::Client;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::builder()
    .base_url("http://127.0.0.1:8080")
    .default_content_type("application/json")
    .build()?;
let stream = client.stream("/example");

stream.create().send().await?;
stream.append_json(&serde_json::json!({ "type": "created" })).await?;

let page = stream.read().send().await?;
println!("next offset: {}", page.next_offset);
# Ok(())
# }
```

## Common Tasks

### Create A Stream And Append JSON

```no_run
use durable_streams_client::Client;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::builder()
    .base_url("http://127.0.0.1:8080")
    .default_content_type("application/json")
    .build()?;
let orders = client.stream("/orders");

orders.create().send().await?;

orders
    .append_json(&serde_json::json!({
        "type": "order.created",
        "id": "ord_123"
    }))
    .await?;
# Ok(())
# }
```

### Read From The Beginning

```no_run
use durable_streams_client::{Client, ClientConfig};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::new(ClientConfig::default())?;
let orders = client.stream("/orders");

let page = orders.read().send().await?;

for chunk in page.chunks {
    println!("chunk at {}: {} bytes", chunk.offset, chunk.data.len());
}
# Ok(())
# }
```

### Resume From A Saved Offset

```no_run
use durable_streams_client::{Client, ClientConfig, Offset};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::new(ClientConfig::default())?;
let orders = client.stream("/orders");
let saved_offset = "42-0";

let page = orders
    .read()
    .offset(Offset::at(saved_offset))
    .send()
    .await?;

println!("resume from next offset {}", page.next_offset);
# Ok(())
# }
```

### Inspect Stream Metadata

```no_run
use durable_streams_client::{Client, ClientConfig};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::new(ClientConfig::default())?;
let orders = client.stream("/orders");

let info = orders.head().await?;
println!("content type: {:?}", info.content_type);
println!("closed: {}", info.closed);
# Ok(())
# }
```

### Drop Down To The Raw API

```no_run
use durable_streams_client::{raw, Client, ClientConfig};
use bytes::Bytes;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::new(ClientConfig::default())?;
let orders = client.stream("/orders");

let response = orders
    .append_raw(&raw::AppendRequest {
        body: Bytes::from_static(b"raw-bytes"),
        content_type: Some("application/octet-stream".to_string()),
        stream_seq: None,
        producer: None,
        options: raw::RequestOptions::default(),
    })
    .await?;

println!("raw next offset: {:?}", response.next_offset);
# Ok(())
# }
```

## Configuration

Example `config/default.toml`:

```toml
[client]
base_url = "http://127.0.0.1:8080"

[auth]
type = "bearer"
bearer_token = "replace-me"

[transport]
connect_timeout_ms = 5000
request_timeout_ms = 30000
user_agent = "my-service/1.0"

[retry]
max_retries = 3
initial_backoff_ms = 100
max_backoff_ms = 2000
backoff_multiplier = 2.0

[defaults]
default_content_type = "application/json"

[defaults.headers]
x-service-name = "orders-api"
```

Load it with:

```rust
use durable_streams_client::{Client, ClientConfigLoader};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let config = ClientConfigLoader::default().load()?;
let _client = Client::new(config)?;
# Ok(())
# }
```

Or build a client fluently:

```rust
use durable_streams_client::Client;
use std::time::Duration;

# fn main() -> Result<(), durable_streams_client::Error> {
let _client = Client::builder()
    .base_url("http://127.0.0.1:8080")
    .bearer_auth("replace-me")
    .default_content_type("application/json")
    .request_timeout(Duration::from_secs(30))
    .build()?;
# Ok(())
# }
```

Environment overrides use the `DURABLE_STREAMS_CLIENT__` prefix.

Common variables:

- `DURABLE_STREAMS_CLIENT__CLIENT__BASE_URL`
- `DURABLE_STREAMS_CLIENT__AUTH__TYPE`
- `DURABLE_STREAMS_CLIENT__AUTH__BEARER_TOKEN`
- `DURABLE_STREAMS_CLIENT__AUTH__USERNAME`
- `DURABLE_STREAMS_CLIENT__AUTH__PASSWORD`
- `DURABLE_STREAMS_CLIENT__AUTH__HEADER_NAME`
- `DURABLE_STREAMS_CLIENT__AUTH__HEADER_VALUE`
- `DURABLE_STREAMS_CLIENT__TRANSPORT__CONNECT_TIMEOUT_MS`
- `DURABLE_STREAMS_CLIENT__TRANSPORT__REQUEST_TIMEOUT_MS`
- `DURABLE_STREAMS_CLIENT__TRANSPORT__USER_AGENT`
- `DURABLE_STREAMS_CLIENT__TRANSPORT__PROXY_URL`
- `DURABLE_STREAMS_CLIENT__RETRY__MAX_RETRIES`
- `DURABLE_STREAMS_CLIENT__RETRY__INITIAL_BACKOFF_MS`
- `DURABLE_STREAMS_CLIENT__RETRY__MAX_BACKOFF_MS`
- `DURABLE_STREAMS_CLIENT__RETRY__BACKOFF_MULTIPLIER`
- `DURABLE_STREAMS_CLIENT__DEFAULTS__DEFAULT_CONTENT_TYPE`
- `DURABLE_STREAMS_CLIENT__DEFAULTS__HEADERS_JSON`
- `DURABLE_STREAMS_CLIENT__DEFAULTS__QUERY_JSON`

`DEFAULTS__HEADERS_JSON` and `DEFAULTS__QUERY_JSON` expect JSON objects.

## Authentication

Supported auth modes:

- `none`
- `bearer`
- `basic`
- `header`

The `header` mode is intended for gatekeepers, API gateways, or custom auth
proxies where a static request header is operationally simpler than standard
HTTP auth schemes.

## Verification

This crate is wired to the upstream client conformance suite through:

- `src/bin/client-conformance-adapter.rs`
- `../../tests/conformance/client/run-adapter.sh`

Local verification:

```bash
cargo test -p durable-streams-client
./scripts/conformance/run-client-suite.sh --fail-fast
```
