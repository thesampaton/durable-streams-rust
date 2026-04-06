# durable-streams-client

Rust client library for Durable Streams.

This crate provides:

- explicit typed request and response models
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
- `ClientConfig` and `ClientConfigLoader` for config-first construction
- `IdempotentProducer` for producer fencing and sequence-aware writes

## Example

```rust
use durable_streams_client::{
    Client, ClientConfig, CreateStreamRequest, ReadRequest, RequestOptions,
};

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::new(ClientConfig::default())?;

client
    .create(
        "/example",
        &CreateStreamRequest {
            content_type: "application/json".to_string(),
            body: None,
            ttl_seconds: None,
            expires_at: None,
            closed: false,
            options: RequestOptions::default(),
        },
    )
    .await?;

let response = client.read("/example", &ReadRequest::default()).await?;
println!("next offset: {}", response.next_offset);
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
