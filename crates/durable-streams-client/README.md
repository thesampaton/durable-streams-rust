# durable-streams-client

Async Rust client for Durable Streams, built on `reqwest` and `tokio`.
The crate is currently unpublished. It supports stream operations, idempotent
producers, JSON/JSONL ingest, and local journal replication.

## Quick start

```no_run
use durable_streams_client::Client;

# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), durable_streams_client::Error> {
let client = Client::builder()
    .base_url("http://127.0.0.1:4437/v1/stream/")
    .default_content_type("application/json")
    .build()?;
let orders = client.stream("/orders");

orders.create().send().await?;
orders.append_json(&serde_json::json!({ "id": "ord_123" })).await?;

let page = orders.read().send().await?;
for chunk in &page.chunks {
    println!("{} bytes; resumes at {}", chunk.data.len(), chunk.resume_offset);
}

// Persist and reuse offsets returned by the server; treat them as opaque.
let next_page = orders.read().offset(page.next_offset).send().await?;
println!("next offset: {}", next_page.next_offset);
# Ok(())
# }
```

## API map

| Type or module | Purpose |
| --- | --- |
| `Client`, `StreamHandle` | Server access and operations on one stream |
| `ClientBuilder` | Configure a client in Rust |
| `ClientConfig`, `ClientConfigLoader` | Typed configuration and TOML/environment loading |
| `IdempotentProducer` | Producer fencing and sequence-aware writes |
| `raw` | Protocol request/response types and per-request options |
| `load_json_input` | Read JSON or JSONL input |
| `JsonJournal`, `JournalStreamIdentity` | Local JSONL persistence |
| `ReadReplica` | Replicate reads and resume from a persisted server offset |

Generate the API reference with `cargo doc -p durable-streams-client --no-deps --open`.

## Configuration

For file-based configuration, `ClientConfigLoader` merges built-in defaults,
`config/default.toml`, `config/<profile>.toml`, `config/local.toml`, an optional
`config_override`, then environment variables. The config directory is relative
to the working directory unless changed on the loader. These are client config
files, separate from the server's deployment profiles.

A minimal client TOML file:

```toml
[client]
base_url = "http://127.0.0.1:4437/v1/stream/"
```

```rust
use durable_streams_client::{Client, ClientConfigLoader};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let config = ClientConfigLoader::default().load()?;
let client = Client::new(config)?;
# let _ = client;
# Ok(())
# }
```

Environment names follow the file path with the `DURABLE_STREAMS_CLIENT__`
prefix, for example `DURABLE_STREAMS_CLIENT__CLIENT__BASE_URL` or
`DURABLE_STREAMS_CLIENT__TRANSPORT__REQUEST_TIMEOUT_MS`.
`DEFAULTS__HEADERS_JSON` and `DEFAULTS__QUERY_JSON` under that prefix take JSON
objects. See the `config` module for transport, retry, and request defaults.

Authentication supports `none`, `bearer`, `basic`, and a custom `header` mode
for gateways. Configure it through `ClientConfig::auth`, the builder's auth
methods, or `[auth]` in TOML. For bearer auth, set `type = "bearer"` and
`bearer_token`, or use `DURABLE_STREAMS_CLIENT__AUTH__TYPE` and
`DURABLE_STREAMS_CLIENT__AUTH__BEARER_TOKEN`.

## JSON CLI

The `durable-streams-json` binary supports `persist` (write a local journal),
`replicate` (resume reads into a journal), and `send` (append through a producer).

```bash
cargo run -p durable-streams-client --bin durable-streams-json -- persist \
  --journal ./orders.jsonl \
  --stream /orders \
  --content-type application/json \
  --input ./orders-input.jsonl
```

Use `--help` for command options. For development checks and upstream suites,
see [Contributing](../../CONTRIBUTING.md) and the
[conformance guide](../../tests/conformance/README.md).
