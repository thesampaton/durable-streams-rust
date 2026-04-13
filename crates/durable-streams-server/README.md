# durable-streams-server

`durable-streams-server` is a Rust implementation of the
[Durable Streams protocol](https://github.com/durable-streams/durable-streams),
built with `axum` and `tokio`.

It can be run as a standalone HTTP server or embedded into an existing axum
application with `build_router` or `build_router_with_ready`.

## Features

- Durable Streams HTTP API with create, append, read, head, close, and delete operations
- Live reads via long-polling and Server-Sent Events (SSE)
- In-memory, file-backed, and ACID (`redb`) storage backends
- Layered configuration from TOML files and `DS_*` environment variables
- Structured `application/problem+json` error responses
- Request telemetry via `tracing` with OpenTelemetry/ECS-friendly field names

## Running

From the workspace root:

```bash
cargo run -p durable-streams-server
```

By default the server listens on `http://localhost:4437`, exposes health checks
at `/healthz` and `/readyz`, and mounts the protocol at `/v1/stream`.

Use `http.stream_base_path` or `DS_HTTP__STREAM_BASE_PATH` to mount the
protocol at another path.

## Storage Backends

The default storage mode is in-memory. For persistence, choose a backend via
`DS_STORAGE__MODE`:

| Mode | Durability | Use case |
|------|------------|----------|
| `memory` | None (lost on restart) | Development and testing |
| `file-fast` | Buffered writes | Lower-latency persistence where recent data loss is acceptable |
| `file-durable` | Fsynced writes | Durable persistence without external dependencies |
| `acid` | Crash-resilient (`redb`) | Production workloads requiring transactional durability |

Examples:

```bash
DS_STORAGE__MODE=file-durable DS_STORAGE__DATA_DIR=./data cargo run -p durable-streams-server
DS_STORAGE__MODE=acid DS_STORAGE__DATA_DIR=./data cargo run -p durable-streams-server
```

## Configuration

Configuration is loaded in this order, with later sources overriding earlier
ones:

1. Built-in defaults
2. `config/default.toml`
3. `config/<profile>.toml`
4. `config/local.toml`
5. `--config <path>`
6. Environment variables

Examples:

```bash
cargo run -p durable-streams-server -- --profile prod
cargo run -p durable-streams-server -- --profile prod --config /etc/durable-streams/server.toml
```

Common environment variables:

- `DS_SERVER__PORT`
- `DS_SERVER__LONG_POLL_TIMEOUT_SECS`
- `DS_SERVER__SSE_RECONNECT_INTERVAL_SECS`
- `DS_HTTP__CORS_ORIGINS`
- `DS_HTTP__STREAM_BASE_PATH`
- `DS_LIMITS__MAX_MEMORY_BYTES`
- `DS_LIMITS__MAX_STREAM_BYTES`
- `DS_STORAGE__MODE`
- `DS_STORAGE__DATA_DIR`
- `DS_STORAGE__ACID_SHARD_COUNT`
- `DS_STORAGE__ACID_BACKEND`
- `DS_TLS__CERT_PATH`
- `DS_TLS__KEY_PATH`
- `DS_LOG__RUST_LOG`
- `RUST_LOG`

## Library Use

For embedding, the main entry points are:

- `Config` and `ConfigLoadOptions` for configuration loading
- `build_router` and `build_router_with_ready` for mounting the HTTP API
- `InMemoryStorage`, `FileStorage`, and `AcidStorage` for backend selection

## Verification

```bash
cargo build -p durable-streams-server
cargo test -p durable-streams-server
cargo clippy -p durable-streams-server --all-targets
cargo fmt --all
```
