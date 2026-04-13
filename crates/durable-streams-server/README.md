# durable-streams-server

Rust implementation of the [Durable Streams protocol](https://github.com/durable-streams/durable-streams).

The current workspace copy is a deliberate lift-and-shift of published
`durable-streams-server` `0.1.3`, kept behaviourally close to that release
while it is housed inside the `durable-streams-rust` workspace.

## Running

```bash
cargo run -p durable-streams-server
```

The server listens on `http://localhost:4437` with streams at `/v1/stream/`.
Set `http.stream_base_path` or `DS_HTTP__STREAM_BASE_PATH` to mount the
protocol at another path.

## Storage backends

The default storage mode is in-memory. For persistence, choose a backend via `DS_STORAGE__MODE`:

| Mode | Durability | Use case |
|------|-----------|----------|
| `memory` | None (lost on restart) | Development, testing, buffer in front of sync layer |
| `file-fast` | Buffered writes | Low-latency persistence where occasional data loss is acceptable |
| `file-durable` | Fsynced writes | Durable persistence without external dependencies |
| `acid` (alias: `redb`) | Crash-resilient (redb) | Production workloads requiring ACID guarantees |

```bash
# Run with durable file storage
DS_STORAGE__MODE=file-durable DS_STORAGE__DATA_DIR=./data cargo run -p durable-streams-server

# Run with crash-resilient acid storage
DS_STORAGE__MODE=acid DS_STORAGE__DATA_DIR=./data cargo run -p durable-streams-server
```

## Configuration

The server supports layered TOML config with environment overrides.

Load order:

1. built-in defaults
2. `config/default.toml`
3. `config/<profile>.toml`
4. `config/local.toml`
5. `--config <path>`
6. environment variables

Examples:

```bash
cargo run -p durable-streams-server -- --profile dev
cargo run -p durable-streams-server -- --profile prod --config /etc/durable-streams/override.toml
```

Environment variables use the `DS_` prefix with double-underscore section
separators.

Common variables:

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
- `DS_TLS__CERT_PATH`
- `DS_TLS__KEY_PATH`
- `DS_LOG__RUST_LOG`
- `RUST_LOG`

## Verification

```bash
cargo build -p durable-streams-server
cargo test -p durable-streams-server
cargo clippy -p durable-streams-server --all-targets
cargo fmt --all
```

Run upstream server conformance from the workspace root:

```bash
./scripts/conformance/run-server-suite.sh
```
