# durable-streams-server

`durable-streams-server` is a Rust implementation of the
[Durable Streams protocol](https://github.com/durable-streams/durable-streams),
built with `axum` and `tokio`.

It can run as a standalone server or be embedded into an existing `axum`
application with `Server`, `StreamService`, and `RouterOptions`.

## Features

- Durable Streams HTTP API with create, append, read, head, close, and delete operations
- Live reads via long-polling and Server-Sent Events (SSE)
- In-memory, file-backed, and ACID (`redb`) storage backends
- Explicit transport modes: `http`, `tls`, and `mtls`
- Reverse-proxy trust gating for `X-Forwarded-*` and RFC 7239 `Forwarded`
- Layered configuration from TOML files and `DS_*` environment variables
- Structured startup diagnostics with phase-aware failures
- Structured `application/problem+json` error responses
- Request telemetry via `tracing` with OpenTelemetry/ECS-friendly field names

## Running

From the workspace root:

```bash
cargo run -p durable-streams-server
```

By default the server listens on `http://0.0.0.0:4437`, exposes health checks
at `/healthz` and `/readyz`, and mounts the protocol at `/v1/stream`. Admin
routes are disabled by default.

Use `http.stream_base_path` or `DS_HTTP__STREAM_BASE_PATH` to mount the
protocol at another path.

## Storage Backends

The default storage mode is in-memory. For persistence, choose a backend via
`DS_STORAGE__MODE`:

| Mode | Durability | Use case |
|------|------------|----------|
| `memory` | None (lost on restart) | Development and testing |
| `file-fast` | Journaled append commits; fewer extra syncs | Local per-stream log files |
| `file-durable` | Journaled commits plus extra write syncing | Local files with stronger initial-write syncing |
| `acid` | Crash-resilient (`redb`) | Production workloads requiring transactional durability |

Both file modes now sync append/replacement journals. See the
[migration notes](../../docs/migrations/server-api.md#import-and-file-persistence)
for recovery limits and the added filesystem cost.

Examples:

```bash
DS_STORAGE__MODE=file-durable DS_STORAGE__DATA_DIR=./data cargo run -p durable-streams-server
DS_STORAGE__MODE=acid DS_STORAGE__DATA_DIR=./data cargo run -p durable-streams-server
```

## Configuration

Configuration is loaded in this order, with later sources overriding earlier
ones:

1. Built-in defaults
2. Built-in profile defaults (`default`, `dev`, `prod`, `prod-tls`, `prod-mtls`)
3. `config/default.toml`
4. `config/<profile>.toml`
5. `config/local.toml`
6. `--config <path>`
7. Environment variables

Examples:

```bash
cargo run -p durable-streams-server -- --profile dev
cargo run -p durable-streams-server -- --profile prod
cargo run -p durable-streams-server -- --profile prod-tls --config /etc/durable-streams/server.toml
cargo run -p durable-streams-server -- --profile prod-mtls --config /etc/durable-streams/server.toml
```

The effective config schema is nested and operator-facing:

```toml
[server]
bind_address = "0.0.0.0:4437"

[limits]
max_memory_bytes = 104857600
max_stream_bytes = 10485760

[http]
cors_origins = "*"
stream_base_path = "/v1/stream"

[admin]
enabled = false
base_path = "/admin"

[storage]
mode = "memory"
data_dir = "./data/streams"

[transport]
mode = "http" # http | tls | mtls

[transport.http]
versions = ["http1"] # http1 | http2

[transport.connection]
long_poll_timeout_secs = 30
sse_reconnect_interval_secs = 60

[transport.tls]
min_version = "1.2"
max_version = "1.3"
alpn_protocols = ["http/1.1"]

[proxy]
enabled = false
forwarded_headers = "none" # none | x-forwarded | forwarded
trusted_proxies = []

[proxy.identity]
mode = "none" # none | header
require_tls = true

[observability]
rust_log = "info"
```

Environment overrides follow the TOML path with `DS_` prefixes and double
underscores. For example:

- `transport.mode` -> `DS_TRANSPORT__MODE`
- `transport.tls.cert_path` -> `DS_TRANSPORT__TLS__CERT_PATH`
- `proxy.identity.header_name` -> `DS_PROXY__IDENTITY__HEADER_NAME`

In practice, operators usually only need a small subset:

| Purpose | Variables | When to set |
|---------|-----------|-------------|
| Bind and logging | `DS_SERVER__BIND_ADDRESS`, `DS_OBSERVABILITY__RUST_LOG`, `RUST_LOG` | Override the listen address or log verbosity without editing TOML |
| Storage selection | `DS_STORAGE__MODE`, `DS_STORAGE__DATA_DIR` | Pick persistence mode and storage path |
| Admin routes | `DS_ADMIN__ENABLED`, `DS_ADMIN__BASE_PATH` | Opt in to operator routes such as `GET /admin/streams`; protect externally |
| Direct TLS | `DS_TRANSPORT__MODE`, `DS_TRANSPORT__TLS__CERT_PATH`, `DS_TRANSPORT__TLS__KEY_PATH` | Required for `tls` deployments |
| Direct mTLS | `DS_TRANSPORT__MODE`, `DS_TRANSPORT__TLS__CLIENT_CA_PATH` | Required in addition to TLS vars for `mtls` deployments |
| HTTP/2 and ALPN | `DS_TRANSPORT__HTTP__VERSIONS`, `DS_TRANSPORT__TLS__ALPN_PROTOCOLS` | Only when you need to override the profile defaults |
| Reverse proxy trust | `DS_PROXY__ENABLED`, `DS_PROXY__FORWARDED_HEADERS`, `DS_PROXY__TRUSTED_PROXIES` | When the backend should trust a proxy’s forwarded origin metadata |
| Proxy identity handoff | `DS_PROXY__IDENTITY__MODE`, `DS_PROXY__IDENTITY__HEADER_NAME`, `DS_PROXY__IDENTITY__REQUIRE_TLS` | Only for trusted proxy identity handoff in `mtls` mode |

Less common overrides, such as stream limits or acid-specific shard tuning,
follow the same `DS_<SECTION>__<FIELD>` pattern and generally only need to be
set when deviating from the profile examples.

## Deployment Styles

The server supports three deployment styles today.

### 1. Dev HTTP

Use this for local development, CI, or when TLS is terminated elsewhere and the
inner network is intentionally trusted.

- Profile: `dev`
- Transport: direct `http`
- HTTP versions: `http1`
- Storage: defaults to in-memory unless overridden

Example:

```bash
cargo run -p durable-streams-server -- --profile dev
```

Or with persistence:

```bash
DS_STORAGE__MODE=file-durable \
DS_STORAGE__DATA_DIR=./data \
cargo run -p durable-streams-server -- --profile dev
```

### 2. Direct TLS

Use this when the server terminates TLS itself and clients connect directly.

- Profile: `prod-tls`
- Transport: `tls`
- TLS versions: `1.2` minimum, `1.3` supported
- ALPN: `http/1.1` and `h2`
- Storage: durable file-backed by default

Example config:

```toml
[storage]
mode = "file-durable"
data_dir = "/var/lib/durable-streams"

[limits]
max_memory_bytes = 536870912
max_stream_bytes = 268435456

[transport]
mode = "tls"

[transport.http]
versions = ["http1", "http2"]

[transport.tls]
cert_path = "/etc/durable-streams/tls/server.crt"
key_path = "/etc/durable-streams/tls/server.key"
min_version = "1.2"
max_version = "1.3"
alpn_protocols = ["http/1.1", "h2"]
```

Run it:

```bash
cargo run -p durable-streams-server -- --profile prod-tls --config /etc/durable-streams/server.toml
```

### 3. Direct mTLS

Use this when clients must authenticate with certificates and the server is the
TLS termination point.

- Profile: `prod-mtls`
- Transport: `mtls`
- TLS versions: `1.2` minimum, `1.3` supported
- ALPN: `http/1.1` and `h2`
- Client authentication: required via `transport.tls.client_ca_path`

Example config:

```toml
[storage]
mode = "file-durable"
data_dir = "/var/lib/durable-streams"

[limits]
max_memory_bytes = 536870912
max_stream_bytes = 268435456

[transport]
mode = "mtls"

[transport.http]
versions = ["http1", "http2"]

[transport.tls]
cert_path = "/etc/durable-streams/tls/server.crt"
key_path = "/etc/durable-streams/tls/server.key"
client_ca_path = "/etc/durable-streams/tls/client-ca.crt"
min_version = "1.2"
max_version = "1.3"
alpn_protocols = ["http/1.1", "h2"]
```

Run it:

```bash
cargo run -p durable-streams-server -- --profile prod-mtls --config /etc/durable-streams/server.toml
```

## Reverse Proxy and TLS Topologies

The server intentionally keeps auth out of the application. If you need edge
authn/authz, terminate it at a proxy or gateway and forward only the minimum
origin metadata the server is configured to trust.

### Edge TLS termination, backend HTTP

This is the common pattern for local ingress or a trusted internal mesh:

- Edge terminates external TLS
- Backend link to durable-streams-server is plain HTTP
- `transport.mode = "http"`
- `proxy.enabled = true`
- `proxy.forwarded_headers = "x-forwarded"` or `"forwarded"`
- `proxy.trusted_proxies` contains only the proxy source IPs/CIDRs

Example:

```toml
[transport]
mode = "http"

[proxy]
enabled = true
forwarded_headers = "x-forwarded"
trusted_proxies = ["10.0.0.0/24"]
```

### Edge TLS termination, backend mTLS to the server

Use this when the proxy itself must authenticate to the server:

- Edge or service proxy terminates external TLS
- Proxy establishes mTLS to durable-streams-server
- `transport.mode = "mtls"`
- `proxy.enabled = true`
- `proxy.trusted_proxies` contains only proxy addresses
- `proxy.identity.mode = "header"` is only allowed in this mode

Example:

```toml
[transport]
mode = "mtls"

[transport.http]
versions = ["http1", "http2"]

[transport.tls]
cert_path = "/etc/durable-streams/tls/server.crt"
key_path = "/etc/durable-streams/tls/server.key"
client_ca_path = "/etc/durable-streams/tls/proxy-ca.crt"
alpn_protocols = ["http/1.1", "h2"]

[proxy]
enabled = true
forwarded_headers = "forwarded"
trusted_proxies = ["10.0.10.15/32", "10.0.10.16/32"]

[proxy.identity]
mode = "header"
header_name = "x-client-identity"
require_tls = true
```

### Envoy example

Illustrative deployment shape:

```text
client -> Envoy (TLS or mTLS) -> durable-streams-server (HTTP or mTLS)
```

If Envoy terminates external TLS and forwards to backend HTTP:

- trust only Envoy addresses in `proxy.trusted_proxies`
- configure Envoy to emit either `X-Forwarded-*` or `Forwarded`
- keep `proxy.forwarded_headers` aligned with what Envoy emits

If Envoy forwards to backend mTLS:

- use `transport.mode = "mtls"`
- issue Envoy a client cert signed by `transport.tls.client_ca_path`
- if Envoy injects an identity header, keep `proxy.identity.mode = "header"` and
  scope `trusted_proxies` narrowly

## HTTP Version Policy

Current protocol support:

- `http` mode: `http1` only
- `tls` mode: `http1` or `http1 + http2`
- `mtls` mode: `http1` or `http1 + http2`
- `http3`: rejected at config validation time because it is not implemented

ALPN must match the configured HTTP versions:

- `http1` requires `http/1.1`
- `http2` requires `h2`

If you enable `http2`, keep the ALPN list consistent:

```toml
[transport.http]
versions = ["http1", "http2"]

[transport.tls]
alpn_protocols = ["http/1.1", "h2"]
```

## Migration From Legacy TLS Config

Legacy compatibility still exists for the old TLS/log fields, but new
deployments should author the explicit transport model directly.

Field mapping:

- `server.port` -> `server.bind_address = "0.0.0.0:<port>"`
- `[tls].cert_path` -> `[transport.tls].cert_path`
- `[tls].key_path` -> `[transport.tls].key_path`
- `[log].rust_log` -> `[observability].rust_log`

Typical migration steps:

1. Replace `server.port` with `server.bind_address`.
2. Move legacy TLS paths under `[transport.tls]`.
3. Add `transport.mode = "tls"` or `transport.mode = "mtls"`.
4. Add `transport.http.versions` and matching `transport.tls.alpn_protocols`.
5. If a proxy is in front, move any old implicit trust assumptions into
   `proxy.enabled`, `proxy.forwarded_headers`, and `proxy.trusted_proxies`.
6. If proxy identity handoff is needed, move to `transport.mode = "mtls"` and
   configure `[proxy.identity]`.

Minimal before:

```toml
[server]
port = 4437

[tls]
cert_path = "/etc/ds/server.crt"
key_path = "/etc/ds/server.key"

[log]
rust_log = "info"
```

Minimal after:

```toml
[server]
bind_address = "0.0.0.0:4437"

[transport]
mode = "tls"

[transport.http]
versions = ["http1", "http2"]

[transport.tls]
cert_path = "/etc/ds/server.crt"
key_path = "/etc/ds/server.key"
alpn_protocols = ["http/1.1", "h2"]

[observability]
rust_log = "info"
```

## Startup Failure Troubleshooting

Startup failures are phase-aware. The process reports the failing phase in the
error message, for example `[check_tls_files]` or `[bind_listener]`.

### `[load_config]`

Likely causes:

- TOML syntax errors
- `--config` points at a missing file
- invalid `DS_*` override values

Check:

- `config/default.toml`, `config/<profile>.toml`, `config/local.toml`
- override file path passed with `--config`
- environment values such as `DS_TRANSPORT__MODE`, `DS_TRANSPORT__TLS__MIN_VERSION`

### `[validate_config]`

Likely causes:

- `transport.mode` and TLS fields do not agree
- `transport.http.versions` and `transport.tls.alpn_protocols` do not match
- `proxy.enabled = true` without `trusted_proxies`
- `proxy.identity.mode = "header"` without `transport.mode = "mtls"`
- invalid `server.bind_address`, base path, or CIDR values

Check:

- transport mode and all required TLS paths
- ALPN vs HTTP version consistency
- proxy trust and identity sections

### `[check_tls_files]`

Likely causes:

- cert, key, or client CA path missing
- path points at a directory, not a file
- file exists but the process cannot read it

Check:

```bash
ls -l /etc/durable-streams/tls
```

Make sure the configured path names exist and are readable by the service user.

### `[build_tls_context]`

Likely causes:

- PEM file contents are malformed
- certificate chain is empty
- private key is missing or does not match the certificate
- client CA bundle is invalid

Check:

- cert/key pair correctness
- PEM encoding
- CA bundle contents for mTLS deployments

### `[bind_listener]`

Likely causes:

- address already in use
- insufficient privileges for the port
- invalid bind target at runtime

Check:

```bash
lsof -iTCP:4437 -sTCP:LISTEN
```

If binding to a privileged port, ensure the process manager and user privileges
match your deployment model.

### `[start_server]`

Likely causes:

- runtime listener or service failure after startup
- storage backend initialisation failure

Check:

- storage directory permissions and available space
- acid/file backend startup logs
- any preceding error chain in the process logs

## Library Use

For embedding, the main entry points are:

- `Config` and `ConfigLoadOptions` for configuration loading
- `Server`, `RunningServer`, and `RouterOptions` for HTTP routes and worker lifecycle
- `InMemoryStorage`, `FileStorage`, and `AcidStorage` for backend selection

## Example Config Files

The crate ships profile-oriented example files:

- `config/default.toml`
- `config/dev.toml`
- `config/prod.toml`
- `config/prod-tls.toml`
- `config/prod-mtls.toml`

Use them as layered building blocks rather than copying the entire merged
config by hand.

## Verification

```bash
cargo build -p durable-streams-server
cargo test -p durable-streams-server
cargo clippy -p durable-streams-server --all-targets
cargo fmt --all
```

### Subscriptions and partial forks

The server supports `Stream-Fork-Sub-Offset` (bytes for non-JSON streams, message
count for JSON streams), including initial-body creation and independent writer
state. Subscriptions are mounted under the stream root's reserved `__ds` prefix.
They support signed webhooks, pull-wake delivery, durable cursors, worker leases,
and generation fencing. See [the subscription guide](../../docs/subscriptions.md)
for request examples, persistence, and deployment configuration.

### Migrating the server API

The next breaking release replaces router construction with an initialized
`Server` and an explicit `start` step. `StreamService` consolidates stream
operations and accepts `Arc<dyn Storage>` for runtime backend selection.

```rust
use durable_streams_server::{Config, InMemoryStorage, RouterOptions, Server, Storage, StreamService};
use std::sync::{Arc, atomic::AtomicBool};
use tokio_util::sync::CancellationToken;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new(1024 * 1024, 1024 * 1024));
let ready = Arc::new(AtomicBool::new(true));
let shutdown = CancellationToken::new();
let options = RouterOptions::default()
    .with_readiness(ready)
    .with_shutdown(shutdown.clone());
let server = Server::new(StreamService::new(storage), &Config::default(), options)?;
// Construction can happen before entering Tokio; start runs inside the serving runtime.
let running = server.start()?;
let app = running.router();
// Serve `app` using your listener. For shutdown, stop accepting requests, cancel
// live reads, drain the listener, and await the worker:
shutdown.cancel();
running.shutdown().await?;
Ok(())
}
```

See the [migration guide](../../docs/migrations/server-api.md) for route groups,
backend implementation changes, stream creation, append results, and import
failure handling. The default readiness route remains opt-in. HTTP listeners
are owned by the embedding application and must be stopped separately.
