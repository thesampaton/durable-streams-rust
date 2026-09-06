# durable-streams-server

A [Durable Streams](https://github.com/durable-streams/durable-streams) server
built with `axum` and `tokio`. Run it as a standalone HTTP server or embed it
with `Server` and `StreamService`.

It supports live reads through long-polling and SSE, stream forks,
[subscriptions](../../docs/subscriptions.md), and memory, file, or transactional
`redb` storage.

## Running

From the workspace root:

```bash
cargo run -p durable-streams-server -- --profile dev
```

The development profile listens on `http://127.0.0.1:4437`. Without a profile,
the default address is `http://0.0.0.0:4437`. Both mount streams at `/v1/stream`
and expose `/healthz` and `/readyz`. Admin routes are disabled by default.

For local persistence:

```bash
DS_STORAGE__MODE=file DS_STORAGE__DATA_DIR=./data \
  cargo run -p durable-streams-server -- --profile dev
```

| Storage mode | Behaviour |
| --- | --- |
| `memory` (default) | Data is lost when the process stops |
| `file` | Per-stream logs with synced initial data and journalled append/replacement commits |
| `acid` | Transactional `redb` storage; defaults to disk, with an optional in-memory backend |

The file backend's creation/deletion recovery limits and import failure
behaviour are documented in the [migration guide](../../docs/migrations/server-api.md#import-and-file-persistence).

## Configuration

The checked-in TOML files are the configuration examples. Start with the
[annotated baseline](config/default.toml), then select a profile:

| Profile | Example file | Deployment |
| --- | --- | --- |
| `default` | [default.toml](config/default.toml) | Minimal HTTP defaults |
| `dev` | [dev.toml](config/dev.toml) | Local development on loopback |
| `prod` | [prod.toml](config/prod.toml) | HTTP behind external TLS termination |
| `prod-tls` | [prod-tls.toml](config/prod-tls.toml) | Server terminates TLS |
| `prod-mtls` | [prod-mtls.toml](config/prod-mtls.toml) | Server requires client certificates |

Later sources override earlier ones:

1. Built-in defaults and profile defaults.
2. `default.toml`, `<profile>.toml`, then `local.toml` in the config directory.
3. An explicit `--config <path>` file.
4. Environment variables.

The default config directory is the server crate's `config` directory captured
at build time, not a `config` directory relative to the running process.
Deployed binaries should use an explicit override file or environment variables.
Embedders can set `ConfigLoadOptions::config_dir`. Missing layered files are
skipped; a missing explicit `--config` file is an error.

```bash
cargo run -p durable-streams-server -- --profile prod-tls --config /etc/durable-streams/server.toml
```

Environment names follow the TOML path: `storage.mode` becomes
`DS_STORAGE__MODE`, and `transport.tls.cert_path` becomes
`DS_TRANSPORT__TLS__CERT_PATH`. `RUST_LOG` overrides the configured tracing filter.

TLS profiles need certificate and key paths; mTLS also needs a client CA.
Production profiles require explicit CORS origins, or `http.allow_wildcard_cors`
if wildcard access is intentional. The profile files include the TLS and proxy
settings to override. Plain HTTP supports HTTP/1; TLS and mTLS also support
HTTP/2. ALPN is derived from the HTTP versions unless explicitly configured.
HTTP/3 is unsupported.

Behind a proxy, enable forwarded headers only for the proxy addresses in
`proxy.trusted_proxies`. Proxy identity headers require an mTLS connection to
the server. The server provides no stream/admin access-control policy; supply
that in the gateway or embedding application.

## Operator commands

The executable provides `serve` (the default), `list`, `export`, and `import`.
Use `cargo run -p durable-streams-server -- --help` or `<command> --help` for
options.

`list` opens local file or ACID storage unless given a full admin endpoint with
`--url`. In-memory storage requires remote listing. Stop the server before
local ACID inspection so the CLI can acquire the database lock. Remote listing
requires `admin.enabled = true`; protect that endpoint with access control.

Export/import transfers application streams, not subscription control state or
signing keys. See the [import contract](../../docs/migrations/server-api.md#import-and-file-persistence)
before using replacement imports.

## Embedding

Use `StreamService` for stream operations and `Server` / `RunningServer` for
HTTP routes and worker ownership. `Server::new` is fallible and can run before
Tokio; `start` requires a runtime. Clone running handles or routers to share one
server across listeners.

`protocol_router()`, `admin_router()`, and `probe_router()` support separate
middleware. Embedded readiness is opt-in through `RouterOptions::with_readiness`.
The application owns its HTTP listener: cancel live reads, drain the listener,
then await `RunningServer::shutdown()` to join the worker. Storage operations
remain synchronous and can block the calling thread.

Run `cargo doc -p durable-streams-server --no-deps --open` for compiled Rust
examples and API details. The [migration guide](../../docs/migrations/server-api.md)
covers construction, ownership, creation options, and append results.

## Troubleshooting startup

Startup errors name the failing phase and include the underlying cause:

| Phase | Check |
| --- | --- |
| `load_config` | TOML syntax, explicit file path, and environment values |
| `validate_config` | Transport/TLS agreement, HTTP versions and ALPN, proxy trust, paths, and limits |
| `check_tls_files` | Certificate, key, and CA files exist and are readable by the service user |
| `build_tls_context` | PEM encoding, certificate/key agreement, and CA bundle contents |
| `bind_listener` | Address availability and permission to bind the port |
| `start_server` | Storage permissions, available space, and the preceding error chain |

For development checks, see [Contributing](../../CONTRIBUTING.md).
