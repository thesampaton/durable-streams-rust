# Overview

`durable-streams-server` is a Rust implementation of the
[Durable Streams protocol](https://github.com/durable-streams/durable-streams),
built with `axum` and `tokio`.

Use it when you need one of two things:

- a standalone Durable Streams HTTP server
- an embeddable router and storage stack inside an existing `axum` application

The crate supports:

- stream create, append, read, head, close, and delete operations
- live reads via long-poll and Server-Sent Events
- in-memory, file-backed, and ACID (`redb`) storage backends
- optional admin routes for trusted operator tooling
- explicit transport modes: `http`, `tls`, and `mtls`
- layered config via TOML files and `DS_*` environment overrides
- phase-aware startup diagnostics and structured telemetry

# Quick Start

Run the server from the workspace root:

```bash
cargo run -p durable-streams-server
```

By default it listens on `http://0.0.0.0:4437`, exposes `/healthz` and
`/readyz`, and mounts the protocol at `/v1/stream`. Admin routes are disabled
by default.

# Architecture

`storage` owns persistence contracts and backend mechanics. `streams` owns
stream-domain types and services such as `StreamMetadata`, `StreamService`, and
operator list projections. Protocol routes expose the Durable Streams protocol
surface under `http.stream_base_path`.

Admin routes are optional, separate, and operator-focused. When
`admin.enabled = true`, `GET <admin.base_path>/streams` lists streams through
the stream-domain service. Admin and protocol routes are built in separate
internal subrouters. The public router builders return the combined application;
they do not expose a hook for adding middleware to just the admin subrouter.
The server does not implement
authentication or authorization for admin routes; put them behind trusted
networks, reverse proxies, or external access-control layers.

The `durable-streams-server list` CLI command is local-by-default for
disk-backed storage. It opens configured `file-*` or file-backed `acid` storage
directly for operator inspection instead of depending on a running HTTP server. For remote
listing, pass an explicit admin endpoint with `--url`. The `memory` mode and
`acid_backend = "in-memory"` have no persistent state for a separate CLI process
to inspect and require remote listing. Local ACID inspection also requires the
server to be stopped so the CLI can acquire the database lock.

# Configuration

Configuration is resolved in this order, with later sources winning:

1. built-in defaults
2. built-in profile defaults
3. `config/default.toml`
4. `config/<profile>.toml`
5. `config/local.toml`
6. `--config <path>`
7. `DS_*` environment variables

Main entry points:

- [`Config`] for the resolved configuration
- [`ConfigLoadOptions`] for profile and file selection
- [`DeploymentProfile`] for built-in profile selection

## Profiles

The built-in profiles are intended as operator starting points:

| Profile | Typical purpose |
|---------|------------------|
| `default` | Minimal HTTP baseline |
| `dev` | Local loopback development |
| `prod` | Production behind external TLS termination |
| `prod-tls` | Direct TLS termination on the server |
| `prod-mtls` | Direct mTLS termination on the server |

Example:

```bash
cargo run -p durable-streams-server -- --profile prod-tls --config /etc/durable-streams/server.toml
```

## Environment Overrides

Environment keys map directly from the TOML path:

- `transport.mode` -> `DS_TRANSPORT__MODE`
- `transport.tls.cert_path` -> `DS_TRANSPORT__TLS__CERT_PATH`
- `proxy.identity.header_name` -> `DS_PROXY__IDENTITY__HEADER_NAME`

In practice, most deployments only need a small subset:

| Purpose | Variables |
|---------|-----------|
| Bind and logging | `DS_SERVER__BIND_ADDRESS`, `DS_OBSERVABILITY__RUST_LOG`, `RUST_LOG` |
| Storage selection | `DS_STORAGE__MODE`, `DS_STORAGE__DATA_DIR` |
| Admin routes | `DS_ADMIN__ENABLED`, `DS_ADMIN__BASE_PATH` |
| Direct TLS | `DS_TRANSPORT__MODE`, `DS_TRANSPORT__TLS__CERT_PATH`, `DS_TRANSPORT__TLS__KEY_PATH` |
| Direct mTLS | `DS_TRANSPORT__TLS__CLIENT_CA_PATH` |
| Reverse proxy trust | `DS_PROXY__ENABLED`, `DS_PROXY__FORWARDED_HEADERS`, `DS_PROXY__TRUSTED_PROXIES` |
| Proxy identity handoff | `DS_PROXY__IDENTITY__MODE`, `DS_PROXY__IDENTITY__HEADER_NAME`, `DS_PROXY__IDENTITY__REQUIRE_TLS` |

The full operator-oriented deployment guide and example TOMLs live in the
crate [README](https://docs.rs/crate/durable-streams-server/latest/source/README.md).

# Embedding

Most embedders only need:

- [`build_router`] to mount the Durable Streams HTTP API into an `axum` app
- [`build_router_with_ready`] when you want readiness and shutdown plumbing
- [`Storage`] plus one of [`InMemoryStorage`], [`FileStorage`], or [`AcidStorage`]

The default mount path constant is [`DEFAULT_STREAM_BASE_PATH`].

# Module Guide

- [`config`] contains config loading, profiles, and validation
- [`router`] exposes the main embedding entry points
- [`storage`] contains the backend trait and backend implementations
- [`startup`] contains startup preflight, typed startup errors, and TLS bootstrap
- [`protocol`] contains lower-level protocol types useful in tests and integrations
- [`transfer`] contains JSON export/import for backup and migration workflows

# Verification

Typical commands when working on the crate:

```bash
cargo build -p durable-streams-server
cargo test -p durable-streams-server
cargo clippy -p durable-streams-server --all-targets
```
