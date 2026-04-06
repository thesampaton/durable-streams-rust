# durable-streams-rust

Canonical Rust workspace scaffold for Durable Streams components.

This repository now contains the Rust client, the migrated Rust server, and the
workspace plumbing around them.

Current focus:

- a clean, explicit Rust client library
- the existing Rust server migrated into the workspace with minimal behavioural change
- upstream client-conformance integration
- upstream server-conformance integration
- explicit protocol and standards alignment
- a stable long-term workspace shape for both crates

It does **not** vendor or wrap the upstream example Rust client.

## Workspace Layout

```text
.
|-- Cargo.toml
|-- CONTRIBUTING.md
|-- README.md
|-- crates
|   |-- durable-streams-client
|   `-- durable-streams-server
|-- docs
|   |-- architecture.md
|   `-- standards.md
|-- scripts
|   `-- conformance
`-- tests
    |-- README.md
    `-- conformance
```

## Repository Intent

- `crates/durable-streams-client` is the active client implementation. It
  provides a typed async API, config loader, auth model, retry policy, and
  idempotent producer support.
- `crates/durable-streams-server` is the migrated production-oriented server
  crate, carried into this workspace from the former standalone repository with
  its existing runtime, storage, and protocol behaviour preserved as much as
  practical.
- `tests/conformance` and `scripts/conformance` define where upstream Durable
  Streams conformance adapters and runners live.
- `docs/architecture.md` describes the intended long-term shape.
- `docs/standards.md` records upstream protocol and conformance-suite alignment.

## Client Status

The initial Rust client is implemented and wired into the upstream client
conformance suite.

- Library crate: `crates/durable-streams-client`
- Conformance adapter:
  `crates/durable-streams-client/src/bin/client-conformance-adapter.rs`
- Client launcher used by the upstream suite:
  `tests/conformance/client/run-adapter.sh`

The most recent local fail-fast client conformance run completed with:

- `255 passed`
- `0 failed`
- `14 skipped`

The skipped cases are optional capabilities currently declared unsupported by
the adapter, such as higher-level batching and retry-options validation.

## Server Status

The Rust server now lives in this workspace as `crates/durable-streams-server`.

- Binary crate and library crate name: `durable-streams-server`
- Existing integration and unit tests are crate-local under
  `crates/durable-streams-server/tests`
- Workspace launcher used by the upstream server suite:
  `tests/conformance/server/start-server.sh`

The migration goal was continuity rather than redesign, so the server code and
its operational semantics are intentionally kept close to the previous
standalone repository.

## Client Usage

The client is designed for service-style construction:

- typed config via `ClientConfig`
- layered TOML loading via `ClientConfigLoader`
- environment overrides with the `DURABLE_STREAMS_CLIENT__` prefix
- explicit auth configuration for bearer/basic/header-based gateways
- direct async operations through `Client` and `StreamHandle`

See `crates/durable-streams-client/README.md` for crate-specific examples.

## Standards Alignment

The workspace treats the upstream Durable Streams protocol and conformance suites
as external standards it aligns with explicitly.

- Protocol baseline and governance notes live in `docs/standards.md`.
- Exact npm package pins for the upstream conformance suites live in
  `package.json`.
- Shell entrypoints in `scripts/conformance` show where client and server
  conformance runs are wired in.

## MSRV Policy

The workspace MSRV is **Rust 1.89**.

- Each crate declares `rust-version = "1.89"`.
- CI validates `cargo check --workspace --all-targets` on Rust `1.89.0` and on
  current stable.
- CI also runs `cargo clippy --workspace --all-targets` with workspace-level
  `clippy::pedantic` enabled as a standing baseline.
- The MSRV may be raised deliberately over time, but only as an explicit policy
  change.

## Quick Start

```bash
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets
cargo test --workspace
```

To install the upstream conformance tooling:

```bash
npm install
```

To run the Rust client crate checks directly:

```bash
cargo check -p durable-streams-client
cargo test -p durable-streams-client
```

To run the upstream client conformance suite against the adapter in this repo:

```bash
./scripts/conformance/run-client-suite.sh
./scripts/conformance/run-client-suite.sh --fail-fast
```

To run the upstream server conformance suite once a server launcher is ready:

```bash
./scripts/conformance/run-server-suite.sh
```

The client suite uses `tests/conformance/client/run-adapter.sh`, which launches
the Rust conformance adapter binary via Cargo. The server suite uses
`tests/conformance/server/start-server.sh` by default, which launches the
workspace server crate on the configured base URL before handing off to the
upstream suite.
