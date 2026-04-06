# durable-streams-rust

Canonical Rust workspace scaffold for Durable Streams components.

This repository now contains a production-oriented initial Rust client and the
workspace plumbing around it.

Current focus:

- a clean, explicit Rust client library
- upstream client-conformance integration
- explicit protocol and standards alignment
- a stable workspace shape for future server work

It does **not** vendor or wrap the upstream example Rust client, and it does
not yet migrate the production server into this workspace.

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
- `crates/durable-streams-server` is a placeholder workspace member so the
  existing server can be brought in later without reshaping the repo.
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
  conformance runs will be wired in once the adapters exist.

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
the Rust conformance adapter binary via Cargo. The server suite expects a
reachable base URL and, in this workspace, an optional launcher script that can
start a local server before the suite runs.
