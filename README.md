# durable-streams-rust

Canonical Rust workspace for Durable Streams components.

This repository contains the Rust client, the Rust server, and the workspace
plumbing around them.

Current focus:

- a clean, explicit Rust client library
- a production-oriented Rust server with multiple storage backends and
  transport modes
- upstream client-conformance integration
- upstream server-conformance integration
- explicit protocol and standards alignment

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
|   |-- check-server-public-api.sh
|   `-- conformance
`-- tests
    |-- README.md
    `-- conformance
```

## Repository Intent

- `crates/durable-streams-client` is the active client implementation. It
  provides a typed async API, config loader, auth model, retry policy,
  idempotent producer support, JSON and JSONL ingest helpers, file-backed
  JSONL journaling, and journal-backed read replication.
- `crates/durable-streams-server` is the production-oriented server crate,
  providing in-memory, file-backed, and ACID (`redb`) storage backends;
  stream forking with soft-delete and cascade garbage collection; typed
  transport (`http`, `tls`, `mtls`); reverse-proxy trust gating; phased
  structured startup; `application/problem+json` error responses; readiness
  and liveness probes; graceful shutdown; request telemetry; and a
  `clap`-based CLI with `serve`, `list`, `export`, and `import`
  subcommands.
- `tests/conformance` and `scripts/conformance` define where upstream Durable
  Streams conformance adapters and runners live.
- `docs/architecture.md` describes the intended long-term shape.
- `docs/standards.md` records upstream protocol and conformance-suite alignment.

## Client Status

The Rust client is implemented and wired into the upstream client conformance
suite.

- Library crate: `crates/durable-streams-client`
- Conformance adapter:
  `crates/durable-streams-client/src/bin/client-conformance-adapter.rs`
- Client launcher used by the upstream suite:
  `tests/conformance/client/run-adapter.sh`

Run the suite locally with `./scripts/conformance/run-client-suite.sh`. The
skipped cases in the adapter are optional capabilities it explicitly declares
unsupported (for example, higher-level batching and retry-options validation).

## Server Status

The Rust server lives in this workspace as `crates/durable-streams-server`.

- Binary crate and library crate name: `durable-streams-server`
- Current version: `0.3.0` (release tag
  `durable-streams-server-v0.3.0`)
- Integration and unit tests are crate-local under
  `crates/durable-streams-server/tests`
- Workspace launcher used by the upstream server suite:
  `tests/conformance/server/start-server.sh`

See `crates/durable-streams-server/README.md` and
`crates/durable-streams-server/CHANGELOG.md` for server-specific documentation
and release notes.

## Client Usage

The client is designed for service-style construction:

- typed config via `ClientConfig`
- layered TOML loading via `ClientConfigLoader`
- environment overrides with the `DURABLE_STREAMS_CLIENT__` prefix
- explicit auth configuration for bearer/basic/header-based gateways
- direct async operations through `Client` and `StreamHandle`
- idempotent producer fencing via `IdempotentProducer`
- local JSONL journaling via `JsonJournal`
- journal-backed read replication with offset resume via `ReadReplica`
- a `raw` module for protocol-shaped request/response types when direct
  protocol control is needed

See `crates/durable-streams-client/README.md` for crate-specific examples.

## Standards Alignment

The workspace treats the upstream Durable Streams protocol and conformance suites
as external standards it aligns with explicitly.

- Protocol revision, client-conformance package, and server-conformance
  package are pinned under `[workspace.metadata.durable-streams]` in
  `Cargo.toml`.
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
- CI enforces `cargo clippy -- -D warnings` with workspace-level
  `clippy::pedantic` on `durable-streams-server`; the same lints run
  advisory on `durable-streams-client` until that crate is ready for
  strict enforcement.
- The MSRV may be raised deliberately over time, but only as an explicit policy
  change.

## Releases

Per-crate release tags follow `<crate-name>-v<version>`, matching
`cargo release` defaults and common Rust-workspace convention.

- Most recent server release: `durable-streams-server-v0.3.0`.
- `crates/durable-streams-client` is currently unpublished
  (`publish = false`) and not yet tagged.

A standing "release PR" is maintained automatically by
[`release-plz`](https://release-plz.ieni.dev/) against `trunk`. It
reflects the current `[Unreleased]` CHANGELOG state and proposes a
version bump derived from Conventional Commit prefixes since the
last tag. To cut a release, review and merge that PR, then tag and
publish — see [CONTRIBUTING.md](CONTRIBUTING.md#releasing) for the
full process.

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
# stop on first failure
./scripts/conformance/run-client-suite.sh --fail-fast
```

To run the upstream server conformance suite from this workspace (defaults to
the in-memory backend):

```bash
./scripts/conformance/run-server-suite.sh
```

To exercise the file-backed or ACID backends against the same suite:

```bash
# file-backed
DS_STORAGE__MODE=file-fast ./scripts/conformance/run-server-suite.sh

# ACID (redb)
DS_STORAGE__MODE=acid DS_STORAGE__ACID_BACKEND=memory ./scripts/conformance/run-server-suite.sh
```

The client suite uses `tests/conformance/client/run-adapter.sh`, which launches
the Rust conformance adapter binary via Cargo. The server suite uses
`tests/conformance/server/start-server.sh` by default, which launches the
workspace server crate on the configured base URL before handing off to the
upstream suite.
