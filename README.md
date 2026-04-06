# durable-streams-rust

Canonical Rust workspace scaffold for Durable Streams components.

This repository is intentionally focused on foundation work first:

- a clean Rust workspace shape
- an initial client crate scaffold
- a placeholder home for the server
- explicit protocol and conformance alignment records
- lightweight CI and harness plumbing

It does **not** yet implement the full Rust client, migrate the production
server, or vendor the upstream example Rust client.

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

- `crates/durable-streams-client` is the future clean Rust client
  implementation.
- `crates/durable-streams-server` is a placeholder workspace member so the
  existing server can be brought in later without reshaping the repo.
- `tests/conformance` and `scripts/conformance` define where upstream Durable
  Streams conformance adapters and runners will live.
- `docs/architecture.md` describes the intended long-term shape.
- `docs/standards.md` records upstream protocol and conformance-suite alignment.

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

To prepare for future conformance runs:

```bash
npm install
./scripts/conformance/run-client-suite.sh
./scripts/conformance/run-server-suite.sh
```

The client suite expects an adapter executable. The server suite expects a
reachable base URL and, in this scaffold, an optional launcher script that can
start the local server process before the suite runs.
