# Conformance Harness

This directory contains workspace-level integration with the upstream Durable
Streams conformance suites.

## Layout

- `client/run-adapter.sh` is the client entrypoint used by the upstream client
  suite. In this workspace it launches the Rust conformance adapter binary from
  `crates/durable-streams-client`.
- `server/start-server.sh` is the local launcher used to boot a server before
  running the upstream server suite against a base URL.

## Intent

The upstream suites are external standards-alignment checks. They are therefore
tracked at workspace level rather than being hidden inside a single crate.

## Client Conformance

Install the pinned npm dependencies first:

```bash
npm install
```

Run the full client suite:

```bash
./scripts/conformance/run-client-suite.sh
```

Run the fail-fast variant used for local iteration:

```bash
./scripts/conformance/run-client-suite.sh --fail-fast
```

The client runner invokes `tests/conformance/client/run-adapter.sh`, which
currently executes:

```bash
cargo run --quiet -p durable-streams-client --bin client-conformance-adapter -- "$@"
```

## Server Conformance

The upstream server suite remains workspace-level. The local launcher now boots
the migrated Rust server crate from this workspace. Use:

```bash
./scripts/conformance/run-server-suite.sh
```

The default launcher is `tests/conformance/server/start-server.sh`, which
executes:

```bash
cargo run --quiet -p durable-streams-server --
```
