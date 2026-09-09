# durable-streams-rust

Rust server and client for the [Durable Streams protocol](https://github.com/durable-streams/durable-streams).
Requires **Rust 1.89 or later**.

- [Server](crates/durable-streams-server/README.md): a standalone HTTP server and
  embeddable Rust library, with live reads, forks, subscriptions, and
  memory, file, or transactional `redb` storage.
- [Client](crates/durable-streams-client/README.md): an async Rust client with
  producer sequencing, JSON/JSONL ingest, and local journal replication.
  The client is currently unpublished.

## Run the server

From the repository root:

```bash
cargo run -p durable-streams-server -- --profile dev
```

The development profile listens on `http://127.0.0.1:4437`, with stream URLs
under `/v1/stream`. Storage is in memory unless configured otherwise.
See the [server guide](crates/durable-streams-server/README.md#configuration)
for persistence and deployment profiles.

## Development

```bash
cargo check --workspace --all-targets
cargo test --workspace
```

- [Contributing](CONTRIBUTING.md): required checks, MSRV policy, and release process.
- [Conformance harness](tests/conformance/README.md): run the upstream suites.
- [Architecture](docs/architecture.md): crate, service, and storage boundaries.
- [Standards](docs/standards.md): pinned protocol, package versions, and validation records.
- [Server migration](docs/migrations/server-api.md) and
  [changelog](crates/durable-streams-server/CHANGELOG.md): breaking changes.

Licensed under the [MIT License](LICENSE).
