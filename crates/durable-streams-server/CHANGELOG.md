# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_No notable changes yet._

## [0.3.0] - 2026-04-15

### Breaking Changes

- The public configuration surface has been reworked around the new nested
  config model. Existing `Config` field access, config file structure, and
  string-based config/load validation handling may need to be updated when
  moving from `0.2.x`.
- `http.stream_base_path` is now validated strictly and no longer accepts a
  trailing slash except for the root path `/`. Existing configs such as
  `"/v1/stream/"` must be updated.

### Added

- Support for the latest draft Durable Streams protocol updates, including
  stream forking, sliding TTL renewal, and the related storage lifecycle
  changes needed to keep server behaviour aligned with the evolving spec.
- First-class transport and proxy configuration via structured `[transport]`
  and `[proxy]` sections, including direct TLS, mTLS, trusted proxy handling,
  and production profile examples for common deployment patterns.
- CLI subcommands — the server binary now uses `clap` and exposes `serve`
  (default), `list`, `export`, and `import` subcommands for operational
  stream management without external tooling.
- Stream export and import — `export` serialises streams and their messages
  to a versioned JSON format; `import` restores them with configurable
  conflict handling (skip, fail, or replace existing streams). Useful for
  backups, cross-backend migration, and disaster recovery.
- Public API snapshot test — a nightly-only test guards against accidental
  changes to the crate's public API surface.

### Changed

- Configuration layout restructured — `port` moves to
  `server.bind_address`, `tls.*` moves to `[transport.tls]`,
  `long_poll_timeout_secs` and `sse_reconnect_interval_secs` move to
  `[transport.connection]`, and `[log]` becomes `[observability]`.
- Startup now reports phased, typed errors for config loading, validation,
  TLS preflight, and listener binding.
- Server conformance suite updated to
  `@durable-streams/server-conformance-tests` 0.3.0 (299 tests), covering the
  latest draft-spec behaviours added in that release.
- Streams now track soft-delete state — deleting a stream that has child
  forks returns 410 Gone instead of removing data, preserving fork
  read integrity. The stream is hard-deleted once all forks are removed.

### Fixed

- Assorted fixes across configuration validation, storage lifecycle
  bookkeeping, and test infrastructure.

## [0.2.0] - 2026-04-13

### Added

- Hierarchical stream names — stream names containing `/` (e.g.
  `slides/abc123`, `org/project/resource`) now work as expected, matching
  the Node.js reference server behaviour.
- Configurable stream name limits via `limits.max_stream_name_bytes` /
  `DS_LIMITS__MAX_STREAM_NAME_BYTES` (default 1024) and
  `limits.max_stream_name_segments` / `DS_LIMITS__MAX_STREAM_NAME_SEGMENTS`
  (default 8) to bound name length and nesting depth.
- `/readyz` readiness endpoint — returns 503 until storage initialization
  completes, 200 thereafter. Use `build_router_with_ready()` to enable it.
- `cleanup_expired_streams()` method on the `Storage` trait for proactive
  removal of expired streams (previously only cleaned lazily on access).
- Graceful shutdown support — long-poll and SSE connections now drain cleanly
  when the server shuts down, returning 204 / ending the stream instead of
  resetting the connection. Controlled via a `CancellationToken` passed to
  `build_router_with_ready()`.
- Request telemetry middleware that records `tracing` fields using
  OpenTelemetry/ECS-style dotted names (`ds.*`, `http.*`, `server.address`,
  `error.*`) so logs can map cleanly into a future OpenTelemetry pipeline.
- Configurable protocol mount path via `http.stream_base_path` /
  `DS_HTTP__STREAM_BASE_PATH`, while keeping `/v1/stream` as the default.
- Configurable redb backend for the acid storage mode. The new `acid_backend`
  config field (`DS_STORAGE__ACID_BACKEND` env var) selects between the default
  file-backed redb and an in-memory redb backend. The in-memory variant provides
  full ACID transaction semantics without disk I/O, useful for testing and
  ephemeral workloads.

### Changed

- Server `4xx`/`5xx` responses now use RFC 9457-style
  `application/problem+json` payloads with machine-readable error codes and
  request `instance` paths.
- Producer fencing, sequence-gap, readiness, and closed-stream failures retain
  their protocol headers while now returning structured problem details bodies.
- Crate now lives in the [`durable-streams-rust`](https://github.com/thesampaton/durable-streams-rust)
  workspace alongside the client crate.
- Overhaul rustdoc surface: add crate-level documentation, module-level
  summaries, and doc comments across config, storage, protocol, and router
  modules.
- Acid storage now uses `Database::builder()` instead of `Database::create()`
  for explicit builder-pattern construction of shard databases.
- Remove unused `wrap_read` function from `json_mode` module. Callers should
  use `wrap_read_iter` directly.
- `wrap_read_iter` now returns `Bytes` instead of `Result<Bytes>` since it
  cannot fail. `build_body` and `build_data_response` updated accordingly.

### Fixed

- Stream names containing `/` no longer return 404. The route wildcard was
  single-segment (`/{name}`) and now uses a catch-all (`/{*name}`).
- Stream names with `.` or `..` segments, empty segments, or trailing
  slashes are now rejected with a 400 `INVALID_STREAM_NAME` problem
  response instead of being silently accepted.
- Fix concurrent read corruption in `FileStorage` where multiple readers
  could clobber each other's file positions via `dup()`'d descriptors.
  Reads now use positional I/O (`pread`) on Unix.
- Fix TOCTOU race in `AcidStorage` notifier management that could produce
  disconnected broadcast senders under concurrent subscribe/unsubscribe.
- Resolve clippy `unnecessary_wraps` and `dead_code` warnings that caused
  `cargo clippy -- -D warnings` to fail.

## [0.1.3] - 2026-04-06

### Changed

- Upgrade `axum-server` 0.7.3 → 0.8.0 (generic `Server`/`Handle`, unix socket
  support, HTTP version filtering).
- Upgrade `redb` 3 → 4.0.0 (picks up `AccessGuardMut` data-loss fix; no code
  changes required, on-disk format compatible with 3.x).
- Update all transitive dependencies (tokio 1.51, hyper 1.9, rustls-webpki
  0.103.10, arc-swap 1.9, mio 1.2, and others).

### Removed

- Remove unused `tokio-test` dev-dependency.

## [0.1.2] - 2026-03-23

### Fixed

- SSE JSON batching: batch messages into single `data` event per read.
- Spec 01 stream lifecycle: PUT accepts body, Content-Type is optional.

### Changed

- Update docs to reflect all storage backends (memory, file, acid).
- Add `DS_LOG__RUST_LOG` to README env var table.

## [0.1.1] - 2026-03-16

### Fixed

- Fix fmt check in memory storage test.
- Include invalid values in env override error messages.

### Changed

- Shorten crate name to `durable-streams-server`.
- Remove unused `tower` and `rand` dependencies, exclude `CLAUDE.md` from crate.
- Exclude non-essential files from crates.io package.
- Add keywords and categories for crates.io discoverability.

[0.3.0]: https://github.com/thesampaton/durable-streams-rust/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/thesampaton/durable-streams-rust/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/thesampaton/durable-streams-rust-server/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/thesampaton/durable-streams-rust-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/thesampaton/durable-streams-rust-server/releases/tag/v0.1.1
