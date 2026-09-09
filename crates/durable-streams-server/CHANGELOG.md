# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0](https://github.com/thesampaton/durable-streams-rust/compare/v0.3.0...v0.4.0) - 2026-09-09

### Added

- *(server)* bound storage execution and drain admitted jobs
- *(server)* [**breaking**] consolidate file storage into one synced mode
- *(server)* [**breaking**] consolidate APIs and make stream mutations atomic
- *(server)* [**breaking**] consolidate router construction with runtime options
- *(server)* implement durable subscriptions and delivery leases

### Fixed

- *(server)* keep ACID reads and TTL renewal in one transaction
- *(server)* restore the protocol and conformance stack base
- *(server)* align fork creation and SSE resume semantics
- *(server)* preserve CLI listing JSON compatibility
- *(server)* resolve listing lints and validate admin route boundaries
- *(server)* rewrite cascade-delete loops as while-let

### Other

- *(server)* document simplification and condense reviews
- *(server)* simplify initialized subscription state
- *(server)* unify configuration and boundary parsing
- *(server)* share storage operations and fork reads
- *(server)* demonstrate bounded blocking execution and shutdown
- simplify guides and link configuration examples
- *(server)* pin the compiler used for public API snapshots
- *(server)* require the public API snapshot on pull requests
- add MIT license to the workspace and crate packages
- Refactor durable streams implementation
- *(server)* clarify storage read capture and lock boundaries
- *(server)* define error responses in one exhaustive mapping
- Revert "feat(server): align with latest protocol and subscription conformance"
- validate stacked PRs with current Clippy
- *(conformance)* upgrade server suite to 0.3.6
- Document admin and stream API boundaries
- Restore local-first stream list CLI
- Add opt-in admin stream listing routes
- Stabilize stream domain metadata API
- describe offset panic invariant
- clean up server helper patterns
- *(server)* widen expiry window in resource_cleanup tests to stop CI flakes
- *(config)* consolidate string enums via macro and dedupe prod patches
- *(protocol)* remove `Error::InvalidBody` and replace with dedicated invalid-body problem response
- *(server)* deduplicate storage construction and serve branches
- Merge branch 'trunk' into refactor/handler-boilerplate
- *(storage)* fix text for documentation FileFast vs FileDurable and expand imports in file storage implementation
- *(storage)* refine storage durability wording
- *(storage)* improve storage backend docs
- *(storage)* split file storage and test helpers by concern
- Refactor storage cascade and create paths
- *(storage)* consolidate StreamMetadata construction via shared helper
- *(storage)* extract create_fork and cascade_delete policy helpers
- *(storage)* collapse create_stream disposition match via resolve_root_create
- *(storage)* extract shared append prechecks across backends
- Expand startup resilience tests per backend
- replace with_each_backend closure with macro-generated per-backend tests
- add PR gates, pre-release workflow, and release-plz standing PR

### Breaking Changes

- Consolidate `file-fast` and `file-durable` into `file` (`StorageMode::File`).
  Remove their aliases, `StorageMode::sync_on_append()`, and the boolean argument
  to `FileStorage::new`. Existing data directories need no conversion. Initial
  data remains synced; appends/replacements sync at the journal commit boundary.
- Replace router builders with `Server::new(StreamService, config, options)` and
  explicit `start()`. Construction is fallible, supports `Arc<dyn Storage>`, and
  validates HTTP settings before mounting. Cloned running handles/route groups
  share control state and one worker; shutdown can await completion.
- Move HTTP stream operations through `StreamService`. Add composable protocol,
  admin, and probe routers for middleware placement.
- Separate creation inputs (`StreamOptions`, `Expiry`) from resolved
  `StreamConfig` metadata. TTL/deadline resolution belongs to storage.
- Replace `batch_append` with atomic `append_batch`, including optional closure.
  Both `append` and `append_batch` return named starting/resume offsets and
  closed state. `exists` and `subscribe` now propagate backend errors.
- Require extended forks, replacement, and subscription persistence in the
  storage contract. Make extensible configuration, error, and output types
  non-exhaustive; add output constructors for backend implementations.
- Remove unused `ShutdownToken`, `LongPollTimeout`, `SseReconnectInterval`, and
  the compatibility `storage::StreamMetadata` path. See `streams::StreamMetadata`.
- Import replacement commits atomically per independent root stream, requires
  staging capacity, and rejects fork lineage. Later failures carry completed
  counts in `TransferError::PartialImport`.

### Added

- Bound server-owned synchronous storage work with `limits.max_storage_jobs`
  (default 64), shared across HTTP routes and subscription persistence. Accepted
  jobs retain capacity and storage ownership after disconnect; shutdown drains
  them while the serving runtime remains alive. Saturation returns 503 with
  `Retry-After: 1`; idle live reads and webhook HTTP remain async.

- Durable subscription APIs with normalized configuration identity, glob and
  explicit membership, signed Ed25519 webhooks and JWKS discovery, pull-wake
  claims, cursor acknowledgements, heartbeats, release, and generation fencing.
- Private subscription persistence in each built-in backend, including signing
  keys, active leases, tombstones, and retry schedules. Failed webhooks retry
  with exponential backoff and jitter; URL validation checks and pins DNS
  results and disables redirects and environment proxies.
- Atomic fork sub-offset creation, inherited content types, and initial bodies
  across the memory, file, and ACID backends.

### Fixed

- ACID creation and replacement retain nonempty batches of zero-byte records,
  advancing record sequence offsets consistently with append and the other backends.
- Memory fork reads retain the requested local payload, tail offset, and closed
  state from one snapshot when another writer appends after the lock is released.
- Long-poll timeout rereads return newly available data instead of advancing an
  empty response past it. Shutdown preserves the last empty snapshot's offset.
- Keep completed webhook results bounded and pending through storage saturation.
  Drain storage even after subscription-worker or listener failure.

- Ordinary final append and closure share one storage commit and resume snapshot.
  File operations use an undo journal to recover interrupted log/metadata updates
  and preserve ordinary append timestamps on reopen. Journal commits sync the
  log, metadata, and directory.
- Validate all import payloads before mutation and preserve originals on pre-commit
  replacement failure. Recovery resolves uncertain final-sync outcomes.
- Initialize direct-Rust TTL streams and reject unrepresentable TTL/deadline arithmetic.
- Limit collected request bodies with `limits.max_request_body_bytes` (default
  10 MiB), including chunked bodies, and return 413 before storage mutation.
- Reject empty JSON arrays in POST even when a close header is present.
- SSE pairs every data event with a control event and a corresponding offset.
- Chained fork reads respect all ancestor bounds and the requested read offset.
- File recovery restores fork-relative offsets and preserves expired ancestors
  still referenced by forks.

### Changed

- Share storage convenience defaults, append validation, byte accounting, fork
  read planning, and backend-local ACID record insertion. Existing storage
  implementations can retain `close_stream` and `create_fork` overrides.
- Load environment settings through the same configuration patch merger as TOML,
  preserving precedence and legacy aliases. Subscription services now hold
  initialized state directly; shared private parsers serve validation and use.
- Track protocol revision `a172acc389351cb3db6deb5cd60e3dec11e7ff39` and server
  conformance `0.3.6`, with subscription coverage enabled.
- The reserved `__ds` namespace is unavailable for application streams.
- `ForkInfo` gains a serde-defaulted `sub_offset` field; Rust struct literals
  must supply it. `Storage` gains extended fork and subscription persistence
  methods; custom backends need implementations to support these features.
- `HttpConfig` gains `allow_insecure_webhooks` (default false), with equivalent
  TOML and `DS_HTTP__ALLOW_INSECURE_WEBHOOKS` settings. Enable only for local
  development callbacks.

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
