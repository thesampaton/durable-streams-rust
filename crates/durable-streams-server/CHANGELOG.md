# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-11

### Added

- Configurable redb backend for the acid storage mode. The new `acid_backend`
  config field (`DS_STORAGE__ACID_BACKEND` env var) selects between the default
  file-backed redb and an in-memory redb backend. The in-memory variant provides
  full ACID transaction semantics without disk I/O, useful for testing and
  ephemeral workloads.

### Changed

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

[0.2.0]: https://github.com/thesampaton/durable-streams-rust/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/thesampaton/durable-streams-rust-server/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/thesampaton/durable-streams-rust-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/thesampaton/durable-streams-rust-server/releases/tag/v0.1.1
