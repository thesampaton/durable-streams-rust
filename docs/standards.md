# Standards Alignment

This workspace aligns to the upstream Durable Streams protocol and conformance
suites as external standards. Alignment changes should be explicit, reviewed,
and updated in-repo rather than inferred from tribal knowledge.

## Verified Baselines

Verified on **2026-09-06**:

| Standard | Baseline | Source |
| --- | --- | --- |
| Protocol document | Durable Streams Protocol `1.0-draft` | `https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md` |
| Protocol revision | `a172acc389351cb3db6deb5cd60e3dec11e7ff39` | `durable-streams` `main` branch tip at verification time |
| Server conformance suite | `@durable-streams/server-conformance-tests@0.3.6` | npm registry |
| Client conformance suite | `@durable-streams/client-conformance-tests@0.2.3` | npm registry |

## Governance Rules

1. Treat the upstream protocol as the semantic baseline for client and server
   behavior.
2. Treat the upstream conformance suites as the primary ecosystem integration
   checks until better cross-implementation standards exist.
3. Update `docs/standards.md`, `Cargo.toml` workspace metadata, and
   `package.json` together when alignment baselines change.
4. Keep repository-internal Rust design idiomatic even when harness plumbing
   needs a small amount of non-Rust tooling.

## Harness Ownership

- `tests/conformance/client` will hold the client adapter contract and fixtures.
- `tests/conformance/server` will hold the server launcher contract and related
  fixtures for running the upstream suite against a base URL.
- `scripts/conformance` will remain the thin execution layer that invokes the
  pinned upstream suites.

## Current State

- The protocol and package baselines are recorded.
- The npm package versions are pinned exactly in `package.json`.
- The client conformance adapter is implemented in
  `crates/durable-streams-client`.
- The server conformance launcher is implemented in
  `tests/conformance/server/start-server.sh` and targets
  `crates/durable-streams-server`.
- The in-tree server crate is currently a lift-and-shift of published
  `durable-streams-server` `0.1.3`, so server maintenance should distinguish
  between migration-preservation work and intentional semantic change.

## Local Compatibility Notes

- Server-side `4xx`/`5xx` responses use RFC 9457-style
  `application/problem+json` payloads with machine-readable `code` values.
- The shared upstream error-code table is not yet broad enough for every
  Durable Streams server case, so the workspace currently uses local extension
  codes for server-specific states such as `STREAM_CLOSED`,
  `PRODUCER_EPOCH_FENCED`, `INTERNAL_ERROR`, and `INSUFFICIENT_STORAGE` (507).
- Storage-originated temporary failures may return HTTP `503 Service Unavailable`
  with RFC 9457 problem code `UNAVAILABLE` plus `Retry-After`, even when the
  upstream protocol text only standardises the problem shape rather than a full
  storage-failure taxonomy.
- Request telemetry field names intentionally mirror OpenTelemetry semantic
  conventions and Elastic-style dotted keys through `tracing`, so they can be
  mapped cleanly into a future OpenTelemetry exporter without renaming the
  server-side instrumentation surface.

## September 2026 Server Alignment

The server implements protocol revision `a172acc389351cb3db6deb5cd60e3dec11e7ff39`, including
fork sub-offsets and the reserved subscription APIs in sections 6–7. The server
suite is pinned to `0.3.6` and its optional subscription tests are enabled by
default in the workspace runner (338 tests total). The runner forwards Vitest
flags correctly and gives each whole test 30 seconds for sequential race probes;
individual upstream read deadlines and assertions are unchanged. The client suite remains at
`0.2.3`; this update does not add subscription APIs to the Rust client.

Fork sub-offsets materialize only the requested prefix, followed atomically by
any initial body. They count bytes for non-JSON streams and flattened messages
for JSON streams. Creation identity includes the sub-offset; omitted and zero
are equivalent. Forks have fresh producer and `Stream-Seq` state. File recovery
preserves the anchor offsets and retained ancestors.

SSE emits a control event with a resumable offset after each data event. Text and
binary message boundaries are preserved; JSON reads may batch messages in an
array. Frame pairs are emitted together while payload line endings remain safely
prefixed as data.

See [Subscriptions](subscriptions.md) for delivery, persistence, authentication,
and deployment details. Subscription errors use the protocol's
`{"error":{"code":...}}` envelope, while stream errors retain RFC 9457 problem
responses. This is an intentional distinction between the two protocol surfaces.

### Validation for this update

- `cargo test --workspace`: 628 passed; the nightly-only API snapshot test is
  ignored in this command and was run separately with success.
- `cargo check --workspace --all-targets` and strict server Clippy passed.
- `cargo fmt --all -- --check` and shell syntax checks passed.
- Server conformance 0.3.6: all 338 tests passed separately on memory, file-fast,
  file-durable, ACID in-memory, and ACID file, using Node 20.20.2 to match CI.
- Added coverage for partial-fork failure atomicity, nested fork read bounds,
  file recovery offsets, subscription snapshot isolation, token tampering,
  invalid ack batches, lease renewal/expiry, deletion fencing, persisted claims
  and signing keys, and webhook retry deadlines across restart.
