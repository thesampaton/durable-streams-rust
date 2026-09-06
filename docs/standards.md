# Standards Alignment

This workspace aligns to the upstream Durable Streams protocol and conformance
suites as external standards. Alignment changes should be explicit, reviewed,
and updated in-repo rather than inferred from tribal knowledge.

## Verified Baselines

Verified on **2026-09-06**:

| Standard | Baseline | Source |
| --- | --- | --- |
| Protocol document | Durable Streams Protocol `1.0-draft` | [Pinned PROTOCOL.md](https://github.com/durable-streams/durable-streams/blob/a172acc389351cb3db6deb5cd60e3dec11e7ff39/PROTOCOL.md) |
| Protocol revision | `a172acc389351cb3db6deb5cd60e3dec11e7ff39` | `durable-streams` `main` branch tip at verification time |
| Server conformance suite | `@durable-streams/server-conformance-tests@0.3.6` | npm registry |
| Client conformance suite | `@durable-streams/client-conformance-tests@0.2.3` | npm registry |

## Governance Rules

1. Treat the recorded upstream protocol revision as the semantic baseline for
   client and server behavior. Adoption of a newer revision is an explicit change.
2. Use the pinned conformance suites as ecosystem integration checks alongside
   local tests. Investigate disagreements between implementation, test assumptions,
   and specification; document deliberate compatibility deviations here.
3. Update `docs/standards.md`, `Cargo.toml` workspace metadata, and
   `package.json` together when alignment baselines change.
4. Keep repository-internal Rust design idiomatic even when harness plumbing
   needs a small amount of non-Rust tooling.

## Harness Ownership

- `tests/conformance/client` holds the client adapter contract and fixtures.
- `tests/conformance/server` holds the server launcher contract and related
  fixtures for running the upstream suite against a base URL.
- `scripts/conformance` is the thin execution layer that invokes the
  pinned upstream suites.

## Current State

- The protocol and package baselines are recorded.
- The npm package versions are pinned exactly in `package.json`.
- The client conformance adapter is implemented in
  `crates/durable-streams-client`.
- The server conformance launcher is implemented in
  `tests/conformance/server/start-server.sh` and targets
  `crates/durable-streams-server`.
- The server is maintained and released from this workspace. Its version and
  release scope are defined by its manifest and changelog; API changes follow
  the compatibility policy in `CONTRIBUTING.md`.

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
- The same local `503`/`UNAVAILABLE` extension covers a full or closed server
  storage-execution boundary, with `Retry-After: 1`. Shutdown rejects new
  catch-up reads as well as writes, while admitted jobs drain. Existing idle
  long-polls retain their last empty snapshot's concrete resume offset on
  shutdown; normal timeout rereads deliver data that arrived before responding.
  Subscription admission failures retain the separate control error envelope.
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

### Server API and correctness follow-up — 6 September 2026

The [release review](reviews/2026-09-06-release-readiness.md) server fixes retain
this protocol revision and both conformance package pins. POST now also rejects
an empty JSON array when a close header is present, as required by §9.1.3;
an empty-body close-only request remains valid.

Working-tree validation used Rust 1.94.1 and Node 20.16.0 (the CI Node major):

- Workspace tests: 674 passed; the nightly API test was ignored there and passed
  separately. Final focused checks passed 157 library tests, 12 import tests,
  three body/TTL tests, and seven embedding/lifecycle tests, including the final
  file metadata and recovery edits.
- Formatting, workspace all-target check, strict server Clippy, advisory client
  Clippy, and rustdoc with warnings denied passed. The updated public API
  snapshot was inspected and then verified without snapshot-update mode.
- Server conformance 0.3.6: all 338 tests passed independently on memory,
  file-fast, file-durable, ACID memory, and ACID file, with separate ports/data
  roots and no competing test load.
- Client conformance 0.2.3: 255 passed, zero failed, and 14 upstream capability
  skips. Client behavior fixes from the review remain separate.
- The blocking-boundary example passed its admission, disconnect, and drain
  assertions against real file storage. It remains a proposal; production
  storage calls have not been offloaded. No throughput or contention benchmark
  was run. The release candidate still needs the repository's CI/MSRV gates.

### 2026-09-06 file backend consolidation

The append-log backend now has one `file` mode. The release conformance matrix
covers memory, file, ACID memory, and ACID file; the earlier results above retain
the mode names used when those runs were made. Protocol and package pins are
unchanged.

Validation used Rust 1.94.1 and Node 20.16.0:

- Workspace tests: 678 passed, zero failed; the ignored nightly API test passed
  separately with the pinned compiler, after reviewing the snapshot changes.
- Formatting, workspace all-target check, strict server Clippy, advisory client
  Clippy, and rustdoc with warnings denied passed. Generated storage API docs
  expose the single mode and the constructor without a durability toggle.
- Server conformance 0.3.6: all 338 tests passed on `file`, using a fresh data
  directory and separate port without competing test load.
- New regressions cover rejected retired configuration names, initial stream
  and fork data after reopen, and capacity release after an initial sync error.
  These checks do not establish power-loss guarantees for creation/deletion.

### 2026-09-06 bounded storage execution integration

The production server now uses the [bounded execution contract](design/blocking-execution-boundary.md)
for HTTP storage operations and subscription persistence. Protocol and
conformance pins, storage formats, and the synchronous storage/service APIs are
unchanged. The public API adds only the job limit and its typed validation error.

Validation used Rust 1.94.1 and Node 20.20.2:

- Workspace tests: 683 passed, zero failed. The nightly API snapshot was ignored
  there and passed separately with `nightly-2026-09-06`, after reviewing its two
  added lines.
- Formatting, workspace all-target check, strict server Clippy, advisory client
  Clippy, and rustdoc with warnings denied passed. Generated API documentation
  was inspected for the job limit, runtime ownership and shutdown contract.
- Server conformance 0.3.6: all 338 tests passed independently on memory, file,
  ACID memory and ACID file, using fresh data directories and separate ports,
  without competing test load.
- New regressions cover queued and detached job ownership, admission/close races,
  storage lease retention, retried/concurrent shutdown and worker failure,
  responsive probes under blocked file I/O, rejected routes without mutation,
  durable final append/close, subscription save/cache continuity and bounded
  pending webhook results, plus resumable SSE and long-poll timeout behavior.
- Local links in the changed Markdown documents resolved. No throughput
  benchmark or object-storage implementation is part of this integration.
