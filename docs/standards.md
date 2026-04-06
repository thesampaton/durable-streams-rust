# Standards Alignment

This workspace aligns to the upstream Durable Streams protocol and conformance
suites as external standards. Alignment changes should be explicit, reviewed,
and updated in-repo rather than inferred from tribal knowledge.

## Verified Baselines

Verified on **2026-04-06**:

| Standard | Baseline | Source |
| --- | --- | --- |
| Protocol document | Durable Streams Protocol `1.0-draft` | `https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md` |
| Protocol revision | `f091f315e4c3ca6769b45135ab8b5f53d70b1751` | `durable-streams` `main` branch tip at verification time |
| Server conformance suite | `@durable-streams/server-conformance-tests@0.2.3` | npm registry |
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
