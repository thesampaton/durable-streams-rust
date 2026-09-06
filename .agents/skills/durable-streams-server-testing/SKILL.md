---
name: durable-streams-server-testing
description: Choose and maintain unit, backend-contract, HTTP, and public API tests for the Durable Streams server.
---

# Durable Streams server testing

Tests provide evidence about specific guarantees. Upstream conformance is a
required release check alongside local tests; it does not cover every storage,
concurrency, or Rust embedding contract.

## Choose the boundary

| Behavior | Coverage location under `crates/durable-streams-server` |
| --- | --- |
| Parsing and internal invariants | `#[cfg(test)]` modules beside source |
| Shared persistence behavior | `tests/storage_backend_contract.rs` |
| Backend-specific recovery and layout | Tests beside the backend implementation |
| HTTP behavior shared across backends | `tests/http_backend_parity_subset.rs` |
| HTTP status, headers, bodies, and live reads | Focused `tests/*.rs` integrations |
| Downstream API shape | `tests/public_api.rs` plus compiled usage examples |

HTTP tests should exercise a real in-process server and observable responses.
Use backend tests for internal storage assertions. A controlled fault or
interleaving backend is appropriate when testing an observable failure boundary.
Reuse `tests/common` helpers; keep backend-neutral suites independent of file
paths, shard layout, and lock implementation.

Use descriptive names and independent fixtures/stream names. Prefer explicit
synchronization for races. Time-based expiry tests should use the smallest
reliable wait and avoid competing conformance load.

## Trace behavior to a source

For protocol tests, identify the requirement in the pinned `PROTOCOL.md`
revision recorded in [standards](../../../docs/standards.md), such as
`PROTOCOL.md §5.1 Create Stream`. For local extensions or regression tests,
reference a real repository document or describe the observable regression.
Embedding, resource-limit, and recovery tests need not invent a protocol clause.

When a test fails, investigate code, test assumptions, environment, and the
pinned specification. Do not weaken an assertion or change semantics solely to
agree with another suite. Record intentional deviations in the standards notes.

## Verification

Use focused `cargo test -p durable-streams-server --test NAME` or `--lib`
commands during development. Full checks are in
[CONTRIBUTING.md](../../../CONTRIBUTING.md#expected-local-checks).

Run `./scripts/check-server-public-api.sh` for public-surface changes. It needs
nightly and is ignored by ordinary `cargo test`; inspect the diff before
updating its snapshot. A matching snapshot establishes API shape, not usability.

Run upstream server conformance via `./scripts/conformance/run-server-suite.sh`.
The package version belongs in `package.json`; runner controls and backend
examples are maintained in [the harness README](../../../tests/conformance/README.md).
Consult the conformance-harness skill when changing those scripts or pins.
