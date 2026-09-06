---
name: durable-streams-server-testing
description: >
  Testing and conformance guidance for the Durable Streams Rust server. Use
  when adding or changing tests under crates/durable-streams-server/tests,
  deciding between unit and integration coverage, or wiring the upstream server
  conformance suite from this workspace.
sources:
  - "crates/durable-streams-server/tests"
  - "crates/durable-streams-server/tests/common/mod.rs"
  - "tests/conformance/README.md"
  - "tests/conformance/server/start-server.sh"
  - "scripts/conformance/run-server-suite.sh"
  - "package.json"
user-invocable: false
---

# Durable Streams Server Testing

This skill captures repo-local testing conventions for
`crates/durable-streams-server`.

The main rule is simple: crate-local tests provide fast feedback, while the
upstream server conformance suite is the final acceptance gate.

## Test Layers

### Unit tests

Unit tests belong in `#[cfg(test)]` modules inside source files.

Use them for:

- parsing and validation logic
- offset and cursor rules
- producer sequencing rules
- internal state-machine behaviour
- backend-specific invariants that do not require HTTP

Keep them fast and focused. Prefer no networking and no external I/O unless the
module being tested is itself storage-specific.

### Integration tests

Integration tests live under `crates/durable-streams-server/tests/*.rs`.

Use them for:

- real HTTP requests against a running server
- observable protocol behaviour
- status/header/body assertions
- end-to-end behaviour that should stay black-box from the caller perspective

Each test should map to a concrete protocol requirement or a documented local
gap.

### External conformance

The upstream `@durable-streams/server-conformance-tests@0.3.6` suite is the
acceptance gate for server behaviour in this workspace.

Run it through the workspace harness:

- `./scripts/conformance/run-server-suite.sh`

The default launcher is:

- `tests/conformance/server/start-server.sh`

## Backend Parity Policy

- Shared backend invariants belong in
  `crates/durable-streams-server/tests/storage_backend_contract.rs`.
- Backend-specific storage behaviour should stay close to the implementation in
  the corresponding `src/storage/*.rs` tests.
- HTTP parity checks that matter across backends belong in
  `crates/durable-streams-server/tests/http_backend_parity_subset.rs`.
- Keep shared suites backend-neutral. Avoid assertions about file paths, shard
  routing details, or lock structure outside backend-specific tests.

## Black-Box Rule For HTTP Tests

HTTP integration tests should validate observable server behaviour, not
implementation details.

They should:

- start a real in-process server instance
- use an HTTP client such as `reqwest`
- assert on status codes, headers, and bodies

They should not depend on internal storage state to decide whether the server is
correct.

## Test Helpers

Shared helpers live in:

- `crates/durable-streams-server/tests/common/mod.rs`

Prefer extending those helpers lightly over building a large custom framework.

Important existing helpers include server spawning, HTTP client creation, unique
stream-name generation, and backend-aware storage setup for contract tests.

## Naming And Independence

- Use descriptive test names of the form
  `test_{operation}_{expected_behaviour}`.
- Keep tests independent.
- Use unique stream names so tests do not interfere with one another.

## Spec And Gap References

For integration tests, prefer doc comments that reference the spec document or
documented gap the test is intended to cover.

Examples:

- `Validates spec: 01-stream-lifecycle.md#create-stream`
- `Validates gap: docs/gaps.md#cors-preflight`

This keeps local tests aligned with protocol intent instead of drifting into
implementation-only assertions.

## Failure Triage

- If a unit test fails, internal logic is broken.
- If a local integration test fails, either the implementation or the test no
  longer matches the protocol expectation.
- If upstream conformance fails, treat that as the authoritative behavioural
  signal and reconcile local expectations to it.

## Running Tests

- `cargo test --workspace`
- `cargo test -p durable-streams-server`
- `cargo test -p durable-streams-server --lib`
- `cargo test -p durable-streams-server --test tls_transport`
- `./scripts/conformance/run-server-suite.sh`

## Coordination With Other Skills

Use this skill together with:

- `durable-streams-protocol` for protocol truth
- `durable-streams-conformance-harness` for workspace runner and package wiring
- `durable-streams-server-code` when a test change depends on source-level
  design constraints
