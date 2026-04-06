---
name: durable-streams-server-code
description: >
  Source-level conventions for the Durable Streams Rust server. Use when
  changing code under crates/durable-streams-server/src, especially around
  protocol types, storage contracts, handlers, streaming paths, concurrency, or
  error handling.
sources:
  - "crates/durable-streams-server/src"
  - "crates/durable-streams-server/src/config.rs"
  - "crates/durable-streams-server/src/protocol"
  - "crates/durable-streams-server/src/storage"
  - "crates/durable-streams-server/src/router.rs"
  - "crates/durable-streams-server/tests"
user-invocable: false
---

# Durable Streams Server Code

This skill captures repo-local coding guidance for
`crates/durable-streams-server/src`.

Use it when changing protocol handling, storage implementations, router wiring,
middleware, configuration, or live-read behaviour in the server crate.

## Design Bias

- Preserve protocol and operational semantics first.
- Prefer explicit, boring code over compact or clever code.
- Parse and validate at module boundaries, then work with typed values
  internally.
- Keep behaviourally meaningful logic out of HTTP handlers.

## Strong Typing

- Use newtypes and enums to make invalid states unrepresentable.
- Prefer domain types such as `Offset`, `ProducerEpoch`, and `ProducerSeq`
  instead of raw strings or integers once input has been parsed.
- Parse and validate at the edges. Internal functions should assume validated
  types rather than re-parse raw input.

## Offset Invariants

- Offsets must be monotonically increasing within a stream.
- The server offset format is `{read_seq:016x}_{byte_offset:016x}`.
- Reserved sentinels are `-1` and `now`.
- Lexicographic ordering must match temporal ordering.
- Concurrent appends must be serialized per stream so monotonicity holds.

When touching append logic, check that locking or storage sequencing still
preserves per-stream monotonic offsets.

## Storage Trait Contract

The `Storage` trait is the core persistence boundary. Implementations should
preserve these properties:

- Thread-safe under concurrent access.
- Atomic appends and create-with-data operations.
- Stream isolation: work on one stream should not leak into another except for
  global resource limits.
- Synchronous operations by default, with async boundaries used only where they
  are genuinely required for notifications or live reads.

Use interior mutability deliberately. Avoid introducing async into storage APIs
unless it is necessary for observable behaviour.

## Error Handling

- Keep one core error surface in
  `crates/durable-streams-server/src/protocol/error.rs`.
- Map domain errors to HTTP status codes in handlers, not in storage or
  protocol modules.
- Avoid opaque error flow in core logic.
- If `expect()` or `panic!()` remains, it should correspond to an internal
  invariant, not user input.

## Boundary Types

- Prefer owned boundary types such as `Bytes`, `String`, and `Arc<T>` when data
  crosses module boundaries.
- Use borrowing freely within a module, but avoid borrow-heavy API surfaces that
  make call sites harder to reason about.

## Handler Boundary

Handlers should stay thin:

- parse request input
- validate request framing
- call protocol or storage logic
- map results into HTTP responses

Do not move business rules or storage coordination into handler code just
because the request arrived there first.

## Streaming Paths

- Keep SSE and long-poll behaviour isolated in dedicated modules or focused
  helper logic.
- Do not leak streaming-specific state machines across unrelated modules.
- Keep live-read behaviour explicit and testable.

## Code Shape

- Prefer single-purpose functions.
- If a function grows to the point where it needs section comments to explain
  itself, extract helpers.
- Avoid iterator-heavy or lifetime-heavy rewrites in hot paths when a simple
  loop is clearer.

## Lints

- Workspace clippy pedantic lints are on by default.
- Suppress lints locally and intentionally, with a short reason, rather than
  weakening crate-wide standards.

## Coordination With Other Skills

Use this skill together with:

- `durable-streams-protocol` for protocol semantics
- `durable-streams-conformance-harness` for workspace-level harness and runner
  wiring
- `coding-guidelines` for general Rust style guidance
