---
name: durable-streams-server-code
description: Maintain Durable Streams server handlers, storage, routing, and runtime code with explicit atomicity, locking, and error boundaries.
---

# Durable Streams server implementation

Source paths below are relative to the repository root. Read the affected
module and its tests; use the protocol skill only when wire semantics are at
issue, and the conformance skill only when changing external suite wiring.

## Storage and concurrency

- `src/storage` under `crates/durable-streams-server` owns backend mechanics.
  Shared semantic rules live in `storage/shared.rs` and `storage/fork.rs`.
  [Architecture](../../../docs/architecture.md#storage-read-boundaries) records
  the backend-specific lock and transaction boundaries.
- Validate and commit a logical operation within its atomic boundary. Return
  offsets and closed state from that same snapshot, avoiding a separate HEAD
  that can race with another writer. Treat known gaps as defects to fix when
  in scope, not as guarantees provided by the current trait.
- Memory/file fork reads release the entry lock before ancestor traversal can
  acquire the stream map. ACID fork lineage reads share a shard transaction.
  Preserve these lock orders when changing read or expiry handling.
- The current `Storage` trait is synchronous. Blocking disk I/O and lock waits
  still need an execution boundary appropriate to the async caller. Decide
  whether to retain the trait or change it based on actual callers; a sync
  signature is not permission to block Tokio workers without consideration.
- Document ownership and shutdown for spawned workers. Router cloning and
  multiple listeners must not accidentally create competing owners of state.

## Protocol and error boundaries

- Parse request framing in handlers, then pass validated domain values to
  storage/domain operations. Check arithmetic and resource bounds before
  mutation; user input must not trigger invariant panics.
- The server owns its offset format, `{read_seq:016x}_{byte_offset:016x}`.
  Preserve monotonic ordering and reserved `-1`/`now` request sentinels.
  Clients treat returned offsets as opaque.
- `protocol/error.rs` owns the exhaustive mapping from domain errors to HTTP
  status, problem code, public detail, and telemetry. Storage emits typed
  errors; handlers attach request context and operation-specific headers.
- `protocol/problem.rs` builds RFC 9457 stream-error responses. Subscription
  control errors use their separate protocol envelope; see
  [subscriptions](../../../docs/subscriptions.md).
- Preserve error causes for diagnostics while keeping internal storage details
  out of public responses. An `expect` must describe a real internal invariant.

## HTTP surfaces

- Keep protocol, subscription, admin, and probe behavior explicit when changing
  routing or middleware. Admin authentication belongs to the embedding or
  deployment layer; document the hooks actually exposed by the public API.
- Long-poll and SSE must preserve resume offsets, closure, cursor propagation,
  and shutdown behavior across framing and concurrency changes.
- Keep separate storage metadata representations when durability/recovery needs
  differ. Extract shared rules rather than forcing one mutable representation
  across all backends.
