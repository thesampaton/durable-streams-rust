# Release follow-ups

The 6 September 2026 review covered commit `3481acf`. Its server correctness,
API ownership, instruction, and lint findings have since been addressed. Current
contracts live in the [migration guide](../migrations/server-api.md),
[architecture](../architecture.md), and
[execution contract](../design/blocking-execution-boundary.md).

The following findings remain open for the unpublished client. Resolve them
before client publication; passing conformance alone did not expose these cases.

- **Ambiguous append retries:** `append_parts` retries ordinary POSTs after a
  timeout even when producer deduplication is absent. The review reproduced two
  commits from one logical append. Make automatic replay depend on operation
  semantics and expose an ambiguous-write outcome for unsafe retries.
  Source: [raw_ops.rs](../../crates/durable-streams-client/src/client/raw_ops.rs).
- **Subscription data loss:** `response_to_event` retains only the last chunk
  while advancing to the response's final offset. A two-message SSE read yielded
  only the second message. Deliver the complete batch or each chunk with its
  corresponding resume offset, and define cancellation on subscription drop.
  Source: [protocol.rs](../../crates/durable-streams-client/src/protocol.rs).
- **SSE resume offsets:** `collect_sse` associates data with the preceding
  control offset; `max_chunks` can stop before its matching control arrives.
  Pair data with its following control before emitting or applying chunk limits.
  Cover split network frames, restart offsets, EOF, and multi-message delivery.
  Source: [protocol.rs](../../crates/durable-streams-client/src/protocol.rs).

After those fixes, simplify client exports and request customization, replace
string-based error inference with structured codes, and narrow blanket lint
allowances. Server storage follow-ups are listed in the
[upstream comparison](2026-09-06-upstream-rust-comparison.md).

Run the [pre-release matrix](../../CONTRIBUTING.md#releasing) on the release
candidate; historical review results are not a substitute for candidate checks.
