---
name: durable-streams-protocol
description: Implement or review Durable Streams HTTP semantics against the workspace's pinned protocol, including lifecycle, offsets, JSON, live reads, forks, and subscriptions.
---

# Durable Streams protocol

Read [docs/standards.md](../../../docs/standards.md) and the protocol revision in
`Cargo.toml` workspace metadata. Fetch that revision of upstream `PROTOCOL.md`
and read the sections relevant to the change. This skill is a navigation and
review aid; the pinned text determines exact status/header requirements.

Adopt upstream changes deliberately. Update the governance records, affected
suite pins, and guidance together when the task changes the baseline. If code,
conformance, and specification disagree, investigate and record the decision.
Do not silently substitute the current upstream branch for the recorded revision.

## Review by operation

| Area in `PROTOCOL.md` | Invariants to check |
| --- | --- |
| Stream model and closure | Acknowledged durability, immutable positions, monotonic closure, atomic final append |
| Create | Idempotent configuration identity, initial-body atomicity, closed/open mismatch, mutually exclusive TTL and absolute expiry |
| Append and idempotent producers | Content type, writer sequence ordering, complete producer header tuple, epoch fencing, sequence gaps, duplicate success, returned producer state |
| Delete and forks | Retained ancestors, soft-delete visibility, fork bounds and independent lifecycle |
| HEAD and catch-up reads | Coherent metadata/body offsets, expiry, closure signaling, cache validation |
| Long-poll and SSE | `offset=now`, resumable progress, cursor propagation, EOF, content framing |
| Reserved subscriptions and delivery | Configuration identity, membership, signatures, claims, acknowledgements, leases, generation fencing |

## Offsets and live reads

- Clients treat returned offsets as opaque; they carry forward
  `Stream-Next-Offset` rather than reconstructing positions. The server may
  implement its own offset format while honoring ordering and sentinel rules.
- `-1` and `now` are request sentinels, not generated stream positions. In
  catch-up mode `now` returns the current tail without historical data; live
  reads wait for future data and closed streams signal EOF immediately.
- Keep long-poll and SSE semantics distinct. SDK auto-selection is a client
  convenience, not a new protocol live mode.
- SSE data/control framing must yield a usable resume offset for consumed data.
  Check binary base64 signaling, text line endings, JSON message boundaries,
  and closure with missing optional cursor fields.
- Live responses on open streams carry cursor information. Clients echo it on
  subsequent long-poll requests. Cache/ETag behavior must preserve closure
  transitions and avoid loops over cached empty responses.

## Creation, JSON, and writes

- TTL is sliding; absolute expiry is a fixed deadline. Validate representable
  values before calculating deadlines or persisting state.
- `Stream-Closed` is active only for a case-insensitive `true` value. Preserve
  the specification's close-only, final-body, and duplicate-close distinctions.
- JSON GETs return arrays. Appended arrays flatten exactly one level; message
  boundaries survive storage and reads. Check empty-array behavior for the
  specific create/append/close operation in the pinned text.
- `Stream-Seq` is opaque lexicographic ordering, separate from the producer
  `(id, epoch, sequence)` protocol. Check equality as well as regressions, and
  the documented scope of writer ordering.

## Forks and reserved subscriptions

- Fork sub-offsets count bytes for non-JSON and flattened messages for JSON.
  The source header is required even for zero. Omitted and zero sub-offsets
  are equivalent; invalid values and overshoots fail before creating a fork.
- Source-derived configuration, prefix materialization, and initial bodies
  form one creation operation. Producer and writer sequence state start fresh.
- A soft-deleted source returns the specified gone response for client-facing
  operations while retaining data required by descendants.
- Keep reserved `__ds` control state separate from application streams and
  ordinary fork/read operations.
- Subscription mutations, signing keys, leases, generation state, and retry
  schedules must honor persistence and fencing requirements across restart.
- Validate webhook destinations, including DNS resolution; HTTPS exceptions
  belong to explicit local development configuration. See
  [subscriptions](../../../docs/subscriptions.md) for local deployment details.

Rust API design belongs in the workspace Rust guidance; backend locking and
HTTP error construction belong in the server implementation guidance.
