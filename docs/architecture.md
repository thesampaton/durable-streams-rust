# Architecture

## Workspace boundaries

- `crates/durable-streams-server` is the published server library and executable.
  It includes protocol handlers, subscriptions, memory/file/ACID storage,
  transport configuration, and operator transfer commands.
- `crates/durable-streams-client` is the unpublished client implementation,
  including HTTP operations, read subscriptions, producer sequencing, and
  journal/replication helpers.
- `tests/conformance` contains process entrypoints for the external suites;
  `scripts/conformance` invokes them using the pins in `package.json`.
- Release PR automation is configured in `release-plz.toml` and
  `.github/workflows/release-plz.yml`. Publishing and tagging are manual; see
  [CONTRIBUTING.md](../CONTRIBUTING.md#releasing).

## Server boundaries

`storage` owns persistence operations and backend mechanics. `streams` contains
metadata and the projections used by CLI/admin listing. Protocol handlers call
storage; the current `StreamService` wraps metadata and listing operations.

`protocol/error.rs` defines the exhaustive domain-error response mapping.
`protocol/problem.rs` owns the RFC 9457 payload and response helpers. Handlers
attach request context and operation-specific headers. Subscription control
uses its own error envelope and private state; see
[subscriptions](subscriptions.md).

Keep semantic helpers shared where backends must agree. Separate persisted
formats and runtime resources where their invariants differ. Introduce a shared
crate only when multiple real consumers need a common implementation.

## Storage read boundaries

The storage implementations share access/expiry rules, fork bounds, append
validation, and metadata construction. Memory and file reads capture an owned
`PendingRead` under the stream lock. Root and `offset=now` reads are complete
at that point; fork reads defer ancestor traversal until the stream lock is
released. This keeps the stream-map/entry lock ordering explicit and handles
root, fork, and tail reads once per backend instead of once per TTL branch.

TTL persistence remains backend-local. Memory renews after successful read
assembly, file storage persists renewal after reading the local payload, and
ACID reads metadata, messages, lineage, and renewal in a single transaction.
The memory/file renewal ordering is retained to preserve existing failure
semantics. ACID fork lineages share a shard, so transaction snapshots cover
all ancestors.

`HEAD` and listing use the same backend-local metadata projection. `exists`,
`subscribe`, and `list_streams` intentionally retain their thin backend loops:
they already share the visibility rule, while locking, notifier ownership,
fallible database access, and deterministic listing are storage concerns.

The entry/meta structs also remain separate. Memory entries own message
buffers and notifiers; file entries own open files and rebuildable indexes,
with a separate serialized metadata format; ACID metadata is a serialized
transaction record with notifiers held outside it. A common mutable struct
would couple durable formats to runtime resources. Shared semantic rules and
projection helpers are the intended boundary, rather than a common entry type.
