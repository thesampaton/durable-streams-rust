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

`storage` owns synchronous atomic persistence operations and backend mechanics.
`StreamService` consolidates creation, append, read, delete, fork, notification,
and metadata/listing access. Protocol and admin handlers use that service;
backend implementations retain their lock and transaction boundaries.

`Server` validates HTTP settings and loads subscription control state before
Tokio is required. Subscription construction produces a `Mutex<Database>` only
after loading and persistence succeed. State changes clone the committed database
and save before replacing it; pull-wake processing retains both save boundaries
around publication. `start` creates one worker; `RunningServer` exposes cloneable
route groups and cancellation with awaited completion. A storage instance has
one independent server owner. Route clones retain that owner and share state.

The private `execution` module owns admission, the captured Tokio runtime,
completion tracking and detached error reporting. Its async stream adapter serves
all HTTP storage paths; subscription control and the private `subscriptions/worker`
scheduler share the same job budget. Subscription transactions acquire their
synchronous database lock inside admitted jobs, keeping persistence and cached
state together. Network delivery and live waits remain async. Server shutdown
closes admission and drains jobs even after caller disconnect or worker failure.
See the [execution contract](design/blocking-execution-boundary.md).

`protocol/error.rs` defines the exhaustive domain-error response mapping.
`protocol/problem.rs` owns the RFC 9457 payload and response helpers. Handlers
attach request context and operation-specific headers. Subscription control
uses its own error envelope and private state; see
[subscriptions](subscriptions.md).

Keep semantic helpers shared where backends must agree. Separate persisted
formats and runtime resources where their invariants differ. Environment and TOML
inputs both produce private configuration patches and use one merge path. Peer parsing, mount-path checks, and structural stream-name
predicates are shared while each boundary retains its limits and error envelope.
Introduce a shared crate only when multiple real consumers need a common
implementation.

## Storage read boundaries

The storage implementations share access/expiry rules, fork bounds, append
validation, and metadata construction. Memory and file reads capture an owned
`PendingRead` under the stream lock. Root and `offset=now` reads are complete
at that point; fork reads defer ancestor traversal until the stream lock is
released. The captured local suffix, tail, and closed state stay together even
if a writer appends before ancestor assembly. Shared `PendingRead::finish` combines
that suffix with the bounded inherited prefix without rereading the leaf.
This keeps the stream-map/entry lock ordering explicit and handles
root, fork, and tail reads once per backend instead of once per TTL branch.

The fallible fork plan builder reads ACID lineage from the caller's transaction;
ordinary reads and fork-prefix creation share its backend-local range reader.
Memory/file indexes share inclusive-start, exclusive-end range selection.

Global capacity reservation and saturating release share atomic arithmetic.
Backend operations retain their reservation, commit, and recovery points,
including replacement staging capacity and uncertain-commit handling.

TTL persistence remains backend-local. Memory renews after successful read
assembly, file storage persists renewal after reading the local payload, and
ACID reads metadata, messages, lineage, and renewal in a single transaction.
The memory/file renewal ordering is retained to preserve existing failure
semantics. ACID fork lineages share a shard, so transaction snapshots cover
all ancestors.

`HEAD` and listing use the same backend-local metadata projection. `exists`
derives its fallible presence check from `HEAD`. Subscription lookup and listing
retain backend loops for locking, notifier ownership, database access, and
deterministic ordering.

The entry/meta structs also remain separate. Memory entries own message
buffers and notifiers; file entries own open files and rebuildable indexes,
with a separate serialized metadata format; ACID metadata is a serialized
transaction record with notifiers held outside it. A common mutable struct
would couple durable formats to runtime resources. Shared semantic rules and
projection helpers are the intended boundary, rather than a common entry type.
