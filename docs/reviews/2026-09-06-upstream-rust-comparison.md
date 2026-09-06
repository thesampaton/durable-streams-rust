# Storage follow-ups from upstream comparison

The 6 September 2026 comparison used the published `durable-streams` 0.1.5
crate and [its recorded source revision](https://github.com/electric-sql/electric/tree/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust).
It was a source review, not a comparative benchmark. Our protocol authority and
suite pins remain in [standards](../standards.md).

The first recommendation, bounded local storage execution, is implemented;
see its [execution contract](../design/blocking-execution-boundary.md).
The remaining work is:

- **Lifecycle durability and crash testing:** extend seeded workloads with
  interrupted operations, injected I/O failures, and repeated recovery. Cover
  creation, forks, deletion, replacement, retained ancestors, and uncertain sync
  outcomes. Creation metadata rename and directory removal lack their own
  durability barriers; audit complete operation ordering before adding syncs.
  Record recovery checks lengths, not payload checksums; stronger corruption
  detection would require an explicit format and migration decision.
- **Bounded reads:** introduce a byte/message budget with resumable offsets and
  accurate tail/closure state. Preserve oversized-record and JSON boundaries.
  Stream bounded batches instead of copying the full backlog into responses.
  Moving file I/O outside the mutation lock requires stable snapshot ownership:
  a cloned descriptor alone cannot protect against in-place replacement.
- **Measurement:** benchmark hot and sparse streams, appends with backfills/SSE,
  slow consumers, and each backend under matching durability settings. Record
  latency, throughput, memory, descriptors, recovery, and rejected work. Add
  bounded-label timing for storage locks, execution queues, reads, and syncs.
- **Caching and distribution:** review shared-cache headers, cursors, multiple
  origins, authentication, expiry, and replacement generations. Optional object
  publication remains a [design direction](../design/object-storage-direction.md),
  with local commit separate from remote publication.
- **Only after measurement:** consider a bounded shared tail cache for fan-out,
  or group commit if durable writes are the bottleneck. Compare the existing
  ACID backend before introducing another persistence engine. Preserve producer
  state, atomic closure, recovery ordering, and public compatibility.

These are proposals, not implemented guarantees. Keep the Axum/Tokio stack and
backend-specific durability boundaries unless measured requirements justify
changing them. Existing persistence limits are documented in the
[migration guide](../migrations/server-api.md#import-and-file-persistence).
