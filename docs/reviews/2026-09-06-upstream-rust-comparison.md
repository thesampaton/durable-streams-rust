# Lessons from upstream `durable-streams` 0.1.5

Reviewed 6 September 2026 against our clean commit
`a60888d76f9408e4165cc361472b841955a8b741`. This is a source comparison and a
proposed work order, not a performance benchmark. The bounded-execution item
was subsequently integrated in the working tree; the other items remain proposals.

## Source and scope

The primary source was the exact [published 0.1.5 crate archive][archive],
SHA-256 `472721e61ca191520c5e2b9bf8859aa8f9aa85599027f9103addbf695724b8ad`.
Its Cargo VCS record identifies commit
`88793e76595d69be300731b9b25c58538923a53b`, path
`packages/durable-streams-rust`. That commit resolves in Electric's
[repository][upstream], although the package's repository field points to the
protocol repository. The archive records a dirty build; the ten source and
documentation files used as upstream links below were downloaded at that commit
and verified byte-for-byte against the archive.

The authorship makes this a valuable source of implementation experience.
Our semantic authority remains the [pinned protocol][protocol]. No protocol or
conformance pin changes are proposed here. Their benchmark numbers describe
their builds, hardware, and workloads; they do not establish a speed ratio
against our server.

Their implementation deliberately does less. Our broader scope is intentional:
a coherent end-to-end DS server with idiomatic Rust APIs, Axum/Tokio integration,
explicit ownership, subscriptions and deployment support. Both approaches are
valid. The comparison asks which mechanisms improve our guarantees and how they
fit together, rather than treating reduced product scope as the goal.

## Recommended order

| Priority | Work | Why it fits this server |
| --- | --- | --- |
| First | Finish the bounded storage execution design, then integrate and validate it | The [completed implementation design][our-boundary] covers admission, cancellation, subscription work and shutdown; production integration is complete |
| Next | Extend crash testing and resolve file lifecycle durability limits | Strengthens the core persistence promise without changing the HTTP stack |
| Next | Add bounded reads and reduce lock duration safely | Limits per-reader memory and the effect of backfills on writers |
| Alongside those mechanisms | Review end-to-end API and deployment coherence, including cache/cursor behavior | Preserves our broader product intent and makes future fan-out credible |
| Alongside performance work | Establish a repeatable mixed-workload benchmark and storage timing | Identifies which optimizations actually matter for our supported backends |
| After measurement | Add a budgeted shared tail cache if fan-out warrants it | Avoids repeated file reads while preserving the existing streaming architecture |
| Later, only with a demonstrated need | Evaluate group commit | Potentially large write benefit, but substantial recovery and compatibility cost |
| Design constraint now; implementation later | Preserve optional object publication and read fan-out | The [direction note][our-objects] identifies staging, ownership and API constraints without implementing tiering |

## 1. Finish bounded storage execution

Upstream [offloads potentially cold file delivery][engine] and runs its
[group-commit writer on dedicated threads][wal-shard]. The transferable principle
is to keep potentially blocking I/O away from async workers, with explicit
ownership of work that outlives a disconnected caller.

At the reviewed commit, handlers and background work invoked synchronous storage
directly. The subsequent integration now offloads these calls.
Moving only the filesystem call is insufficient: waiting on a stream's
synchronous lock can block a Tokio worker too. Our existing
[execution-boundary design][our-boundary] now specifies the integration:
one complete storage operation per admitted job, permits owned by jobs, shared
subscription admission, bounded pending webhook completions, tracked outcomes,
and shutdown that drains accepted work even after a caller disappears.

Integrate and validate that design before replacing the HTTP engine. Keep SSE
and long-poll notification waits async; they need storage capacity only while
obtaining a snapshot. Treat request-body admission separately so slow uploads
cannot consume all storage slots. Measure overload rejections and control-request
latency as well as throughput. The completed design includes deterministic
acceptance criteria; its example is scheduling evidence, and production
integration is now covered by focused HTTP, file and lifecycle regressions.

## 2. Borrow their crash-testing method

Their [simulation][simulation] drives real handlers through create, append,
cancel, close, fork, delete, and checkpoint operations. It restarts repeatedly,
injects faults only in regions the fault model allows to be lost, and distinguishes
acknowledged operations from operations whose outcome the caller never learned.
The assertions cover whole records, ordering, offsets, closure, and deletion.
Workloads have replayable seeds; concurrent scheduling still deserves separate
deterministic failpoints.

The [findings][crash-findings] document real fixes: recovery skipped acknowledged
records beyond a segment boundary; a quiet stream exposed a torn unacknowledged
tail; and acknowledged deletes could reappear after restart. This is useful
evidence that ordinary protocol tests leave important storage transitions
unexamined.

We already have [property-based operation tests][our-properties],
[file recovery tests][our-crash], ACID recovery coverage, and append undo-journal
regressions. The useful extension is to combine workloads, interrupted operations,
injected I/O failures, and multiple recovery generations. Adapt the oracle to our
undo journal and redb guarantees; their WAL-specific fault rules do not transfer
unchanged. Include replacement imports and retained fork ancestors.

There are concrete local audit targets:

- [Creation/deletion limits are already documented][our-migration]. Initial
  payloads are synced, but [metadata replacement][our-filesys] writes and renames
  without its own durability barrier, and directory removal does not sync the
  storage root. Creation and deletion use these helpers outside append's journal
  commit. Check the complete ordering for create, fork, tombstone, and hard delete;
  merely adding one sync does not make a multi-file operation atomic.
- Upstream's [WAL barriers stop the process on failure][wal-shard]. Our
  [append transaction][our-transaction] marks a stream unavailable when rollback
  fails or the final commit sync is uncertain, but can resume after an earlier
  error if rollback succeeds. Investigate that distinction under injected sync
  failures. A successful later sync alone should not be treated as proof that
  earlier bytes are durable. Preserve evidence and define when recovery is
  required. An embeddable library should expose a typed unavailable state and let
  its owner choose process policy, rather than copy an internal process abort.
- Our [record recovery][our-recovery] validates lengths, not payload checksums.
  It can identify a partial final record, but does not detect same-length payload
  corruption. Decide the desired corruption-detection contract before adding a
  versioned record format and migration. Upstream's 0.1.5 changelog records both
  payload checksum enforcement and recovery hardening.

These are source-inspected gaps and audit questions. No power-loss experiment or
new corruption reproduction was performed in this comparison.

## 3. Bound reads before pursuing zero-copy networking

Upstream's [response representation][api] can describe file ranges without
materializing their payloads. Its [catch-up implementation][handlers] resolves
local ranges for file delivery and uses bounded windows for the local portions
of mixed cold-storage responses. This is a useful separation of read planning
from delivery; it does not mean every upstream SSE path or every server
allocation is globally bounded.

Our [file reader][our-reads] allocates the requested suffix in one buffer,
then creates shared `Bytes` slices for individual messages. That sharing is
already good. However, [HTTP body assembly][our-get] then copies the payload into
a contiguous response, and SSE formats the available read into a string.
Memory therefore grows with backlog per concurrent reader, within the configured
stream-size limit. The default 10 MiB limit bounds a single stream, not aggregate
response allocations.

The local disk read also occurs while the [stream guard is held][our-file-impl];
TTL reads take its write guard. Large/cold reads can therefore delay writes to
that stream. This is a lock-duration concern as well as an allocation concern.

A staged improvement:

1. Add a read byte/message budget with a resumable next offset. The pinned
   protocol's GET section explicitly allows server-defined response chunk limits.
   Keep JSON/message boundaries and report `at_tail` and closure accurately.
   Specify behavior for a single message larger than the target read budget.
2. Feed bounded batches into Axum bodies; avoid concatenating the whole backlog.
   Ensure mid-response I/O failure cannot look like successful delivery through
   an offset whose data was never sent.
3. Capture stable read metadata and move disk I/O outside the mutation lock only
   after defining snapshot lifetime. Our replacement path truncates the existing
   file in place, so cloning its file descriptor and dropping the guard is not
   enough. Immutable file generations or equivalent coordination must protect
   reads from replacement and rollback as well as deletion and fork traversal.

This touches the public storage contract and needs backend parity, API snapshot,
migration review, and conformance verification when implemented. Preserve the
simple message-oriented API where it remains useful; a second internal bounded
read path is an option to evaluate, not a settled API design.

## 4. Measure interference and many-stream costs

Their [mixed-workload report][mixed] records writes alongside paced backfills and
live delivery, with p99 and resource measurements. It also reports write
degradation under unpaced readers without 429/503 load shedding. Adopt that
honesty about overload and measure the behavior we want from admission control.

The [0.1.5 changelog][changelog] shows that timer tasks and metadata rewrites
became expensive with many infrequently written streams; a shared dirty-set
sweeper replaced memory-mode per-stream timers. Their [tuning report][tuning]
also shows how strongly storage placement, checkpoint work, and CPU allocation
affect results. These are reasons to benchmark realistic stream counts and
storage, not reasons to adopt their deployment complexity by default.

No tracked benchmark harness was found in our reviewed commit. Start with a
small reproducible matrix: one hot stream versus many sparse streams; isolated
appends versus concurrent backfills and SSE; text, JSON and binary payloads;
fast and slow consumers; memory, file and ACID file. Use matched resources and
durability settings for any upstream comparison. Measure append and delivery
p50/p99, throughput, RSS, CPU, open descriptors, recovery time and rejected work.
Use representative Linux storage for performance conclusions and keep client
resource use separate.

Our [request spans][our-telemetry] provide useful context, but response
construction timing does not measure the lifetime of a streaming body. Add
storage lock wait, operation duration, sync time, execution queue wait,
active jobs, read bytes and active subscribers. Keep metric labels bounded;
stream identifiers can stay in diagnostic traces rather than metric dimensions.

## 5. Share hot bytes where it pays

Upstream optionally keeps a shared immutable last-chunk cache. Caught-up
subscribers can reuse the same payload instead of each reading the file. In the
inline [SSE source][handlers], each subscriber still performs encoding; do not
read the architectural prose as a guarantee that all framing is shared. The
cache is disabled by default on Linux, further reason to measure its benefit.

We already use pull-based `Body::from_stream` and do not add a producer task plus
message channel per SSE connection. Upstream's removal of that overhead is
therefore largely an existing strength here. Replacing our notification-only
broadcast channel is not by itself a demonstrated optimization.

If fan-out profiling warrants it, add a bounded shared payload cache for the file
backend, with generation/offset keys and invalidation for replacement, deletion,
and forks. Account for aggregate cache memory, not just a per-stream cap. Consider
shared framing only if it preserves message boundaries and response-specific
control fields. The Linux [epoll reactor][reactor] is a much larger tradeoff:
raw descriptors, unsafe code, platform-specific scheduling and another lifecycle
to maintain. Retain Axum/Tokio unless measured requirements justify that cost.

## 6. Group commit is an option with a substantial price

Our normal successful [file append transaction][our-transaction] contains six
explicit sync calls: journal, directory, log, metadata, directory, and final
directory. They protect an undo-journal protocol; removing them individually
would change correctness. Upstream amortizes its commit barrier over writes from
many streams using a sharded WAL, then checkpoints the per-stream read files.

If durable append throughput is the measured bottleneck, compare batching
possibilities with our existing ACID backend before designing another persistence
engine. A WAL needs durable visibility frontiers, cancellation semantics,
checkpoint/recycle ordering, failed-barrier handling, recovery and format
migration. Explicitly preserve producer state and close atomicity; upstream's
lagging metadata policies should not silently weaken our current guarantees.

Custom HTTP/1.1 and automatic storage tiering would be substantial changes of
scope. Our embeddable library, typed errors, configurable transport, multiple
backends, explicit server lifecycle and subscription support are capabilities to
make more predictable under load and faults.

## 7. Keep the end-to-end implementation coherent

Apply each mechanism across supported backends and public entry points. An
execution limit that omits subscription writes, a bounded read that loses fork
snapshot safety, or a fast response that advances beyond delivered bytes would
undermine the whole system. Carry each change through configuration, typed
errors, runtime ownership, shutdown, embedding examples, client behavior and
conformance against the pinned specification.

CDN behavior is a concrete follow-up, not an assumed existing guarantee. Current
middleware forces ordinary reads to `no-store`, and the cursor path needs review
for shared caching and multiple origins. The [object-storage direction][our-objects]
records those source findings and the constraints around authentication,
replacement generations, expiry and the distinction between replica progress
and the authoritative tail.

Optional object publication is worth preserving as a future distribution path:
immutable history can serve many readers while an owner retains the writable
tail and control state. Keep local execution separate from async network
transfer, and local commit separate from remote publication. No Redis dependency,
new storage format, object implementation or automatic tiering is proposed for
the bounded-execution work. Future Cargo features should add adapters and
optional dependencies while preserving existing local APIs; they cannot conceal
breaking trait changes.

[archive]: https://static.crates.io/crates/durable-streams/durable-streams-0.1.5.crate
[upstream]: https://github.com/electric-sql/electric/tree/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust
[api]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/src/api.rs
[handlers]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/src/handlers.rs
[engine]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/src/engine_raw.rs
[wal-shard]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/src/wal/shard.rs
[simulation]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/src/wal/sim_tests.rs
[reactor]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/src/sse_reactor.rs
[crash-findings]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/CRASH_SIM_FINDINGS.md
[mixed]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/MIXED_WORKLOAD_VALIDATION.md
[changelog]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/CHANGELOG.md
[tuning]: https://github.com/electric-sql/electric/blob/88793e76595d69be300731b9b25c58538923a53b/packages/durable-streams-rust/WAL_TUNING.md
[protocol]: https://github.com/durable-streams/durable-streams/blob/a172acc389351cb3db6deb5cd60e3dec11e7ff39/PROTOCOL.md
[our-properties]: ../../crates/durable-streams-server/tests/proptest_storage.rs
[our-crash]: ../../crates/durable-streams-server/tests/crash_recovery.rs
[our-migration]: ../migrations/server-api.md#import-and-file-persistence
[our-filesys]: ../../crates/durable-streams-server/src/storage/file/filesys.rs
[our-transaction]: ../../crates/durable-streams-server/src/storage/file/transaction.rs
[our-recovery]: ../../crates/durable-streams-server/src/storage/file/recovery.rs
[our-boundary]: ../design/blocking-execution-boundary.md
[our-reads]: ../../crates/durable-streams-server/src/storage/file/reads.rs
[our-get]: ../../crates/durable-streams-server/src/handlers/get.rs
[our-file-impl]: ../../crates/durable-streams-server/src/storage/file/storage_impl.rs
[our-telemetry]: ../../crates/durable-streams-server/src/middleware/telemetry.rs

[our-objects]: ../design/object-storage-direction.md
