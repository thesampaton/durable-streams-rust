# Release readiness review — 6 September 2026

Reviewed clean `trunk` at `3481acf`, including the breaking router-options change. The server is the publication target; the client is assessed separately because its manifest has `publish = false`. This is a review and implementation proposal; its findings and line references describe that commit.

**Instruction-audit follow-up, 6 September 2026:** the working tree now contains root repository guidance, five shortened and distinctly named skills, corrected contributor and governance documentation, and enforced server workspace lints with public documentation and scoped test exceptions. The instruction findings below are retained as the audit record. The server remediation status is recorded below; the client behavior findings remain open.

**Server follow-up, 6 September 2026:** S1–S7 and the server API changes are
implemented in the working tree. Atomic batch/close and replacement operations
now have backend and recovery regressions; import validates payloads before
writing and reports partial progress. Server ownership, startup, and shutdown
are explicit, and route groups share one initialized subscription service.
Creation uses validated options; HTTP body collection and TTL arithmetic are
bounded. Append outcomes and backend errors are explicit, required capabilities
are checked by trait implementation, and the API snapshot is part of PR CI.

`StreamService` was deliberately introduced as the beginning of public API
consolidation. The correction retains that direction: handlers now use the
shared service for stream operations, while storage owns atomic persistence.
Its documentation describes this current role. The original suggestion below
to make the wrapper private is superseded by this clarification.

See the [migration guide](../migrations/server-api.md) for the final surface and
persistence limits. The file backend now has one synced mode (`file`);
replacement requires capacity for old and new payloads. Uncertain final-sync
outcomes require recovery and do not prove a write was absent.

Blocking execution is intentionally left as a
[proposal and runnable demonstration](../design/blocking-execution-boundary.md)
for review after the other server changes. Production callers still execute
synchronous storage directly. Client findings C1–C3 remain outside this server
implementation. The findings and line references below remain the historical
review of `3481acf`, not a description of the updated working tree.

**Original recommendation at `3481acf`: resolve the correctness findings and settle the API ownership boundaries before freezing this release.** The recent router consolidation, exhaustive error mapping, and storage read refactoring are useful improvements. The remaining concerns include observable data loss, incomplete lifecycle contracts, and public interfaces that would force another breaking change to repair.

“Slop” here means redundant instructions, inaccurate explanations, unused public scaffolding, and abstractions whose claimed purpose exceeds their implementation. These findings concern the work itself; they make no assumptions about authorship.

## Instruction audit, completed first

The five repository skills total 961 lines. Their separation into protocol, implementation, testing, and harness concerns is useful. The main problem is duplicated policy and stale facts.

| Finding | Evidence | Proposed correction |
| --- | --- | --- |
| Migration instructions describe an obsolete baseline. | [CONTRIBUTING.md:49](/Users/sampaton/IdeaProjects/durable-streams-rust/CONTRIBUTING.md:49), [architecture.md:10](/Users/sampaton/IdeaProjects/durable-streams-rust/docs/architecture.md:10), [conformance skill:47](/Users/sampaton/IdeaProjects/durable-streams-rust/.agents/skills/durable-streams-conformance-harness/SKILL.md:47), and `docs/standards.md` still describe a current lift-and-shift of `0.1.3`. The crate is `0.3.0` with substantial later changes. Architecture also says release automation is undecided, although it exists. | Replace the migration posture with the current release policy and actual component boundaries. Preserve historical facts as dated history where useful. |
| Error-mapping guidance contradicts the implementation just merged. | [Server skill:70](/Users/sampaton/IdeaProjects/durable-streams-rust/.agents/skills/durable-streams-server-code/SKILL.md:70) requires mapping in handlers; [error.rs:195](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/protocol/error.rs:195) now owns the exhaustive mapping. The enum documentation still claims handler mapping. | State that domain error definitions own the central mapping, handlers add request context, and storage emits typed errors. Keep the subscription envelope distinction explicit. |
| Claimed workspace lint policy is not enforced on the server. | [Server manifest:15](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/Cargo.toml:15) declares only local pedantic Clippy. It lacks `[lints] workspace = true`. An explicit `-D missing_docs` check finds **137 undocumented public items/fields/variants** despite ordinary strict Clippy passing. | Inherit workspace lints, then address the resulting diagnostics with useful documentation or narrower visibility. Use scoped exceptions with reasons. Cargo requires explicit member inheritance: [Cargo workspace lints](https://doc.rust-lang.org/cargo/reference/workspaces.html#the-lints-table). |
| Protocol authority and test authority are conflated. | The protocol skill names the pinned specification as authoritative. [Testing skill:128](/Users/sampaton/IdeaProjects/durable-streams-rust/.agents/skills/durable-streams-server-testing/SKILL.md:128) says unit-test failure means broken logic and conformance failure requires reconciling expectations to the suite. | Treat tests as evidence. Investigate implementation, test, and specification disagreements and record justified deviations. Passing conformance does not prove every protocol property. |
| Operational references have drifted. | The testing skill cites absent `01-stream-lifecycle.md` and `docs/gaps.md`. The conformance skill lists `DURABLE_STREAMS_SERVER_STARTUP_DELAY`; the runner actually uses `DURABLE_STREAMS_SERVER_READY_TIMEOUT` and also supports `DURABLE_STREAMS_SERVER_TEST_TIMEOUT_MS`. | Link to real pinned protocol sections and actual runner options. Store changing version facts in the governance records and reference them from skills. |
| Two available skills have the same `coding-guidelines` name. | The repository skill and `/Users/sampaton/.agents/skills/coding-guidelines/SKILL.md` share the name and keyword-heavy trigger. The personal version suggests `G_CONFIG` static prefixes, unlike repository conventions. | Rename the repository skill to a specific name such as `durable-streams-rust-guidelines`, narrow its description, and explicitly give repository policy precedence over generic preferences. |
| Repository-wide policy is discoverable only through optional skill loading and prose documents. | There is no root `AGENTS.md` or override. Several skills cross-reference the others, encouraging broad context loading. | Add a short root `AGENTS.md` with the MSRV, authoritative paths, and relevant verification commands. Keep detailed domain procedures in selectively loaded skills. |

This proposed structure follows current documented instruction discovery and skill loading: durable repository rules belong in `AGENTS.md`; skills load their detailed instructions on demand. Keep the repository rules concise and leave formatting enforcement to CI. [OpenAI instruction guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md), [OpenAI skill guidance](https://learn.chatgpt.com/docs/build-skills).

A suitable root instruction draft is:

```markdown
# Repository guidance

- Rust MSRV: 1.89. Cargo manifests and CI define the enforced build policy.
- Protocol and suite pins are recorded in docs/standards.md, Cargo.toml
  workspace metadata, and package.json. Read the pinned specification when
  changing protocol semantics; document disagreements with conformance tests.
- Consult the relevant repository skill for protocol, server storage,
  integration testing, or conformance harness work.
- Keep library failures typed and validate public inputs before mutation.
  Preserve atomic writes, coherent reads, and resumable offsets.
- For public API changes, review downstream usage examples, the API snapshot,
  and migration notes. Treat runtime ownership and shutdown as API behavior.
- Run focused tests while developing. Before a server release, run the
  workspace tests, strict server Clippy, public API snapshot, and the
  pre-release conformance matrix described in CONTRIBUTING.md.
```

Also correct the shell-check example: `bash -n scripts/conformance/*.sh` checks only the first expanded script; iterate over the files.

## Server findings to resolve before release

### S1 — P1: ordinary append-and-close is not atomic

[post.rs:112](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/handlers/post.rs:112) calls `batch_append`, releases its storage lock/transaction, then calls `close_stream`. A second writer can append between those calls. Close-only similarly reads the tail separately from closing. The response can acknowledge a stale final offset.

**Reproduced:** a forwarding backend inserted a second writer at the gap after the real in-memory append returned. The close request returned 204; stored messages were `final`, then `racer`. The acknowledged offset was `0000000000000001_0000000000000005`, while the closed stream's actual tail was `0000000000000002_000000000000000a`.

**Proposal:** make ordinary append, optional close, sequence validation, and returned metadata one storage operation. Close-only must return its tail from the same operation. Implement that contract in each backend. Test interleaving and persistence failure around the final append. The pinned protocol explicitly requires atomic final append and closure. [Protocol §5.2](https://github.com/durable-streams/durable-streams/blob/a172acc389351cb3db6deb5cd60e3dec11e7ff39/PROTOCOL.md#52-append-to-stream).

### S2 — P1: failed replacement import deletes the original stream

[import.rs:88](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/transfer/import.rs:88) deletes an existing stream before base64 decoding and replacement creation. Malformed payloads and later storage failures leave the original gone.

**Reproduced:** export a valid stream, replace one encoded payload with invalid base64, then import with `ConflictPolicy::Replace`. Import returns a decoding error and the original stream no longer exists.

**Proposal:** decode and validate the complete input before mutation; provide atomic replacement or an explicit staged replacement/rollback mechanism for storage failures. Define partial-import behavior and preserve useful progress in errors. Prevalidation alone fixes the malformed-input case but does not make delete-then-create atomic.

### S3 — P1: separate routers sharing storage overwrite subscription state

Every [router construction creates a separate subscription service](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/subscriptions/mod.rs:110). Each service caches its own database; [save replaces the entire persisted snapshot](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/subscriptions/mod.rs:156) without checking whether another service changed it.

**Reproduced:** construct two routers around the same `Arc<InMemoryStorage>`, initialize both caches, then create subscription `one` through the first and `two` through the second. Both requests return 201; persisted state contains only `two`. Workers were stopped in the probe to isolate the request-cache problem.

**Proposal:** give subscription state and its worker one owner per storage service. Clone routers from that shared service. Either prevent a second independent owner or use a storage revision/CAS contract if independent owners are intentionally supported. Document the ownership requirement in the public embedding API.

### S4 — P1: router construction silently changes behavior with ambient Tokio state

[subscriptions/mod.rs:120](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/subscriptions/mod.rs:120) starts reconciliation only when `Handle::try_current()` succeeds. A router assembled before starting Tokio still accepts subscription requests, but its worker never starts when the router is later served. Construction inside Tokio also detaches the task, exposing no completion handle.

**Reproduced:** build the router synchronously, serve it within a runtime, create a pull-wake subscription, and append matching data. Creation succeeds; the wake stream remains empty after six reconciliation intervals.

**Proposal:** make worker startup explicit and fallible, with a handle or future the embedder can shut down and await. Share that lifecycle owner with S3's state. Return initialization failures before reporting readiness. At minimum, eliminate silent feature disabling outside a runtime.

### S5 — P1: an accepted TTL value panics during request handling

TTL parsing accepts any `u64`. [put.rs:162](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/handlers/put.rs:162) converts oversized values to `i64::MAX` and passes them to `chrono::Duration::seconds`, which panics. Date addition also needs checked arithmetic. The same pattern appears in [TTL renewal](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/storage/fork.rs:275).

**Reproduced:** a PUT with `Stream-TTL: 18446744073709551615` triggers `TimeDelta::seconds out of bounds` and drops the connection.

**Proposal:** validate representable TTL/deadline bounds with checked duration construction and checked date addition. Reject invalid values with a typed error. Apply the same validation to direct Rust construction and imported metadata before acquiring mutation locks.

### S6 — P1: request buffering bypasses configured resource limits

[common.rs:22](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/handlers/common.rs:22) collects request bodies with `usize::MAX`. PUT and POST receive raw `Body`, so the default limit associated with Axum byte extractors does not bound this collection. Storage limits are checked after the complete body has already been allocated and JSON may have been expanded into more allocations.

**Evidence:** source inspection; no memory-exhaustion experiment was performed.

**Proposal:** add an explicit request-body byte limit and enforce it while reading, including chunked bodies. Return 413 for limit violations. Keep wire-body limits distinct from retained stream bytes because JSON framing and whitespace affect their sizes differently. Bound concurrent buffered requests if the intended memory guarantee is process-wide.

### S7 — P2: public stream configuration does not establish its own invariants

[`StreamConfig::with_ttl`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/storage/mod.rs:96) only stores the number; the HTTP handler separately calculates the initial deadline. Direct users can create TTL streams with no deadline. Public fields also allow conflicting expiration settings and bypass content-type normalization.

**Reproduced:** `storage.create_stream("x", StreamConfig::new("text/plain".into()).with_ttl(0))` leaves a visible stream with `ttl_seconds = Some(0)` and `expires_at = None`.

**Proposal:** separate caller-supplied creation options from resolved persisted metadata. Represent expiry as one choice—none, sliding TTL, or absolute deadline—and resolve it once in the storage/domain boundary. Avoid requiring Rust callers to reconstruct private HTTP initialization rules.

## API decisions to make during this breaking change

| Area | Current cost | Proposed direction |
| --- | --- | --- |
| Fallible router construction | [`build_router`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/router.rs:91) accepts mutable, unvalidated configuration and returns `Router`. An invalid mount path becomes an Axum panic; this was reproduced. | Return a typed construction error or require a validated input type. Keep validation focused on settings the embedding operation actually uses. |
| Runtime-selected storage | `Storage` is object safe, and transfer APIs accept `&dyn Storage`; router/service generics implicitly require `Sized`. A downstream `Arc<dyn Storage>` router example fails with E0277. | Support `?Sized` through the relevant handlers/services or accept a shared trait object at the embedding boundary. Preserve static dispatch where useful. |
| Extensible types | Public config structs require field-complete literals; public error enums permit exhaustive matching. Recent `HttpConfig` and `ForkInfo` additions already force downstream edits. | Use private fields plus builders for inputs with invariants. Consider `#[non_exhaustive]` for errors and extensible output metadata, with constructors/accessors for backend implementers. Keep closed domain enums exhaustive where that is intentional. Adding these protections later is itself breaking. [Cargo compatibility guidance](https://doc.rust-lang.org/cargo/reference/semver.html). |
| Storage result semantics | `append` returns the appended message's offset; `batch_append` returns the next offset, both as `Result<Offset>`. `exists` and `subscribe` cannot express backend errors; [ACID maps them to absence](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/storage/acid/storage_impl.rs:578). | Use named append outcomes with explicit resume offsets and closed state. Make fallible backend lookups return `Result`. Combine this work with S1. |
| Backend capabilities | [Default extended-fork and subscription methods](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/storage/mod.rs:425) preserve compilation while mounted routes fail at runtime. Unsupported extended forks are classified as an invalid request header. | Decide whether custom backends must support the full server contract. Require the methods if they must; otherwise declare capabilities and validate enabled routes at startup. Use a capability error rather than blaming a valid request. |
| Blocking storage execution | Async HTTP handlers directly execute synchronous file reads/writes and redb transactions; subscription reconciliation does the same. | Keep the synchronous trait if appropriate, but put potentially blocking work behind a bounded execution boundary. Specify shutdown behavior for work already running. Benchmark disk latency and contention before making throughput claims. This is an architectural risk identified in source, not a measured throughput regression. [Tokio blocking-task documentation](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html). |
| Middleware composition | The public builder bundles protocol, subscriptions, admin, and probes. Its documentation acknowledges that admin-only middleware cannot be attached through the API. | Expose composable route groups tied to the same shared server state, or a narrow way to layer the admin surface. Avoid creating a second worker when exposing multiple routers. |

The smallest coherent direction is a shared server/service handle that owns initialized control state and worker lifecycle, provides routers using that state, and exposes fallible construction. Preserve the useful `RouterOptions` consolidation. This proposal does not require a general plugin architecture or a new shared core crate.

## Client findings — keep publication separate

### C1 — P1 before client publication: retries can duplicate plain appends

[`append_parts`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-client/src/client/raw_ops.rs:434) retries POSTs regardless of whether producer identity is present. A timeout cannot tell the client whether the server committed the body. Defaults allow three retries.

**Reproduced:** a local endpoint commits immediately but delays its response beyond the timeout. One logical append with one configured retry causes two commits and returns an error to the caller.

**Proposal:** make replay policy depend on operation semantics. Retry writes automatically when producer deduplication makes replay safe; return an explicit ambiguous-write outcome for ordinary appends unless the caller deliberately opts into at-least-once behavior. Keep retryability of an error separate from safety of replaying a request. Preserve `Retry-After` if the client promises server-directed backoff.

### C2 — P1 before client publication: subscription delivery skips data

[`response_to_event`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-client/src/protocol.rs:352) keeps only `read.chunks.last()` but carries the response's final offset. The subscription loop then resumes from that final position.

**Reproduced:** subscribe via SSE to a closed text stream containing `first` and `second`. The caller receives only `second`, then completion, with the resume offset after both messages.

**Proposal:** deliver every chunk with its corresponding resume token, or make the event explicitly contain the complete batch. A `Stream<Item = Result<...>>` interface would fit Rust consumers and permit natural backpressure. Define what dropping the subscription does; the current join handle detaches unless callers explicitly abort.

### C3 — P1 before client publication: SSE chunk offsets describe the previous position

[`collect_sse`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-client/src/protocol.rs:189) assigns each data chunk the previous control offset. Subsequent control events update only the aggregate offset. `max_chunks` can terminate before consuming the data event's matching control event.

**Reproduced:** `.read().live(LiveMode::Sse).max_chunks(1).send()` against this server fails with `missing SSE control event with streamNextOffset` despite receiving valid data and control frames.

**Proposal:** keep a data event pending until its matching control establishes the post-chunk offset, then emit it and evaluate the chunk limit. Test both combined and split network frames, per-chunk restart, EOF, and C2's multi-message case.

Additional client API cleanup should follow those correctness fixes: provide a normal subscription entry point instead of only `subscribe_raw`; clarify collected long-poll termination and timeout semantics; make `head`/`delete` request customization consistent with builder operations; parse structured server error codes instead of inferring `InvalidOffset` from any body containing “invalid”; and consolidate the same raw types currently exported through `model`, `raw`, and crate root.

## Slop and maintainability cleanup

- **Remove misleading public leftovers at the breaking boundary.** [`ShutdownToken`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/router.rs:19), [`LongPollTimeout` and `SseReconnectInterval`](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/config.rs:1463) are no longer used by handlers. Their docs still promise extraction behavior. The compatibility `storage::StreamMetadata` alias explicitly says it can be removed at the next breaking version.
- **Reduce the claims around `StreamService`.** [Its documentation](/Users/sampaton/IdeaProjects/durable-streams-rust/crates/durable-streams-server/src/streams.rs:89) describes a domain layer used by protocol handlers. Its actual public behavior is two storage forwards and a useful listing projection. Either make it the service owner needed above or keep the projection and make the thin wrapper private. Keep explanations aligned with the implementation.
- **Document reasons and contracts, rather than the development process.** Examples include the client `RetryableRequest` explanation about staying below Clippy's argument-count threshold, the public storage trait calling its methods “internal,” and generic “avoid proliferation” commentary. Describe the values that travel together, error behavior, and ownership guarantees instead.
- **Replace blanket client suppressions incrementally.** `#![allow(dead_code)]` and `#![allow(missing_docs)]` hide unfinished surface and stale scaffolding. Remove unused items, document the supported API, then scope any remaining allowances. The advisory client CI status should remain visibly distinct from the published server policy.
- **Retain useful existing structure.** The exhaustive error definition, backend-local persistence mechanics, shared semantic helpers, and runtime options each have concrete purposes. Backend representation duplication alone is not a reason to invent another abstraction. Keep descriptive comments on lock ordering, durability, and offset invariants.

## Proposed implementation order

1. Correct instruction drift and lint inheritance. Establish the intended server/client release scope and preserve the review findings as tests.
2. Implement atomic append/close and safe replacement; check TTL/deadline bounds and bound request buffering. Validate all affected backends.
3. Establish shared server state and explicit worker lifecycle; add fallible construction and test multiple listeners, construction outside Tokio, startup failure, and shutdown completion.
4. Finalize backend capabilities, append outcomes, fallible lookups, extensible public types, and dynamic-storage embedding. Remove obsolete exports and publish concrete migration examples.
5. Repair client replay and streaming semantics before publishing it. It can remain unpublished while the server release proceeds.
6. Run the release matrix on the exact candidate commit, review the resulting public API diff, then finalize the version/changelog. Make API review part of the PR or release-PR gate: the current snapshot job runs only manually or after a tag is pushed.

## Verification and scope

- `cargo test --workspace`: **635 passed, 0 failed, 1 ignored**, including doctests. The ignored test is the nightly API snapshot.
- `./scripts/check-server-public-api.sh`: **passed** separately.
- `cargo clippy -p durable-streams-server --all-targets -- -D warnings`: **passed** under the manifest's current lint configuration.
- `cargo fmt --all -- --check` and individual syntax checks for conformance shell scripts: **passed**.
- Pinned server conformance `0.3.6`, memory backend, Node `20.16.0`, isolated port: **338 passed**.
- Pinned client conformance `0.2.3`, Node `20.16.0`: **255 passed, 0 failed, 14 declared capability skips**.
- Explicit server `-D missing_docs`: **failed with 137 diagnostics**, confirming the instruction/enforcement mismatch.
- Ten temporary behavioral probes reproduced the failures described above. These tests assert the current defects to establish reproducibility; they are not passing regression tests for corrected behavior.
- A separate downstream compile probe confirmed `Arc<dyn Storage>` cannot be passed to `build_router`.

Reproduction sources and logs are in [/tmp/durable-streams-release-review](/tmp/durable-streams-release-review/src/lib.rs). Run the behavioral probes with:

```sh
CARGO_TARGET_DIR=/Users/sampaton/IdeaProjects/durable-streams-rust/target \
  cargo test --manifest-path /tmp/durable-streams-release-review/Cargo.toml \
  --lib -- --nocapture
```

The temporary project's `dyn-probe` binary intentionally fails compilation. The workspace's product source remains unchanged. This review covers instructions, public interfaces, selected storage/HTTP paths, subscriptions, transfer, and client streaming/retry behavior. The full disk-backend conformance matrix, an independent MSRV run, and performance benchmarks were not repeated; this report is not a release certification.
