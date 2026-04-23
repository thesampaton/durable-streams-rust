# Server Retention and Read Admission Architecture

## Purpose

This note defines the server-side architecture needed to make stream retention
and read-side activity semantics conform to the Durable Streams protocol across
all storage backends, not just the transactional ACID implementation.

It is motivated by two linked problems:

- sliding TTL is currently modeled as mutable configuration instead of mutable
  liveness state
- the current storage read contract conflates request admission, protocol side
  effects, and response data assembly

The result is both protocol drift and backend-specific race windows.

This note is intentionally adjacent to, not a replacement for:

- `docs/server-request-outcome-architecture.md`
- `docs/server-implementation-sketch.md`

Those notes define broader request and outcome shaping. This note narrows in on
retention semantics, request admission, and storage/runtime ownership.

## Problem

The current design has three structural issues.

### 1. Sliding TTL is stored as drifting config

Today `StreamConfig` carries both:

- immutable policy such as content type
- mutable expiry state such as the TTL-derived expiry instant

That leaks into equality, persistence, and cleanup logic. A sliding TTL is not
immutable stream identity. It is policy plus mutable liveness state.

### 2. `Storage::read()` owns too many concerns

The current read contract mixes together:

- existence and visibility checks
- protocol activity side effects
- data snapshotting
- fork-chain assembly
- live-read behavior used by long-poll and SSE handlers

That is the wrong seam for protocol correctness. A read request can have
control-plane side effects even when the returned body is empty.

### 3. Live reads are not representable as first-class activity

The protocol defines sliding TTL in terms of request activity at the origin. A
single long-poll or SSE request is one admitted client read, not an unbounded
sequence of new client reads each time the handler re-reads internal state.

The current model cannot express that distinction cleanly.

## Protocol Constraints

This design is driven by the protocol, not by the current backend shape.

The relevant constraints are:

- `Stream-TTL` defines a sliding idle timeout that resets on each read or write
- `HEAD` does not reset sliding TTL
- for live reads, the TTL reset happens when the server begins processing the
  request, not when data is later delivered
- a stream with active live readers should not expire merely because no new
  data is produced during the request
- forks inherit retention policy, but inherited sliding TTL refreshes
  independently on the fork and source after fork creation

Those requirements apply across `memory`, `file`, and `acid`.

## Non-Goals

This note does not attempt to decide:

- the final public error taxonomy
- RFC 9457 payload structure
- logging field names beyond the retention/read domain
- async storage APIs
- conformance-harness expansion beyond feasibility notes

## Architectural Direction

The target shape is:

1. immutable retention policy
2. mutable durable liveness state
3. runtime live-read lease state
4. explicit read admission
5. snapshot reads that do not themselves imply new client activity

The core rule is:

- policy decides how expiry is computed
- mutable state records durable client activity
- runtime lease state prevents sliding-TTL expiry while a live read is active
- request admission is where protocol-visible read side effects occur
- data assembly happens after admission and should not redefine protocol
  activity semantics

## Model

### Immutable Stream Policy

The stream configuration should become truly immutable after creation.

Conceptually:

```rust
pub enum RetentionPolicy {
    None,
    SlidingTtl { ttl_seconds: u64 },
    AbsoluteExpiry { expires_at: DateTime<Utc> },
}

pub struct StreamConfig {
    pub content_type: String,
    pub retention: RetentionPolicy,
    pub created_closed: bool,
}
```

Implications:

- `StreamConfig` equality becomes stable again
- TTL no longer needs special-case equality semantics
- fork inheritance copies retention policy, not mutable expiry state

### Mutable Durable Stream State

Each backend should store mutable liveness separately from immutable config.

Conceptually:

```rust
pub struct StreamMutableState {
    pub closed: bool,
    pub next_read_seq: u64,
    pub next_byte_offset: u64,
    pub total_bytes: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_client_activity_at: Option<DateTime<Utc>>,
    pub last_seq: Option<String>,
    pub producers: HashMap<String, ProducerState>,
    pub fork_info: Option<ForkInfo>,
    pub ref_count: u32,
    pub lifecycle_state: StreamState,
}
```

For sliding TTL streams:

- expiry is derived from `last_client_activity_at + ttl_seconds`

For absolute-expiry streams:

- expiry is derived from immutable policy

For streams with no retention:

- no expiry is derived

### Protocol Projection

The internal retention model does not remove the server's obligation to realize
protocol headers on successful HTTP responses.

The protocol-visible requirement is:

- when a `200 OK` response should expose stream retention information, the
  server must be able to project the current retention state into
  `Stream-TTL` and/or `Stream-Expires-At` as required by the protocol

That means:

- internal state should be structured around retention policy and mutable
  liveness
- the wire layer must retain an explicit projection step that turns that state
  into protocol headers
- any future server- or stream-level option to suppress optional retention
  headers would be a projection policy choice, not a reason to collapse the
  underlying model back into header-shaped storage

The design goal is therefore:

- richer internal retention structure
- mandatory protocol-compatible header projection

The cleanest shape is for `StreamMetadata` to carry an immutable retention
snapshot view for the current request, rather than forcing handlers to
re-derive retention math from lower-level fields.

Conceptually:

```rust
pub struct StreamMetadata {
    pub config: StreamConfig,
    pub next_offset: Offset,
    pub closed: bool,
    pub total_bytes: u64,
    pub message_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub retention_view: RetentionView,
}

pub struct RetentionView {
    pub effective_expiry: Option<DateTime<Utc>>,
    pub ttl_seconds_remaining: Option<u64>,
}
```

Then the ownership split is:

- storage and shared model code compute the immutable retention snapshot once
- `StreamMetadata` carries that snapshot as convenience for upper layers
- handlers project that snapshot directly into `Stream-TTL` and
  `Stream-Expires-At`

This keeps:

- retention policy and liveness math out of handlers
- protocol-header realization explicit at the wire layer
- storage internals free to use structured retention state rather than
  header-shaped persistence

### Runtime Live-Read State

Sliding TTL also needs non-durable runtime state for active long-poll and SSE
requests.

Conceptually:

```rust
pub struct LiveReadLease {
    pub stream_name: String,
    pub mode: LiveReadMode,
    pub admitted_at: DateTime<Utc>,
}
```

Backends do not persist leases. They are runtime guards only.

For sliding TTL streams, effective expiry should consider:

- durable client activity watermark
- active live-read leases

This is what allows a single admitted SSE or long-poll request to keep the
stream alive without pretending each internal re-read is a new client request.

## Shared Semantics

`storage/shared.rs` should become the single source of truth for:

- effective expiry calculation
- visibility checks
- client activity recording
- TTL/expiry reporting for `HEAD`

The shared helpers should operate on:

- immutable retention policy
- mutable stream state
- runtime lease state
- `now`

Conceptually:

```rust
fn effective_expiry(...) -> Option<DateTime<Utc>>;
fn is_stream_expired(...) -> bool;
fn record_client_activity(...) -> bool;
fn ttl_remaining_seconds(...) -> Option<u64>;
```

This should replace the current pattern where sliding TTL mutates
`config.expires_at` in-place.

## Read Admission

The current `Storage::read()` contract is too broad.

The storage boundary should distinguish:

- admitting a client read request
- reading a snapshot of stream data
- holding a live-read lease for the duration of a request

Conceptually:

```rust
pub enum ReadMode {
    CatchUp,
    LongPoll,
    Sse,
}

pub struct ReadAdmission {
    pub metadata: StreamMetadata,
    pub lease: Option<ReadLeaseGuard>,
}

fn admit_read(&self, name: &str, mode: ReadMode) -> Result<ReadAdmission>;
fn read_snapshot(&self, name: &str, from_offset: &Offset) -> Result<ReadResult>;
```

Semantics:

- `admit_read` performs visibility checks
- `admit_read` records client activity when the protocol requires it
- `admit_read` acquires a live-read lease for long-poll and SSE
- `read_snapshot` performs no additional activity side effects

This gives the handlers the correct vocabulary:

- one admitted live request
- many internal rereads from the same admitted request

## Handler Shape

This design implies a handler refactor in `handlers/get.rs`.

### Catch-up

1. resolve read mode and offset
2. admit the read
3. perform one snapshot read
4. render response

### Long-poll

1. subscribe before reading
2. admit live read and hold lease
3. perform initial snapshot read
4. if data exists, return immediately
5. if open and at tail, wait on notifier
6. re-read by snapshot only
7. drop lease when request ends

### SSE

1. subscribe before reading
2. admit live read and hold lease
3. perform initial snapshot read
4. stream frames
5. internal loop re-reads by snapshot only
6. drop lease when stream ends

This is also the clean place to remove the current `head()` then `read()`
double-admission shape in `GET`.

## Backend Responsibilities

### Memory

The in-memory backend is the simplest proving ground:

- move retention policy into immutable config
- add durable activity watermark to stream state
- add active live-read tracking to runtime state
- make read admission explicit before snapshotting

### File

The file backend should:

- version its persisted stream metadata format
- persist immutable retention policy separately from mutable activity state
- stop serializing TTL-derived mutable expiry as config
- keep live-read leases purely in memory

### ACID

The ACID backend should:

- version its stored metadata shape
- store immutable retention policy plus durable activity watermark
- keep live-read leases in runtime memory beside notifier state
- use a write transaction for read admission when protocol-visible read-side
  mutation is required
- use read transactions only for snapshot reads

The ACID TOCTOU bug is then a symptom of the old abstraction, not a special
case to patch in isolation.

## Fork Semantics

Fork creation should copy retention policy, not mutable expiry state.

Examples:

- source sliding TTL + fork no override -> fork gets the same sliding TTL
  policy, but starts its own independent activity timeline
- source absolute expiry + fork no override -> fork gets the same absolute
  expiry deadline
- fork TTL override -> fork gets a new sliding TTL policy independent of source
- fork absolute-expiry override -> fork gets a new absolute expiry deadline

After creation:

- source activity refreshes only the source
- fork activity refreshes only the fork

That matches the protocol more closely than inheriting a mutable `expires_at`
value and rewriting it later.

## Migration

This change should be treated as a format and architecture migration, not as a
small compatibility tweak.

The release should include:

- explicit versioning for file metadata and ACID stored metadata
- migration logic from the legacy TTL representation
- startup validation that fails clearly if legacy state cannot be transformed
  deterministically

For legacy sliding TTL data:

- old form: `ttl_seconds` plus a drifting TTL-derived `expires_at`
- new form: sliding retention policy plus durable activity watermark

Migration should reconstruct `last_client_activity_at` from the old deadline and
TTL window where possible. If that cannot be done safely, startup should fail
with an actionable migration error instead of silently guessing.

## Cross-Cutting With Request/Outcome Work

This design overlaps with the in-flight request/outcome architecture, but it is
not blocked on finishing that larger redesign.

The main interaction points are:

- read admission is a protocol/domain concern and should fit the future domain
  seam cleanly
- live-read lease failures and migration failures need to project cleanly into
  the future outcome seam
- moving away from `head()` plus `read()` duplication should reduce duplicated
  outcome shaping later

The intended sequencing is:

- use this note to fix storage/runtime semantics first
- keep error/outcome public surfaces stable during the first pass
- let the broader request/outcome work later absorb the cleaner read-admission
  concepts rather than the current overloaded `read()` model

## Testing Direction

The testing strategy should split protocol semantics from backend race coverage.

### Protocol/Behavior Tests

Shared behavior tests should cover:

- `GET` resets sliding TTL
- `HEAD` does not reset sliding TTL
- fork activity refreshes inherited sliding TTL independently of source
- long-poll and SSE admission keep a TTL stream alive while the live request is
  active

### Backend-Specific Tests

Backend-focused tests should cover:

- memory and file honoring the new admission model
- ACID read-admission transactionality
- ACID cleanup not expiring a stream whose TTL-bearing read has already been
  admitted

The last item is not a good fit for the upstream black-box conformance harness.
It needs backend-aware control over admission and cleanup timing.

## PR Shape

The most reviewable implementation sequence is:

1. design PR for this note and any related architectural terminology cleanup
2. shared model and storage API redesign
3. handler migration to admission plus snapshot reads
4. memory backend migration and behavior tests
5. file backend migration and metadata-versioning work
6. ACID backend migration and race-oriented tests
7. cleanup, docs, and changelog updates

## Open Questions

- Whether live-read lease state should track only a count or keep explicit lease
  identities for diagnostics
- Whether `updated_at` should remain "last mutation" only, or also reflect
  read-side activity in some public or internal form
- The exact shape of the immutable retention snapshot view carried by
  `StreamMetadata`, including whether it should also include explicit
  header-emission policy flags in addition to effective expiry and remaining TTL
