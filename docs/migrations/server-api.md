# Server API migration

This breaking change establishes `StreamService` as the shared stream API and
makes server startup and shutdown explicit. The client crate remains unpublished.

## Embedding and ownership

Replace `build_router(storage, &config, options)` with:

```rust
let streams = StreamService::new(storage); // accepts Arc<dyn Storage>
let server = Server::new(streams, &config, options)?;
let running = server.start()?; // inside Tokio
let app = running.router();
```

`Server::new` validates HTTP route/middleware settings and initializes persisted
subscription state. It can run before a Tokio runtime exists. Unused listener,
TLS-file, and storage-construction settings do not prevent embedding.
Construction returns an error for invalid mounts and failed control-state
initialization. `start` returns an error outside Tokio.

Clone `running` or `app` for additional listeners. Do not independently construct
a second server around the same storage `Arc`: construction rejects that owner.
The ownership guard remains alive until both routers/handles and the worker are
gone. Distinct wrappers over the same physical database remain subject to the
backend's ownership rules; pointer identity cannot detect those aliases.

For shutdown, stop accepting requests, cancel the external token to release live
reads, drain the listener, and call `running.shutdown().await`. The method cancels
and joins the subscription worker and its delivery tasks. Dropping the last
handle/router initiates cancellation but cannot await it. The server uses a
child of a supplied cancellation token, so its own shutdown does not cancel
unrelated users of that token.

## Middleware on separate HTTP surfaces

The running server provides `protocol_router()`, `admin_router()`, and
`probe_router()`, each mounted at its configured paths. They share the same
service, control state, and worker. For example:

```rust
let admin = running.admin_router().route_layer(admin_auth_layer);
let app = running.protocol_router()
    .merge(admin)
    .merge(running.probe_router());
```

`admin_auth_layer` is supplied by the embedding application. Enable admin routes
in configuration before layering them; otherwise `admin_router()` is empty.
Use these groups instead of merging them with `running.router()`, which already
contains all three surfaces. Readiness remains caller-controlled and opt-in.

## Creation and expiration

Use `StreamOptions` for creation. `StreamConfig` now describes resolved metadata:

```rust
let options = StreamOptions::new("text/plain")
    .with_ttl(60)
    .with_closed(true);
let created = streams.create("events", options, vec![Bytes::from_static(b"done")])?;
```

`Expiry` is `Never`, `Sliding(seconds)`, or `At(deadline)`. `with_ttl` and
`with_expires_at` replace the previous expiry choice. Storage normalizes the
content type, checks timestamp arithmetic, and initializes the sliding deadline
before changing state. Zero TTL expires immediately. Invalid TTLs return an
error without creating a stream. Persisted TTL metadata converted with
`creation_options()` starts a new sliding window when imported.

Stream PUT/POST payload collection is bounded by `limits.max_request_body_bytes`, default
10 MiB, or `DS_LIMITS__MAX_REQUEST_BODY_BYTES`. This counts wire bytes, including
JSON whitespace/framing, and applies to chunked requests as well as fixed bodies.
It is separate from retained stream payload limits and is not a process-wide
memory budget.

## Append outcomes and backend implementations

`append` and `append_batch` return `AppendResult`:

| Field | Meaning |
| --- | --- |
| `start_offset` | Position at which this batch began; unchanged tail for close-only |
| `next_offset` | Position from which to resume after this batch |
| `closed` | Closed state in the same committed snapshot |

Replace `batch_append(name, messages, content_type, seq)` with
`append_batch(name, messages, content_type, seq, false)`. Use `true` for a final
batch, including an empty close-only batch. Do not compose append and close as
two operations. Sequence validation, body, closure, and returned state have one
commit boundary. Close-only ignores content type but validates a supplied writer
sequence. Ordinary final appends are not idempotent; producer deduplication is
still a separate operation.

Custom backends must implement extended forks, atomic replacement, and private
subscription persistence. Unsupported defaults have been removed. `exists` is
now `Result<bool>` and `subscribe` is `Result<Option<Receiver<()>>>`: a backend
failure is not absence. Several extensible input/output types and error enums
are non-exhaustive. Start configuration from `Config::default()` and modify the
relevant fields. Use the metadata/result constructors when implementing storage
and include wildcard arms when matching extensible enums.

`storage::StreamMetadata` has been removed; import `streams::StreamMetadata`.
The unused `ShutdownToken`, `LongPollTimeout`, and `SseReconnectInterval` wrappers
have also been removed.

## File backend configuration

The append-log backend now has one mode: `file`. Change `storage.mode` in TOML
or `DS_STORAGE__MODE` from `file-fast`, `fast`, `file-durable`, or `durable` to
`file`. The old names are rejected. Existing data directories keep the same
layout and can be reopened without conversion.

Replace `StorageMode::FileFast` and `StorageMode::FileDurable` with
`StorageMode::File`. Remove calls to `StorageMode::sync_on_append()` and the
last boolean argument to `FileStorage::new`:

```rust
let storage = FileStorage::new(data_dir, max_total_bytes, max_stream_bytes)?;
```

Initial stream and fork data is always synced. Append/replacement transactions
sync at commit without a redundant sync during the record write. Creation and
deletion still have separate recovery limits; this consolidation does not make
all filesystem operations transactional.

## Import and file persistence

Import decodes and validates every payload and creation option before its first
write, including entries that a skip policy would skip. Duplicate stream names
are rejected. Each stream commits separately. A later failure is
`TransferError::PartialImport { stream, completed, source }`; completed counts
make earlier successful writes explicit. Import does not promise a transaction
across an entire document or an exclusive lock against other writers.

Replacement preserves the original on validation or pre-commit failure. It requires enough
staging capacity for both old and new payloads, and rejects sources with retained
forks and fork streams themselves. This prevents changing data already inherited
by descendants. Replacements reset offsets and producer/writer sequencing.

File append/replacement operations use an undo journal before changing the log
and metadata. An interrupted transaction is rolled back before the recovery
index is built. Failed rollback or an uncertain final sync makes the affected
stream unavailable until storage is reopened and recovered. If the final commit
acknowledgement fails, recovery may expose the committed operation; the error
does not prove that the write was absent. Replacement uses a
full backup of the old log; normal append records only its original length.
These commits sync their journal, log, metadata, and directory before returning;
no throughput improvement is claimed.

Storage and `StreamService` remain synchronous. The proposed execution boundary
for HTTP/background callers is a [separate design review with a runnable
example](../design/blocking-execution-boundary.md), following the correctness
and API work above.

## Legacy transport configuration

Legacy TOML fields are still accepted, but new configuration should use these paths:

| Legacy field | Current field |
| --- | --- |
| `server.port` | `server.bind_address` (for example, `0.0.0.0:4437`) |
| `tls.cert_path`, `tls.key_path` | `transport.tls.cert_path`, `transport.tls.key_path` |
| `log.rust_log` | `observability.rust_log` |

Set `transport.mode` explicitly when migrating TLS configuration. The
[profile files](../../crates/durable-streams-server/README.md#configuration)
show matching HTTP versions, TLS settings, and proxy trust options.
