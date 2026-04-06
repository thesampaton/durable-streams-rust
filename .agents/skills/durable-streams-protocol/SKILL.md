---
name: durable-streams-protocol
description: >
  Durable Streams protocol semantics. Use when implementing client or server
  behaviour at the HTTP protocol level: stream lifecycle, create/append/close/
  delete/head/read semantics, offsets and reserved sentinels, JSON mode, live
  read modes (`live=long-poll`, `live=sse`), `Stream-Seq`, idempotent producer
  headers, caching/collapsing headers, TTL/expiry, and `Stream-Closed` EOF
  signalling.
sources:
  - "durable-streams/durable-streams:PROTOCOL.md"
user-invocable: false
---

# Durable Streams Protocol

Protocol-only reference for this workspace. This skill is about HTTP semantics
defined by upstream `PROTOCOL.md`, not about any specific client SDK or server
implementation.

When in doubt, the upstream protocol document is authoritative.

## Standards Alignment

This skill is expected to track the workspace's currently recorded protocol
baseline, not an unspecified "recent" protocol.

Current baseline for this repo:

- protocol document: Durable Streams Protocol `1.0-draft`
- protocol revision: `f091f315e4c3ca6769b45135ab8b5f53d70b1751`
- source of truth in-repo: `docs/standards.md`
- machine-readable metadata: `Cargo.toml` under `[workspace.metadata.durable-streams]`

Maintenance rule:

- if the upstream protocol changes, verify this skill against the latest
  `PROTOCOL.md`
- then update `docs/standards.md`, `Cargo.toml` metadata, and this skill
  together
- do not treat this skill as current if those records are out of sync

## Scope

This skill should define:

- what the protocol requires or recommends
- which headers, query parameters, and status codes matter
- semantics shared by client and server implementations

This skill should not define:

- Rust crate API shape
- convenience client behaviour
- SDK retry strategies
- callback models, buffering, or `flush()`-style APIs

Those belong in crate-specific skills later.

## Stream Model

A durable stream is a URL-addressable, append-only byte stream with these core
properties:

- durable once acknowledged
- immutable by position
- strictly ordered by opaque, lexicographically sortable offsets
- created with a fixed content type
- either open or closed

Closure is durable, monotonic, and observable. After closure, existing data
remains readable, but new appends are rejected.

## Core Operations

| Operation | Method | Key Semantics |
|-----------|--------|---------------|
| Create | `PUT {url}` | Idempotent create-or-ensure semantics. `201` on create, `200` if existing config matches, `409` if it does not. |
| Append | `POST {url}` | Appends bytes to an existing stream. Empty body is invalid unless closing. |
| Close | `POST {url}` + `Stream-Closed: true` | Close-only or append-and-close in one atomic step. |
| Delete | `DELETE {url}` | Deletes the stream. |
| Metadata | `HEAD {url}` | Returns metadata such as content type, tail offset, expiry, and closure status. |
| Catch-up read | `GET {url}?offset=<offset>` | Returns existing data from an offset and stops. |
| Long-poll read | `GET {url}?offset=<offset>&live=long-poll` | Waits for new data or timeout. |
| SSE read | `GET {url}?offset=<offset>&live=sse` | Streams data via Server-Sent Events. |

The protocol does not define URL structure. It defines the HTTP behaviour at a
stream URL.

## Offsets

Offsets are opaque tokens. Clients and libraries must never parse, construct, or
perform arithmetic on them.

Reserved sentinel request values:

| Value | Meaning |
|-------|---------|
| `"-1"` | Start of stream |
| `"now"` | Current tail position; skip historical data |

Rules:

- Servers must not generate `-1` or `now` as real offsets.
- Successful reads include `Stream-Next-Offset`.
- Successful appends include `Stream-Next-Offset`.
- Clients should persist the returned offset for resumption.

`offset=now` has protocol-defined behaviour:

- catch-up mode returns immediately with no data and `Stream-Up-To-Date: true`
- long-poll mode waits for future data without an initial empty response
- SSE mode starts the live stream at the current tail
- on closed streams, the server must return the closure signal immediately

## Stream Creation And Closure

Creation is by `PUT`.

Important rules:

- `Content-Type` sets the stream content type
- `Stream-TTL` and `Stream-Expires-At` are optional lifecycle controls
- supplying both TTL and absolute expiry should be rejected with `400 Bad Request`
- `Stream-Closed: true` on `PUT` creates the stream already closed

Closure is by `POST` with `Stream-Closed: true`.

Important rules:

- header value is only treated as present when it is exactly `true`
  case-insensitively
- close-only `POST` with empty body is valid
- append-and-close is atomic
- close-only requests to an already closed stream should behave idempotently
- appending to a closed stream returns `409 Conflict` and should expose
  `Stream-Closed: true`

## Content Types

The protocol is byte-oriented for most content types. `application/json` is
special and has message-boundary semantics.

### Generic byte mode

For non-JSON content types:

- reads return raw bytes
- framing is application-specific
- clients are responsible for interpreting message boundaries

### JSON mode

For `Content-Type: application/json`:

- servers must preserve message boundaries
- `GET` responses must return a JSON array
- if no messages are in range, the body must be `[]`
- `POST` bodies containing a JSON array must be flattened exactly one level
- empty JSON array append bodies (`[]`) must be rejected with `400 Bad Request`
- `PUT` with an empty JSON array body is valid and creates an empty stream

This is protocol behaviour, not a client-library convenience.

## Live Read Modes

The protocol defines two explicit live modes:

| Query | Semantics |
|-------|-----------|
| `live=long-poll` | Request/response long-polling |
| `live=sse` | SSE stream |

Long-poll:

- clients re-request after each response
- `204 No Content` with `Stream-Up-To-Date: true` indicates caught-up state
- `Stream-Cursor` is required on live responses while the stream is open

SSE:

- response `Content-Type` must be `text/event-stream`
- control events carry `streamNextOffset`
- binary content types must be base64-encoded in SSE data events and signalled
  via the response header
- clients must tolerate missing cursor fields once the stream is closed

`live=true` auto-selection is not protocol-level semantics. If a client SDK
supports that convenience, it belongs in a client-specific skill.

## Ordering And Idempotency

The protocol has two distinct coordination mechanisms.

### `Stream-Seq`

`Stream-Seq` is an optional monotonic, lexicographic writer sequence for
coordination.

- values are opaque strings
- ordering is simple byte-wise lexicographic comparison
- regressions must be rejected with `409 Conflict`
- servers must document the scope they enforce

### Idempotent producers

The protocol also defines Kafka-style idempotent producer headers:

- `Producer-Id`
- `Producer-Epoch`
- `Producer-Seq`

Rules:

- all three must be provided together or omitted together
- stale epochs are rejected with `403 Forbidden`
- sequence gaps are rejected with `409 Conflict`
- duplicates are idempotent success
- successful and error responses include producer state headers defined by the
  protocol

This is protocol-level transport/write coordination. Any higher-level producer
API belongs in a client crate skill, not here.

## Caching And Collapsing

The protocol is designed to support CDN-friendly historical reads and collapsed
live waiting.

Important headers:

| Header | Role |
|--------|------|
| `Cache-Control` | Cache policy, especially for historical reads |
| `ETag` | Validation for repeat catch-up requests |
| `Stream-Cursor` | Live-mode collapsing key for long-poll/SSE workflows |
| `Stream-Next-Offset` | Resume position for subsequent requests |
| `Stream-Up-To-Date` | Explicit caught-up signal |

Important rules:

- historical reads can be cached
- HEAD responses should be effectively non-cacheable
- `offset=now` responses should not be cached
- live-mode responses on open streams must carry cursor information
- clients should echo `Stream-Cursor` as `cursor=<cursor>` on subsequent
  long-poll requests
- cursor behaviour exists to improve collapsing and avoid cache-loop pathologies

## Implementation Pitfalls

### Offsets

- never inspect offset structure
- do not assume `"0"` means start-of-stream
- always carry forward the returned `Stream-Next-Offset`

### Closure

- treat `Stream-Closed` as a durable stream-state signal
- do not model closure as merely "no data available right now"
- preserve closed/open mismatch semantics on idempotent `PUT`

### JSON mode

- do not treat JSON streams as arbitrary text blobs
- do not lose message boundaries
- do not accept empty JSON array append bodies

### Live reads

- do not collapse protocol semantics into client convenience flags
- keep long-poll and SSE behaviour distinct
- handle `offset=now` explicitly

### Caching

- do not treat `Stream-Cursor` as optional live-mode decoration
- do not make HEAD responses broadly cacheable
- do not let `ETag` hide closure transitions

## Working Rule For This Repo

If a behaviour is shared across client and server implementations and can be
traced directly to `PROTOCOL.md`, it belongs here.

If a behaviour describes how a specific Rust crate chooses to expose or
implement the protocol, it belongs in a future crate-specific skill instead.
