# Subscriptions

Subscriptions wake a worker when linked streams have data beyond their saved
cursor. Both delivery types use the same generation fencing and lease model.
The API is mounted at `<stream-root>/__ds`; the default stream root is
`/v1/stream`. Application streams cannot use `__ds` as their first segment.

## Create and manage

Create a JSON wake stream first for pull-wake delivery:

```http
PUT /v1/stream/wake/pool
Content-Type: application/json

[]
```

Then create the subscription:

```http
PUT /v1/stream/__ds/subscriptions/orders
Content-Type: application/json

{"type":"pull-wake","pattern":"orders/**","streams":["manual/orders"],"wake_stream":"wake/pool","lease_ttl_ms":30000}
```

Creation returns 201. Repeating the normalized configuration returns 200;
changing a configuration under the same ID returns 409. Explicit streams are
sorted and deduplicated for identity. A deleted ID remains tombstoned and cannot
be reused. Choose a new ID when replacing a subscription configuration.

`*` matches one path segment; `**` matches zero or more. Existing matching streams
and newly added explicit streams are linked at their current tail. A stream
created later starts with its initial data pending. An absent explicit stream
can be linked before creation. Explicit membership takes precedence over a glob
link; removing it preserves the glob link when the pattern still matches.

| Method | Path under the stream root | Purpose |
| --- | --- | --- |
| PUT / GET / DELETE | `__ds/subscriptions/{id}` | Create, inspect, tombstone |
| POST | `__ds/subscriptions/{id}/streams` | Add `{"streams":["path"]}` |
| DELETE | `__ds/subscriptions/{id}/streams/{path}` | Remove an explicit link; URL-encode the path |
| POST | `__ds/subscriptions/{id}/claim` | Claim with `{"worker":"name"}` |
| POST | `__ds/subscriptions/{id}/ack` | Acknowledge or extend a pull-wake lease |
| POST | `__ds/subscriptions/{id}/release` | Release a pull-wake lease |
| POST | `__ds/subscriptions/{id}/callback` | Acknowledge webhook work |
| GET | `__ds/jwks.json` | Discover webhook verification keys |

## Claims and acknowledgements

Read the ordinary wake stream to discover work, then POST to `claim`. Only one
worker can hold a subscription lease. A successful claim returns `wake_id`,
`generation`, `token`, `streams`, and `lease_ttl_ms`. A concurrent claim returns
409 `ALREADY_CLAIMED`, including the current holder.

Send the returned token using `Authorization: Bearer <token>` to ack or release:

```json
{"wake_id":"<wake_id>","generation":1,"acks":[{"stream":"orders/one","offset":"<processed-tail>"}],"done":true}
```

Offsets are opaque: carry values returned by the stream server. Acking the
processed tail records that everything before that next-read position has been
processed. Acks cannot regress, exceed the current tail, or name unlinked
streams. The whole acknowledgement batch is validated before any cursor moves.
Omit `done` (and optionally `acks`) to extend the lease. Leases range from one
second to ten minutes and default to 30 seconds. A release does not advance
cursors. Pending work after release or expiry causes a new generation.

Tokens are Ed25519-signed and scoped to the subscription and wake generation.
Every use verifies the signature and the current persisted lease; expiry,
completion, deletion, or replacement of the generation revokes the token.
Heartbeat renewal extends the server-side lease without replacing the token.
Stale requests return 409 `FENCED`; a deleted subscription returns 404.

## Webhooks

Use `"type":"webhook"` with `"webhook":{"url":"https://worker.example/hook"}`
in place of `wake_stream`. Notifications include stream cursor/tail snapshots,
`callback_url`, and `callback_token`. Respond with `{"done":true}` to ack exactly
the delivered snapshot and release the lease. New data after the snapshot remains
pending. Otherwise use the callback endpoint with the token to ack or heartbeat.

The `Webhook-Signature` header contains `t`, `kid`, and `ed25519`. Verify the
Ed25519 signature over the exact `<timestamp>.<raw_body>` bytes, using the key
selected by `kid` from the advertised JWKS URL. Apply a replay window such as five
minutes. Keys are persisted across restarts; automatic key rotation is not
currently exposed. JWKS responses use `application/jwk-set+json` with a five-minute
public cache lifetime and never disclose private key material.

The server validates all DNS answers and pins them for each delivery, disables
redirects and environment proxies, limits callback response bodies, and applies
DNS/request timeouts. Private, loopback, link-local, metadata, and reserved IP
ranges are rejected. HTTPS is required by default. For local testing only, set
`http.allow_insecure_webhooks = true` or
`DS_HTTP__ALLOW_INSECURE_WEBHOOKS=true` to permit HTTP on `localhost` and
`127.0.0.x`. This is enabled by the conformance launcher for its local receivers.

Failed delivery retries start at one second, doubling up to 60 seconds with
20% jitter. The next attempt deadline is persisted before retries. Subscription
metadata reports `status: failed` while a retry is scheduled.

## Persistence and operation

The memory backend keeps control state in memory. The file backend atomically
replaces and fsyncs `subscriptions.json` alongside their stream directories; on
Unix it is created with mode 0600. ACID backends use a separate redb table in
shard zero with immediate durability. ACID in-memory mode is ephemeral. Stored
state includes signing keys, normalized configuration hashes, membership,
cursors, generations, leases, tombstones, and retry deadlines. It is private to
the server and cannot be read or forked through stream URLs or stream listings.
Protect the storage directory and include the control state in backups.
The stream export/import command transfers application streams, not subscription
control state or signing keys.

A background worker reconciles stream tails every 100 ms, without renewing stream
TTLs, and runs up to 16 webhook deliveries concurrently. It resumes persisted
subscriptions on startup and stops when the server is cancelled. Wake delivery
is at least once: a crash after a wake-stream append but before recording delivery
may repeat that generation. Claims and generation fencing prevent duplicate
workers from holding its lease. Callbacks should tolerate repeated delivery.

Use one `Server` owner per storage instance; cloned running handles and routers
share its worker and state. Construct the server before or inside Tokio, then
call `start` inside the runtime and await `RunningServer::shutdown` when stopping.
Custom `Storage` backends must implement subscription snapshot persistence. See
the [embedding guide](../crates/durable-streams-server/README.md#embedding) for
listener ownership and shutdown.

As with this server's stream and admin surfaces, subscription management and
claim authentication belong to the embedding application or trusted reverse
proxy. Protect these endpoints with the deployment's service authentication and
stream-access policy; a worker name is not authentication. Callback/ack/release
bearer tokens are verified by the server and must be forwarded unchanged.
Production traffic must use TLS, and the configured proxy trust policy must
provide the public origin used to construct callback and JWKS URLs.
