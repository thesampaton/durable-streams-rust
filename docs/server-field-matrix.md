# Server Field Matrix

## Purpose

This note turns the conceptual taxonomy in
`docs/server-taxonomy-and-field-catalog.md` into an explicit server-local field
matrix.

It is intended to answer, for each field the server may carry or emit:

- what concept it represents
- which conceptual owner type it belongs to
- whether it is public or internal
- whether it is safe for logs, traces, and metrics
- whether it is stable enough to use as a classification dimension

This matrix remains adapter-neutral. It is not an ECS or OpenTelemetry mapping
table.

## Reading The Matrix

Columns:

- **Concept**
  The semantic field, not the final wire/key name.
- **Owner**
  Which conceptual type owns the field.
- **Category**
  Transport, domain, outcome, or failure-related grouping.
- **Visibility**
  `public`, `internal`, `restricted`, or `never_public`.
- **Cardinality**
  `stable`, `high`, or `restricted`.
- **Log**
  Whether the field is normally safe to emit into structured logs.
- **Trace**
  Whether the field is normally safe to attach to spans/events.
- **Metric**
  Whether the field is normally safe as a general metric label.
- **Response**
  Whether the field may appear in the HTTP response surface.
- **Notes**
  Short explanation or caveat.

Interpretation rules:

- `stable` means bounded enough to be considered for metrics and dashboards.
- `high` means useful for logs/traces but not safe as a general metric label.
- `restricted` means explicit policy/redaction review is required.
- `Response=yes` means it may influence or appear in the HTTP surface when the
  protocol or transport contract calls for it, not that it must always do so.

## RequestContext Fields

| Concept | Owner | Category | Visibility | Cardinality | Log | Trace | Metric | Response | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| HTTP method | RequestContext | request envelope | internal | stable | yes | yes | yes | no | Stable request dimension. |
| Normalized route | RequestContext | request envelope | internal | stable | yes | yes | yes | no | Preferred metric-safe request shape. |
| Raw path / request target | RequestContext | request envelope | internal | high | yes | yes | no | no | Useful for investigation, not metrics. |
| Query facts (allowlisted) | RequestContext | request envelope | internal | high | yes | yes | no | no | Only allowlisted/query-policy-approved facts. |
| Raw query string | RequestContext | request envelope | restricted | restricted | maybe | maybe | no | no | Requires explicit review/redaction policy. |
| HTTP protocol version | RequestContext | request envelope | internal | stable | yes | yes | yes | no | Usually bounded. |
| Request header fact (allowlisted) | RequestContext | request envelope | internal | varies | yes | yes | no | maybe | Only specific reviewed headers. |
| Non-allowlisted raw header value | RequestContext | request envelope | restricted | restricted | no | no | no | no | Not emitted by default. |
| Client address | RequestContext | peer/transport | internal | high | yes | yes | no | no | Debuggable, not metric-safe. |
| Proxy peer address | RequestContext | peer/transport | internal | high | yes | yes | no | no | Internal only. |
| Server address | RequestContext | peer/transport | internal | high | yes | yes | no | no | Internal deployment detail. |
| Scheme | RequestContext | peer/transport | internal | stable | yes | yes | yes | maybe | May affect redirects/absolute URLs later. |
| Forwarded-header trust result | RequestContext | peer/transport | internal | stable | yes | yes | yes | no | Useful bounded dimension. |
| TLS version | RequestContext | peer/transport | internal | stable | yes | yes | yes | no | If available from transport. |
| TLS cipher | RequestContext | peer/transport | internal | stable | yes | yes | maybe | no | Bounded-ish but still review before metrics. |
| ALPN protocol | RequestContext | peer/transport | internal | stable | yes | yes | yes | no | Bounded transport dimension. |
| Service name | RequestContext | service identity | internal | stable | yes | yes | yes | no | Useful deployment dimension. |
| Service version | RequestContext | service identity | internal | stable | yes | yes | yes | no | Useful release dimension. |
| Environment | RequestContext | service identity | internal | stable | yes | yes | yes | no | e.g. prod/staging/dev. |
| Instance id | RequestContext | service identity | internal | high | yes | yes | no | no | Useful for investigation, not metrics by default. |
| Host identity | RequestContext | service identity | internal | high | yes | yes | no | no | Internal deployment detail. |
| Trace id | RequestContext | correlation | internal | high | yes | yes | no | no | Transport/distributed tracing primitive. |
| Span id | RequestContext | correlation | internal | high | yes | yes | no | no | Trace-local identifier. |
| Parent span id | RequestContext | correlation | internal | high | yes | yes | no | no | Trace-local linkage. |
| Trace flags / sampling state | RequestContext | correlation | internal | stable | yes | yes | yes | no | Bounded trace metadata. |
| Request id | RequestContext | correlation | internal | high | yes | yes | no | maybe | May be propagated or surfaced intentionally. |
| Adopted inbound request correlation id | RequestContext | correlation | internal | high | yes | yes | no | maybe | Separate from server-generated request id. |
| Request start instant / timing anchor | RequestContext | timing | internal | high | no | yes | no | no | Internal timing primitive, not emitted directly in logs by default. |

## DomainContext Fields

| Concept | Owner | Category | Visibility | Cardinality | Log | Trace | Metric | Response | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Operation kind | DomainContext | operation | internal | stable | yes | yes | yes | maybe | Core domain dimension. |
| Live mode | DomainContext | operation | internal | stable | yes | yes | yes | maybe | e.g. catch-up, long-poll, sse. |
| Stream name | DomainContext | resource | internal | high | yes | yes | no | maybe | High-cardinality domain identifier. |
| Stream kind | DomainContext | resource | internal | stable | yes | yes | yes | maybe | Only if model adopts bounded kinds. |
| Source stream name | DomainContext | resource | internal | high | yes | yes | no | maybe | Relevant for fork flows. |
| Producer id | DomainContext | coordination | internal | high | yes | yes | no | maybe | Debuggable protocol actor id. |
| Producer epoch (current) | DomainContext | coordination | internal | high | yes | yes | no | maybe | Often relevant on failure. |
| Producer epoch (received) | DomainContext | coordination | internal | high | yes | yes | no | maybe | Often relevant on failure. |
| Producer seq (expected) | DomainContext | coordination | internal | high | yes | yes | no | maybe | Often relevant on failure. |
| Producer seq (actual/received) | DomainContext | coordination | internal | high | yes | yes | no | maybe | Often relevant on failure. |
| Stream seq (expected) | DomainContext | coordination | internal | high | yes | yes | no | maybe | Often relevant on failure. |
| Stream seq (actual/received) | DomainContext | coordination | internal | high | yes | yes | no | maybe | Often relevant on failure. |
| Offset from | DomainContext | lifecycle/content | internal | high | yes | yes | no | maybe | Useful for reads/debugging. |
| Offset to | DomainContext | lifecycle/content | internal | high | yes | yes | no | maybe | Useful for writes/debugging. |
| Tail offset | DomainContext | lifecycle/content | internal | high | yes | yes | no | maybe | Useful for heads/conflicts. |
| Content type | DomainContext | lifecycle/content | internal | stable | yes | yes | maybe | yes | Bounded enough to review for metrics if needed. |
| Stream closed/open state | DomainContext | lifecycle/content | internal | stable | yes | yes | yes | yes | Bounded lifecycle fact. |
| TTL present / expiry present | DomainContext | lifecycle/content | internal | stable | yes | yes | yes | maybe | Prefer coarse flags over raw timestamps for metrics. |
| Absolute expiry timestamp | DomainContext | lifecycle/content | internal | high | yes | yes | no | maybe | High-cardinality timestamp. |
| Client-supplied domain correlation id | DomainContext | correlation/domain | internal | high | yes | yes | no | maybe | Optional future field; not a server correlation primitive. |
| Storage backend | DomainContext | internal operation | internal | stable | yes | yes | yes | no | Bounded backend dimension. |
| Storage operation | DomainContext | internal operation | internal | stable | yes | yes | yes | no | Bounded if normalized. |
| Storage shard id | DomainContext | internal operation | never_public | high | yes | yes | no | no | Internal-only diagnostic detail. |
| File/log position | DomainContext | internal operation | never_public | high | yes | yes | no | no | Internal-only diagnostic detail. |

## RequestOutcome Fields

| Concept | Owner | Category | Visibility | Cardinality | Log | Trace | Metric | Response | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Outcome kind (success/failure) | RequestOutcome | outcome | internal | stable | yes | yes | yes | no | Core dashboard dimension. |
| Final HTTP status | RequestOutcome | outcome | public | stable | yes | yes | yes | yes | Canonical response dimension. |
| Status family | RequestOutcome | outcome | internal | stable | yes | yes | yes | no | Derived/coarse dimension if useful. |
| Response body shape class | RequestOutcome | outcome | internal | stable | yes | yes | yes | no | e.g. no-body/problem/json/sse/raw. |
| Response bytes sent | RequestOutcome | outcome | internal | high | yes | yes | no | no | Valuable measurement, not a label. |
| Request bytes received | RequestOutcome | outcome | internal | high | yes | yes | no | no | Valuable measurement, not a label. |
| Duration | RequestOutcome | outcome | internal | high | yes | yes | no | no | Observation value, not a label. |
| Retryable flag | RequestOutcome | outcome side effect | internal | stable | yes | yes | yes | maybe | Coarse semantic dimension. |
| Retry-after seconds | RequestOutcome | outcome side effect | public | high | yes | yes | no | yes | Header/value, not a metric label. |
| Cache behavior / cache-control class | RequestOutcome | outcome side effect | internal | stable | yes | yes | yes | yes | Prefer coarse class, not raw header strings. |
| Idempotency side-effect class | RequestOutcome | outcome side effect | internal | stable | yes | yes | yes | maybe | Coarse result such as duplicate/accepted/fenced if modeled that way. |

## FailureOutcome Fields

| Concept | Owner | Category | Visibility | Cardinality | Log | Trace | Metric | Response | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Stable failure classification | FailureOutcome | classification | internal | stable | yes | yes | yes | indirectly | Core operator/client semantic class. |
| Failure category/class | FailureOutcome | classification | internal | stable | yes | yes | yes | indirectly | Coarse class for dashboards/alerts. |
| Public problem type | FailureOutcome | public problem | public | stable | yes | yes | maybe | yes | Stable public problem semantic. |
| Public problem code | FailureOutcome | public problem | public | stable | yes | yes | yes | yes | Machine-readable public code. |
| Public problem title | FailureOutcome | public problem | public | stable | yes | yes | no | yes | Human-readable summary. |
| Public problem detail | FailureOutcome | public problem | public | high | yes | yes | no | yes | Must already be sanitized before emission. |
| Problem instance / request reference | FailureOutcome | public problem | public | high | yes | yes | no | yes | Request-specific pointer/reference. |
| Internal detail summary | FailureOutcome | internal annotation | internal | high | yes | yes | no | no | Operator-facing cause summary. |
| Internal cause chain / stack-like detail | FailureOutcome | internal annotation | restricted | restricted | maybe | maybe | no | no | Only under explicit controlled policy. |
| Failure subsystem | FailureOutcome | internal annotation | internal | stable | yes | yes | yes | no | e.g. protocol/storage/transport/startup-like boundary if used for request failures. |
| Failure backend | FailureOutcome | internal annotation | internal | stable | yes | yes | yes | no | Bounded backend dimension. |
| Failure operation | FailureOutcome | internal annotation | internal | stable | yes | yes | yes | no | Prefer normalized bounded operation names. |
| Expected value | FailureOutcome | internal annotation | internal | high | yes | yes | no | maybe | For sequencing/coordination failures. |
| Actual / received value | FailureOutcome | internal annotation | internal | high | yes | yes | no | maybe | For sequencing/coordination failures. |
| Retry-after seconds | FailureOutcome | side effect | public | high | yes | yes | no | yes | When part of failure semantics. |
| Response-visible protocol side-effect header facts | FailureOutcome | side effect | public | high | yes | yes | no | yes | Only when explicitly part of protocol behavior. |

## Placement Checklist

When deciding where a new field belongs, apply this checklist in order:

1. Does it describe the raw HTTP request or transport before domain parsing?
   If yes, it belongs in `RequestContext`.
2. Does it describe Durable Streams semantics learned during parsing or
   execution?
   If yes, it belongs in `DomainContext`.
3. Does it describe how the request finished regardless of success or failure?
   If yes, it belongs in `RequestOutcome`.
4. Does it describe why the request failed after classification?
   If yes, it belongs in `FailureOutcome`.
5. Is it only needed because of one telemetry backend's field naming rules?
   If yes, it should not enter the core field matrix.

## Operational Rules

This matrix implies these operational rules:

1. Every response-visible field must come from `RequestOutcome` or
   `FailureOutcome`, never directly from internal request or domain context.
2. Every metric label must be chosen from fields marked `Metric=yes`, or
   through an explicit exception review.
3. Fields marked `restricted` require explicit redaction and sink policy before
   emission.
4. Fields marked `never_public` must have no serialization path into client
   payloads.
5. High-cardinality identifiers are expected in logs/traces, but not in general
   dashboards or counters.

## Current Repo Implications

Applying this matrix to the current codebase would push the server toward:

- a typed request context created at ingress rather than implicit telemetry
  field assembly
- a typed domain context enriched by handlers and protocol helpers
- a canonical request outcome that owns final status and side effects
- a failure outcome that owns the public/internal split
- telemetry projection that reads from these conceptual owners instead of
  directly defining field semantics inside middleware and error builders

## Next Step

The next useful step would be to turn this field matrix into one of:

- an ADR for implementation sequencing
- a server-local constants module and field inventory
- a first cut of the concrete Rust types and edge adapters

That code-sketch work now lives at `docs/server-implementation-sketch.md`.
