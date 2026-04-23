# Server Taxonomy and Field Catalog

## Purpose

This note defines the taxonomy and field catalog implied by:

- `docs/server-request-outcome-architecture.md`
- `docs/server-context-outcome-model.md`
- `docs/server-tower-edge-stack.md`

It is the first note that moves from broad architecture into concrete
categories, but it is still intentionally server-local and pre-adapter. The
goal is to define a canonical semantic model for identifiers, context fields,
and outcomes before mapping anything into ECS, OpenTelemetry exporters, or
backend-specific logging/tracing systems.

## Scope

This note defines:

- identifier taxonomy
- request-context field categories
- domain-context field categories
- request outcome taxonomy
- failure outcome taxonomy
- visibility policy
- cardinality policy

It does not define:

- exact Rust struct definitions
- exact JSON or tracing field names
- sink-specific schema mappings
- exporter or transport details
- retention or sampling policy

## Design Goal

The taxonomy should make these questions answerable without guesswork:

- what kind of identifier is this?
- where does this field belong?
- is this field request-shaped, domain-shaped, or outcome-shaped?
- is this field public, internal-only, or sink-restricted?
- is this field safe for logs, traces, and metrics?
- is this field a stable classification or a high-cardinality annotation?

## Taxonomy Principles

The taxonomy should follow these principles:

1. Context and outcome are different categories.
2. Transport correlation and domain correlation are different categories.
3. Stable classifications and high-cardinality annotations are different
   categories.
4. Public problem payload fields and internal telemetry fields are different
   categories.
5. Metric-safe labels are a strict subset of log/trace-safe fields.
6. A field belongs to one canonical category even if multiple projections emit
   it later.

## Identifier Taxonomy

Identifiers should be treated as separate semantic classes, not as one generic
"correlation id" bucket.

### 1. Trace Identifiers

Role:

- cross-process distributed tracing

Examples:

- trace id
- span id
- parent span id
- sampling or trace flags

Ownership:

- ingress context

Properties:

- transport-level correlation primitive
- not a domain identifier
- generally log-safe and trace-safe
- not suitable as a metric label

### 2. Request Identifiers

Role:

- server-side correlation for one HTTP request through the server edge

Examples:

- server-generated request id
- optionally propagated request id if the server supports it

Ownership:

- ingress context

Properties:

- request-scoped
- transport-facing
- distinct from trace id
- useful in logs and support workflows
- not a replacement for domain identifiers

### 3. Client Correlation Identifiers

Role:

- caller-supplied identifiers intended to help correlate client and server
  activity

Examples:

- explicit client correlation header if the protocol or deployment later adopts
  one
- client-provided transaction-like request marker

Ownership:

- discovered at ingress or protocol parsing depending on where it appears

Properties:

- optional
- never the server's sole correlation primitive
- may be response-visible if explicitly part of the protocol
- log-safe and trace-safe subject to validation/redaction policy
- not suitable as a metric label

### 4. Domain Identifiers

Role:

- identify Durable Streams resources and protocol actors

Examples:

- stream name
- producer id
- fork source reference
- stream path lineage identifiers if introduced later

Ownership:

- domain context

Properties:

- domain-scoped
- often high-cardinality
- useful in logs and traces
- generally unsafe as metric labels by default

### 5. Domain Coordination Identifiers

Role:

- express ordering, fencing, and write coordination facts

Examples:

- stream seq
- producer epoch
- producer seq
- offsets
- tail position
- expected/actual values on conflict

Ownership:

- domain context and failure outcome

Properties:

- domain-semantic, not transport-semantic
- often useful on failure and debugging paths
- safe in logs and traces with care
- not suitable as general metric labels

## RequestContext Field Catalog

`RequestContext` should carry transport-shaped request facts.

### Request Envelope Fields

Examples:

- HTTP method
- normalized route
- request target/path shape
- selected query facts under explicit policy
- protocol version
- request header facts that are explicitly allowed

Purpose:

- describe what request entered the system

Visibility:

- internal telemetry
- small subset may influence public response behavior

Metric safety:

- route and method may be metric-safe
- raw path and query are not metric-safe by default

### Peer And Transport Fields

Examples:

- client address
- proxy peer address
- server address
- scheme
- forwarded-header trust result
- TLS version/cipher/alpn if available

Purpose:

- describe network provenance and transport context

Visibility:

- internal telemetry only

Metric safety:

- mostly not metric-safe

### Service Identity Fields

Examples:

- service name
- version
- environment
- instance id
- host identity if recorded

Purpose:

- locate the request within the deployed service topology

Visibility:

- internal telemetry

Metric safety:

- service/environment may be metric-safe
- instance/host generally not by default

### Correlation Fields

Examples:

- trace identifiers
- request identifier
- optional adopted inbound correlation identifiers

Purpose:

- join request-scoped evidence across signals and systems

Visibility:

- internal telemetry
- selected request ids may optionally be response-visible if explicitly adopted
  as part of the transport contract

Metric safety:

- not metric-safe

## DomainContext Field Catalog

`DomainContext` should carry Durable Streams semantics discovered during
processing.

### Operation Fields

Examples:

- append/read/fork/delete/close/create
- live read mode
- protocol sub-operation or mode when relevant

Purpose:

- describe what the request is trying to do in domain terms

Visibility:

- internal telemetry
- operation semantics may also affect response construction

Metric safety:

- generally metric-safe

### Resource Fields

Examples:

- stream name
- stream kind such as root/fork/tombstone if the model adopts it later
- source stream identifiers for fork operations

Purpose:

- identify the resource being acted on

Visibility:

- internal telemetry
- selected resource identifiers may appear in public error detail when the
  protocol already exposes them

Metric safety:

- not metric-safe by default

### Coordination Fields

Examples:

- producer id
- producer epoch
- producer seq
- stream seq
- expected/actual coordination values

Purpose:

- describe concurrency, fencing, and idempotency facts

Visibility:

- internal telemetry
- selected values may also be surfaced through response headers if the protocol
  requires them

Metric safety:

- not metric-safe by default

### Content And Lifecycle Fields

Examples:

- content type
- stream closed/open state
- ttl or expiry facts
- offset range or tail position

Purpose:

- describe content, lifecycle, and position semantics

Visibility:

- internal telemetry
- selected fields may influence public responses and headers

Metric safety:

- operation-level lifecycle booleans may be metric-safe
- offsets and exact content metadata are not metric-safe by default

### Internal Operation Fields

Examples:

- storage backend
- shard id
- file/log position
- storage operation name

Purpose:

- support operator diagnosis and backend-specific debugging

Visibility:

- internal telemetry only
- never part of public problem payloads

Metric safety:

- backend name may be metric-safe
- shard ids and positions are not metric-safe by default

## RequestOutcome Taxonomy

`RequestOutcome` should classify completion at a stable semantic level.

### 1. Success Outcome

Role:

- canonical successful completion

Examples of properties:

- final status
- result class
- response side effects
- completion metadata needed for emission

Stable dimensions:

- success
- operation kind
- status family or exact status where useful

### 2. Failure Outcome

Role:

- canonical failed completion

Examples of properties:

- final status
- failure classification
- public problem shape
- internal detail
- retry/cache/idempotency side effects

Stable dimensions:

- failure
- failure kind/class/category
- status family or exact status where useful

### Outcome Dimensions

The outcome model should support stable dimensions such as:

- success vs failure
- operation
- status
- retryability
- response side effects

These dimensions are appropriate for metrics and dashboards only when their
cardinality remains bounded.

## FailureOutcome Taxonomy

`FailureOutcome` needs two layers of taxonomy:

1. stable classification for operators and clients
2. richer internal annotations for debugging

### Stable Failure Classification

This is the coarse-grained classification used for:

- public problem semantics
- high-level dashboards and alerts
- metric-safe outcome dimensions

Examples of categories that likely belong here:

- invalid request
- not found
- conflict
- forbidden
- unavailable
- insufficient capacity
- internal error

This level should stay relatively small and stable.

### Failure Annotations

These are richer facts attached to a specific failure instance.

Examples:

- subsystem
- backend
- operation
- expected vs actual values
- internal cause summary
- retry-after seconds

These are primarily for logs and traces, not general metric labels.

### Public Problem Taxonomy

Public problem payloads should be derived from `FailureOutcome`, not defined
independently.

That public taxonomy should include concepts such as:

- public type/code/title/detail shape
- status
- allowed response-visible side effects

It should not expose:

- backend internals
- stack or source error chains by default
- internal-only operation fields

## Visibility Policy

Every field category should be classified by visibility.

### Public

May be sent to clients as part of the protocol surface.

Examples:

- status
- public problem fields
- response headers explicitly defined by protocol behavior

### Internal Telemetry

May be emitted to logs, traces, and internal observability sinks.

Examples:

- request context
- domain context
- failure annotations
- backend and subsystem details

### Internal Restricted

Should be available only to tightly controlled sinks or omitted by default.

Examples:

- potentially sensitive raw header/query material
- unusually verbose internal detail
- stack-like diagnostic chains if later added

### Never Public

Some categories should have an explicit "never public" rule.

Examples:

- storage backend internals
- shard ids
- file/log offsets
- unreviewed raw source error strings

## Cardinality Policy

Cardinality policy should be first-class, not an afterthought.

Every field should eventually be assigned one of these operational classes.

### Metric-Safe Stable Fields

Bounded cardinality and suitable for counters/histograms.

Examples:

- normalized route
- method
- operation kind
- success/failure
- stable failure classification
- backend class if the set is bounded

### Log/Trace-Only High-Cardinality Fields

Useful for investigation but unsafe for general metrics.

Examples:

- stream name
- request id
- trace id
- producer id
- exact offsets
- shard ids

### Restricted Diagnostic Fields

Potentially high-cardinality and sensitive enough that they require explicit
policy before emission.

Examples:

- raw query values
- non-allowlisted headers
- internal detail copied from raw lower-layer errors

## Field Placement Rules

The taxonomy should follow these placement rules:

1. If a field describes the HTTP request before domain parsing, it belongs in
   `RequestContext`.
2. If a field describes Durable Streams semantics discovered during processing,
   it belongs in `DomainContext`.
3. If a field describes how the request completed, it belongs in
   `RequestOutcome`.
4. If a field describes why the request failed after classification, it belongs
   in `FailureOutcome`.
5. If a field only exists to satisfy a sink-specific schema, it does not belong
   in the core taxonomy.

## Current Repo Implications

Against the current server implementation, this taxonomy implies:

- `middleware::telemetry` should eventually project from categorized request,
  domain, and outcome fields rather than defining the field surface directly
- `protocol::error::Error` should eventually map into a failure taxonomy rather
  than being the de facto owner of both HTTP and telemetry semantics
- `protocol::problem::ProblemResponse` should eventually become a response
  projection of `FailureOutcome`, not the canonical failure data model itself

## Suggested Next Step

The next useful design step is to turn this taxonomy into a more explicit field
table for the server:

- conceptual field
- category
- owner type
- visibility
- cardinality class
- response-visible or not
- log-safe / trace-safe / metric-safe

That table now lives at `docs/server-field-matrix.md`.
