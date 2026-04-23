# Server Request, Outcome, and Observability Architecture

## Purpose

This note defines the broad server architecture we are moving toward for:

- request-scoped context
- error classification and response shaping
- structured observability

It is intentionally earlier and broader than any detailed taxonomy. The goal is
to define the seams, ownership boundaries, and request flow first, then let the
taxonomy and field catalog fall out of that structure.

## Problem

The current server has partial structure, but the shape is split across more
than one seam:

- inbound request telemetry is assembled in
  `crates/durable-streams-server/src/middleware/telemetry.rs`
- RFC 9457 problem rendering lives in
  `crates/durable-streams-server/src/protocol/problem.rs`
- protocol/storage error mapping lives in
  `crates/durable-streams-server/src/protocol/error.rs`

That is already enough to be useful, but not enough to be a stable contract.
The result is that:

- request context is not the single source of truth for telemetry
- error classification and telemetry shaping are partially duplicated
- framework-originated failures can still bypass the same path
- public problem payloads and internal telemetry remain too close in shape
- the architecture does not yet state which layer owns context, classification,
  transport side effects, and emission

## Non-Goals

This note does not decide:

- exact field names for every log/span/metric attribute
- ECS, OpenTelemetry, or sink-specific adapter details
- exporter or transport choice
- sampling, retention, or backend query tooling
- client SDK behavior beyond the server's emitted HTTP surface

Those are follow-on decisions once the architecture is fixed.

## Architectural Direction

The target shape is four explicit seams:

1. Ingress context seam
2. Domain/application seam
3. Outcome seam
4. Emission seam

The core rule is:

- ingress gathers context
- domain code enriches context
- outcome code classifies what happened
- emission code renders responses and telemetry

No single concern should be owned by more than one seam.

## Seams

### 1. Ingress Context Seam

This is the HTTP/framework edge. It is responsible for creating the canonical
request-scoped context before domain logic runs.

Inputs:

- method, route, path, query policy
- peer and transport metadata
- ambient trace context
- server-generated request correlation
- service identity

Outputs:

- root request span
- request-scoped context value available to downstream code

Responsibilities:

- normalize request envelope fields once
- create or adopt correlation identifiers
- attach safe request metadata to the request span/context
- ensure later layers do not need to reconstruct basic HTTP facts

Non-responsibilities:

- protocol validation
- error classification
- ad hoc client response building

### 2. Domain/Application Seam

This is where handlers, extractors, protocol parsing, and application logic
interpret the request and attach Durable Streams semantics.

Inputs:

- request context from ingress
- raw protocol inputs from axum/extractors

Outputs:

- enriched domain context
- success/failure results from protocol and storage operations

Responsibilities:

- parse and validate protocol inputs
- attach domain identifiers and operation facts as they become known
- keep handler code thin while still being the place where request-specific
  domain context is discovered
- open child spans for meaningful internal operations

Examples of fields discovered here:

- stream name
- operation kind (`append`, `read`, `fork`, `delete`, `close`)
- producer id / epoch / seq
- stream seq
- offsets
- content type
- closed/open state
- optional client correlation fields, if the protocol later defines them

Non-responsibilities:

- deciding the final public problem payload
- emitting final request logs/metrics
- inventing separate telemetry copies of already-known context

### 3. Outcome Seam

This is the canonical decision point for "what happened".

Every request must terminate in one typed outcome. At minimum:

- success outcome
- failure outcome

This seam owns:

- status classification
- public/internal detail split
- transport-level side effects
- canonical error/outcome semantics

This seam should be the only place where a failure becomes a public problem
response shape. Subsystems may produce rich local errors, but they must be
adapted into the canonical outcome before the HTTP response is rendered.

This is the seam that should absorb:

- protocol/domain failures
- storage failures
- axum extractor rejections
- body and content-type failures
- tower/layer/service failures that should surface as HTTP errors

### 4. Emission Seam

This seam projects request context plus request outcome into concrete outputs:

- HTTP response
- structured logs/events
- completed request span fields
- metric emissions

This seam renders. It does not classify.

That means:

- the problem payload is rendered here from the canonical failure outcome
- telemetry fields are projected here from request context plus outcome
- metric labels are selected here under an explicit cardinality policy

## Core Types

The exact Rust types can change, but the architecture needs these conceptual
shapes.

### Request Context

Canonical request-scoped metadata created at ingress.

Expected contents:

- request envelope
- peer/network data
- service identity
- trace/request correlation
- redaction-aware request metadata

### Domain Context

Durable Streams-specific metadata discovered after parsing begins.

Expected contents:

- stream identifiers and operation kind
- producer and sequencing fields
- offset and content metadata
- optional client-supplied correlation/domain identifiers
- internal operation tags that are safe to carry in-process

This may be a distinct type or a mutably enriched portion of the request
context, but the ownership model must stay explicit.

### Request Outcome

Canonical representation of what the request produced.

Expected contents:

- success vs failure
- HTTP status
- outcome/result semantics
- response side effects
- bytes/duration/finalization facts as needed

### Failure Outcome

Failure-specific portion of the request outcome.

Expected contents:

- stable error classification
- public problem payload data
- internal-only detail
- retry/cache/idempotency side effects
- subsystem-specific annotations needed for telemetry

The public problem payload and internal telemetry data must not share the same
serialized type.

## Broad Request Flow

1. Ingress middleware receives the HTTP request.
2. Ingress creates `RequestContext`, request correlation, and the root span.
3. Handler and extractor code parse inputs and enrich domain context.
4. Internal/storage operations open child spans and record internal operation
   fields.
5. Application code returns a typed `RequestOutcome`.
6. Emission renders the HTTP response and final telemetry from
   `RequestContext + RequestOutcome`.

This gives one canonical path for request completion regardless of whether the
request ended in success, protocol failure, storage failure, or framework
rejection.

## Boundary Ownership

The architecture should make these ownership rules explicit.

### Framework Edge

Owns:

- request entry
- raw extraction and middleware plumbing
- adaptation of framework-native failures into canonical outcomes

### Protocol Edge

Owns:

- HTTP header/query/body semantics
- request validation
- discovery of domain-specific context

### Storage Edge

Owns:

- backend-specific errors and internal operation metadata
- child spans for backend work
- internal-only operation annotations such as backend/shard/log position

### Response/Telemetry Edge

Owns:

- HTTP serialization
- structured event/log projection
- metric-safe projection

## Architectural Rules

These rules are more important than exact type names.

1. Every request creates one canonical request context.
2. Every request terminates in one canonical outcome.
3. The outcome seam is the only place where failures are classified for public
   response purposes.
4. Public problem payloads and internal telemetry are separate projections from
   the same outcome, not the same data model.
5. Framework-originated failures must pass through the same outcome path as
   domain failures.
6. Domain identifiers are annotations on the request context/span, not a
   replacement for request or trace correlation primitives.
7. High-cardinality fields may be carried in logs/spans, but never become
   metric labels by default.
8. The rendering layer must not invent semantics that were not present in the
   canonical outcome.

## Correlation Model

This architecture assumes multiple distinct correlation concepts:

- trace correlation
- server-side request correlation
- optional client-supplied correlation
- domain identifiers such as stream name, producer id, sequence/epoch, and
  any future transaction-like identifier

These must remain distinct even when they are emitted together. A domain field
must not silently become the server's canonical request correlation id.

## Why Taxonomy Comes Later

Taxonomy should be a realization of this structure, not a precursor to it.

Once the seams above are fixed, the later taxonomy work can answer:

- which fields belong in request context
- which belong in domain context
- which belong in failure outcome
- which are log-safe, trace-safe, response-visible, internal-only, or
  metric-safe

Without this architecture first, taxonomy work risks encoding accidental
properties of the current implementation.

## Follow-On Design Notes

- `docs/server-context-outcome-model.md` defines the conceptual request,
  domain, and outcome model that realizes this architecture without yet fixing
  a detailed field taxonomy.

## Migration Direction

The likely migration path is:

1. Define the canonical request/outcome interfaces in server-local code.
2. Move response shaping behind the outcome seam.
3. Adapt existing `protocol::error::Error` and `ProblemResponse` usage into the
   new outcome model.
4. Route framework-originated failures through the same path.
5. Move telemetry projection to consume request context plus outcome rather than
   ad hoc copies of overlapping data.
6. Only then freeze field taxonomy and adapter mappings.

## Current Repo Implications

This note implies eventual change around:

- `crates/durable-streams-server/src/middleware/telemetry.rs`
- `crates/durable-streams-server/src/protocol/problem.rs`
- `crates/durable-streams-server/src/protocol/error.rs`
- custom extractors and rejection adapters such as
  `crates/durable-streams-server/src/protocol/stream_name.rs`
- router-level middleware/error adaptation in
  `crates/durable-streams-server/src/router.rs`

It does not imply immediate workspace extraction or new crates. The right first
step is to stabilize the server-local architecture and obligations.
