# Server Context and Outcome Model

## Purpose

This note defines the conceptual server-local types that realize the broad
architecture in `docs/server-request-outcome-architecture.md`.

It is still intentionally pre-taxonomy. The goal here is to make ownership and
responsibility explicit before deciding exact field catalogs, namespacing, or
adapter mappings.

## Scope

This note describes the conceptual model for:

- request-scoped context
- domain-specific request enrichment
- canonical request outcome
- canonical failure outcome

It does not freeze:

- exact Rust type names
- exact field names
- exact serialization formats
- exact metric label sets
- ECS or OpenTelemetry mappings

## Why These Types Exist

The target architecture needs a small number of stable conceptual shapes so
that:

- request metadata is gathered once
- domain metadata is enriched in one place
- request completion is represented once
- failures are classified once
- response rendering and telemetry emission become projections, not parallel
  hand-maintained models

Without these conceptual types, the code drifts toward ad hoc builders and
duplicated matches.

## Type Overview

The model has four primary shapes:

1. `RequestContext`
2. `DomainContext`
3. `RequestOutcome`
4. `FailureOutcome`

The first two describe what is known about the request while it is being
processed. The latter two describe what the request produced when it completes.

## RequestContext

### Role

`RequestContext` is the canonical request-scoped context created at ingress.

It is the stable home for transport-shaped facts that should not need to be
re-derived later in the request path.

### Ownership

Owned by the ingress seam.

Created before domain logic runs.

Available to downstream handlers, middleware, and emitters through request
extensions, request-scoped state, span context, or a combination of those
mechanisms.

### Contents

Conceptually, `RequestContext` contains:

- request envelope facts
- peer and transport facts
- service identity
- request/trace correlation
- request-level timing anchors
- redaction-aware request metadata

Examples of the kind of information that belongs here:

- method
- normalized route
- request target/path shape
- protocol version
- peer address
- TLS/session transport metadata
- request correlation id
- trace/span linkage
- service name/version/instance/environment

### What Does Not Belong Here

`RequestContext` should not directly own:

- parsed protocol semantics discovered later in handlers
- backend/storage operation internals
- final response classification
- client-facing problem payloads

### Invariants

`RequestContext` should satisfy these rules:

1. Exactly one exists per request.
2. It is created before domain parsing begins.
3. Later layers may enrich telemetry using it, but should not need to recreate
   its facts from raw HTTP state.
4. It is safe to reference from failure paths, including framework-originated
   failures, as long as ingress succeeded.

## DomainContext

### Role

`DomainContext` carries request-scoped Durable Streams semantics discovered
after parsing and validation begin.

It exists because request context and domain context have different ownership
models. The HTTP edge should not need to know protocol-specific identifiers,
but once handlers and protocol code parse them, they should be attached in one
canonical place.

### Ownership

Owned by the domain/application seam.

Enriched incrementally as handlers, extractors, and protocol helpers learn more
about the request.

May be:

- a separate type attached alongside `RequestContext`
- a nested component within a larger mutable request context
- a span-attached set of typed annotations backed by request-local state

The exact implementation is secondary to the ownership model.

### Contents

Conceptually, `DomainContext` contains:

- operation kind
- stream identity and lifecycle facts
- producer/idempotency/sequencing facts
- offset and content metadata
- optional client-supplied domain or correlation identifiers
- internal operation annotations discovered before final emission

Examples of the kind of information that belongs here:

- stream name
- append/read/fork/delete/close operation
- producer id
- expected vs actual sequence values
- current vs received epoch
- offset boundaries or tail position
- content type
- stream closed/open state
- ttl/expiry-related facts

### What Does Not Belong Here

`DomainContext` should not directly own:

- transport-derived peer/service identity
- final HTTP status
- public/internal error split
- response serialization
- sink-specific logging/tracing conventions

### Invariants

`DomainContext` should satisfy these rules:

1. It is request-scoped, never global.
2. It is enriched only as facts become known.
3. It can be absent or partial for requests that fail before full protocol
   parsing completes.
4. It is the canonical home for domain identifiers; code should not invent
   duplicate side channels for the same facts.

## RequestOutcome

### Role

`RequestOutcome` is the canonical representation of how a request finished.

It is the output of the outcome seam. The emission seam consumes it to produce
the response, logs, span completion fields, and metrics.

### Ownership

Owned by the outcome seam.

Constructed once the application has enough information to say what happened.

### Shape

At minimum, `RequestOutcome` should distinguish:

- successful completion
- failed completion

Depending on later needs, it may also grow specialized variants for streaming,
upgraded, or partially committed response shapes, but the initial model should
stay simple.

### Contents

Conceptually, `RequestOutcome` contains:

- overall result kind
- final HTTP status
- high-level outcome semantics
- response side effects
- response metadata needed by emitters

Examples of the kind of information that may belong here:

- success/failure
- canonical operation result
- response headers implied by the result
- retry hints
- cache-related side effects
- idempotency-related side effects
- response body shape classification

### Why It Matters

This type prevents the system from scattering the meaning of completion across:

- HTTP status matches
- problem payload builders
- logging-only fields
- retry-after header branches
- metric labels assembled elsewhere

`RequestOutcome` is the thing that all of those later projections should read.

### Invariants

`RequestOutcome` should satisfy these rules:

1. Every request produces exactly one canonical outcome.
2. It is the only semantic input to final response rendering.
3. It is the only semantic input to final request telemetry emission.
4. Success and failure projections should be symmetric in structure even if
   they differ in detail.

## FailureOutcome

### Role

`FailureOutcome` is the failure-specific portion of `RequestOutcome`.

It is the canonical typed representation of a request failure after
classification and public/internal separation have already happened.

### Ownership

Owned by the outcome seam.

Built from domain errors, framework rejections, storage errors, and layer
failures once they cross into the canonical request outcome path.

### Contents

Conceptually, `FailureOutcome` contains:

- stable classification
- public problem semantics
- internal-only detail
- transport side effects
- failure annotations relevant to telemetry

This includes ideas such as:

- public code/title/type/detail shape
- internal detail or cause summary
- retryability or retry-after hints
- classification category/kind/class
- subsystem-specific context needed for operators

### Public/Internal Separation

This type exists largely to force the split between:

- what the client is allowed to see
- what operators and telemetry need to know

That split should already be decided by the time `FailureOutcome` reaches the
emission seam. Emission should not be deciding whether a raw internal detail is
safe to return to a client.

### What Does Not Belong Here

`FailureOutcome` should not directly own:

- raw request envelope data
- sink-specific logging representations
- direct serialization logic
- local subsystem error enums

Subsystem errors may be inputs to classification, but are not themselves the
canonical failure outcome.

### Invariants

`FailureOutcome` should satisfy these rules:

1. Every failed request has exactly one canonical failure outcome.
2. Public problem payload data and internal-only detail are distinct parts of
   the model.
3. Transport side effects are carried together with the classification so they
   cannot drift into separate matches.
4. Framework and domain failures end in the same shape.

## Relationship Between The Types

The broad data flow should be:

1. Ingress creates `RequestContext`.
2. Domain code enriches `DomainContext`.
3. Application completion is normalized into `RequestOutcome`.
4. Failure cases carry a `FailureOutcome` inside `RequestOutcome`.
5. Emission consumes `RequestContext + DomainContext + RequestOutcome`.

The most important design feature is that the request context path and the
outcome path remain separate until final emission:

- context describes the request and the work
- outcome describes the result

This is what prevents error types from accidentally becoming the sole home of
observability semantics.

## Projection Model

These conceptual types are not the final wire or sink shapes.

Instead:

- HTTP response is a projection of request context plus request outcome
- logs/events are a projection of request context plus domain context plus
  request outcome
- spans are enriched during processing from request and domain context, then
  finalized from request outcome
- metrics are a cardinality-controlled projection of the same model

This is the main architectural payoff: one semantic model, multiple emission
surfaces.

## Design Constraints

Any concrete implementation of these conceptual types should preserve these
constraints:

1. No ad hoc response shaping outside the outcome path.
2. No parallel telemetry-only classification path.
3. No requirement to manually thread every correlation/domain field through
   every call site.
4. No sink-specific vocabulary embedded into core domain types.
5. No assumption that all domain fields are always present.

## Current Repo Implications

In the current server, these concepts are only partially present:

- `middleware::telemetry` approximates part of `RequestContext`
- `protocol::error::Error` approximates part of failure classification
- `protocol::problem::ProblemResponse` approximates one response projection

The current model does not yet cleanly separate:

- request context from failure data
- failure semantics from response rendering
- telemetry projection from response construction

This note describes the conceptual model needed to remove that overlap.

## Next Step

Once this conceptual model is accepted, the next design note should define the
taxonomy and field catalog implied by it:

- identifier taxonomy
- outcome/error taxonomy
- request/domain field catalog
- visibility and cardinality policy

That later note should realize this structure, not replace it.

## Related Design Notes

- `docs/server-tower-edge-stack.md` sketches the Tower-shaped HTTP edge that
  should host ingress context creation, failure normalization, and final
  emission around the context/outcome model.
