# Server Tower Edge Stack

## Purpose

This note sketches the Tower-shaped HTTP edge stack the server should converge
on once the request/context/outcome architecture is implemented.

It is not proposing that the whole application become a Tower-first design.
Instead, it defines where Tower is useful:

- normalizing the HTTP edge
- enforcing middleware ordering
- ensuring every request passes through the same ingress and egress path

And it defines what should remain outside Tower:

- protocol/domain semantics
- request outcome classification
- storage-internal execution logic

## Relationship To Other Notes

This note realizes:

- `docs/server-request-outcome-architecture.md`
- `docs/server-context-outcome-model.md`

Those notes define the seams and conceptual types. This note places those
concepts onto the HTTP boundary in a concrete ordered stack.

## Design Goal

The edge stack should make this true for every request:

1. the request enters through one canonical ingress path
2. request context is created exactly once
3. downstream code can enrich domain context without recreating request facts
4. success and failure both terminate in one canonical outcome path
5. final response and telemetry emission happen once, at the edge

Tower is useful here because it can enforce the shape of the boundary and the
ordering of boundary concerns.

## Two Different Kinds Of Structure

The server should distinguish between:

- **Tower edge structure**
  The ordered HTTP middleware/service stack around the application.

- **Application semantic structure**
  The request/domain/outcome model inside the application.

The Tower stack should host context creation, boundary normalization, and final
emission. It should not be the thing that defines application semantics.

## Proposed Edge Model

There are two nested stacks:

1. an application-wide edge stack
2. a protocol-route stack inside the Durable Streams mounted subtree

### Application-Wide Edge Stack

This stack applies to all requests, including:

- protocol routes
- health and readiness routes
- future auth or admin routes
- framework-generated responses such as CORS preflight

Conceptually, the request flow should be:

1. outer emission/finalization wrapper
2. unhandled failure normalization wrapper
3. proxy / transport normalization
4. trace and request correlation setup
5. canonical request context creation
6. generic transport policy layers
7. axum router / route dispatch

### Protocol Route Stack

Inside the Durable Streams route subtree, the request should then pass through
protocol-specific boundary helpers:

1. protocol/security response decoration
2. protocol configuration injection
3. route matching and extractors
4. handlers and protocol parsing
5. domain context enrichment
6. outcome construction

The key distinction is:

- application-wide stack owns transport-shaped concerns
- protocol route stack owns protocol-specific boundary concerns

## Proposed Layer Ordering

This section gives the intended logical order from outermost to innermost.

If implemented with `tower::ServiceBuilder`, this should be read as the request
order seen by the application.

### 1. RequestEmissionLayer

Outermost wrapper.

Owns:

- timing the whole request
- observing the final status/outcome
- final response telemetry emission
- final span completion
- response-size and other finalization facts when available

Inputs:

- `RequestContext`
- partial or complete `DomainContext`
- final `RequestOutcome` or normalized fallback failure

Non-responsibilities:

- deciding semantics
- parsing protocol data
- building request context

This layer exists so that success, failure, and short-circuit responses all
produce one final edge emission path.

### 2. FailureNormalizationLayer

Wraps the inner stack so that framework or service failures do not bypass the
canonical outcome path.

Owns:

- converting layer/service/framework failures into canonical failure outcomes
- ensuring the request always exits as a response plus normalized outcome data
- optionally catching panic-like terminal failures if the chosen stack supports
  that pattern

Inputs:

- tower/layer/service errors
- framework rejections that can be intercepted at the edge

Non-responsibilities:

- classifying normal domain failures discovered by handlers
- emitting final telemetry

This is the main edge safety net.

### 3. ProxyTrustAndTransportLayer

Normalizes peer- and transport-derived request facts before request context is
frozen.

Owns:

- forwarded-header trust decisions
- normalized client/server addressing
- request scheme/authority normalization
- transport facts derived from the accepted connection

This layer should run before request context creation because request context
should be built from normalized transport facts, not raw spoofable headers.

### 4. CorrelationAndTraceLayer

Establishes correlation primitives for the request.

Owns:

- adopting inbound trace context
- creating or propagating a server request id
- attaching correlation handles needed by the request span/context

This is where the server decides how transport-level correlation is carried.
Domain identifiers discovered later must not replace these primitives.

### 5. RequestContextLayer

Creates the canonical `RequestContext`.

Owns:

- assembling transport-shaped request facts once
- opening or binding the root request span
- attaching `RequestContext` to request-local state/extensions

After this point, downstream code should not need to reconstruct:

- method
- route
- peer identity
- scheme
- correlation primitives
- other ingress-owned fields

### 6. Generic Transport Policy Layers

These are reusable HTTP-edge policies that may short-circuit before handlers.

Examples:

- CORS
- timeouts
- body size limits implemented at the transport edge
- authentication/admission/rate limiting later

These layers are appropriate for Tower because they are transport-policy
concerns, not domain semantics.

The important architectural rule is:

- if they can fail or short-circuit, they must still feed the canonical
  failure/outcome path through the surrounding normalization and emission
  layers

### 7. Axum Router And Route Dispatch

Once the request reaches the router, transport-level normalization should
already be in place.

At this point the application may branch into:

- health/readiness handlers
- protocol subtree
- future route groups

## Protocol Route Stack

Inside the mounted Durable Streams subtree, the stack should stay narrower and
protocol-focused.

### ProtocolSecurityHeadersLayer

Owns:

- response decoration specific to the protocol subtree
- headers such as cache-control or security policy that are mechanical and do
  not depend on domain classification

This should remain a response-decorating concern, not a semantic one.

### ProtocolConfigInjection

Owns:

- stream base path
- shutdown token
- long-poll and reconnect config
- validated protocol limits

These are boundary inputs needed by handlers and extractors, not outcomes.

### Extractors And Handlers

This is where Tower stops being the main organizational tool and the
application semantic model takes over.

Owns:

- request parsing
- protocol validation
- domain context enrichment
- calls into storage/application logic
- construction of success/failure outcomes

This is where `DomainContext` is learned and enriched.

### Outcome Construction

This may be implemented as:

- handler return types
- adapter helpers
- dedicated boundary functions

But conceptually it is not another Tower layer. It is the application seam
where:

- protocol and storage results become `RequestOutcome`
- failures become `FailureOutcome`
- public/internal split is fixed before edge emission

## What Stays Outside Tower

The following concerns should not be "made Tower-shaped" just for consistency:

### RequestOutcome Semantics

The meaning of success/failure, classification, and transport side effects
belongs to the application outcome model, not to generic middleware.

### Protocol Parsing And Validation

Stream semantics, producer sequencing, TTL rules, offset handling, and related
protocol behavior belong in protocol/domain code.

### Storage Logic

Storage coordination, backend-specific error details, locking, persistence, and
child-span annotations belong in storage/application code, not in the Tower
edge.

### Domain Context Enrichment

While Tower can host ingress context creation, it is the handlers, extractors,
and protocol helpers that learn:

- stream name
- producer metadata
- sequence/epoch facts
- offsets
- content semantics

That work should remain in application code, with the result attached to the
request-scoped context/span model.

## Boundary Data Flow

The full conceptual flow should be:

1. Tower ingress layers normalize transport and correlation state.
2. `RequestContext` is created.
3. Router dispatches into route-specific application code.
4. Protocol/handler code enriches `DomainContext`.
5. Application code constructs `RequestOutcome`.
6. Tower egress layers normalize any unhandled failures and emit the final
   response telemetry.

This can be summarized as:

- Tower owns boundary flow
- application code owns semantics
- outcome owns meaning
- emission owns projection

## Why This Is Better Than Ad Hoc Middleware

Using an explicit Tower-shaped edge stack gives several benefits:

- one place to reason about ingress ordering
- one place to reason about short-circuit behavior
- one place to guarantee final emission
- one place to absorb framework and layer failures

But it avoids the common mistake of trying to force all application behavior
into `Service`/`Layer` abstractions where simple application types are clearer.

## Current Repo Implications

Relative to the current router shape, this suggests moving toward:

- a clearer app-wide `ServiceBuilder`-style edge stack in
  `crates/durable-streams-server/src/router.rs`
- a dedicated boundary normalization layer for non-handler failures
- request context creation as an explicit ingress concern rather than an
  incidental property of telemetry middleware
- final emission as an explicit outer wrapper rather than a telemetry helper
  that only sees whatever the inner code happened to attach

It does **not** imply:

- converting handlers into generic Tower services by hand
- turning storage into middleware
- replacing the application outcome model with Tower types

## Suggested Next Design Step

If this edge sketch is acceptable, the next design note should define the
taxonomy and field catalog against this structure:

- which identifiers live in `RequestContext`
- which identifiers live in `DomainContext`
- which classifications live in `FailureOutcome`
- which parts are response-visible vs internal-only
- which parts are safe for logs, traces, and metrics

That note now lives at `docs/server-taxonomy-and-field-catalog.md`.
