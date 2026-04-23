# Server Implementation Sketch

## Purpose

This note turns the design stack into a concrete Rust-facing implementation
sketch:

- `docs/server-request-outcome-architecture.md`
- `docs/server-context-outcome-model.md`
- `docs/server-tower-edge-stack.md`
- `docs/server-taxonomy-and-field-catalog.md`
- `docs/server-field-matrix.md`

The intent is to start sketching code structure and transition shape without
committing to a single large rewrite.

## Transition Goal

The transition should aim for:

1. new request/context/outcome machinery introduced as **internal-only**
2. existing public `protocol::problem` and `protocol::error` surface preserved
   initially as compatibility projections
3. router-level boundary flow improved incrementally
4. handlers/storage migrated gradually behind the compatibility surface

This should keep the first implementation steps low-risk even though the design
is cross-cutting.

## Public API Impact Summary

The current public surface that matters for this design is concentrated in:

- `durable_streams_server::protocol::problem`
- `durable_streams_server::protocol::error`
- `durable_streams_server::router::{build_router, build_router_with_ready, DEFAULT_STREAM_BASE_PATH, ShutdownToken}`

### Public Surface That Can Stay Stable Initially

These can remain as they are during the first implementation passes:

- `build_router` and `build_router_with_ready`
- route behavior and wire-format behavior
- `ProblemDetails`
- `ProblemResponse`
- `ProblemTelemetry`
- `Error`
- `From<Error> for ProblemResponse`
- `IntoResponse for Error`

The new context/outcome model can be introduced behind these without forcing a
public API break.

### Public Surface Likely To Need Attention Later

These are the public items most likely to become semver-relevant if we decide
to fully align the public API with the new internal architecture:

- `ProblemResponse::new`
- `ProblemResponse::with_detail`
- `ProblemResponse::with_instance`
- `ProblemResponse::with_header`
- `ProblemResponse::with_telemetry`
- `protocol::problem::Result<T>`
- `ProblemTelemetry` as a public concrete struct
- potentially parts of `protocol::error::Error` and its helper constructors

Reason:

- today these public types are also the main internal shaping mechanism
- the target architecture wants them to become projections or compatibility
  adapters, not the canonical semantic model

That means:

- **Phase 1-3** should avoid changing these signatures
- later cleanup may de-emphasize them, deprecate pieces, or reduce their role
- none of that needs to happen to get the first internal architecture in place

### Public API Recommendation

Keep the first migration strictly internal-first:

- add new internal modules
- migrate handlers and middleware to use them
- keep existing public `problem` and `error` APIs as compatibility façades

Only revisit public API cleanup after the internal model has stabilized.

## Proposed Internal Module Shape

The names here are illustrative. The important part is the split of
responsibility.

```rust
crates/durable-streams-server/src/
  edge/
    mod.rs
    request_context.rs
    domain_context.rs
    outcome.rs
    failure.rs
    emit.rs
    normalize.rs
    fields.rs
  middleware/
    telemetry.rs         // eventually becomes thin edge-emission glue
    proxy_trust.rs
    security.rs
  protocol/
    error.rs            // compatibility layer into internal failure/outcome model
    problem.rs          // compatibility response projection
```

### Why A New Internal `edge` Area

The design wants a clean home for:

- request context types
- domain context enrichment hooks
- request outcome / failure outcome
- response and telemetry projection
- field constants / catalog wiring

These concerns do not belong naturally in:

- `protocol::problem`
- `protocol::error`
- `middleware::telemetry`

because those modules are either public compatibility surfaces or currently too
specific to one projection.

## Core Internal Type Sketch

The first pass should introduce internal-only types, something close to:

```rust
// crate::edge::request_context
#[derive(Debug, Clone)]
pub(crate) struct RequestContext {
    pub(crate) correlation: CorrelationContext,
    pub(crate) request: RequestFacts,
    pub(crate) peer: PeerFacts,
    pub(crate) service: ServiceFacts,
    pub(crate) timing: RequestTiming,
}

#[derive(Debug, Clone)]
pub(crate) struct CorrelationContext {
    pub(crate) trace_id: Option<String>,
    pub(crate) span_id: Option<String>,
    pub(crate) parent_span_id: Option<String>,
    pub(crate) request_id: String,
    pub(crate) adopted_request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RequestFacts {
    pub(crate) method: http::Method,
    pub(crate) route: Option<String>,
    pub(crate) path: String,
    pub(crate) query: Option<String>,
    pub(crate) version: &'static str,
}
```

```rust
// crate::edge::domain_context
#[derive(Debug, Clone, Default)]
pub(crate) struct DomainContext {
    pub(crate) operation: Option<OperationKind>,
    pub(crate) stream: Option<StreamFacts>,
    pub(crate) producer: Option<ProducerFacts>,
    pub(crate) sequencing: Option<SequencingFacts>,
    pub(crate) content: Option<ContentFacts>,
    pub(crate) storage: Option<StorageFacts>,
}

#[derive(Debug, Clone)]
pub(crate) enum OperationKind {
    Create,
    Append,
    Read,
    Delete,
    Head,
    Close,
    Fork,
    Health,
    Readiness,
}
```

```rust
// crate::edge::failure
#[derive(Debug, Clone)]
pub(crate) struct FailureOutcome {
    pub(crate) classification: FailureClass,
    pub(crate) public: PublicProblem,
    pub(crate) internal: InternalFailureDetail,
    pub(crate) effects: ResponseEffects,
}

#[derive(Debug, Clone)]
pub(crate) struct PublicProblem {
    pub(crate) problem_type: &'static str,
    pub(crate) title: &'static str,
    pub(crate) code: &'static str,
    pub(crate) detail: Option<String>,
    pub(crate) instance: Option<String>,
    pub(crate) status: http::StatusCode,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InternalFailureDetail {
    pub(crate) class: Option<&'static str>,
    pub(crate) subsystem: Option<&'static str>,
    pub(crate) backend: Option<&'static str>,
    pub(crate) operation: Option<String>,
    pub(crate) detail: Option<String>,
}
```

```rust
// crate::edge::outcome
#[derive(Debug, Clone)]
pub(crate) enum RequestOutcome {
    Success(SuccessOutcome),
    Failure(FailureOutcome),
}

#[derive(Debug, Clone)]
pub(crate) struct SuccessOutcome {
    pub(crate) status: http::StatusCode,
    pub(crate) response_kind: ResponseKind,
    pub(crate) effects: ResponseEffects,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ResponseEffects {
    pub(crate) retry_after_secs: Option<u32>,
    pub(crate) cache_control_kind: Option<CacheControlKind>,
    pub(crate) protocol_headers: Vec<(http::header::HeaderName, http::HeaderValue)>,
}
```

These are not meant as final data structures. They are the first code shape
that makes the design real.

## Compatibility Projection Sketch

The existing public `protocol::problem` types should be treated as projections
of the internal failure/outcome model.

That suggests code like:

```rust
impl From<&FailureOutcome> for ProblemDetails {
    fn from(failure: &FailureOutcome) -> Self {
        let public = &failure.public;
        let mut details = ProblemDetails::new(
            public.problem_type,
            public.title,
            public.status,
            public.code,
        );
        if let Some(detail) = &public.detail {
            details = details.with_detail(detail.clone());
        }
        if let Some(instance) = &public.instance {
            details = details.with_instance(instance.clone());
        }
        details
    }
}
```

```rust
impl From<&FailureOutcome> for ProblemTelemetry {
    fn from(failure: &FailureOutcome) -> Self {
        let mut telemetry = ProblemTelemetry::from(&ProblemDetails::from(failure));
        telemetry.error_class = failure.internal.class.map(str::to_string);
        telemetry.storage_backend = failure.internal.backend.map(str::to_string);
        telemetry.storage_operation = failure.internal.operation.clone();
        telemetry.internal_detail = failure.internal.detail.clone();
        telemetry.retry_after_secs = failure.effects.retry_after_secs;
        telemetry
    }
}
```

```rust
impl From<FailureOutcome> for ProblemResponse {
    fn from(failure: FailureOutcome) -> Self {
        let mut response = ProblemResponse::new(ProblemDetails::from(&failure))
            .with_telemetry(ProblemTelemetry::from(&failure));

        if let Some(retry_after) = failure.effects.retry_after_secs {
            response = response.with_header(
                http::header::RETRY_AFTER,
                http::HeaderValue::from_str(&retry_after.to_string())
                    .expect("validated retry-after"),
            );
        }

        for (name, value) in failure.effects.protocol_headers {
            response = response.with_header(name, value);
        }

        response
    }
}
```

This preserves the current public surface while moving semantics into the
internal model.

## Error Mapping Sketch

The current public `protocol::error::Error` should become an input to the new
failure model, not the final semantic home.

That suggests something like:

```rust
impl Error {
    pub(crate) fn to_failure_outcome(&self) -> FailureOutcome {
        match self {
            Self::NotFound(name) => FailureOutcome {
                classification: FailureClass::NotFound,
                public: PublicProblem {
                    problem_type: "/errors/not-found",
                    title: "Stream Not Found",
                    code: "NOT_FOUND",
                    detail: Some(format!("Stream not found: {name}")),
                    instance: None,
                    status: http::StatusCode::NOT_FOUND,
                },
                internal: InternalFailureDetail::default(),
                effects: ResponseEffects::default(),
            },
            // ...
            Self::Unavailable(failure) => FailureOutcome {
                classification: FailureClass::Unavailable,
                public: PublicProblem {
                    problem_type: "/errors/unavailable",
                    title: "Service Unavailable",
                    code: "UNAVAILABLE",
                    detail: Some(
                        "The server is temporarily unable to complete the request."
                            .to_string(),
                    ),
                    instance: None,
                    status: http::StatusCode::SERVICE_UNAVAILABLE,
                },
                internal: InternalFailureDetail {
                    class: Some(failure.class.as_str()),
                    subsystem: Some("storage"),
                    backend: Some(failure.backend),
                    operation: Some(failure.operation.clone()),
                    detail: Some(failure.detail.clone()),
                },
                effects: ResponseEffects {
                    retry_after_secs: failure.retry_after_secs,
                    ..ResponseEffects::default()
                },
            },
        }
    }
}
```

Then the public compatibility path becomes:

```rust
impl From<Error> for ProblemResponse {
    fn from(error: Error) -> Self {
        ProblemResponse::from(error.to_failure_outcome())
    }
}
```

This is a good first migration because it changes internal ownership without
breaking external behavior.

## Handler Boundary Sketch

Handlers should gradually move from returning:

```rust
pub type Result<T> = std::result::Result<T, ProblemResponse>;
```

toward internal helpers that construct `RequestOutcome` or `FailureOutcome`
before final projection.

The first transition does **not** need to change all handler signatures.

Instead, introduce internal helpers:

```rust
pub(crate) type HandlerResult<T> = std::result::Result<T, FailureOutcome>;
```

and compatibility adapters:

```rust
pub(crate) fn into_problem_result<T>(
    result: HandlerResult<T>,
) -> std::result::Result<T, ProblemResponse> {
    result.map_err(ProblemResponse::from)
}
```

That allows gradual migration inside handlers without forcing a wide signature
rewrite in one pass.

## Request Context And Domain Context Plumbing Sketch

The first rollout should likely use request extensions plus span enrichment,
not an elaborate new plumbing system.

### RequestContext creation

At ingress:

```rust
pub(crate) async fn build_request_context(
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let ctx = RequestContext::from_http_request(&request);
    request.extensions_mut().insert(ctx.clone());

    let span = ctx.root_span();
    next.run(request).instrument(span).await
}
```

### DomainContext enrichment

In handlers/extractors:

```rust
pub(crate) fn with_domain_context<F>(request: &mut Parts, update: F)
where
    F: FnOnce(&mut DomainContext),
{
    let ctx = request.extensions.get_mut::<DomainContext>();
    if let Some(ctx) = ctx {
        update(ctx);
    }
}
```

This is intentionally boring. The point is to create a canonical place to put
request-scoped domain facts before worrying about a fancier propagation model.

## Emission Sketch

The current `middleware::telemetry` should eventually become a projection layer
over:

- `RequestContext`
- `DomainContext`
- `RequestOutcome`

Conceptually:

```rust
pub(crate) fn emit_completion(
    span: &tracing::Span,
    request: &RequestContext,
    domain: Option<&DomainContext>,
    outcome: &RequestOutcome,
) {
    record_request_fields(span, request);
    if let Some(domain) = domain {
        record_domain_fields(span, domain);
    }
    record_outcome_fields(span, outcome);
}
```

This is where the field matrix should eventually be enforced.

## Router Sketch

The current `router.rs` can move incrementally toward the target stack without
changing its public constructors.

Conceptually:

```rust
let app = Router::new()
    .route("/healthz", get(...))
    .nest(stream_base_path.as_ref(), protocol_routes(...))
    .layer(
        ServiceBuilder::new()
            .layer(RequestEmissionLayer::new())
            .layer(FailureNormalizationLayer::new())
            .layer(ProxyTrustLayer::new(proxy_trust_state))
            .layer(CorrelationAndTraceLayer::new())
            .layer(RequestContextLayer::new(service_identity))
            .layer(cors_layer(&config.http.cors_origins))
    );
```

The concrete implementation may still use `axum::middleware::from_fn` for the
first passes, but the responsibility split should follow this structure.

## Suggested Implementation Order

This is the lowest-risk order I can see.

### Phase 1: Introduce Internal Types

Add crate-private:

- `RequestContext`
- `DomainContext`
- `RequestOutcome`
- `FailureOutcome`
- compatibility conversion helpers

No public API change required.

### Phase 2: Move Error Semantics Internally

Refactor `protocol::error::Error` so it maps to `FailureOutcome`, then project
back out to the existing public `ProblemResponse`.

No public API change required if behavior stays the same.

### Phase 3: Introduce Request Context At The Edge

Create canonical request context at ingress and attach it to the request and
span.

No public API change required.

### Phase 4: Introduce Domain Context Enrichment

Start updating extractors/handlers/storage helpers to enrich `DomainContext`
instead of manually duplicating telemetry data.

No public API change required.

### Phase 5: Move Final Emission To Read Context + Outcome

Refactor telemetry emission to read from the new model.

No public API change required.

### Phase 6: Normalize Framework Failures

Add failure normalization at the edge so axum/tower-originated failures feed
the same outcome path.

No public API change required if wire behavior remains stable.

### Phase 7: Revisit Public API Cleanup

Only after the internal model is stable, decide whether to:

- deprecate direct `ProblemResponse` constructors
- reduce the role of public `ProblemTelemetry`
- expose newer public projection helpers

This is the first phase that likely becomes semver-sensitive.

## Explicit Public API Notes

These are the most important explicit cautions.

### `protocol::problem` Is Public Today

Anything that changes the shape, constructors, or semantics of:

- `ProblemDetails`
- `ProblemResponse`
- `ProblemTelemetry`
- `protocol::problem::Result<T>`

is public API work.

Recommendation:

- do not change these in the first implementation passes
- implement the new semantics behind them first

### `protocol::error::Error` Is Public Today

Anything that removes variants, changes helper constructors, or changes public
mapping methods is public API work.

Recommendation:

- keep `Error` as the public input type for now
- add internal conversion methods rather than replacing it

### Router Constructors Are Public

Anything that changes:

- `build_router`
- `build_router_with_ready`
- `ShutdownToken`
- `DEFAULT_STREAM_BASE_PATH`

is public API work.

Recommendation:

- keep the router construction surface stable
- change only the internal middleware composition behind it

## Bottom Line

The first implementation steps can stay mostly internal if we are disciplined:

- add internal edge/context/outcome types
- map public error/problem types onto them
- migrate handlers and middleware behind compatibility helpers
- preserve current public router and problem/error surfaces

That gives room to make the cross-cutting changes without turning the first
pass into a public API redesign.
