# Architecture Note

## Current Shape

This repository is a Rust workspace that is now the long-term home for Durable
Streams Rust components without prematurely committing to internal shared
abstractions.

- `durable-streams-client` is the actively developed client crate.
- `durable-streams-server` now contains the migrated server crate. The current
  workspace copy is a deliberate lift-and-shift of the published
  `durable-streams-server` `0.1.3` codebase so behaviour is preserved before any
  later cleanup or redesign.
- Workspace-level `tests/conformance` and `scripts/conformance` exist because
  the upstream conformance suites exercise process-level behavior and external
  standards alignment, not just crate-local Rust APIs.

## Deliberate Non-Decisions

The workspace intentionally avoids adding a shared `core`, `protocol`, or
`common` crate at this stage. Those splits should only appear once concrete code
demands them.

Likewise, the repository does not yet define shared-core extraction, release
automation, or packaging policy beyond the minimum needed to make the workspace
compile, test, and evolve cleanly.

One concrete seam now exists: RFC 9457-style problem details and related error
code mapping currently live in `durable-streams-server::protocol::problem`.
That remains server-local for now because the client does not yet parse the
same wire type directly, but it is the most likely candidate for future
workspace extraction if shared problem serialization/deserialization becomes
real code instead of anticipation.

## Planned Evolution

1. Continue evolving the Rust client inside `crates/durable-streams-client`.
2. Keep the migrated server building and conforming inside
   `crates/durable-streams-server` without mixing preservation work and
   redesign work.
3. Introduce additional crates only when real code boundaries justify them.
4. Revisit release automation and packaging policy once the workspace shape has
   stabilised.
