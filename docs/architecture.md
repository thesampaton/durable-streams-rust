# Architecture Note

## Current Shape

This repository is a Rust workspace that establishes the intended long-term home
for Durable Streams Rust components without prematurely committing to internal
shared abstractions.

- `durable-streams-client` is the first-class target for the next stage of work.
- `durable-streams-server` is present only as a placeholder boundary so the
  existing production server can later be migrated in cleanly.
- Workspace-level `tests/conformance` and `scripts/conformance` exist because
  the upstream conformance suites exercise process-level behavior and external
  standards alignment, not just crate-local Rust APIs.

## Deliberate Non-Decisions

The workspace intentionally avoids adding a shared `core`, `protocol`, or
`common` crate at this stage. Those splits should only appear once concrete code
demands them.

Likewise, the repository does not yet define release automation, packaging
policy, or server/client binary layouts beyond the minimum needed to make the
workspace compile and evolve cleanly.

## Planned Evolution

1. Build the production-quality Rust client inside
   `crates/durable-streams-client`.
2. Replace the conformance adapter placeholders with runnable adapters that
   invoke the client surface under test, and replace the server launcher
   placeholder with a runnable server startup script.
3. Import the existing server into `crates/durable-streams-server` when the
   migration plan is ready.
4. Introduce additional crates only when real code boundaries justify them.
