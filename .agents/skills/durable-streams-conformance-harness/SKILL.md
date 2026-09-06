---
name: durable-streams-conformance-harness
description: >
  Durable Streams workspace conformance and standards-alignment guidance. Use
  when wiring or updating upstream conformance suites, adapter/launcher
  entrypoints, npm package pins, CI hooks, or standards-governance records for
  this repository.
sources:
  - "docs/standards.md"
  - "package.json"
  - "tests/conformance/README.md"
  - "scripts/conformance/run-client-suite.sh"
  - "scripts/conformance/run-server-suite.sh"
user-invocable: false
---

# Durable Streams Conformance Harness

Repo-local guidance for how this workspace aligns with the upstream Durable
Streams ecosystem.

This is not protocol truth. It is the local shape for exercising that protocol
against the upstream conformance suites.

## Repository Intent

This workspace treats the upstream protocol and conformance suites as external
standards that are tracked explicitly in-repo.

Current governance records live in:

- `docs/standards.md`
- workspace metadata in `Cargo.toml`
- exact npm package pins in `package.json`

When standards alignment changes, those records should move together.

## Recorded Baselines

Current tracked baselines for this repo:

- protocol document: Durable Streams Protocol `1.0-draft`
- protocol revision: `a172acc389351cb3db6deb5cd60e3dec11e7ff39`
- client conformance suite: `@durable-streams/client-conformance-tests@0.2.3`
- server conformance suite: `@durable-streams/server-conformance-tests@0.3.6`

The current server crate in this workspace is a lift-and-shift of published
`durable-streams-server` `0.1.3`.

These values should match:

- `docs/standards.md`
- `Cargo.toml` workspace metadata
- `package.json`

If they do not match, treat the standards picture as stale and reconcile it
before relying on the harness documentation.

## Current Suite Pins

The current workspace pins:

- `@durable-streams/client-conformance-tests`
- `@durable-streams/server-conformance-tests`

Exact versions belong in `package.json`, not in ad hoc shell commands.

## Local Harness Shape

The local harness is split between runners and entrypoints.

### Runner scripts

These invoke the upstream npm packages:

- `scripts/conformance/bootstrap.sh`
- `scripts/conformance/run-client-suite.sh`
- `scripts/conformance/run-server-suite.sh`

### Client side

The upstream client suite expects an adapter executable.

Local path:

- `tests/conformance/client/run-adapter.sh`

The workspace-level runner:

- checks that the adapter is executable
- invokes `durable-streams-client-conformance -- --run <adapter>`

## Server side

The upstream server suite runs against a base URL, not a stdin/stdout adapter.

Local paths:

- `tests/conformance/server/start-server.sh`
- `scripts/conformance/run-server-suite.sh`

The workspace-level runner:

- optionally launches the local server via `start-server.sh`
- uses `DURABLE_STREAMS_SERVER_URL` as the target base URL
- creates a temporary Vitest entrypoint that calls
  `runConformanceTests({ baseUrl, subscriptions: true })` from the pinned npm package
- invokes that entrypoint via `npm exec vitest run ...`

Supported local environment knobs:

- `DURABLE_STREAMS_SERVER_URL`
- `DURABLE_STREAMS_SERVER_LAUNCHER`
- `DURABLE_STREAMS_SERVER_SKIP_LAUNCH`
- `DURABLE_STREAMS_SERVER_STARTUP_DELAY`

## Responsibilities

Use workspace-level conformance plumbing when work spans crates or external
process boundaries.

Keep crate-local tests inside the relevant crate when they do not depend on the
external suites.

Conformance harness updates should stay thin:

- avoid hiding the upstream suite behind too much custom logic
- keep runner scripts understandable from a quick read
- keep adapter/launcher entrypoints in `tests/conformance`

## Update Checklist

When upstream standards alignment changes:

1. Update `docs/standards.md`
2. Update workspace metadata in `Cargo.toml`
3. Update exact npm pins in `package.json`
4. Update runner scripts or entrypoint docs if the suite contract changed
5. Update CI if validation expectations changed

## Boundaries

This skill should define:

- repo-local paths
- runner expectations
- conformance-suite wiring
- standards-governance update points

This skill should not define:

- protocol semantics that belong in `durable-streams-protocol`
- client crate ergonomics
- server crate internals

Those should live in the protocol skill or future crate-specific skills.

The 0.3.6 server suite runs all 338 cases including its six subscription tests.
The local launcher enables `DS_HTTP__ALLOW_INSECURE_WEBHOOKS=true` for the suite's
localhost callback receivers. Production defaults keep that exception disabled.
Use separate data directories and ports for backend validation. Avoid competing
load during the upstream suite's short TTL and SSE timing checks.
