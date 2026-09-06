---
name: durable-streams-conformance-harness
description: Maintain Durable Streams conformance adapters, launchers, package pins, CI hooks, and standards-governance records.
---

# Durable Streams conformance harness

## Sources of truth

- [docs/standards.md](../../../docs/standards.md) records the verified protocol
  baseline, compatibility notes, and dated validation results.
- `Cargo.toml` workspace metadata records the protocol revision and suite pins.
- `package.json` and its lockfile select the actual upstream npm packages.
- [tests/conformance/README.md](../../../tests/conformance/README.md) documents
  current commands, runner controls, and backend setup.

Read those records when needed instead of copying versions into skills or
ad hoc commands. Check that they agree before changing alignment. Keep protocol
semantics tied to the recorded revision; an upstream test failure is evidence to
investigate rather than an automatic instruction to adopt a different baseline.

## Runner contracts

- `scripts/conformance/bootstrap.sh` installs tooling.
- `scripts/conformance/run-client-suite.sh` verifies the executable adapter and
  passes it to the pinned client suite. The adapter entrypoint is
  `tests/conformance/client/run-adapter.sh`.
- `scripts/conformance/run-server-suite.sh` optionally starts
  `tests/conformance/server/start-server.sh`, waits for its TCP listener, and
  writes a temporary Vitest entrypoint with
  `runConformanceTests({ baseUrl, subscriptions: true })`. It forwards Vitest
  arguments after npm's separator.
- Keep upstream individual deadlines and assertions intact. Any whole-test
  timeout accommodation must be justified and documented in the harness README.
- The launcher enables insecure localhost webhook callbacks for conformance.
  Keep that exception confined to local test configuration.

Use the Node major configured in CI. Give backend runs separate temporary data
roots and ports, and avoid concurrent load during short TTL/SSE timing tests.
A successful memory run does not stand in for disk-backend release validation.

## Updating alignment or wiring

1. Read the intended protocol revision and suite contract. Compare them with
   the recorded baseline; make any adoption an explicit change.
2. Update affected governance metadata, exact npm pins and lockfile, and
   compatibility notes together. Revise protocol guidance only where needed.
3. Update affected runners, entrypoints, README commands, and CI jobs. Keep the
   scripts thin enough that the upstream invocation remains easy to inspect.
4. Syntax-check each changed shell script, execute the affected suite, and
   report the backend, toolchain, failures, and capability skips. Release checks
   must cover the matrix in `.github/workflows/pre-release.yml`.

For syntax checks, iterate over scripts; `bash -n` accepts one script file per
invocation. Verification and release policy live in
[CONTRIBUTING.md](../../../CONTRIBUTING.md).
