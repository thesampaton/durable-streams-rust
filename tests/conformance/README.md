# Conformance Harness

This directory is reserved for workspace-level integration with the upstream
Durable Streams conformance suites.

## Layout

- `client/run-adapter.sh` is the future entrypoint expected by the upstream
  client suite.
- `server/start-server.sh` is the future local launcher used by this repository
  to boot a server process before running the upstream server suite against a
  base URL.

## Intent

The upstream suites are external standards-alignment checks. They are therefore
tracked at workspace level rather than being hidden inside a single crate.

The client adapter and server launcher are placeholders today; their shape is
scaffolded so the future implementation has an obvious home.
