# Tests

Workspace-level tests and harness support live here when they are broader than a
single crate.

- Crate-local unit tests should stay inside the relevant crate.
- The migrated server keeps its own unit and integration coverage under
  `crates/durable-streams-server/tests`.
- Durable Streams conformance adapters and shared fixtures belong under
  `tests/conformance`.
