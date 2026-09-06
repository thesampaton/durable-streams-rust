---
name: durable-streams-rust-guidelines
description: Review or design Rust APIs and code structure in the Durable Streams workspace using its library boundaries and compatibility policy.
---

# Durable Streams Rust guidelines

Repository policy and verification commands are in
[AGENTS.md](../../../AGENTS.md) and [CONTRIBUTING.md](../../../CONTRIBUTING.md).
Use these conventions over conflicting generic Rust guidance.

## Public interfaces

- Distinguish request inputs from resolved state and persisted metadata. Put
  validation at the boundary that all callers use, including direct Rust users.
- Review the public surface from downstream code: construction, customization,
  error matching, trait-object use, ownership, and shutdown. Document required
  runtime context and failure behavior.
- Re-export intended entry points at crate root when that improves discovery.
  Keep backend mechanics private. A public item is a compatibility commitment,
  even when its documentation describes it as internal.
- Prefer typed library errors. Use opaque aggregation at binary/harness edges
  when callers do not need to recover by error kind.
- Use owned values when data must outlive a call; borrow otherwise. Do not
  require ownership solely because a function crosses a module boundary.
- Use conventional Rust naming, including `SCREAMING_SNAKE_CASE` for statics and
  constants. Avoid blanket naming rules that conflict with established domain
  operations such as HTTP GET.

## Structure and dependencies

- Share semantic rules when multiple backends must agree. Preserve separate
  runtime and persisted representations where their invariants differ; see
  [storage boundaries](../../../docs/architecture.md#storage-read-boundaries).
- Add abstractions or crates for demonstrated integration needs. A forwarding
  wrapper or repeated boilerplate alone does not establish a new domain layer.
- Prefer existing workspace dependencies and established Rust ecosystem tools.
  Evaluate additional dependencies against concrete needs and maintenance cost.
- Comments should explain contracts, tradeoffs, and invariants. Remove stale
  migration narratives, repeated implementation descriptions, and speculative
  future architecture.

## Verification

Review the rendered rustdoc for changed public interfaces. Document observable
behavior, error conditions, and meaningful examples. For a breaking change,
update migration notes and review the API diff against downstream usage.
Use scoped lint exceptions with a reason; avoid adding blanket crate allowances
as a shortcut to passing checks.
