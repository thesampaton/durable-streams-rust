---
name: coding-guidelines
description: "Use when asking about Rust code style or best practices. Keywords: naming, formatting, comment, clippy, rustfmt, lint, code style, best practice, P.NAM, G.FMT, code review, naming convention, variable naming, function naming, type naming, 命名规范, 代码风格, 格式化, 最佳实践, 代码审查, 怎么命名"
source: https://rust-coding-guidelines.github.io/rust-coding-guidelines-zh/
user-invocable: false
---

# Rust Coding Guidelines For This Workspace

Use this skill for repo-local Rust guidance, not just generic language advice.
When this file conflicts with generic Rust preferences, follow the workspace
policy in `Cargo.toml`, `README.md`, and `CONTRIBUTING.md`.

## Workspace Tenets

| Tenet | Guidance |
|------|----------|
| Boring first | Prefer conventional, explicit Rust over clever abstractions |
| Workspace discipline | Design crates and modules with clear boundaries |
| Standards-aware | Keep protocol/conformance alignment explicit in docs and tooling |
| Conservative dependencies | Prefer `std` and existing workspace deps before adding crates |
| Incremental architecture | Do not create speculative crates or abstractions "for later" |

## Lint And Policy Baseline

The current workspace policy is the minimum bar:

| Policy | Current Setting |
|--------|-----------------|
| MSRV | Rust `1.89` |
| Public docs | `missing_docs = "warn"` |
| Unsafe | `unsafe_code = "forbid"` |
| Clippy | `clippy::pedantic = "warn"` |
| Panicky shortcuts | `clippy::unwrap_used = "warn"` |

Implications:

- Public items should have `///` docs unless there is a strong reason not to.
- `unsafe` is not a casual escape hatch. If it ever becomes necessary, treat it
  as a design event, not an implementation detail.
- Code should be written to satisfy pedantic linting by default, not patched
  after the fact.

## Naming And API Shape

| Rule | Guidance |
|------|-----------|
| No `get_` prefix | Prefer `fn name()` over `fn get_name()` |
| Iterator convention | Use `iter()` / `iter_mut()` / `into_iter()` |
| Conversion naming | `as_` = cheap view, `to_` = allocation/copy, `into_` = ownership move |
| Static/const naming | `SCREAMING_SNAKE_CASE` for both `static` and `const` |
| Types before flags | Prefer explicit types and enums over boolean-heavy APIs |
| Hide invariants | Prefer private fields plus methods over public mutable structs |

For libraries in this workspace:

- Prefer small, intention-revealing public APIs.
- Do not expose modules or fields broadly just to make scaffolding easier.
- Make invalid states hard to represent when the domain is stable enough to do so.

## Dependency Policy

Before adding a crate, ask:

1. Can `std` solve this cleanly?
2. Is there already a workspace dependency that should be reused?
3. Is the dependency justified by current code, not speculation?

## Follow Ecosystem Gravity

Conservative engineering does **not** mean avoiding mainstream dependencies for
their own sake. It means preferring proven, supportable defaults and requiring
real justification before deviating from them.

Heuristic:

- If a problem is already well served by a mature, widely used crate, using that
  crate is usually the boring choice.
- If the Rust ecosystem has a clear center of gravity for a domain, start there.
- If you want something more unusual, the burden of proof is on the unusual choice.

For this repo, examples of "gravity" choices include:

| Domain | Gravity Default | Why |
|--------|-----------------|-----|
| Async runtime | `tokio` | Mature ecosystem center, broad compatibility |
| Rust web stack | `axum` / `hyper` / `tower` | Strong adoption and composable ecosystem |
| Serialization | `serde` | De facto standard |
| Error typing | `thiserror` | Common, explicit library boundary choice |
| Tracing | `tracing` | Strong ecosystem support for async/server code |

Questions to ask before choosing something more esoteric:

1. Is it materially simpler?
2. Is it materially faster on a relevant workload?
3. Is it materially easier to operate or maintain?
4. Is it better supported for our exact constraints?
5. Will another competent Rust maintainer expect this choice?

If the answer is vague or mostly aesthetic, prefer the ecosystem default.

Repo-specific defaults:

| Prefer | Over | Reason |
|--------|------|--------|
| `std::sync::OnceLock` / `LazyLock` | `lazy_static!`, `once_cell` | Meets current MSRV and avoids extra deps |
| `std` sync/channel primitives by default | Immediate `crossbeam` / `parking_lot` adoption | Keep dependency surface boring unless the need is real |
| `thiserror` in libraries | Stringly or opaque library errors | Better API boundaries |
| `anyhow` at binaries/harness edges | `anyhow` everywhere in reusable crates | Preserve typed public errors |

Do not describe third-party crates as universally "better" than `std`. They are
tradeoffs to justify, not defaults to assume.

Equally, do not write bespoke infrastructure code just to avoid a dependency
that already solves the problem well. A mature dependency plus its community
knowledge and test surface is often the more supportable choice.

Avoid building custom implementations of things like:

- async runtimes
- HTTP stacks
- retry/backoff plumbing
- CLI parsers
- serialization formats

unless the existing options are genuinely insufficient for the problem at hand.

## Error Handling

| Rule | Guidance |
|------|-----------|
| Use `?` for recoverable propagation | Keep call paths readable |
| Prefer typed errors in library crates | Usually `thiserror` |
| Avoid `unwrap()` in non-test code | Workspace lint warns on it |
| Use `expect()` only with a real invariant message | Explain why failure would be a bug |
| Assert invariants deliberately | `assert!` / `debug_assert!` when the failure is programmer error |

As a rough default:

- Reusable crates: typed `Result<T, E>`.
- CLI or harness entrypoints: ergonomic aggregation is acceptable at the edge.

## Concurrency And Async

| Rule | Guidance |
|------|-----------|
| Async is for I/O concurrency | Not a universal answer for throughput |
| Do not hold locks across `.await` | Scope guards tightly or redesign |
| Prefer message passing or ownership clarity over `Arc<Mutex<T>>` by reflex | Reduce contention and complexity |
| Use atomics for simple shared counters/flags | Avoid heavier locks when not needed |
| `Send`/`Sync` are design constraints | Async alone does not make code thread-safe |

## Data And Allocation

| Rule | Guidance |
|------|-----------|
| Use newtypes when domain meaning matters | `struct StreamName(String)` can be clearer than raw `String` |
| Pre-allocate when the size is known or obvious | `Vec::with_capacity`, `String::with_capacity` |
| Prefer `&str` / slices / iterators when ownership is unnecessary | Avoid incidental allocation |
| Use arrays for fixed-size data | Avoid heap allocation by habit |
| Reach for `Cow` only when it simplifies a real borrowed/owned boundary | Not as a default style move |

## Testing And Documentation

| Area | Guidance |
|------|----------|
| Unit tests | Keep near the code they exercise |
| Workspace integration/conformance | Keep at workspace level when they span crates or external suites |
| Placeholder code | Document intent clearly so future implementation has a stable target |
| Architecture notes | Record repo-shape and standards decisions explicitly |

For this repo specifically:

- Conformance harness plumbing belongs at workspace level.
- Protocol and conformance version alignment should be recorded, not implied.
- New crates and public modules should be shaped for rustdoc/docs.rs, not just
  for local source navigation.

### Rustdoc And Crate Surface

When adding or refining a crate in this workspace:

- Add a crate-level `//!` narrative or `#![doc = include_str!(...)]` so the
  rustdoc landing page explains the crate on first open.
- Re-export the intended integration surface at crate root when that materially
  improves discoverability.
- Keep internal plumbing modules private or `pub(crate)` unless they are part of
  the real supported API.
- Add short `//!` docs to public boundary modules so rustdoc lists show purpose,
  not just names.
- Prefer docs that describe integration entry points, invariants, and
  operational tradeoffs over restating implementation details.
- Before considering docs done, sanity-check what `cargo doc` or docs.rs will
  render for the crate index and main public modules.

## Review Checklist

- Is the code compatible with the workspace MSRV?
- Does it satisfy pedantic clippy without resorting to lint suppression?
- Are public items documented appropriately?
- Does the crate/module public surface look intentional in rustdoc/docs.rs?
- Is a new dependency actually justified?
- Is the API explicit and boring in the good sense?
- Does the code fit the current crate boundary instead of inventing a new one?
- If standards-related, did docs/tooling metadata get updated together?

## Quick Reference

```text
Naming: snake_case (fn/var), CamelCase (type), SCREAMING_SNAKE_CASE (const/static)
Format: rustfmt
Docs: /// for public items, //! for module docs when the module is a public boundary
Rustdoc: shape crate root and public modules for docs.rs, not just source readers
Lint: workspace lints are policy; write code to satisfy clippy::pedantic by default
Deps: prefer std first; justify every new crate
```

The point of this workspace is not "maximum abstraction." It is a clean,
maintainable, standards-aware Rust codebase with explicit boundaries.
