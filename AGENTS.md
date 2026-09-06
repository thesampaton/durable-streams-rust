# Repository guidance

This workspace contains the published Durable Streams server and an unpublished
client. Release configuration lives in `release-plz.toml`; contributor and
release procedures live in `CONTRIBUTING.md`.

- MSRV is Rust 1.89. Cargo manifests and CI define the enforced lint/build policy.
  Repository conventions take precedence over generic Rust style preferences.
- `docs/standards.md`, workspace metadata in `Cargo.toml`, and `package.json`
  record the protocol revision and exact conformance pins. Read the pinned
  specification for protocol changes. Investigate disagreements between code,
  tests, and the specification; record deliberate deviations in the standards
  notes rather than silently changing the baseline.
- Keep library failures typed and validate inputs before mutation. Preserve
  atomic operations, coherent reads, resumable offsets, and backend durability
  guarantees. Known deficiencies are tracked in `docs/reviews`; a review
  proposal is not a description of an implemented guarantee.
- For public API changes, check downstream construction and usage, runtime
  ownership, shutdown behavior, rustdoc, the API snapshot, and migration notes.

Load only the repository skill relevant to the task:

| Work | Skill under `.agents/skills/` |
| --- | --- |
| Rust API/style review | `durable-streams-rust-guidelines` |
| HTTP protocol semantics | `durable-streams-protocol` |
| Server implementation and storage | `durable-streams-server-code` |
| Server tests and coverage choices | `durable-streams-server-testing` |
| Conformance runners, pins, and CI wiring | `durable-streams-conformance-harness` |

Use focused tests during development. Before completing Rust changes, run the
applicable checks from `CONTRIBUTING.md`: formatting, workspace check/tests,
strict server Clippy, advisory client Clippy, and rustdoc. Run the API snapshot
for public-surface changes and the relevant conformance suite for protocol or
harness changes. Documentation-only work needs link/command verification;
skill changes also need frontmatter and scope validation. Before release, run
the full pre-release matrix on the candidate commit.
