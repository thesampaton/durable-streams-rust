# Contributing

## Toolchain Policy

- MSRV: Rust `1.89`
- Current stable Rust is also validated in CI.
- The MSRV is a workspace-wide policy and may be raised deliberately over time.

## Expected Local Checks

```bash
cargo fmt --all
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets
cargo test --workspace
```

If you touch conformance harness plumbing, also verify the shell entrypoints:

```bash
bash -n scripts/conformance/*.sh
```

## Optional: local pre-commit formatter

CI is the source of truth for format checks. If you'd prefer `cargo fmt`
to run automatically on each commit (useful when committing outside the
IDE), opt in with a one-off config change:

```bash
git config core.hooksPath scripts/git-hooks
```

The hook at `scripts/git-hooks/pre-commit` auto-formats staged Rust files
and re-stages only the files that were originally staged. Bypass for a
single commit with `git commit --no-verify`. Uninstall with
`git config --unset core.hooksPath`.

## Standards Governance

When protocol or conformance alignment changes, update the relevant records
together:

1. `docs/standards.md`
2. `Cargo.toml` workspace metadata
3. `package.json` conformance package pins
4. Any related CI or harness scripts

## Server Migration Posture

The current server crate in this workspace is a deliberate lift-and-shift of
published `durable-streams-server` `0.1.3`.

When touching `crates/durable-streams-server`, preserve behaviour first and do
not mix routine migration continuity work with opportunistic redesign unless the
change is explicitly intended.
