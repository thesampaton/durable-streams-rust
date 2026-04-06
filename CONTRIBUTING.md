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
