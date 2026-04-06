# Contributing

## Toolchain Policy

- MSRV: Rust `1.89`
- Current stable Rust is also validated in CI.
- The MSRV is a workspace-wide policy and may be raised deliberately over time.

## Expected Local Checks

```bash
cargo fmt --all
cargo check --workspace --all-targets
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
