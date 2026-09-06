# Contributing

## Toolchain Policy

- MSRV: Rust `1.89`
- Current stable Rust is also validated in CI.
- The MSRV is a workspace-wide policy and may be raised deliberately over time.

## Expected Local Checks

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy -p durable-streams-server --all-targets -- -D warnings
cargo clippy -p durable-streams-client --all-targets
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

If you touch conformance harness plumbing, also verify the shell entrypoints:

```bash
for script in scripts/conformance/*.sh tests/conformance/client/*.sh tests/conformance/server/*.sh; do
  bash -n "$script" || exit
done
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

## Library and Release Policy

The server is a published library and executable. Preserve its documented
protocol, persistence, and public API contracts; make intentional breaking
changes explicit in the changelog and provide migration examples. The client
remains unpublished and is excluded from release automation.

Both crates inherit workspace lints. Server Clippy is enforced with warnings
denied; client Clippy remains advisory, with existing crate-level documentation
and dead-code allowances. Server tests allow `unwrap` locally because a failed
setup or assertion should fail the test; production code retains the lint.

For public API changes, inspect the rustdoc and run
`./scripts/check-server-public-api.sh` with the installed nightly toolchain.
Review snapshot changes as compatibility changes, rather than accepting a new
snapshot solely to make the test pass. For protocol changes, also run the
applicable conformance suite through `scripts/conformance`.

## Releasing

Releases are managed via a standing release PR maintained automatically
by [`release-plz`](https://release-plz.ieni.dev/). The workflow in
`.github/workflows/release-plz.yml` runs on every push to `trunk`:

1. A PR titled `chore: release` is kept open against `trunk`.
2. It reflects the current `[Unreleased]` section of each managed
   crate's `CHANGELOG.md`, plus a proposed version bump derived from
   Conventional Commit prefixes and compatibility analysis. Review the proposed
   bump against the crate's current version and actual public API changes,
   particularly while versions are below 1.0.
3. Successful workflow runs update that PR to reflect the latest trunk state.

The workflow only runs the `release-pr` subcommand. It never publishes
to crates.io and never creates tags or GitHub releases (also
enforced in `release-plz.toml`: `publish = false`, `git_tag_enable = false`,
`git_release_enable = false`).

### Cutting a release

1. Check that the release-PR workflow succeeded and its standing PR reflects
   the intended scope. An absent PR can also indicate a workflow failure.
2. Review the rewritten `CHANGELOG.md` and version bump. Edit the
   PR contents directly if the generated notes need polish.
3. Merge the PR. This lands the version bump and finalised changelog
   on `trunk`.
4. Tag the merge commit with `<crate>-v<version>` and push the tag.
   Signed annotated tags preferred:

   ```bash
   git tag -s -m "durable-streams-server 0.3.1" durable-streams-server-v0.3.1
   git push origin durable-streams-server-v0.3.1
   ```

5. Wait for `.github/workflows/pre-release.yml` to pass on the tag for nightly
   public-API validation, all-backend conformance, and client
   conformance against the tagged commit.
6. Publish to crates.io manually for now:

   ```bash
   cargo publish -p durable-streams-server
   ```

   A separate `release-plz release` workflow can be wired in later
   once trusted publishing is configured.

### Prerequisites and notes

- The release-plz workflow authenticates via a dedicated GitHub App
  installation token (`RELEASE_PLZ_APP_ID` + `RELEASE_PLZ_APP_PRIVATE_KEY`
  repo secrets). PRs opened with the default `GITHUB_TOKEN` do not
  trigger workflow runs, which would permanently block the `CI pass`
  merge gate — the App token authenticates as a distinct actor so CI
  runs normally on release PRs.
- `crates/durable-streams-client/Cargo.toml` sets `publish = false` and
  `release-plz.toml` sets `release = false` for that crate. Client publication
  requires updating both settings and adding a changelog and release metadata.
- Branch protection requires the aggregate `CI pass` check and
  up-to-date-with-`trunk` status before merge, which applies to the
  release PR too.
