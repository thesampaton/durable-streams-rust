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

## Releasing

Releases are managed via a standing release PR maintained automatically
by [`release-plz`](https://release-plz.ieni.dev/). The workflow in
`.github/workflows/release-plz.yml` runs on every push to `trunk`:

1. A PR titled `chore: release` is kept open against `trunk`.
2. It reflects the current `[Unreleased]` section of each managed
   crate's `CHANGELOG.md`, plus a proposed version bump derived from
   Conventional Commit prefixes since the last release tag
   (`feat:` → minor, `fix:` → patch, a `BREAKING CHANGE:` footer or
   `!` marker → major). Prefixes like `ci:`, `docs:`, `chore:`,
   `style:`, and `refactor:` do not trigger a bump.
3. Every trunk merge rewrites that PR to reflect the latest state.
   You don't re-run anything — it's always up to date.

The workflow only runs the `release-pr` subcommand. It never publishes
to crates.io and never creates tags or GitHub releases (belt-and-suspenders
enforced in `release-plz.toml`: `publish = false`, `git_tag_enable = false`,
`git_release_enable = false`).

### Cutting a release

1. Wait until the standing release PR reflects the scope you want
   to ship. If there's no PR open, there are no release-worthy
   commits since the last tag.
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

5. `.github/workflows/pre-release.yml` runs on tag push for nightly
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
- `crates/durable-streams-client` is currently `publish = false` in
  `release-plz.toml` and is not part of the release flow. Flip
  `release = true` when the client is ready to publish.
- Branch protection requires the aggregate `CI pass` check and
  up-to-date-with-`trunk` status before merge, which applies to the
  release PR too.
