#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
toolchain="$(cat crates/durable-streams-server/tests/public-api-toolchain.txt)"
cargo "+$toolchain" test --locked -p durable-streams-server --test public_api -- --ignored
