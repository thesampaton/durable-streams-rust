#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

cd "$repo_root"
exec cargo run --quiet -p durable-streams-client --bin client-conformance-adapter -- "$@"
