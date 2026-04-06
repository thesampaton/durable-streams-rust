#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
adapter="$repo_root/tests/conformance/client/run-adapter.sh"

if [[ ! -x "$adapter" ]]; then
    echo "expected executable adapter at $adapter" >&2
    exit 1
fi

cd "$repo_root"
npm exec durable-streams-client-conformance -- --run "$adapter" "$@"
