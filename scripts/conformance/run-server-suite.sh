#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
launcher="${DURABLE_STREAMS_SERVER_LAUNCHER:-$repo_root/tests/conformance/server/start-server.sh}"
server_url="${DURABLE_STREAMS_SERVER_URL:-http://127.0.0.1:4437}"
server_pid=""
tmp_dir=""

cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    if [[ -n "$tmp_dir" ]]; then
        rm -rf "$tmp_dir"
    fi
}

trap cleanup EXIT

if [[ "${DURABLE_STREAMS_SERVER_SKIP_LAUNCH:-0}" != "1" ]]; then
    if [[ ! -x "$launcher" ]]; then
        echo "expected executable launcher at $launcher" >&2
        exit 1
    fi

    "$launcher" &
    server_pid="$!"
    sleep "${DURABLE_STREAMS_SERVER_STARTUP_DELAY:-2}"

    if ! kill -0 "$server_pid" 2>/dev/null; then
        wait "$server_pid"
        exit 1
    fi
fi

if [[ -z "$server_url" ]]; then
    echo "expected non-empty server URL" >&2
    exit 1
fi

mkdir -p "$repo_root/target"
tmp_dir="$(mktemp -d "$repo_root/target/ds-server-conformance.XXXXXX")"
cat >"$tmp_dir/conformance.test.mjs" <<'EOF'
import { runConformanceTests } from "@durable-streams/server-conformance-tests";

runConformanceTests({ baseUrl: process.env.CONFORMANCE_TEST_URL });
EOF

cd "$repo_root"
CONFORMANCE_TEST_URL="$server_url" npm exec vitest run "$tmp_dir/conformance.test.mjs" "$@"
