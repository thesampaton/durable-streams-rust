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

    # Derive host/port from the server URL so we can wait for readiness
    # instead of relying on a fixed sleep. The previous approach flaked in
    # CI when `cargo run` had to compile before listening.
    host_port="${server_url#*://}"
    host_port="${host_port%%/*}"
    host="${host_port%%:*}"
    port="${host_port##*:}"

    ready_timeout="${DURABLE_STREAMS_SERVER_READY_TIMEOUT:-60}"
    deadline=$(( $(date +%s) + ready_timeout ))
    until (echo > "/dev/tcp/${host}/${port}") 2>/dev/null; do
        if ! kill -0 "$server_pid" 2>/dev/null; then
            echo "server exited before becoming ready" >&2
            wait "$server_pid" || true
            exit 1
        fi
        if (( $(date +%s) >= deadline )); then
            echo "server did not accept connections on ${host}:${port} within ${ready_timeout}s" >&2
            exit 1
        fi
        sleep 0.2
    done
fi

if [[ -z "$server_url" ]]; then
    echo "expected non-empty server URL" >&2
    exit 1
fi

mkdir -p "$repo_root/target"
tmp_dir="$(mktemp -d "$repo_root/target/ds-server-conformance.XXXXXX")"
cat >"$tmp_dir/conformance.test.mjs" <<'EOF'
import { runConformanceTests } from "@durable-streams/server-conformance-tests";

runConformanceTests({ baseUrl: process.env.CONFORMANCE_TEST_URL, subscriptions: true });
EOF

cd "$repo_root"
# Recent suites run multiple timed live-read scenarios inside one test. Keep
# their individual deadlines while allowing enough time for the whole test.
CONFORMANCE_TEST_URL="$server_url" npm exec -- vitest run "$tmp_dir/conformance.test.mjs" \
    --testTimeout "${DURABLE_STREAMS_SERVER_TEST_TIMEOUT_MS:-30000}" "$@"
