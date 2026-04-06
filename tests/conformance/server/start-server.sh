#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
server_url="${DURABLE_STREAMS_SERVER_URL:-http://127.0.0.1:4437}"
host_port="${server_url#*://}"
host_port="${host_port%%/*}"
port="${host_port##*:}"

if [[ -z "$port" || "$port" == "$host_port" ]]; then
    echo "failed to derive listen port from DURABLE_STREAMS_SERVER_URL=$server_url" >&2
    exit 1
fi

cd "$repo_root"
exec env \
    DS_SERVER__PORT="$port" \
    DS_SERVER__LONG_POLL_TIMEOUT_SECS="${DS_SERVER__LONG_POLL_TIMEOUT_SECS:-2}" \
    DS_SERVER__SSE_RECONNECT_INTERVAL_SECS="${DS_SERVER__SSE_RECONNECT_INTERVAL_SECS:-5}" \
    cargo run --quiet -p durable-streams-server -- "$@"
