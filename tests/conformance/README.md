# Conformance Harness

This directory contains workspace-level integration with the upstream Durable
Streams conformance suites.

Run commands below from the workspace root. The [standards record](../../docs/standards.md)
tracks the pinned protocol, suite versions, and validation results. The runners
use the exact npm pins in [package.json](../../package.json).

## Client Conformance

Install the pinned npm dependencies first:

```bash
npm install
```

Run the full client suite:

```bash
./scripts/conformance/run-client-suite.sh
```

Add `--fail-fast` to stop on the first failure. The runner uses
[run-adapter.sh](client/run-adapter.sh) to launch the Rust client adapter.

## Server Conformance

The server suite runs against an HTTP base URL. To build and launch the
workspace server before running the suite:

```bash
./scripts/conformance/run-server-suite.sh
```

The default [launcher](server/start-server.sh) builds and runs the server from
the current checkout.

## Runner controls

| Variable | Effect / default |
| --- | --- |
| `DURABLE_STREAMS_SERVER_URL` | Target origin including port; default `http://127.0.0.1:4437` |
| `DURABLE_STREAMS_SERVER_LAUNCHER` | Alternate executable launcher; default `tests/conformance/server/start-server.sh` |
| `DURABLE_STREAMS_SERVER_SKIP_LAUNCH` | Set to `1` to test an already running server |
| `DURABLE_STREAMS_SERVER_READY_TIMEOUT` | Seconds to wait for the launched TCP listener; default `60` |
| `DURABLE_STREAMS_SERVER_TEST_TIMEOUT_MS` | Whole Vitest test deadline; default `30000` |

The built-in launcher derives its port from the target URL. For a remote server,
set `DURABLE_STREAMS_SERVER_SKIP_LAUNCH=1`. Listener readiness is a TCP check;
it does not validate application health or background worker initialization.

## Server suite 0.3.6

The server runner enables `subscriptions: true`, so all 338 upstream tests run,
including the six subscription tests. Its local launcher enables insecure
localhost webhooks for the suite's callback receivers. Production defaults keep
this option disabled. Use a separate data directory and port for each backend
run; short TTL and SSE tests should run without competing load.

```bash
DS_STORAGE__MODE=file DS_STORAGE__DATA_DIR=/tmp/ds-file-conformance \
  ./scripts/conformance/run-server-suite.sh
DS_STORAGE__MODE=acid DS_STORAGE__ACID_BACKEND=file \
  DS_STORAGE__DATA_DIR=/tmp/ds-acid-conformance \
  ./scripts/conformance/run-server-suite.sh
```

The runner forwards Vitest arguments after npm's `--` separator (for example
`./scripts/conformance/run-server-suite.sh -t "Reserved subscription APIs"`).
Whole tests have a 30-second deadline because some upstream tests contain many
sequential live-read race probes; individual protocol deadlines and assertions
remain upstream-controlled. Override this with
`DURABLE_STREAMS_SERVER_TEST_TIMEOUT_MS` when needed.

Use Node 20 for the validation matrix, matching CI (verified with Node 20.20.2).
During this update, Node 26.7.0's built-in fetch delayed reused-connection GETs
by roughly 2.5 seconds on this macOS host, causing two-second TTL tests to expire
before the request arrived. Node 20 did not exhibit that delay. No TTL grace
period or protocol assertion changes were made to accommodate that behavior.
