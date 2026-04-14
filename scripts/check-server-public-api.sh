#!/usr/bin/env bash
set -euo pipefail

cargo +nightly test -p durable-streams-server --test public_api -- --ignored
