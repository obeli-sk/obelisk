#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo build

tempfile=$(mktemp)
echo 'api.listening_addr = "127.0.0.1:5005"' > "$tempfile"

./target/debug/obelisk server run -c "$tempfile" &
obelisk_pid=$!

# Give server time to start
sleep 1

# Fetch OpenAPI spec safely
curl -sf http://127.0.0.1:5005/openapi.json | jq > ./assets/openapi.json

# Cleanup
kill "$obelisk_pid"
rm "$tempfile"
