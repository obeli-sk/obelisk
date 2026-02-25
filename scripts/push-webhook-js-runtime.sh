#!/usr/bin/env bash

# Rebuild activity-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
OUTPUT_FILE="${2:-assets/webhook-js-runtime-version.txt}"

cargo check --workspace # triggers build.rs of webhook-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    OUTPUT=$(cargo run --  component push \
        "target/release_wasm_runtime/wasm32-wasip2/release_wasm_runtime/webhook_js_runtime.wasm" \
        "docker.io/getobelisk/webhook-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
