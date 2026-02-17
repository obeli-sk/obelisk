#!/usr/bin/env bash

# Rebuild activity-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
OUTPUT_FILE="${2:-assets/activity-js-version.txt}"

cargo check --workspace # triggers build.rs of activity-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    OUTPUT=$(cargo run --  component push \
        "target/release_testprograms/wasm32-wasip2/release_wasm/activity_js_runtime.wasm" \
        "docker.io/getobelisk/activity-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
