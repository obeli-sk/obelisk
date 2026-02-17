#!/usr/bin/env bash

# Rebuild workflow-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
OUTPUT_FILE="${2:-assets/workflow-js-version.txt}"

cargo check --workspace # triggers build.rs of workflow-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    OUTPUT=$(cargo run --  component push \
        "target/release_testprograms/wasm32-unknown-unknown/release_wasm/workflow_js_runtime.wasm" \
        "docker.io/getobelisk/workflow-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
