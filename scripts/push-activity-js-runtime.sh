#!/usr/bin/env bash

# Rebuild activity-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
OUTPUT_FILE="${2:-assets/activity-js-runtime-version.txt}"

cargo check --workspace # triggers build.rs of activity-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    TMP_TOML=$(mktemp -t workflow-deployment-XXXXXX.toml)
    trap "rm -f $TMP_TOML" EXIT
    cat > "$TMP_TOML" <<EOF
[[activity_wasm]]
name = "pushed"
location = "$(pwd)/target/release_wasm_runtime/wasm32-wasip2/release_wasm_runtime/activity_js_runtime.wasm"
EOF
    OUTPUT=$(cargo run -- component push --deployment "$TMP_TOML" \
        pushed "oci://docker.io/getobelisk/activity-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
