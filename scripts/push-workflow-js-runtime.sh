#!/usr/bin/env bash

# Rebuild workflow-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

if ! command -v obelisk >/dev/null; then
    echo "error: obelisk must be on PATH" >&2
    exit 1
fi

TAG="$1"
OUTPUT_FILE="${2:-assets/workflow-js-runtime-version.txt}"

cargo check --package workflow-js-runtime-builder # triggers build.rs of workflow-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    TMP_TOML=$(mktemp -t workflow-deployment-XXXXXX.toml)
    trap "rm -f $TMP_TOML" EXIT
    cat > "$TMP_TOML" <<EOF
[[workflow_wasm]]
name = "pushed"
location = "$(pwd)/target/wasm-cache/workflow_js_runtime_component.wasm"
EOF
    OUTPUT=$(obelisk component push --deployment "$TMP_TOML" \
        pushed "oci://docker.io/getobelisk/workflow-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
