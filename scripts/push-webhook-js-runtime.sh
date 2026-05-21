#!/usr/bin/env bash

# Rebuild webhook-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

if ! command -v obelisk >/dev/null; then
    echo "error: obelisk must be on PATH" >&2
    exit 1
fi

TAG="$1"
OUTPUT_FILE="${2:-assets/webhook-js-runtime-version.txt}"

cargo check --package webhook-js-runtime-builder # triggers build.rs of webhook-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    TMP_TOML=$(mktemp -t webhook-deployment-XXXXXX.toml)
    trap "rm -f $TMP_TOML" EXIT
    cat > "$TMP_TOML" <<EOF
[[webhook_endpoint_wasm]]
name = "pushed"
location = "$(pwd)/target/release_wasm_runtime/wasm32-wasip2/release_wasm_runtime/webhook_js_runtime.wasm"
routes = [""]
EOF
    OUTPUT=$(obelisk component push --deployment "$TMP_TOML" \
        pushed "oci://docker.io/getobelisk/webhook-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
