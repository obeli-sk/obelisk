#!/usr/bin/env bash

# Rebuild activity-js-runtime, then push the WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

if ! command -v obelisk >/dev/null; then
    echo "error: obelisk must be on PATH" >&2
    exit 1
fi

if ! command -v wasm-tools >/dev/null; then
    echo "error: wasm-tools must be on PATH" >&2
    exit 1
fi

TAG="$1"
OUTPUT_FILE="${2:-crates/embedded-assets/activity-js-runtime-version.txt}"

cargo check --package activity-js-runtime-builder # triggers build.rs of activity-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    STRIPPED="target/release_wasm_runtime/wasm32-wasip2/release_wasm_runtime/activity_js_runtime.stripped.wasm"
    wasm-tools strip --all "target/release_wasm_runtime/wasm32-wasip2/release_wasm_runtime/activity_js_runtime.wasm" -o "$STRIPPED"
    TMP_TOML="activity-deployment-for-push.toml"
    trap "rm -f $TMP_TOML" EXIT
    cat > "$TMP_TOML" <<EOF
[[activity_wasm]]
name = "target_component"
location = "$STRIPPED"
EOF
    OUTPUT=$(obelisk component push --deployment "$TMP_TOML" \
        target_component "oci://docker.io/getobelisk/activity-js-runtime:$TAG")
    echo -n $OUTPUT > $OUTPUT_FILE
fi
