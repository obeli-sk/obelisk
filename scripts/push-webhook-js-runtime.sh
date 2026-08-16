#!/usr/bin/env bash

# Rebuild webhook-js-runtime, then push the WASM component to Docker Hub.

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
OUTPUT_FILE="${2:-crates/embedded-assets/webhook-js-runtime-version.txt}"

cargo check --package webhook-js-runtime-builder # triggers build.rs of webhook-js-runtime-builder

if [ "$TAG" != "dry-run" ]; then
    STRIPPED="target/wasm-cache/webhook_js_runtime.stripped.wasm"
    wasm-tools strip --all "target/wasm-cache/webhook_js_runtime.wasm" -o "$STRIPPED"
    TMP_TOML="webhook-deployment-for-push.toml"
    trap "rm -f $TMP_TOML" EXIT
    cat > "$TMP_TOML" <<EOF
[[webhook_endpoint_wasm]]
name = "target_component"
location = "$STRIPPED"
routes = [""]
EOF
    OUTPUT=$(obelisk component push --deployment "$TMP_TOML" \
        target_component "oci://docker.io/getobelisk/webhook-js-runtime:$TAG")
    printf '%s' "$OUTPUT" > "$OUTPUT_FILE"
fi
