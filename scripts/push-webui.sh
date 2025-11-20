#!/usr/bin/env bash

# Rebuild webui and webui-proxy, then push the proxy WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

which trunk # nix develop .#web --command

TAG="$1"
rm -rf crates/webui/dist

# Run trunk in webui-builder
CARGO_PROFILE_RELEASE_DEBUG=limited RUN_TRUNK=true \
    cargo build --package webui-proxy --target=wasm32-wasip2 --profile=release_trunk

if [ "$TAG" != "dry-run" ]; then

    # Download obelisk into a temp directory
    TMPDIR="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR"' EXIT
    (
        cd "$TMPDIR"
        curl -L --tlsv1.2 -sSf https://raw.githubusercontent.com/obeli-sk/obelisk/main/download.sh | bash
    )
    OBELISK="$TMPDIR/obelisk"

    OUTPUT=$($OBELISK client component push "target/wasm32-wasip2/release_trunk/webui_proxy.wasm" \
        "docker.io/getobelisk/webui:$TAG")
    echo -n $OUTPUT > assets/webui-version.txt
fi
