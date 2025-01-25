#!/usr/bin/env bash

# Rebuild webui and webui-proxy, then push the proxy WASM component to Docker Hub.

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
rm -rf crates/webui/dist

# Run trunk in webui-builder
RUN_TRUNK=true cargo build --package webui-proxy --target=wasm32-wasip2 --profile=release_trunk
echo -n $(obelisk client component push "target/wasm32-wasip2/release_trunk/webui_proxy.wasm" \
    "docker.io/getobelisk/webui:$TAG") > assets/webui-version.txt
