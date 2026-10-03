#!/usr/bin/env bash

# Push a WASM component to an OCI registry in the format Obelisk pulls.
# Usage: push-wasm-oci.sh <wasm> <reference without oci:// prefix> <component metadata JSON>
# Prints e.g. "oci://docker.io/getobelisk/name:tag@sha256:...".

set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <wasm> <reference> <metadata-json>" >&2
    exit 1
fi
WASM="$1"
REF="$2"
METADATA="$3"

for tool in jq oras sha256sum wasm-tools; do
    if ! command -v "$tool" >/dev/null; then
        echo "error: $tool must be on PATH" >&2
        exit 1
    fi
done

wasm-tools validate "$WASM"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT
# Config mirrors the one Obelisk generates: root world imports and exports, layer digest.
world_items() {
    wasm-tools component wit "$WASM" |
        awk '/^world root \{/ { in_root = 1; next } in_root && /^}/ { exit } in_root { print }' |
        sed -nE "s/^  $1 ([^;{ ]+)(:? .*|;)\$/\1/p" | sed 's/:$//' | jq -R . | jq -s .
}
jq -n -c \
    --arg created "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)" \
    --arg digest "sha256:$(sha256sum "$WASM" | cut -d' ' -f1)" \
    --argjson exports "$(world_items export)" \
    --argjson imports "$(world_items import)" \
    '{created: $created, author: null, architecture: "wasm", os: "wasip2", layerDigests: [$digest],
      component: {exports: $exports, imports: $imports, target: null}}' > "$TMP_DIR/config.json"
# oras stores layer titles from the path, so push from the file's directory with a bare file name.
cp "$WASM" "$TMP_DIR/component.wasm"
DIGEST=$(cd "$TMP_DIR" && oras push --no-tty --format 'go-template={{.digest}}' \
    --config "config.json:application/vnd.wasm.config.v0+json" \
    --annotation "obelisk.component_metadata:0.2.0=$METADATA" \
    "$REF" "component.wasm:application/wasm")
printf 'oci://%s@%s' "$REF" "$DIGEST"
