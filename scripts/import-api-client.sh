#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode=update
if [[ "${1:-}" == --check ]]; then
    mode=check
    shift
fi
if [[ $# -gt 1 ]]; then
    echo "Usage: $0 [--check] [obelisk-source]" >&2
    exit 2
fi
revision=$(cat assets/schemas/obelisk-openapi.rev)
kind=$(cat assets/schemas/obelisk-api.language)
case "$kind" in
    rust) artifact=crates/api-types ;;
    javascript) artifact=clients/javascript ;;
    *) echo "Unknown API client language: $kind" >&2; exit 2 ;;
esac
if [[ -n "${1:-}" ]]; then
    source=$(realpath "$1")
    if [[ $(git -C "$source" rev-parse HEAD) != "$revision" ]]; then
        echo "Source HEAD does not match assets/schemas/obelisk-openapi.rev" >&2
        exit 1
    fi
    git -C "$source" diff --quiet HEAD -- "$artifact" assets/openapi/lifecycle.json scripts/import-api-client.sh
else
    source=$(nix eval --raw --expr "(builtins.getFlake \"github:obeli-sk/obelisk/$revision\").outPath")
fi
if [[ "$mode" == check ]]; then
    diff -ru "$source/$artifact" vendor/obelisk-api
    diff -u "$source/assets/openapi/lifecycle.json" assets/schemas/obelisk-openapi.json
    diff -u "$source/scripts/import-api-client.sh" scripts/update-obelisk-api.sh
else
    mkdir -p vendor assets/schemas
    rm -rf vendor/obelisk-api
    cp -R "$source/$artifact" vendor/obelisk-api
    chmod -R u+w vendor/obelisk-api
    cp "$source/assets/openapi/lifecycle.json" assets/schemas/obelisk-openapi.json
    chmod u+w assets/schemas/obelisk-openapi.json
    importer=$(mktemp)
    cp "$source/scripts/import-api-client.sh" "$importer"
    chmod +x "$importer"
    mv "$importer" scripts/update-obelisk-api.sh
fi
