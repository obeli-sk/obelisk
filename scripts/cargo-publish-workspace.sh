#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

EXCLUDE_PACKAGES=()
while read -r pkg; do
    EXCLUDE_PACKAGES+=("--exclude" "$pkg")
done < assets/unpublishable-packages.txt

cargo publish -Z package-workspace --workspace "${EXCLUDE_PACKAGES[@]}" "$@"
