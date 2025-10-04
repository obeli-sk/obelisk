#!/usr/bin/env bash

# Publish all packages in the workspace except for those listed in assets/unpublishable-packages.txt.
set -exuo pipefail
cd "$(dirname "$0")/.."

scripts/unpublishable-packages.sh

EXCLUDE_PACKAGES=$(awk '{printf " --exclude %s", $1}' "assets/unpublishable-packages.txt")

cargo publish --workspace ${EXCLUDE_PACKAGES[@]} "$@"
