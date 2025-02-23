#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

EXCLUDE_PACKAGES=$(awk '{printf " --exclude %s", $1}' "assets/unpublishable-packages.txt")

cargo publish -Z package-workspace --workspace ${EXCLUDE_PACKAGES[@]} "$@"
