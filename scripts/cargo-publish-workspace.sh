#!/usr/bin/env bash

# Publish all packages in the workspace.
set -exuo pipefail
cd "$(dirname "$0")/.."

scripts/strip-js-local-deps.sh

cargo publish --workspace "$@"
