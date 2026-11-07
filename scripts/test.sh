#!/usr/bin/env bash

# To print success output prepend with: ADDITIONAL_FEATURES="--success-output=immediate"

set -exuo pipefail
cd "$(dirname "$0")/.."

# First compile WASM files. Skipping this makes testing much slower as each test will try to compile WASM separately.
scripts/test-phase1.sh
scripts/test-phase2.sh "$@"
