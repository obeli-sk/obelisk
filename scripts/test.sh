#!/usr/bin/env bash

# To print success output prepend with: ADDITIONAL_FEATURES="--success-output=immediate"

set -exuo pipefail
cd "$(dirname "$0")/.."

scripts/test-phase1.sh
scripts/test-phase2.sh "$@"
