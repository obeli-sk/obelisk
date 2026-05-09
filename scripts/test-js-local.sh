#!/usr/bin/env bash

# Run regular tests. Use `--success-output=immediate` to print successful test output.

set -exuo pipefail
cd "$(dirname "$0")/.."
DEFAULT_FEATURES="-F activity-js-local,workflow-js-local,webhook-js-local"

export ADDITIONAL_FEATURES="${DEFAULT_FEATURES}${ADDITIONAL_FEATURES:+ $ADDITIONAL_FEATURES}"
scripts/test.sh "$@"
