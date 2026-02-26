#!/usr/bin/env bash

# Run regular tests. Use `--success-output=immediate` to print successful test output.

set -exuo pipefail
cd "$(dirname "$0")/.."
DEFAULT_FEATURES="-F activity-js-local -F workflow-js-local -F webhook-js-local"

export ADDITIONAL_FEATURES="${DEFAULT_FEATURES}${ADDITIONAL_FEATURES:+ $ADDITIONAL_FEATURES}"
scripts/test.sh "$@"
