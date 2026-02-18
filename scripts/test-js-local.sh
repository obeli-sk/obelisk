#!/usr/bin/env bash

# Run regular tests. Use `--success-output=immediate` to print successful test output.

set -exuo pipefail
cd "$(dirname "$0")/.."

ADDITIONAL_FEATURES="-F activity-js-local -F workflow-js-local" scripts/test.sh "$@"
