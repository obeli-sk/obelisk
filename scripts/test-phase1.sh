#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo nextest run --no-output-indent --workspace --profile ci-test-populate-codegen-cache \
  populate_codegen_cache \
  ${ADDITIONAL_FEATURES:-}
