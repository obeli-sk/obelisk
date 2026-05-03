#!/usr/bin/env bash
# Speed up testing by compiling WASM components upfront.
set -exuo pipefail
cd "$(dirname "$0")/.."

export RUST_BACKTRACE=1
export RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}"

cargo nextest run --no-output-indent --workspace --profile ci-test-populate-codegen-cache \
  populate_codegen_cache populate_js_codegen_cache \
  ${ADDITIONAL_FEATURES:-}
