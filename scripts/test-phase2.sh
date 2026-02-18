#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

RUST_BACKTRACE=1 \
RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}" \
cargo nextest run --no-output-indent --workspace --profile ci-test \
  ${ADDITIONAL_FEATURES:-} \
  -- --skip populate_codegen_cache "$@"
