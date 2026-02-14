#!/usr/bin/env bash

# Run regular tests. Use `--success-output=immediate` to print successful test output.

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo nextest run  --no-output-indent --workspace -P ci-test-populate-codegen-cache populate_codegen_cache

RUST_BACKTRACE=1 \
RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}" \
cargo nextest run  --no-output-indent --workspace -P ci-test "$@"
