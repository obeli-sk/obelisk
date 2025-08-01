#!/usr/bin/env bash

# Run regular tests. Use `--success-output=immediate` to print successful test output.

set -exuo pipefail
cd "$(dirname "$0")/.."

RUST_BACKTRACE=1 \
RUST_LOG="${RUST_LOG:-obeli=debug}" \
cargo nextest run  --no-output-indent --workspace -P ci-test-nosim "$@"
