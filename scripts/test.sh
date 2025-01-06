#!/usr/bin/env bash

# Run regular tests.

set -exuo pipefail
cd "$(dirname "$0")/.."

RUST_BACKTRACE=1 RUST_LOG=obeli=debug cargo nextest run --workspace -P ci-test-nosim "$@"
