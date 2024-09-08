#!/usr/bin/env bash

# Run regular tests.

set -exuo pipefail
cd $(dirname "$0")/..

RUST_LOG=info cargo nextest run --workspace -P ci-test-nosim "$@"
