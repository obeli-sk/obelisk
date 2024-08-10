#!/usr/bin/env bash

# Run regular tests.

set -exuo pipefail
cd $(dirname "$0")/..

cargo nextest run --workspace -P ci-test-nosim "$@"
