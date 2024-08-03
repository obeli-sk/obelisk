#!/usr/bin/env bash
set -e; set -o pipefail;
cd $(dirname "$0")/..

cargo nextest run --workspace -P ci-test-nosim
