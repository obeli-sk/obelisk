#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo fmt

cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged  "$@" -- -D warnings

git status -s
