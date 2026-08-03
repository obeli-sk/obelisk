#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo fmt # format first to allow cache hits of clippy on re-runs
cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged  "$@" -- -D warnings
cargo fmt # format fixes

git status -s
