#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged -- -D warnings

cargo fmt

git status -s
