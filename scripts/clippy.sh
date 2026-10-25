#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo fmt # format first to allow cache hits of clippy on re-runs
cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged  "$@" -- -D warnings
cargo fmt # format fixes

cargo fmt --manifest-path tools/obelisk-api-codegen/Cargo.toml
cargo clippy --locked --manifest-path tools/obelisk-api-codegen/Cargo.toml --all-targets -- -D warnings

git status -s
