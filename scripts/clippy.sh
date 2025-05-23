#!/usr/bin/env bash

# Run clippy normally and with `madsim` cfg flag.

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged -- -D warnings
RUSTFLAGS="--cfg madsim" cargo clippy --workspace --target-dir=target/debug/madsim --fix --allow-dirty --allow-staged --all-targets  -- -D warnings
cargo fmt

git status -s
