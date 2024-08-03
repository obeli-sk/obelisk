#!/usr/bin/env bash
set -e; set -o pipefail;
cd $(dirname "$0")/..

cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged -- -D warnings
RUSTFLAGS="--cfg madsim" cargo clippy --workspace --target-dir=target/debug/madsim --fix --allow-dirty --allow-staged --all-targets  -- -D warnings

cargo fmt
