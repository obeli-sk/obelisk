#!/usr/bin/env bash

# Run madsim tests

set -exuo pipefail
cd "$(dirname "$0")/.."
# set MADSIM_TEST_NUM to run multiple simulations per test
RUST_BACKTRACE=1 MADSIM_ALLOW_SYSTEM_THREAD=1 RUSTFLAGS="--cfg madsim" \
RUST_LOG="${RUST_LOG:-obeli=debug}" \
cargo nextest run --workspace --target-dir target/debug/madsim -P ci-test-madsim "$@"
