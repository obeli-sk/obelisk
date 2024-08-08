#!/usr/bin/env bash
set -e; set -o pipefail;
cd $(dirname "$0")/..

MADSIM_ALLOW_SYSTEM_THREAD=1 RUSTFLAGS="--cfg madsim" cargo nextest run --workspace --target-dir target/debug/madsim -P ci-test-madsim "$@"
