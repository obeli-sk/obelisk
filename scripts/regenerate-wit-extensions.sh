#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo build
CARGO_WORKSPACE_DIR=$(pwd)

(
    cd crates/testing/test-programs/fibo/activity/wit
    rm -rf gen
    $CARGO_WORKSPACE_DIR/target/debug/obelisk generate extensions activity_wasm . gen
)
(
    cd crates/testing/test-programs/fibo/workflow/wit
    rm -rf gen
    $CARGO_WORKSPACE_DIR/target/debug/obelisk generate extensions workflow . gen
)
(
    cd crates/testing/test-programs/http/activity/wit
    rm -rf gen
    $CARGO_WORKSPACE_DIR/target/debug/obelisk generate extensions activity_wasm . gen
)
(
    cd crates/testing/test-programs/sleep/activity/wit
    rm -rf gen
    $CARGO_WORKSPACE_DIR/target/debug/obelisk generate extensions activity_wasm . gen
)
(
    cd crates/testing/test-programs/stub/activity/wit
    rm -rf gen
    $CARGO_WORKSPACE_DIR/target/debug/obelisk generate extensions activity_stub . gen
)
