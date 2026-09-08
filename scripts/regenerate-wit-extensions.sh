#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo build
OBELISK_BIN=$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)/debug/obelisk

(
    cd crates/testing/test-programs/fibo/activity/wit
    if [ "${RECREATE:-}" = "true" ]; then rm -rf gen; fi
    $OBELISK_BIN generate wit-extensions "$@" activity . gen
)
(
    cd crates/testing/test-programs/fibo/workflow/wit
    if [ "${RECREATE:-}" = "true" ]; then rm -rf gen; fi
    $OBELISK_BIN generate wit-extensions "$@" workflow . gen
)
(
    cd crates/testing/test-programs/http/activity/wit
    if [ "${RECREATE:-}" = "true" ]; then rm -rf gen; fi
    $OBELISK_BIN generate wit-extensions "$@" activity . gen
)
(
    cd crates/testing/test-programs/sleep/activity/wit
    if [ "${RECREATE:-}" = "true" ]; then rm -rf gen; fi
    $OBELISK_BIN generate wit-extensions "$@" activity . gen
)
(
    cd crates/testing/test-programs/stub/activity/wit
    if [ "${RECREATE:-}" = "true" ]; then rm -rf gen; fi
    $OBELISK_BIN generate wit-extensions "$@" activity_stub . gen
)
