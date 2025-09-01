#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo build
target/debug/obelisk generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/fibo/activity/wit
target/debug/obelisk generate exported-ext-wits --component-type workflow         crates/testing/test-programs/fibo/workflow/wit
target/debug/obelisk generate exported-ext-wits --component-type webhook_endpoint crates/testing/test-programs/fibo/webhook/wit
target/debug/obelisk generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/http/activity/wit
target/debug/obelisk generate exported-ext-wits --component-type workflow         crates/testing/test-programs/http/workflow/wit
target/debug/obelisk generate exported-ext-wits --component-type activity_stub    crates/testing/test-programs/stub/activity/wit
target/debug/obelisk generate exported-ext-wits --component-type workflow         crates/testing/test-programs/stub/workflow/wit
target/debug/obelisk generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/dir/activity/wit
target/debug/obelisk generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/sleep/activity/wit
target/debug/obelisk generate exported-ext-wits --component-type workflow         crates/testing/test-programs/sleep/workflow/wit
target/debug/obelisk generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/process/activity/wit
target/debug/obelisk generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/serde/activity/wit
