#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo run -- generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/fibo/activity/wit
cargo run -- generate exported-ext-wits --component-type workflow         crates/testing/test-programs/fibo/workflow/wit
cargo run -- generate exported-ext-wits --component-type webhook_endpoint crates/testing/test-programs/fibo/webhook/wit
cargo run -- generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/http/activity/wit
cargo run -- generate exported-ext-wits --component-type workflow         crates/testing/test-programs/http/workflow/wit
cargo run -- generate exported-ext-wits --component-type activity_stub    crates/testing/test-programs/stub/activity/wit
cargo run -- generate exported-ext-wits --component-type workflow         crates/testing/test-programs/stub/workflow/wit
cargo run -- generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/dir/activity/wit
cargo run -- generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/sleep/activity/wit
cargo run -- generate exported-ext-wits --component-type workflow         crates/testing/test-programs/sleep/workflow/wit
cargo run -- generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/process/activity/wit
cargo run -- generate exported-ext-wits --component-type activity_wasm    crates/testing/test-programs/serde/activity/wit
