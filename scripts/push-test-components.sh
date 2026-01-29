#!/usr/bin/env bash

# Push all WASM components from crates/testing/test-programs to the Docker Hub and update obelisk-oci.toml

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
PREFIX="docker.io/getobelisk/"

cargo check --workspace # starts build.rs of each -builder

push() {
    COMPONENT_TYPE=$1
    RELATIVE_PATH=$2
    FILE_NAME_WITHOUT_EXT=$(basename "$RELATIVE_PATH" | sed 's/\.[^.]*$//')
    OCI_LOCATION="${PREFIX}${FILE_NAME_WITHOUT_EXT}:${TAG}"
    echo "Pushing ${RELATIVE_PATH} to ${OCI_LOCATION}..."
    OUTPUT=$(cargo run -- component push "$RELATIVE_PATH" "$OCI_LOCATION")

    # Replace the old location with the actual OCI location
    cargo run -- component add ${COMPONENT_TYPE} ${OUTPUT} --name ${FILE_NAME_WITHOUT_EXT} --config obelisk-testing-sqlite-oci.toml
    cargo run -- component add ${COMPONENT_TYPE} ${OUTPUT} --name ${FILE_NAME_WITHOUT_EXT} --config obelisk-testing-postgres-oci.toml
}

# Make sure all components are fresh
cargo check --workspace

push activity_wasm      "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_fibo_activity.wasm"
push workflow           "target/release_testprograms/wasm32-unknown-unknown/release_wasm/test_programs_fibo_workflow.wasm"
push webhook_endpoint   "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_fibo_webhook.wasm"
push activity_wasm      "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_http_get_activity.wasm"
push workflow           "target/release_testprograms/wasm32-unknown-unknown/release_wasm/test_programs_http_get_workflow.wasm"
push activity_wasm      "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_sleep_activity.wasm"
push workflow           "target/release_testprograms/wasm32-unknown-unknown/release_wasm/test_programs_sleep_workflow.wasm"

echo "All components pushed and TOML file updated successfully."
