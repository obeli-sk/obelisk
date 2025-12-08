#!/usr/bin/env bash

# Push all WASM components from crates/testing/test-programs to the Docker Hub and update obelisk-oci.toml

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
TOML_FILE="obelisk.toml"
PREFIX="docker.io/getobelisk/"

cargo build

push() {
    RELATIVE_PATH=$1
    FILE_NAME_WITHOUT_EXT=$(basename "$RELATIVE_PATH" | sed 's/\.[^.]*$//')
    OCI_LOCATION="${PREFIX}${FILE_NAME_WITHOUT_EXT}:${TAG}"
    echo "Pushing ${RELATIVE_PATH} to ${OCI_LOCATION}..."
    OUTPUT=$(cargo run -- client component push "$RELATIVE_PATH" "$OCI_LOCATION")

    # Replace the old location with the actual OCI location
    sed -i -E "/name = \"${FILE_NAME_WITHOUT_EXT}\"/{n;s|location\.oci = \".*\"|location.oci = \"${OUTPUT}\"|}" "$TOML_FILE"
}

# Make sure all components are fresh
cargo check --workspace

push "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_fibo_activity.wasm"
push "target/release_testprograms/wasm32-unknown-unknown/release_wasm/test_programs_fibo_workflow.wasm"
push "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_fibo_webhook.wasm"
push "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_http_get_activity.wasm"
push "target/release_testprograms/wasm32-unknown-unknown/release_wasm/test_programs_http_get_workflow.wasm"
push "target/release_testprograms/wasm32-wasip2/release_wasm/test_programs_sleep_activity.wasm"
push "target/release_testprograms/wasm32-unknown-unknown/release_wasm/test_programs_sleep_workflow.wasm"

echo "All components pushed and TOML file updated successfully."
