#!/usr/bin/env bash

# Push all WASM components from obelisk-testing-wasm-local.toml to the Docker Hub
# and update obelisk-testing-wasm-oci.toml

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
PREFIX="docker.io/getobelisk/"
SOURCE_TOML="obelisk-testing-wasm-local.toml"
TARGET_TOML="obelisk-testing-wasm-oci.toml"

# Make sure all components are fresh
cargo check --workspace

push() {
    COMPONENT_NAME=$1
    OCI_REF="oci://${PREFIX}${COMPONENT_NAME}:${TAG}"
    echo "Pushing ${COMPONENT_NAME} to ${OCI_REF}..."
    # Outputs e.g. "oci://docker.io/getobelisk/name:tag@sha256:..."
    OUTPUT=$(cargo run -- component push "${COMPONENT_NAME}" "${OCI_REF}" --deployment "${SOURCE_TOML}")
    cargo run -- component add "${OUTPUT}" "${COMPONENT_NAME}" --deployment "${TARGET_TOML}"
}

push test_programs_fibo_activity
push test_programs_fibo_workflow
push test_programs_fibo_webhook
push test_programs_http_get_activity
push test_programs_http_get_workflow
push test_programs_sleep_activity
push test_programs_sleep_workflow

echo "All components pushed and TOML file updated successfully."
