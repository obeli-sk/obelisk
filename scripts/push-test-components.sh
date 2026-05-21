#!/usr/bin/env bash

# Push all WASM components from obelisk-testing-wasm-local.toml to the Docker Hub
# and update obelisk-testing-wasm-oci.toml

set -exuo pipefail
cd "$(dirname "$0")/.."

if ! command -v obelisk >/dev/null; then
    echo "error: obelisk must be on PATH" >&2
    exit 1
fi

TAG="$1"
PREFIX="docker.io/getobelisk/"
SOURCE_TOML="obelisk-testing-wasm-local.toml"
TARGET_TOML="obelisk-testing-wasm-oci.toml"

# Make sure all pushed components are fresh.
cargo check \
    --package test-programs-fibo-activity-builder \
    --package test-programs-fibo-workflow-builder \
    --package test-programs-fibo-webhook-builder \
    --package test-programs-http-get-activity-builder \
    --package test-programs-http-get-workflow-builder \
    --package test-programs-sleep-activity-builder \
    --package test-programs-sleep-workflow-builder

push() {
    COMPONENT_NAME=$1
    OCI_REF="oci://${PREFIX}${COMPONENT_NAME}:${TAG}"
    echo "Pushing ${COMPONENT_NAME} to ${OCI_REF}..."
    # Outputs e.g. "oci://docker.io/getobelisk/name:tag@sha256:..."
    OUTPUT=$(obelisk component push "${COMPONENT_NAME}" "${OCI_REF}" --deployment "${SOURCE_TOML}")
    obelisk component add "${OUTPUT}" "${COMPONENT_NAME}" --deployment "${TARGET_TOML}"
}

push test_programs_fibo_activity
push test_programs_fibo_workflow
push test_programs_fibo_webhook
push test_programs_http_get_activity
push test_programs_http_get_workflow
push test_programs_sleep_activity
push test_programs_sleep_workflow

echo "All components pushed and TOML file updated successfully."
