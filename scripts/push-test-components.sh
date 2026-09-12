#!/usr/bin/env bash

# Push all WASM components from deployment-testing-wasm-local.toml to the Docker Hub
# and update deployment-testing-wasm-oci.toml

set -exuo pipefail
cd "$(dirname "$0")/.."

if ! command -v obelisk >/dev/null; then
    echo "error: obelisk must be on PATH" >&2
    exit 1
fi

TAG="$1"
PREFIX="docker.io/getobelisk/"
SOURCE_TOML="deployment-testing-wasm-local.toml"
TARGET_TOML="deployment-testing-wasm-oci.toml"
# TODO: After Rust 1.100 is released, add the P3 fixtures to deployment-testing-wasm-local.toml and remove this temporary manifest.
WASIP3_SOURCE_TOML=$(mktemp .wasip3-push.XXXXXX.toml)
trap 'rm -f "$WASIP3_SOURCE_TOML"' EXIT

# Make sure all pushed components are fresh.
cargo check \
    --package test-programs-fibo-activity-builder \
    --package test-programs-fibo-workflow-builder \
    --package test-programs-fibo-webhook-builder \
    --package test-programs-http-get-activity-builder \
    --package test-programs-http-get-workflow-builder \
    --package test-programs-sleep-activity-builder \
    --package test-programs-sleep-workflow-builder

# Build native P3 fixtures with the pinned nightly from the wasip3-components shell.
cargo build \
    --locked \
    --profile release_testprograms \
    --target wasm32-wasip3 \
    --package test-programs-wasip3-activity \
    --package test-programs-wasip3-webhook

WASIP3_ACTIVITY="target/sandbox/wasm32-wasip3/release_testprograms/test_programs_wasip3_activity.wasm"
WASIP3_WEBHOOK="target/sandbox/wasm32-wasip3/release_testprograms/test_programs_wasip3_webhook.wasm"
wasm-tools validate "$WASIP3_ACTIVITY"
wasm-tools validate "$WASIP3_WEBHOOK"

printf '%s\n' \
    '[[activity_wasm]]' \
    'name = "test_programs_wasip3_activity"' \
    "location = \"$WASIP3_ACTIVITY\"" \
    'max_retries = 0' \
    '' \
    '[[webhook_endpoint_wasm]]' \
    'name = "test_programs_wasip3_webhook"' \
    "location = \"$WASIP3_WEBHOOK\"" \
    'routes = [{ methods = ["GET"], route = "/wasip3" }]' \
    > "$WASIP3_SOURCE_TOML"

obelisk server verify --deployment "$WASIP3_SOURCE_TOML"

push() {
    COMPONENT_NAME=$1
    DEPLOYMENT_TOML=${2:-$SOURCE_TOML}
    OCI_REF="oci://${PREFIX}${COMPONENT_NAME}:${TAG}"
    echo "Pushing ${COMPONENT_NAME} to ${OCI_REF}..."
    # Outputs e.g. "oci://docker.io/getobelisk/name:tag@sha256:..."
    OUTPUT=$(obelisk component push "${COMPONENT_NAME}" "${OCI_REF}" --deployment "${DEPLOYMENT_TOML}")
    obelisk component add "${OUTPUT}" "${COMPONENT_NAME}" --deployment "${TARGET_TOML}"
}

push test_programs_fibo_activity
push test_programs_fibo_workflow
push test_programs_fibo_webhook
push test_programs_http_get_activity
push test_programs_http_get_workflow
push test_programs_sleep_activity
push test_programs_sleep_workflow
push test_programs_wasip3_activity "$WASIP3_SOURCE_TOML"
push test_programs_wasip3_webhook "$WASIP3_SOURCE_TOML"

echo "All components pushed and TOML file updated successfully."
