#!/usr/bin/env bash

# Push all WASM components from deployment-testing-wasm-local.toml to the Docker Hub
# and update deployment-testing-wasm-oci.toml

set -exuo pipefail
cd "$(dirname "$0")/.."

TAG="$1"
PREFIX="docker.io/getobelisk/"
TARGET_TOML="deployment-testing-wasm-oci.toml"
CARGO_TARGET_ROOT=${CARGO_TARGET_DIR:-target}

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

WASIP3_ACTIVITY="$CARGO_TARGET_ROOT/wasm32-wasip3/release_testprograms/test_programs_wasip3_activity.wasm"
WASIP3_WEBHOOK="$CARGO_TARGET_ROOT/wasm32-wasip3/release_testprograms/test_programs_wasip3_webhook.wasm"
WASM_CACHE="$CARGO_TARGET_ROOT/wasm-cache"
NO_HOSTS='"env_vars":[],"allowed_hosts":[]'
WORKFLOW='{"component_type":"workflow_wasm"}'
WEBHOOK="{\"component_type\":\"webhook_endpoint_wasm\",$NO_HOSTS}"
activity() {
    echo "{\"component_type\":\"activity_wasm\",$NO_HOSTS,\"lock_duration\":{\"seconds\":$1}}"
}
# Allowed hosts must match deployment-testing-wasm-local.toml.
HTTP_GET_ACTIVITY='{"component_type":"activity_wasm","env_vars":[],"allowed_hosts":[{"pattern":"*","methods":"*","request_url_regex":null,"secrets":[],"replace_in":[]},{"pattern":"httpbin.org","methods":"*","request_url_regex":null,"secrets":["MY_SECRET"],"replace_in":["headers","params","body"]}],"lock_duration":{"seconds":5}}'

push() {
    COMPONENT_NAME=$1
    WASM=$2
    METADATA=$3
    OCI_REF="${PREFIX}${COMPONENT_NAME}:${TAG}"
    echo "Pushing ${COMPONENT_NAME} to ${OCI_REF}..."
    OUTPUT=$(scripts/push-wasm-oci.sh "$WASM" "$OCI_REF" "$METADATA")
    LOCATION_PATTERN="^location = \"oci://${PREFIX}${COMPONENT_NAME}:[^\"]*\"$"
    if ! grep -q "$LOCATION_PATTERN" "$TARGET_TOML"; then
        echo "error: ${COMPONENT_NAME} location not found in ${TARGET_TOML}" >&2
        exit 1
    fi
    sed -i "s|${LOCATION_PATTERN}|location = \"${OUTPUT}\"|" "$TARGET_TOML"
}

push test_programs_fibo_activity "$WASM_CACHE/test_programs_fibo_activity.wasm" "$(activity 30)"
push test_programs_fibo_workflow "$WASM_CACHE/test_programs_fibo_workflow_component.wasm" "$WORKFLOW"
push test_programs_fibo_webhook "$WASM_CACHE/test_programs_fibo_webhook.wasm" "$WEBHOOK"
push test_programs_http_get_activity "$WASM_CACHE/test_programs_http_get_activity.wasm" "$HTTP_GET_ACTIVITY"
push test_programs_http_get_workflow "$WASM_CACHE/test_programs_http_get_workflow_component.wasm" "$WORKFLOW"
push test_programs_sleep_activity "$WASM_CACHE/test_programs_sleep_activity.wasm" "$(activity 10)"
push test_programs_sleep_workflow "$WASM_CACHE/test_programs_sleep_workflow_component.wasm" "$WORKFLOW"
push test_programs_wasip3_activity "$WASIP3_ACTIVITY" "$(activity 30)"
push test_programs_wasip3_webhook "$WASIP3_WEBHOOK" "$WEBHOOK"

echo "All components pushed and TOML file updated successfully."
