#!/usr/bin/env bash
# Remove *-js-runtime-builder / embedded-assets optional deps and their features from
# publishable crates. These are local-build-only and must not appear in published manifests.
set -euo pipefail
cd "$(dirname "$0")/.."

JS_BUILDER_OPTIONAL_PATTERN='activity-js-runtime-builder.*optional\|webhook-js-runtime-builder.*optional\|workflow-js-runtime-builder.*optional\|embedded-assets.*optional'

# Root Cargo.toml: remove optional dep entries for builders from [dependencies]
sed -i "/${JS_BUILDER_OPTIONAL_PATTERN}/d" Cargo.toml

# Replace feature definitions with empty features so cfg(feature = "...") in source remains valid
sed -i 's/"activity-js-local" = \["dep:activity-js-runtime-builder"\]/"activity-js-local" = []/' Cargo.toml
sed -i 's/"webhook-js-local" = \["dep:webhook-js-runtime-builder"\]/"webhook-js-local" = []/' Cargo.toml
sed -i 's/"workflow-js-local" = \["dep:workflow-js-runtime-builder"\]/"workflow-js-local" = []/' Cargo.toml
sed -i 's#"embed-assets" = \["dep:embedded-assets", "embedded-assets/download"\]#"embed-assets" = []#' Cargo.toml

# Remove the js-local test matrix entry from check-test.yml
sed -i '/- name: test-js-local.sh/,+2d' .github/workflows/check-test.yml

cargo metadata
