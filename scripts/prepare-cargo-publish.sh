#!/usr/bin/env bash
# Prepare the workspace for Cargo publishing by removing unpublished local build dependencies.
set -euo pipefail
cd "$(dirname "$0")/.."

JS_BUILDER_OPTIONAL_PATTERN='activity-js-runtime-builder.*optional\|webhook-js-runtime-builder.*optional\|workflow-js-runtime-builder.*optional'

# Root Cargo.toml: remove optional builder dependencies from [dependencies].
sed -i "/${JS_BUILDER_OPTIONAL_PATTERN}/d" Cargo.toml

# Keep feature names so cfg(feature = "...") remains valid in the published crate.
sed -i 's/"activity-js-local" = \["dep:activity-js-runtime-builder"\]/"activity-js-local" = []/' Cargo.toml
sed -i 's/"webhook-js-local" = \["dep:webhook-js-runtime-builder"\]/"webhook-js-local" = []/' Cargo.toml
sed -i 's/"workflow-js-local" = \["dep:workflow-js-runtime-builder"\]/"workflow-js-local" = []/' Cargo.toml

# The removed dependencies make this CI-only test configuration invalid.
sed -i '/- name: test-js-local.sh/,+2d' .github/workflows/check-test.yml

# Resolve the full workspace to regenerate Cargo.lock after removing the local dependencies.
cargo metadata --format-version 1 > /dev/null
