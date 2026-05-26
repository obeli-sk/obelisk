#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p assets/schemas
cargo run -- generate open-api-schema ./assets/schemas/openapi.json
