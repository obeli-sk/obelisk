#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."

cargo run -- generate open-api-schema ./assets/openapi.json
