#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."

cargo build

./target/debug/obelisk generate open-api-schema ./assets/openapi.json
