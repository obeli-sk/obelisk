#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

mkdir -p assets/schemas
cargo run -- generate cli-schema assets/schemas/cli.json
