#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo run -- generate config-schema toml/schema.json
