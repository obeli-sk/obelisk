#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo run -- generate server-config-schema assets/toml/server.json
cargo run -- generate deployment-schema assets/toml/deployment.json
