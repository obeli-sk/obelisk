#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

mkdir -p assets/schemas/toml
cargo run -- generate server-config-schema assets/schemas/toml/server.json
cargo run -- generate deployment-schema assets/schemas/toml/deployment.json
mkdir -p assets/schemas
cargo run -- generate deployment-canonical-schema assets/schemas/deployment-canonical.json
