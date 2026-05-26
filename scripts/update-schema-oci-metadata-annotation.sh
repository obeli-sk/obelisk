#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

mkdir -p assets/schemas
cargo run -- generate component-metadata-annotation-schema assets/schemas/oci-metadata-annotation.json
