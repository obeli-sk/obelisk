#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo run -- generate db-schema assets/db/schema.json
