#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo check --config 'build.warnings="deny"' --workspace --all-targets "$@"
