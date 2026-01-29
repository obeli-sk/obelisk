#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo check --workspace --all-targets "$@"
