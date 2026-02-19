#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

scripts/test-phase1.sh
scripts/test-phase2.sh "$@"
