#!/usr/bin/env bash

# Run activity VM integration tests on the `firecracker` backend.
# OBELISK_FIRECRACKER_BUNDLE may point to a locally built activity-vm-firecracker-runtime.

set -exuo pipefail
cd "$(dirname "$0")/.."

export OBELISK_UNSTABLE_ACTIVITY_VM=firecracker
scripts/test-phase1.sh
scripts/test-phase2.sh -E 'test(/^command::integration_tests::activity_vm::/)' "$@"
