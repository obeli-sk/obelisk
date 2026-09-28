#!/usr/bin/env bash

# Run activity VM integration tests on the `qemu-kvm` backend.

set -exuo pipefail
cd "$(dirname "$0")/.."

export OBELISK_UNSTABLE_ACTIVITY_VM=qemu-kvm
scripts/test-phase1.sh
scripts/test-phase2.sh -E 'test(/^command::integration_tests::activity_vm::/)' "$@"
