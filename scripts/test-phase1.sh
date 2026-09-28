#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

export RUST_BACKTRACE=1
export RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}"

if [[ -z "${OBELISK_UNSTABLE_ACTIVITY_VM:-}" ]]; then
  unset OBELISK_UNSTABLE_ACTIVITY_VM
fi

if [[ "${OBELISK_UNSTABLE_ACTIVITY_VM:-}" == qemu-tcg || "${OBELISK_UNSTABLE_ACTIVITY_VM:-}" == qemu-kvm ]]; then
  cargo nextest run --no-output-indent --workspace --profile ci-test-populate-codegen-cache \
    populate_activity_vm_qemu_cache ${ADDITIONAL_FEATURES:-}
elif [[ "${OBELISK_UNSTABLE_ACTIVITY_VM:-}" == bochs-wasm ]]; then
  cargo nextest run --no-output-indent --workspace --profile ci-test-populate-codegen-cache \
    populate_activity_vm_codegen_cache ${ADDITIONAL_FEATURES:-}
else
  cargo nextest run --no-output-indent --workspace --profile ci-test-populate-codegen-cache \
    populate_codegen_cache populate_js_codegen_cache \
    ${ADDITIONAL_FEATURES:-}
fi
