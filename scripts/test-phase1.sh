#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

export RUST_BACKTRACE=1
export RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}"

activity_vm_module=""
if [[ -n "${OBELISK_ACTIVITY_VM_RUNTIME_MODULE:-}" ]]; then
  activity_vm_module="$OBELISK_ACTIVITY_VM_RUNTIME_MODULE"
elif [[ -n "${OBELISK_ACTIVITY_VM_RUNTIME_DIR:-}" ]]; then
  activity_vm_module="$OBELISK_ACTIVITY_VM_RUNTIME_DIR/qemu-system-x86_64.wasm"
fi
if [[ -n "$activity_vm_module" ]]; then
  test -f "$activity_vm_module"
  export OBELISK_ACTIVITY_VM_EXPECTED_MODULE_SHA256
  OBELISK_ACTIVITY_VM_EXPECTED_MODULE_SHA256="$(sha256sum "$activity_vm_module" | cut -d ' ' -f 1)"
fi

cargo nextest run --no-output-indent --workspace --profile ci-test-populate-codegen-cache \
  populate_codegen_cache populate_js_codegen_cache populate_activity_vm_codegen_cache \
  ${ADDITIONAL_FEATURES:-}

if [[ -n "$activity_vm_module" ]]; then
  mkdir -p test-codegen-cache
  printf '%s\n' "$OBELISK_ACTIVITY_VM_EXPECTED_MODULE_SHA256" \
    > test-codegen-cache/activity-vm-phase1.sha256
fi
