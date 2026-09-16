#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

export RUST_BACKTRACE=1
export RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}"
export NEXTEST_NO_OUTPUT_INDENT=1

activity_vm_module=""
if [[ -n "${OBELISK_ACTIVITY_VM_RUNTIME_MODULE:-}" ]]; then
  activity_vm_module="$OBELISK_ACTIVITY_VM_RUNTIME_MODULE"
elif [[ -n "${OBELISK_ACTIVITY_VM_RUNTIME_DIR:-}" ]]; then
  activity_vm_module="$OBELISK_ACTIVITY_VM_RUNTIME_DIR/qemu-system-x86_64.wasm"
fi
if [[ -n "$activity_vm_module" ]]; then
  phase1_digest_file="test-codegen-cache/activity-vm-phase1.sha256"
  test -f "$activity_vm_module"
  test -f "$phase1_digest_file"
  test "$(sha256sum "$activity_vm_module" | cut -d ' ' -f 1)" = "$(<"$phase1_digest_file")"
fi

args=("$@")

has_double_dash=false
for arg in "${args[@]}"; do
  if [[ "$arg" == "--" ]]; then
    has_double_dash=true
    break
  fi
done

if [ "${CARGO_INSTA_TEST:-}" = "true" ]; then
  cmd=(
    cargo insta test --test-runner nextest --check --disable-nextest-doctest
    --workspace
    --nextest-profile ci-test
  )
else
  cmd=(
    cargo nextest run
    --workspace
    --profile ci-test
  )
fi

# Additional features are inserted before `--`
if [[ -n "${ADDITIONAL_FEATURES:-}" ]]; then
  cmd+=(${ADDITIONAL_FEATURES})
fi

cmd+=("${args[@]}")

if ! $has_double_dash; then
  cmd+=(--)
fi

cmd+=(--skip populate_codegen_cache --skip populate_js_codegen_cache)

"${cmd[@]}"
