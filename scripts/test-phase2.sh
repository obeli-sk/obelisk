#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

export RUST_BACKTRACE=1
export RUST_LOG="${RUST_LOG:-info,obeli=debug,app=trace}"

args=("$@")

has_double_dash=false
for arg in "${args[@]}"; do
  if [[ "$arg" == "--" ]]; then
    has_double_dash=true
    break
  fi
done

cmd=(
  cargo nextest run
  --no-output-indent
  --workspace
  --profile ci-test
)

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
