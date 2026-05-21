#!/usr/bin/env bash

set -euo pipefail

STORE_PATH="$(nix build --refresh --no-link --print-out-paths github:obeli-sk/obelisk/latest | tail -n 1)"
BIN_DIR="${STORE_PATH}/bin"

if [ ! -x "${BIN_DIR}/obelisk" ]; then
    echo "error: obelisk binary not found in ${BIN_DIR}" >&2
    exit 1
fi

echo "${BIN_DIR}"
