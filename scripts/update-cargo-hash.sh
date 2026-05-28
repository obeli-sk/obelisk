#!/usr/bin/env bash

set -euo pipefail

flake_file="flake.nix"
log_file="${TMPDIR:-/tmp}/cargo-hash.log"
tmp_file="${TMPDIR:-/tmp}/flake.nix.tmp.$$"

if ! grep -q 'cargoHash = ' "$flake_file"; then
  echo "cargoHash assignment not found in $flake_file" >&2
  exit 1
fi

replace_cargo_hash() {
  local replacement="$1"
  awk -v replacement="$replacement" '
    !done && /cargoHash = / {
      print replacement
      done = 1
      next
    }
    { print }
  ' "$flake_file" >"$tmp_file"
  mv "$tmp_file" "$flake_file"
}

replace_cargo_hash '                cargoHash = pkgs.lib.fakeHash;'

set +e
nix build -L .#obeliskLibcNixDev >"$log_file" 2>&1
status=$?
set -e

if [ "$status" -eq 0 ]; then
  echo "Expected fake cargoHash build to fail with a hash mismatch." >&2
  exit 1
fi

computed_hash="$(
  sed -n 's/.*got:[[:space:]]*\(sha256-[A-Za-z0-9+/=]*\).*/\1/p' "$log_file" | tail -n1
)"

if [ -z "$computed_hash" ]; then
  cat "$log_file"
  echo "Could not extract computed cargoHash from nix build output." >&2
  exit 1
fi

replace_cargo_hash "                cargoHash = \"$computed_hash\";"
