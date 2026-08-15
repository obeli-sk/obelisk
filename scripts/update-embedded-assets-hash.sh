#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."

log=$(mktemp)
trap 'rm -f "$log"' EXIT

if nix build .#embeddedAssets --no-link >"$log" 2>&1; then
    if nix build .#embeddedAssets --no-link --rebuild >"$log" 2>&1; then
        echo "embeddedAssets outputHash is up to date"
        exit 0
    fi
fi

actual=$(sed -n 's/^[[:space:]]*got:[[:space:]]*\(sha256-[A-Za-z0-9+\/=]*\).*$/\1/p' "$log" | tail -n 1)
if [ -z "$actual" ]; then
    cat "$log" >&2
    exit 1
fi

current=$(sed -n 's/^[[:space:]]*outputHash = "\([^"]*\)";$/\1/p' flake.nix)
if [ -z "$current" ]; then
    echo "cannot find embeddedAssets outputHash in flake.nix" >&2
    exit 1
fi

sed -i "s|outputHash = \"$current\";|outputHash = \"$actual\";|" flake.nix
echo "updated embeddedAssets outputHash: $current -> $actual"
nix build .#embeddedAssets --no-link
