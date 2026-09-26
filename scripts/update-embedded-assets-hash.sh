#!/usr/bin/env bash

# Update the `outputHash` of the embedded asset derivations in flake.nix.
# Usage: update-embedded-assets-hash.sh [webui|js-runtimes]...  (default: both)

set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
    set -- webui js-runtimes
fi

log=$(mktemp)
trap 'rm -f "$log"' EXIT

for group in "$@"; do
    case "$group" in
    webui)
        attr=embeddedWebui
        version_files=(crates/embedded-assets/webui-version.txt)
        ;;
    js-runtimes)
        attr=embeddedJsRuntimes
        version_files=(
            crates/embedded-assets/activity-js-runtime-version.txt
            crates/embedded-assets/workflow-js-runtime-version.txt
            crates/embedded-assets/webhook-js-runtime-version.txt
        )
        ;;
    *)
        echo "unknown asset group: $group (expected webui or js-runtimes)" >&2
        exit 1
        ;;
    esac

    for version_file in "${version_files[@]}"; do
        if [ -z "$(tail -c 1 "$version_file")" ]; then
            echo "$version_file must not end with a newline" >&2
            exit 1
        fi
    done

    if nix build ".#$attr" --no-link >"$log" 2>&1 &&
        nix build ".#$attr" --no-link --rebuild >"$log" 2>&1; then
        echo "$attr outputHash is up to date"
    else
        actual=$(sed -n 's/^[[:space:]]*got:[[:space:]]*\(sha256-[A-Za-z0-9+\/=]*\).*$/\1/p' "$log" | tail -n 1)
        if [ -z "$actual" ]; then
            cat "$log" >&2
            exit 1
        fi
        range="/^[[:space:]]*$attr = fetchOciAssets {/,/^[[:space:]]*};/"
        current=$(sed -n "${range}s/^[[:space:]]*outputHash = \"\([^\"]*\)\";$/\1/p" flake.nix)
        if [ -z "$current" ]; then
            echo "cannot find $attr outputHash in flake.nix" >&2
            exit 1
        fi
        sed -i "${range}s|outputHash = \"$current\";|outputHash = \"$actual\";|" flake.nix
        echo "updated $attr outputHash: $current -> $actual"
        nix build ".#$attr" --no-link
    fi
done
