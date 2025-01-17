#!/bin/sh

# Downloads the latest binary from GitHub Releases into the current directory.
# Usage:
# curl -L --tlsv1.2 -sSf https://raw.githubusercontent.com/obeli-sk/obelisk/main/install.sh | bash

set -eux

# Set pipefail if it works in a subshell, disregard if unsupported
(set -o pipefail 2> /dev/null) && set -o pipefail

base_url="https://github.com/obeli-sk/obelisk/releases/latest/download/obelisk-"

os="$(uname -s)"
if [ "$os" = "Linux" ]; then
    machine="$(uname -m)"
    if [ "$machine" != "x86_64" ]; then
        echo "Unsupported architecture ${machine}"
        exit 1
    fi
    target="${machine}-unknown-linux-"
    # Check for musl or glibc
    ldd_version=$(ldd --version 2>&1 || true)
    issue=$(cat /etc/issue 2>/dev/null || true)
    if echo "$ldd_version" | grep -q "musl" || echo "$issue" | grep -q "NixOS"; then
        lib="musl"
    else
        lib="gnu"
    fi
    url="${base_url}${target}${lib}.tar.gz"

elif [ "$os" = "Darwin" ]; then
    machine="$(uname -m)"
    case "$machine" in
        x86_64)   target="x86_64-apple-darwin" ;;
        arm64)    target="aarch64-apple-darwin" ;;
        *)        echo "Unsupported architecture ${machine}" && exit 1 ;;
    esac

    url="${base_url}${target}.tar.gz"

else
    echo "Unsupported OS ${os}"
    exit 1
fi

# Download and extract the tarball
curl -L --proto '=https' --tlsv1.2 -sSf "$url" | tar -xvzf -
