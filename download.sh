#!/bin/sh

# Downloads the latest binary from GitHub Releases into the current directory.
# Usage:
# curl -L --tlsv1.2 -sSf https://raw.githubusercontent.com/obeli-sk/obelisk/main/download.sh | bash

set -eu

# Set pipefail if it works in a subshell, disregard if unsupported
(set -o pipefail 2> /dev/null) && set -o pipefail

base_url="https://github.com/obeli-sk/obelisk/releases/latest/download/obelisk-"

os="$(uname -s)"
if [ "$os" = "Linux" ]; then
    machine="$(uname -m)"
    case "$machine" in
        x86_64)   target="x86_64-unknown-linux-";  glibc_loader="/lib64/ld-linux-x86-64.so.2" ;;
        aarch64)  target="aarch64-unknown-linux-"; glibc_loader="/lib/ld-linux-aarch64.so.1" ;;
        *)        echo "Unsupported architecture ${machine}" && exit 1 ;;
    esac

    # The gnu binary needs the glibc loader at its FHS path, missing on NixOS, musl distros and some sandboxes.
    if [ -e "$glibc_loader" ]; then
        lib="gnu"
    else
        lib="musl"
        if [ -e /etc/NIXOS ] || grep -qs "NixOS" /etc/issue /etc/os-release; then
            printf "Downloading musl-based binary on NixOS. Consider installing with\nnix profile install github:obeli-sk/obelisk/latest\n"
        fi
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
echo "Downloading and extracting $url"
curl -L --proto '=https' --tlsv1.2 -sSf "$url" | tar -xvzf -
