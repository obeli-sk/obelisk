#!/bin/sh

# Downloads a binary from GitHub Releases into the current directory.
# Usage (latest release):
# curl -L --tlsv1.2 -sSf https://raw.githubusercontent.com/obeli-sk/obelisk/main/download.sh | bash
# Usage (specific release tag, passed as an argument or via OBELISK_VERSION):
# curl -L --tlsv1.2 -sSf https://raw.githubusercontent.com/obeli-sk/obelisk/main/download.sh | bash -s -- v0.42.0-rc.8

set -eu

# Set pipefail if it works in a subshell, disregard if unsupported
(set -o pipefail 2> /dev/null) && set -o pipefail

version="${1:-${OBELISK_VERSION:-latest}}"
if [ "$version" = "latest" ]; then
    base_url="https://github.com/obeli-sk/obelisk/releases/latest/download/obelisk-"
else
    case "$version" in
        v*) ;;
        *)  version="v${version}" ;;
    esac
    base_url="https://github.com/obeli-sk/obelisk/releases/download/${version}/obelisk-"
fi

os="$(uname -s)"
if [ "$os" = "Linux" ]; then
    machine="$(uname -m)"
    case "$machine" in
        x86_64)   target="x86_64-unknown-linux-";  glibc_loader="/lib64/ld-linux-x86-64.so.2" ;;
        aarch64)  target="aarch64-unknown-linux-"; glibc_loader="/lib/ld-linux-aarch64.so.1" ;;
        *)        echo "Unsupported architecture ${machine}" && exit 1 ;;
    esac

    # NixOS may place a stub at the glibc loader path, so it is checked before the loader.
    if [ -e /etc/NIXOS ] || grep -qs "NixOS" /etc/issue /etc/os-release; then
        lib="musl"
        printf "Downloading musl-based binary on NixOS. Consider installing with\nnix profile install github:obeli-sk/obelisk/latest\n"
    elif [ -e "$glibc_loader" ]; then
        lib="gnu"
    else
        # Musl distros and sandboxes without FHS paths.
        lib="musl"
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
