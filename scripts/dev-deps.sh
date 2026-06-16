#!/usr/bin/env bash

# Collect versions of binaries installed by `nix develop` producing file `dev-deps.txt`.
# This script should be executed after every `nix flake update`.

set -exuo pipefail
cd "$(dirname "$0")/.."


get_litestream_version() {
  WORKSPACE_DIR="$(pwd -P)"
  TMP_DIR=$(mktemp -d)
  (
    cd "$TMP_DIR" || exit 1
    touch obelisk obelisk.toml server.toml

    docker build -f "$WORKSPACE_DIR/.github/workflows/release/docker-image/ubuntu-24.04-litestream.Dockerfile" . --tag temp >/dev/null
    docker run --rm --entrypoint litestream temp version
    docker rmi temp >/dev/null
  )
  rm -rf "$TMP_DIR"
}

rm -f dev-deps.txt
cargo upgrade --version >> dev-deps.txt
cargo-insta --version >> dev-deps.txt
cargo nextest --version | head -n 1 >> dev-deps.txt
echo "litecli $(litecli --version)" >> dev-deps.txt
echo $(nixpkgs-fmt --version) >> dev-deps.txt
echo "pkg-config $(pkg-config --version)" >> dev-deps.txt
# protobuf
protoc --version >> dev-deps.txt
rustc --version >> dev-deps.txt
cargo metadata --no-deps --format-version 1 \
    | jq -r '.workspace_root as $root | .packages[] | select(.manifest_path == ($root + "/Cargo.toml")) | "rust-version " + .rust_version' \
    >> dev-deps.txt
wasm-tools --version >> dev-deps.txt
nix develop .#cargo-zigbuild --command cargo-zigbuild --version >> dev-deps.txt

# docker deps
echo "litestream $(get_litestream_version)" >> dev-deps.txt

# libc
ldd --version | head -n 1 >> dev-deps.txt

