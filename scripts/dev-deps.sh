#!/usr/bin/env bash

# Collect versions of binaries installed by `nix develop` producing file `dev-deps.txt`.
# This script should be executed after every `nix flake update`.

set -exuo pipefail
cd "$(dirname "$0")/.."

rm -f dev-deps.txt
echo "cargo-binstall $(cargo-binstall -V)" >> dev-deps.txt
cargo upgrade --version >> dev-deps.txt
cargo-expand --version >> dev-deps.txt
cargo-insta --version >> dev-deps.txt
cargo nextest --version | head -n 1 >> dev-deps.txt
cargo semver-checks --version >> dev-deps.txt
echo "litecli $(litecli --version)" >> dev-deps.txt
echo $(nixpkgs-fmt --version) >> dev-deps.txt
echo "pkg-config $(pkg-config --version)" >> dev-deps.txt
# protobuf
protoc --version >> dev-deps.txt
rustc --version >> dev-deps.txt
wasm-tools --version >> dev-deps.txt
wasmtime --version >> dev-deps.txt
cargo-zigbuild --version >> dev-deps.txt
# web
nix develop .#web --command wasm-opt --version >> dev-deps.txt # binaryen
echo "trunk $(grep wasm_opt crates/webui/Trunk.toml)" >> dev-deps.txt
nix develop .#web --command trunk --version >> dev-deps.txt
nix develop .#web --command wasm-bindgen --version >> dev-deps.txt
echo "trunk $(grep wasm_bindgen crates/webui/Trunk.toml)" >> dev-deps.txt

# libc
ldd --version | head -n 1 >> dev-deps.txt


