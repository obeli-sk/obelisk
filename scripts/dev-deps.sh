#!/usr/bin/env bash

# Collect versions of binaries installed by `nix develop` producing file `dev-deps.txt`.
# This script should be executed after every `nix flake update`.

set -exuo pipefail
cd $(dirname "$0")/..

rm -f dev-deps.txt
echo "cargo-binstall $(cargo-binstall -V)" >> dev-deps.txt
cargo-component --version >> dev-deps.txt
cargo dist --version >> dev-deps.txt
cargo upgrade --version >> dev-deps.txt
cargo-expand --version >> dev-deps.txt
# cargo-insta --version >> dev-deps.txt  # Broken as of 1.39-unstable-2024-08-22
cargo-nextest --version >> dev-deps.txt
dive --version >> dev-deps.txt
echo "litecli $(litecli --version)"s >> dev-deps.txt
lldb --version >> dev-deps.txt
nixpkgs-fmt --version   >> dev-deps.txt || true
pkg-config --version >> dev-deps.txt
# protobuf
protoc --version >> dev-deps.txt
# rustToolchain
rustc --version >> dev-deps.txt
tokio-console --version >> dev-deps.txt
wasm-tools --version >> dev-deps.txt

# libc
ldd --version | head -n 1 >> dev-deps.txt
