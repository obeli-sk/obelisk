#!/usr/bin/env bash
set -xe
cd "$(dirname "$0")"
cargo component build --release --target wasm32-unknown-unknown -p wasm-email-provider
cargo component build --release --target wasm32-unknown-unknown -p hello-world
