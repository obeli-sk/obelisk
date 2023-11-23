#!/usr/bin/env bash
set -xe
cd "$(dirname "$0")"
(
  cd crates/activities/wasm-email-provider
  cargo component build --release --target wasm32-unknown-unknown
)
(
  cd crates/workflows/hello-world
  cargo component build --release --target wasm32-unknown-unknown
)
