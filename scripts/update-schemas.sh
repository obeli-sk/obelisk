#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

mkdir -p assets/schemas/toml

cargo test --bin obelisk command::generate::tests::update_ -- --ignored
