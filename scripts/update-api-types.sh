#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-}" in
    ""|--check) ;;
    *) echo "Usage: $0 [--check]" >&2; exit 2 ;;
esac
host_target=$(rustc -vV | sed -n 's/^host: //p')
output=$(mktemp --suffix=.rs)
trap 'rm -f "$output"' EXIT
cargo run --locked --quiet --manifest-path tools/obelisk-api-codegen/Cargo.toml \
    --target "$host_target" -- "$output"
rustfmt --edition 2024 "$output"
if [[ "${1:-}" == --check ]]; then
    diff -u crates/rest-types/src/lifecycle_generated.rs "$output"
else
    cp "$output" crates/rest-types/src/lifecycle_generated.rs
fi
