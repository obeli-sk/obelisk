#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-}" in
    ""|--check) ;;
    *) echo "Usage: $0 [--check]" >&2; exit 2 ;;
esac
host_target=$(rustc -vV | sed -n 's/^host: //p')
work=$(mktemp -d)
output="$work/generated.rs"
trap 'rm -rf "$work"' EXIT
cargo run --locked --quiet --manifest-path tools/obelisk-api-codegen/Cargo.toml \
    --target "$host_target" -- "$output"
rustfmt --edition 2024 "$output"
if [[ "${1:-}" == --check ]]; then
    diff -u crates/api-types/src/generated.rs "$output"
else
    cp "$output" crates/api-types/src/generated.rs
fi

npm ci --prefix tools/obelisk-api-codegen-js --ignore-scripts --no-audit --no-fund
node tools/obelisk-api-codegen-js/generate.mjs "$work"
for file in index.js index.d.ts; do
    if [[ "${1:-}" == --check ]]; then
        diff -u "clients/javascript/$file" "$work/$file"
    else
        cp "$work/$file" "clients/javascript/$file"
    fi
done
