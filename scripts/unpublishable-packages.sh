#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

OUTPUT_FILE="assets/unpublishable-packages.txt" > "$OUTPUT_FILE"  # Clear the output file

# Find all Cargo.toml files and check if "publish = false" is set
grep -rl "publish\s*=\s*false" --include="Cargo.toml" crates | while read -r file; do
    # Extract package name
    PACKAGE_NAME=$(grep -m1 '^name\s*=\s*"' "$file" | sed -E 's/name\s*=\s*"([^"]+)"/\1/')
    if [ -n "$PACKAGE_NAME" ]; then
        echo "$PACKAGE_NAME" >> "$OUTPUT_FILE"
    fi
done

echo "Unpublishable packages saved to $OUTPUT_FILE"
