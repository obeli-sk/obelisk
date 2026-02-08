#!/usr/bin/env bash

# Find all workspace member packages with "publish = false" in their Cargo.toml files and save them to a file.

set -exuo pipefail
cd "$(dirname "$0")/.."

OUTPUT_FILE="assets/unpublishable-packages.txt"

# Get workspace member Cargo.toml paths and check for "publish = false"
PACKAGES=()
while IFS= read -r file; do
    if grep -qE "publish\s*=\s*false" "$file"; then
        PACKAGE_NAME=$(grep -m1 '^name\s*=\s*"' "$file" | sed -E 's/name\s*=\s*"([^"]+)"/\1/')
        if [ -n "$PACKAGE_NAME" ]; then
            PACKAGES+=("$PACKAGE_NAME")
        fi
    fi
done < <(cargo metadata --no-deps --format-version 1 | jq -r '.packages[].manifest_path')

# Sort and write to file
printf "%s\n" "${PACKAGES[@]}" | sort > "$OUTPUT_FILE"

echo "Unpublishable packages saved to $OUTPUT_FILE"
