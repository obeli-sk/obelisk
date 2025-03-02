#!/usr/bin/env bash

# Find all packages with "publish = false" in their Cargo.toml files and save them to a file.

set -exuo pipefail
cd "$(dirname "$0")/.."

OUTPUT_FILE="assets/unpublishable-packages.txt"

# Find all Cargo.toml files and check if "publish = false" is set
PACKAGES=()
while IFS= read -r file; do
    PACKAGE_NAME=$(grep -m1 '^name\s*=\s*"' "$file" | sed -E 's/name\s*=\s*"([^"]+)"/\1/')
    if [ -n "$PACKAGE_NAME" ]; then
        PACKAGES+=("$PACKAGE_NAME")
    fi
done < <(grep -rl "publish\s*=\s*false" --include="Cargo.toml" crates)

# Sort and write to file
printf "%s\n" "${PACKAGES[@]}" | sort > "$OUTPUT_FILE"

echo "Unpublishable packages saved to $OUTPUT_FILE"
