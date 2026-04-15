#!/usr/bin/env bash
# Copies and pre-processes website docs into obelisk/ai-prompt/ for embedding in the binary.
# Run after bumping the website docs version or editing any of the included pages.
# Requires: awk, sed
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OBELISK_ROOT="$SCRIPT_DIR/.."
WEBSITE_DOCS="$OBELISK_ROOT/../website/content/docs/latest"
DEST="$OBELISK_ROOT/assets/ai-prompt"

if [ ! -d "$WEBSITE_DOCS" ]; then
    echo "Error: website docs not found at $WEBSITE_DOCS" >&2
    exit 1
fi

mkdir -p "$DEST"

# Strip Zola frontmatter (+++...+++ block at start) and replace shortcodes with their title text.
process() {
    local src="$1" dst="$2"
    awk '
        NR==1 && /^\+\+\+$/ { in_fm=1; next }
        in_fm && /^\+\+\+$/ { in_fm=0; next }
        in_fm { next }
        { print }
    ' "$src" |
    sed -E 's/\{\{ [a-z_]+\(title="([^"]*)"[^)]*\) \}\}/\1/g' \
    > "$dst"
}

FILES=(
    "concepts/wit-reference.md"
    "js/js-components.md"
    "js/js-activities.md"
    "js/js-workflows.md"
    "js/js-webhooks.md"
    "js/js-patterns.md"
)

echo "Updating ai-prompt/ from $(realpath "$WEBSITE_DOCS")"
for f in "${FILES[@]}"; do
    dest="$DEST/$(basename "$f")"
    process "$WEBSITE_DOCS/$f" "$dest"
    echo "  $f -> $dest"
done

curl --fail "https://raw.githubusercontent.com/obeli-sk/components/main/README.md" -o $DEST/components-repo.md

echo "Done. Run 'cargo build' to embed the updated content."
