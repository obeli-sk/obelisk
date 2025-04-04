#!/usr/bin/env bash

# Called by the release CI to extract the changelog section for a given version.
# Usage: ./scripts/extract-release-info.sh <VERSION>

set -exuo pipefail
cd "$(dirname "$0")/.."

# Check for version argument.
if [ -z "$1" ]; then
  echo "Usage: $0 <VERSION>"
  exit 1
fi

VERSION="$1"

START_LINE=$(awk -v version="$VERSION" '$0 ~ "^## \\[" version "\\]" {print NR}' CHANGELOG.md)

# END_LINE=$(tail -n +$((START_LINE + 1)) CHANGELOG.md | awk '/^## /{print NR; exit}')
END_LINE=$(awk -v start="$((START_LINE + 1))" 'NR >= start && /^## / {print NR; exit}' CHANGELOG.md)

# tail -n +$((START_LINE + 2)) CHANGELOG.md | head -n $((END_LINE - 2))
sed -n "$((START_LINE + 1)),$((END_LINE - 1))p" CHANGELOG.md
