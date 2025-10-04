#!/usr/bin/env bash

# Called by the release CI job to extract the changelog section for a given version.
# Usage: ./scripts/extract-release-info.sh <VERSION>

set -exuo pipefail
cd "$(dirname "$0")/.."

if [ -z "$1" ]; then
  echo "Usage: $0 <VERSION>"
  exit 1
fi

VERSION="$1"

START_LINE=$(awk -v version="$VERSION" '$0 ~ "^## \\[" version "\\]" {print NR}' CHANGELOG.md)

END_LINE=$(awk -v start="$((START_LINE + 1))" 'NR >= start && /^## / {print NR; exit}' CHANGELOG.md)

sed -n "$((START_LINE + 1)),$((END_LINE - 1))p" CHANGELOG.md
