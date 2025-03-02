#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

# Check for version argument.
if [ -z "$1" ]; then
  echo "Usage: $0 <VERSION>"
  exit 1
fi

VERSION="$1"

# 1. Find the START line number.
START_LINE=$(grep -n "^## \[$VERSION\]" CHANGELOG.md | head -n 1 | cut -d ':' -f 1) || true

# Check if the version was found.
if [ -z "$START_LINE" ]; then
  # Version not found in CHANGELOG.md
  exit
fi
START_LINE=$((START_LINE + 2))

# 2. Find the END line number (the *next* line starting with "## ").
#    Use sed to print from START_LINE + 2 to the end, then grep to find the next "## ".
END_LINE=$(sed -n "$((START_LINE + 2)),\$p" CHANGELOG.md | grep -n -m 1 "^## " | cut -d ':' -f 1)

# 3. Extract the section using sed.
if [ -z "$END_LINE" ]; then
  # If END_LINE is empty, it's the last section.
  sed -n "${START_LINE},\$p" CHANGELOG.md
else
    #Adjust end line to be relative.

  ADJUSTED_END_LINE=$((END_LINE + START_LINE))

  # Print from START_LINE to ADJUSTED_END_LINE-1 (exclusive of the next header).
  sed -n "${START_LINE},${ADJUSTED_END_LINE}p" CHANGELOG.md
fi
