#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <folder> <version-txt-file>" >&2
    exit 2
fi

FOLDER="$1"
VERSION_FILE="$2"

if [ ! -d "$FOLDER" ]; then
    echo "error: folder does not exist: $FOLDER" >&2
    exit 2
fi

if [ ! -f "$VERSION_FILE" ]; then
    echo "error: version file does not exist: $VERSION_FILE" >&2
    exit 2
fi

FOLDER_COMMIT="$(git rev-list -1 HEAD -- "$FOLDER")"
VERSION_COMMIT="$(git rev-list -1 HEAD -- "$VERSION_FILE")"

if [ -z "$FOLDER_COMMIT" ]; then
    echo "error: no commits found for folder: $FOLDER" >&2
    exit 2
fi

if [ -z "$VERSION_COMMIT" ]; then
    echo "error: no commits found for version file: $VERSION_FILE" >&2
    exit 2
fi

if git merge-base --is-ancestor "$FOLDER_COMMIT" "$VERSION_COMMIT"; then
    echo "ok: $VERSION_FILE was updated in or after the latest change to $FOLDER"
    echo "folder commit:  $FOLDER_COMMIT"
    echo "version commit: $VERSION_COMMIT"
    exit 0
fi

echo "error: $VERSION_FILE was not updated after the latest change to $FOLDER" >&2
echo "latest folder commit:  $FOLDER_COMMIT $(git log -1 --pretty=%s "$FOLDER_COMMIT")" >&2
echo "latest version commit: $VERSION_COMMIT $(git log -1 --pretty=%s "$VERSION_COMMIT")" >&2
exit 1
