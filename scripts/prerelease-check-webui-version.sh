#!/usr/bin/env bash
# Verify that HEAD pins the latest obeli-sk/webui commit, whose image tag is its short SHA.

set -euo pipefail
cd "$(dirname "$0")/.."

WEBUI_REPO_URL="${WEBUI_REPO_URL:-https://github.com/obeli-sk/webui.git}"
WEBUI_BRANCH="${WEBUI_BRANCH:-main}"
VERSION_FILE=crates/embedded-assets/webui-version.txt

HEAD_SHA="$(git ls-remote "$WEBUI_REPO_URL" "refs/heads/$WEBUI_BRANCH" | awk '{ print $1 }')"
if [ -z "$HEAD_SHA" ]; then
    echo "error: branch $WEBUI_BRANCH not found in $WEBUI_REPO_URL" >&2
    exit 2
fi
echo "latest webui commit: $HEAD_SHA"

PINNED_TAG="$(sed -nE 's|^oci://[^:]+:([0-9a-f]+)@sha256:[0-9a-f]+$|\1|p' "$VERSION_FILE")"
if [ -z "$PINNED_TAG" ]; then
    echo "error: $VERSION_FILE does not reference a commit-tagged webui image" >&2
    exit 1
fi
if [[ "$HEAD_SHA" != "$PINNED_TAG"* ]]; then
    echo "error: $VERSION_FILE pins webui $PINNED_TAG, latest is $HEAD_SHA" >&2
    exit 1
fi

EXPECTED="chore: bump webui to $PINNED_TAG"
if ! git log --format=%s HEAD | grep -xF -- "$EXPECTED" >/dev/null; then
    echo "error: no commit in HEAD's history has subject '$EXPECTED'" >&2
    exit 1
fi
echo "ok: found commit '$EXPECTED'"
