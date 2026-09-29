#!/usr/bin/env bash
# Verify that the latest obeli-sk/webui commit is tagged and that HEAD contains `chore: bump webui to <tag>`.

set -euo pipefail
cd "$(dirname "$0")/.."

WEBUI_REPO_URL="${WEBUI_REPO_URL:-https://github.com/obeli-sk/webui.git}"
WEBUI_BRANCH="${WEBUI_BRANCH:-main}"

REMOTE_REFS="$(git ls-remote "$WEBUI_REPO_URL" "refs/heads/$WEBUI_BRANCH" 'refs/tags/*')"
HEAD_SHA="$(awk -v ref="refs/heads/$WEBUI_BRANCH" '$2 == ref { print $1 }' <<<"$REMOTE_REFS")"
if [ -z "$HEAD_SHA" ]; then
    echo "error: branch $WEBUI_BRANCH not found in $WEBUI_REPO_URL" >&2
    exit 2
fi
echo "latest webui commit: $HEAD_SHA"

# Lightweight tags point at the commit directly, annotated tags through their peeled `^{}` entry.
mapfile -t TAGS < <(awk -v sha="$HEAD_SHA" '$1 == sha && $2 ~ /^refs\/tags\// {
    tag = substr($2, 11); sub(/\^\{\}$/, "", tag); print tag }' <<<"$REMOTE_REFS" | sort -u)
if [ "${#TAGS[@]}" -eq 0 ]; then
    echo "error: latest webui commit $HEAD_SHA has no tag" >&2
    exit 1
fi

for TAG in "${TAGS[@]}"; do
    EXPECTED="chore: bump webui to $TAG"
    if git log --format=%s HEAD | grep -xF -- "$EXPECTED" >/dev/null; then
        echo "ok: found commit '$EXPECTED'"
        exit 0
    fi
done

echo "error: no commit in HEAD's history has subject 'chore: bump webui to <tag>' for tags: ${TAGS[*]}" >&2
exit 1
