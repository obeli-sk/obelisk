#!/usr/bin/env bash

# Rebuild and push all JavaScript runtimes, then update the embedded-assets hash.

set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <tag>" >&2
    exit 1
fi

TAG="$1"

scripts/push-activity-js-runtime.sh "$TAG" &
activity_pid=$!
scripts/push-webhook-js-runtime.sh "$TAG" &
webhook_pid=$!
scripts/push-workflow-js-runtime.sh "$TAG" &
workflow_pid=$!

push_failed=0
for pid in "$activity_pid" "$webhook_pid" "$workflow_pid"; do
    if ! wait "$pid"; then
        push_failed=1
    fi
done

if [ "$push_failed" -ne 0 ]; then
    echo "error: one or more JavaScript runtime pushes failed" >&2
    exit 1
fi

scripts/update-embedded-assets-hash.sh
