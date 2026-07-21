#!/usr/bin/env bash

# Fail if a published JS runtime pin is older than the sources it is built from.
#
# Each JS runtime is published out-of-band by the manual `push-js-runtime` workflow,
# which records the pushed OCI reference in `assets/<name>-js-runtime-version.txt`.
# Tests run via `scripts/test.sh` and real deployments load that published image, so
# if a runtime's sources change without the pin being refreshed, they silently run an
# outdated runtime (only `scripts/test-js-local.sh` builds the runtime locally).
#
# This check enforces the invariant: the commit that last touched each pin file must
# be at or after every commit that touches the sources that runtime is built from.
# When it fails, republish the runtime (Actions -> push-js-runtime) which updates the
# pin, then rebase/merge.
#
# Note: boa version bumps in the root Cargo.toml/Cargo.lock also change the runtimes
# but are intentionally not tracked here to avoid false positives from unrelated
# dependency churn.

set -euo pipefail
cd "$(dirname "$0")/.."

# Sources embedded in every JS runtime.
COMMON="crates/boa-common"

status=0

check() {
    local name="$1"
    local pin="assets/${name}-js-runtime-version.txt"
    local sources=("crates/${name}-js-runtime" "$COMMON")

    local pin_commit
    pin_commit=$(git rev-list -1 HEAD -- "$pin")
    if [ -z "$pin_commit" ]; then
        echo "::error::${pin} is not tracked or has no commit history"
        status=1
        return
    fi

    local newer
    newer=$(git rev-list "${pin_commit}..HEAD" -- "${sources[@]}")
    if [ -n "$newer" ]; then
        echo "::error::${pin} is stale: sources changed after the pin was last updated. Republish the ${name} JS runtime (Actions -> push-js-runtime) to refresh it."
        echo "Commits touching ${sources[*]} since ${pin_commit}:" >&2
        git --no-pager log --oneline "${pin_commit}..HEAD" -- "${sources[@]}" >&2
        status=1
    else
        echo "${pin} is up to date."
    fi
}

check activity
check workflow
check webhook

exit "$status"
