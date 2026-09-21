#!/usr/bin/env bash
# Applies branch protection for `main` and enables repository auto-merge, so
# these rules live in git instead of only existing as manual GitHub UI clicks
# that can drift silently.
#
# Required status checks are derived from every .github/workflows/*.yml file
# that triggers on `pull_request`, rather than hardcoded here — add/rename/
# remove a job in one of those workflows and the required checks follow
# automatically. Matrix jobs are expanded (e.g. `strategy.matrix.backend:
# [rs, js]` turns one job into one context per matrix value).
#
# Usage: ./scripts/sync-branch-protection.sh [branch]
# Requires: gh (authenticated with repo admin rights), yq, jq
#   (both available via `nix develop`)

set -euo pipefail
cd "$(dirname "$0")/.."

BRANCH="${1:-main}"

# Expands a workflow's `.jobs` into one required-check name per job, per
# matrix combination, substituting `${{ matrix.<key> }}` in job names.
JOB_NAMES_JQ='
def cartesian(m):
  (m | to_entries) as $entries
  | if ($entries | length) == 0 then [{}]
    else
      reduce $entries[] as $e ([{}];
        [ .[] as $acc | $e.value[] | ($acc + {($e.key): .}) ]
      )
    end;

to_entries[] | . as $job
| ($job.value.name // $job.key) as $tmpl
| ($job.value.strategy.matrix // {} | with_entries(select(.key != "include" and .key != "exclude"))) as $matrix
| cartesian($matrix)[] as $combo
| reduce ($combo | to_entries[]) as $e ($tmpl;
    gsub("\\$\\{\\{\\s*matrix\\." + $e.key + "\\s*\\}\\}"; ($e.value | tostring))
  )
'

derive_required_checks() {
    local wf triggers_on_pr
    for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
        [ -f "$wf" ] || continue

        triggers_on_pr="$(yq -o=json '.on' "$wf" | jq '
            if type == "array" then any(. == "pull_request")
            elif type == "object" then has("pull_request")
            else . == "pull_request"
            end
        ')"
        [ "$triggers_on_pr" = "true" ] || continue

        yq -o=json '.jobs' "$wf" | jq -r "$JOB_NAMES_JQ"
    done
}

REQUIRED_CHECKS="$(derive_required_checks)"
if [ -z "$REQUIRED_CHECKS" ]; then
    echo "!!! No workflow jobs found that trigger on pull_request under .github/workflows/" >&2
    exit 1
fi

echo ">>> Required status checks derived from .github/workflows/*.yml:"
echo "$REQUIRED_CHECKS" | sed 's/^/    - /'

CONTEXTS_JSON="$(echo "$REQUIRED_CHECKS" | jq -R . | jq -s .)"

PAYLOAD="$(jq -n --argjson contexts "$CONTEXTS_JSON" '{
    required_status_checks: {
        strict: true,
        contexts: $contexts
    },
    enforce_admins: false,
    required_pull_request_reviews: null,
    restrictions: null,
    required_linear_history: false,
    allow_force_pushes: false,
    allow_deletions: false,
    required_conversation_resolution: true
}')"

if [ "${DRY_RUN:-}" = "1" ]; then
    echo "$PAYLOAD"
    exit 0
fi

REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
echo ">>> Applying branch protection to $REPO@$BRANCH"
echo "$PAYLOAD" | gh api \
    --method PUT \
    -H "Accept: application/vnd.github+json" \
    "repos/$REPO/branches/$BRANCH/protection" \
    --input - \
    > /dev/null

# Auto-merge is a repository setting, not part of branch protection: enable it so
# a PR can be queued to merge automatically once the required checks above pass.
echo ">>> Enabling auto-merge on $REPO"
gh api \
    --method PATCH \
    -H "Accept: application/vnd.github+json" \
    "repos/$REPO" \
    -F allow_auto_merge=true \
    > /dev/null

echo ">>> Done. Current protection:"
gh api "repos/$REPO/branches/$BRANCH/protection" | jq .
