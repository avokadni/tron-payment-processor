#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${GH_ADMIN_TOKEN:-}" ]]; then
  echo "GH_ADMIN_TOKEN is required."
  echo "Create a Personal Access Token with repository admin rights and add it as repository secret GH_ADMIN_TOKEN."
  exit 1
fi

if [[ -z "${GITHUB_REPOSITORY:-}" ]]; then
  echo "GITHUB_REPOSITORY is required (format: owner/repo)."
  exit 1
fi

BRANCH_NAME="${BRANCH_NAME:-main}"
API_URL="https://api.github.com/repos/${GITHUB_REPOSITORY}/branches/${BRANCH_NAME}/protection"

read -r -d '' PAYLOAD <<'JSON' || true
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "lint",
      "typecheck",
      "tests",
      "security"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "require_last_push_approval": true
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": true,
  "lock_branch": false,
  "allow_fork_syncing": true
}
JSON

curl \
  --fail \
  --show-error \
  --silent \
  --request PUT \
  --url "${API_URL}" \
  --header "Accept: application/vnd.github+json" \
  --header "Authorization: Bearer ${GH_ADMIN_TOKEN}" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  --data "${PAYLOAD}" >/dev/null

echo "Branch protection updated: ${GITHUB_REPOSITORY} (${BRANCH_NAME})"
