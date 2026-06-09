#!/usr/bin/env bash
# Commit → push → deploy to VPS. Run from repo root after hub/api/deploy changes.
#
# Usage:
#   ./deploy/ship.sh "Short commit message"
#   ./deploy/ship.sh                    # default message
#
# Requires: Tailscale ON (100.111.41.78), SSH key loaded, git remote configured.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MSG="${1:-Update MLB Ops}"

cd "$REPO_ROOT"

echo "=== git status ==="
git status -sb

if git diff --quiet && git diff --cached --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
  echo "No file changes — skip commit."
else
  git add -A
  # Never commit secrets or local runtime DB (gitignore should also block these).
  git reset HEAD -- mlbops/.env .env data/hub.db 2>/dev/null || true
  if git diff --cached --quiet; then
    echo "Nothing staged after excludes — skip commit."
  else
    git commit -m "$(cat <<EOF
${MSG}
EOF
)"
  fi
fi

if git rev-parse --abbrev-ref @{u} >/dev/null 2>&1; then
  echo "=== git push ==="
  git push
else
  echo "No upstream branch — skip push (run: git push -u origin HEAD)."
fi

echo ""
"${REPO_ROOT}/deploy/sync_app_to_vps.sh"
