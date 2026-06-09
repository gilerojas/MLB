#!/usr/bin/env bash
# Print warehouse file counts. Run on the VPS directly, or from the Mac via SSH.
#
# On VPS:
#   bash /srv/mlbops/app/deploy/vps_verify_warehouse.sh 2026
#
# From Mac:
#   ./deploy/vps_verify_warehouse.sh --remote 2026

set -euo pipefail

REMOTE=0
SEASON="${1:-2026}"
WAREHOUSE="${MLBOPS_VPS_WAREHOUSE:-/srv/mlbops/warehouse/mlb}"

if [[ "${1:-}" == "--remote" ]]; then
  REMOTE=1
  SEASON="${2:-2026}"
fi

report() {
  local base="$1"
  local path="${base%/}/${SEASON}"
  echo "=== warehouse ${SEASON} @ ${path} ==="
  if [[ ! -d "$path" ]]; then
    echo "MISSING: $path"
    return 1
  fi
  du -sh "$path"
  echo "RAW:     $(find "$path" -path '*/raw/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
  echo "PITCHES: $(find "$path" -path '*/pitches_enriched/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
  echo "FILES:   $(find "$path" -type f 2>/dev/null | wc -l | tr -d ' ')"
  for f in players_registry.json schedule_post.csv schedule_regular_season.json; do
    if [[ -f "${path}/${f}" ]]; then
      echo "OK ${f}"
    else
      echo "MISSING ${f}"
    fi
  done
}

if [[ "$REMOTE" -eq 1 ]]; then
  REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
  VPS_HOST="${MLBOPS_VPS_HOST:-2.24.123.57}"
  VPS_PORT="${MLBOPS_VPS_SSH_PORT:-2222}"
  VPS_USER="${MLBOPS_VPS_SSH_USER:-root}"
  VPS_KEY="${MLBOPS_VPS_SSH_KEY:-/Users/gilrojasb/Desktop/Hermes/id_ed25519}"
  ssh -i "$VPS_KEY" -p "$VPS_PORT" -o IdentitiesOnly=yes \
    "${VPS_USER}@${VPS_HOST}" \
    "MLBOPS_VPS_WAREHOUSE='${WAREHOUSE}' bash -s -- '${SEASON}'" \
    < "$REPO_ROOT/deploy/vps_verify_warehouse.sh"
else
  report "$WAREHOUSE"
fi
