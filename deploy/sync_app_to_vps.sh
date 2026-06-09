#!/usr/bin/env bash
# Push local repo changes to the VPS and rebuild hub/api containers.
#
# Usage (from repo root on Mac, Tailscale ON):
#   ./deploy/sync_app_to_vps.sh
#   ./deploy/sync_app_to_vps.sh --no-build    # rsync only
#
# Env:
#   MLBOPS_VPS_HOST       default 100.111.41.78 (Tailscale)
#   MLBOPS_VPS_SSH_PORT   default 2222
#   MLBOPS_VPS_SSH_USER   default root
#   MLBOPS_VPS_SSH_KEY    default /Users/gilrojasb/Desktop/Hermes/id_ed25519
#   MLBOPS_VPS_APP_DIR    default /srv/mlbops/app

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NO_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build) NO_BUILD=1; shift ;;
    -h|--help)
      echo "Usage: deploy/sync_app_to_vps.sh [--no-build]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

VPS_HOST="${MLBOPS_VPS_HOST:-100.111.41.78}"
VPS_PORT="${MLBOPS_VPS_SSH_PORT:-2222}"
VPS_USER="${MLBOPS_VPS_SSH_USER:-root}"
VPS_KEY="${MLBOPS_VPS_SSH_KEY:-/Users/gilrojasb/Desktop/Hermes/id_ed25519}"
VPS_APP="${MLBOPS_VPS_APP_DIR:-/srv/mlbops/app}"
ENV_FILE="/srv/mlbops/env/mlbops.env"

SSH_OPTS=(
  -i "$VPS_KEY"
  -p "$VPS_PORT"
  -o IdentitiesOnly=yes
  -o GSSAPIAuthentication=no
  -o ConnectTimeout=20
)
RSYNC_SSH="ssh ${SSH_OPTS[*]}"

RSYNC_FLAGS=(
  -avh
  --delete
  --exclude '.git/'
  --exclude '.claude/'
  --exclude '.cursor/'
  --exclude '.codex/'
  --exclude 'node_modules/'
  --exclude 'mlbops/hub/node_modules/'
  --exclude 'mlbops/hub/.next/'
  --exclude 'mlb_env/'
  --exclude 'mlb_env.nosync/'
  --exclude 'mlb_env*/'
  --exclude '__pycache__/'
  --exclude '*.pyc'
  --exclude '.DS_Store'
  --exclude 'graphify-out/'
  --exclude 'mlbops/graphify-out/'
  --exclude 'logs/'
  --exclude '*.log'
  --exclude 'data/warehouse/'
  --exclude 'data/hub.db'
  --exclude 'outputs/'
  --exclude 'mlbops/.env'
  --exclude '.env'
  --exclude 'notebooks/.ipynb_checkpoints/'
  --exclude '*.parquet'
  --exclude '*.tar'
)

echo "=== Sync app → VPS ==="
echo "  local:  ${REPO_ROOT}/"
echo "  remote: ${VPS_USER}@${VPS_HOST}:${VPS_APP}/"
echo ""

rsync "${RSYNC_FLAGS[@]}" -e "$RSYNC_SSH" "${REPO_ROOT}/" "${VPS_USER}@${VPS_HOST}:${VPS_APP}/"

if [[ "$NO_BUILD" -eq 1 ]]; then
  echo "Rsync done (--no-build)."
  exit 0
fi

echo ""
echo "=== Rebuild hub + api on VPS ==="
ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_HOST}" bash -s -- "$VPS_APP" "$ENV_FILE" <<'REMOTE'
set -euo pipefail
app="$1"
env_file="$2"
cd "$app"
docker compose --env-file "$env_file" build hub api
docker compose --env-file "$env_file" up -d hub api
docker compose --env-file "$env_file" ps hub api
REMOTE

echo ""
echo "Done. Hard-refresh the hub in your browser (Cmd+Shift+R)."
