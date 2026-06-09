#!/usr/bin/env bash
# Control the VPS Hub dev-mode container without changing API/Postgres/storage.
#
# Usage:
#   ./deploy/vps_hub_dev.sh enable   # sync app, start Hub with next dev
#   ./deploy/vps_hub_dev.sh sync     # sync app only; Hub dev server picks up files
#   ./deploy/vps_hub_dev.sh status
#   ./deploy/vps_hub_dev.sh logs
#   ./deploy/vps_hub_dev.sh disable  # recreate normal production Hub

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ACTION="${1:-status}"

VPS_HOST="${MLBOPS_VPS_HOST:-100.111.41.78}"
VPS_PORT="${MLBOPS_VPS_SSH_PORT:-2222}"
VPS_USER="${MLBOPS_VPS_SSH_USER:-root}"
VPS_KEY="${MLBOPS_VPS_SSH_KEY:-/Users/gilrojasb/Desktop/Hermes/id_ed25519}"
VPS_APP="${MLBOPS_VPS_APP_DIR:-/srv/mlbops/app}"
ENV_FILE="${MLBOPS_VPS_ENV_FILE:-/srv/mlbops/env/mlbops.env}"

SSH_OPTS=(
  -i "$VPS_KEY"
  -p "$VPS_PORT"
  -o IdentitiesOnly=yes
  -o GSSAPIAuthentication=no
  -o ConnectTimeout=20
)

remote() {
  ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_HOST}" "$@"
}

sync_app() {
  MLBOPS_VPS_HOST="$VPS_HOST" \
  MLBOPS_VPS_SSH_PORT="$VPS_PORT" \
  MLBOPS_VPS_SSH_USER="$VPS_USER" \
  MLBOPS_VPS_SSH_KEY="$VPS_KEY" \
  MLBOPS_VPS_APP_DIR="$VPS_APP" \
    "${REPO_ROOT}/deploy/sync_app_to_vps.sh" --no-build
}

case "$ACTION" in
  enable)
    sync_app
    remote bash -s -- "$VPS_APP" "$ENV_FILE" <<'REMOTE'
set -euo pipefail
app="$1"
env_file="$2"
cd "$app"
docker compose --env-file "$env_file" -f docker-compose.yml -f docker-compose.hub-dev.yml up -d hub
docker compose --env-file "$env_file" -f docker-compose.yml -f docker-compose.hub-dev.yml ps hub
docker compose --env-file "$env_file" -f docker-compose.yml -f docker-compose.hub-dev.yml logs --tail=60 hub
REMOTE
    ;;
  sync)
    sync_app
    ;;
  status)
    remote bash -s -- "$VPS_APP" "$ENV_FILE" <<'REMOTE'
set -euo pipefail
app="$1"
env_file="$2"
cd "$app"
docker compose --env-file "$env_file" ps hub api postgres
printf "\nHub bind:\n"
grep -E '^(MLBOPS_HUB_BIND|MLBOPS_HUB_PORT)=' "$env_file" || true
printf "\nHub command:\n"
docker inspect app-hub-1 --format '{{json .Config.Cmd}}' 2>/dev/null || true
REMOTE
    ;;
  logs)
    remote bash -s -- "$VPS_APP" "$ENV_FILE" <<'REMOTE'
set -euo pipefail
app="$1"
env_file="$2"
cd "$app"
docker compose --env-file "$env_file" -f docker-compose.yml -f docker-compose.hub-dev.yml logs --tail=120 hub
REMOTE
    ;;
  disable)
    remote bash -s -- "$VPS_APP" "$ENV_FILE" <<'REMOTE'
set -euo pipefail
app="$1"
env_file="$2"
cd "$app"
docker compose --env-file "$env_file" build hub
docker compose --env-file "$env_file" up -d hub
docker compose --env-file "$env_file" ps hub
REMOTE
    ;;
  -h|--help)
    sed -n '1,12p' "$0"
    ;;
  *)
    echo "Unknown action: $ACTION" >&2
    echo "Use: enable | sync | status | logs | disable" >&2
    exit 1
    ;;
esac
