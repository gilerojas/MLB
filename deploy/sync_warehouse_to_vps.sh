#!/usr/bin/env bash
# Push the local MLB warehouse (or one season) from the Mac to the VPS volume.
#
# Typical flow:
#   1. ./scripts/pull_mlbops_from_drive.sh
#   2. ./deploy/sync_warehouse_to_vps.sh --season 2026
#   3. ./deploy/vps_verify_warehouse.sh --remote 2026
#
# If SSH hangs, use the VPS-side Drive pull instead (no Mac rsync):
#   bash /srv/mlbops/app/deploy/vps_pull_warehouse_from_drive.sh 2026
#
# Env overrides:
#   MLBOPS_VPS_HOST, MLBOPS_VPS_SSH_PORT, MLBOPS_VPS_SSH_USER, MLBOPS_VPS_SSH_KEY
#   MLBOPS_VPS_WAREHOUSE, MLB_LOCAL_WAREHOUSE

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SEASON=""
PULL_DRIVE=0
DRY_RUN=0
SKIP_SSH_CHECK=0

usage() {
  cat <<'EOF'
Usage: deploy/sync_warehouse_to_vps.sh [--season YEAR] [--pull-drive] [--dry-run]

  --season YEAR   Sync only data/warehouse/mlb/YEAR/ (default: entire warehouse tree)
  --pull-drive    Run scripts/pull_mlbops_from_drive.sh before rsync
  --dry-run       rsync --dry-run (no writes on VPS)

If SSH hangs after the header, see deploy/README.md section "SSH hangs".
Fallback: pull from Drive on the VPS with vps_pull_warehouse_from_drive.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --season)
      SEASON="${2:?--season requires a year}"
      shift 2
      ;;
    --pull-drive) PULL_DRIVE=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --skip-ssh-check) SKIP_SSH_CHECK=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

VPS_HOST="${MLBOPS_VPS_HOST:-2.24.123.57}"
VPS_PORT="${MLBOPS_VPS_SSH_PORT:-2222}"
VPS_USER="${MLBOPS_VPS_SSH_USER:-root}"
VPS_KEY="${MLBOPS_VPS_SSH_KEY:-/Users/gilrojasb/Desktop/Hermes/id_ed25519}"
VPS_WAREHOUSE="${MLBOPS_VPS_WAREHOUSE:-/srv/mlbops/warehouse/mlb}"
LOCAL_WAREHOUSE="${MLB_LOCAL_WAREHOUSE:-$REPO_ROOT/data/warehouse/mlb}"

# Mac OpenSSH often stalls on GSSAPI / slow DNS; keep connects bounded.
SSH_OPTS=(
  -i "$VPS_KEY"
  -p "$VPS_PORT"
  -o IdentitiesOnly=yes
  -o GSSAPIAuthentication=no
  -o ConnectTimeout=15
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=3
  -o StrictHostKeyChecking=accept-new
)
RSYNC_SSH="ssh ${SSH_OPTS[*]}"

ssh_hang_help() {
  cat <<'EOF'

SSH did not respond in time. Common fixes:

  1. Load your key (passphrase keys hang until unlocked):
       ssh-add --apple-use-keychain /Users/gilrojasb/Desktop/Hermes/id_ed25519

  2. Test SSH alone (should print "connected" within ~5s):
       ssh -i /Users/gilrojasb/Desktop/Hermes/id_ed25519 -p 2222 \
         -o GSSAPIAuthentication=no -o ConnectTimeout=15 \
         root@2.24.123.57 'echo connected'

  3. If SSH never works from Mac, skip rsync — pull on the VPS from Drive:
       bash /srv/mlbops/app/deploy/vps_pull_warehouse_from_drive.sh 2026
     (run via VPS web console, or tailscale ssh, if public SSH is stuck)

EOF
}

run_ssh_with_timeout() {
  local label="$1"
  local timeout_secs="$2"
  shift 2
  local cmd=("$@")

  echo "→ ${label} (timeout ${timeout_secs}s)..."
  "${cmd[@]}" &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null && [[ "$waited" -lt "$timeout_secs" ]]; do
    sleep 1
    waited=$((waited + 1))
    if [[ $((waited % 5)) -eq 0 ]]; then
      echo "   ...still waiting (${waited}s)"
    fi
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    echo "ERROR: ${label} timed out after ${timeout_secs}s." >&2
    ssh_hang_help >&2
    return 1
  fi
  wait "$pid"
}

if [[ ! -d "$LOCAL_WAREHOUSE" ]]; then
  echo "Local warehouse missing: $LOCAL_WAREHOUSE" >&2
  exit 1
fi

if [[ ! -f "$VPS_KEY" ]]; then
  echo "SSH key missing: $VPS_KEY" >&2
  exit 1
fi

if [[ "$PULL_DRIVE" -eq 1 ]]; then
  echo "→ Pulling latest warehouse from Google Drive..."
  "$REPO_ROOT/scripts/pull_mlbops_from_drive.sh"
fi

if [[ -n "$SEASON" ]]; then
  SRC="${LOCAL_WAREHOUSE%/}/${SEASON}/"
  DEST="${VPS_WAREHOUSE%/}/${SEASON}/"
  VERIFY_PATH="${VPS_WAREHOUSE%/}/${SEASON}"
else
  SRC="${LOCAL_WAREHOUSE%/}/"
  DEST="${VPS_WAREHOUSE%/}/"
  VERIFY_PATH="${VPS_WAREHOUSE%/}"
fi

if [[ ! -d "$SRC" ]]; then
  echo "Source path missing: $SRC" >&2
  exit 1
fi

RSYNC_FLAGS=(-avh --info=progress2 --partial --human-readable)
RSYNC_FLAGS+=(
  --exclude '.DS_Store'
  --exclude '__MACOSX/'
  --exclude '* 2.json'
  --exclude '*.tar'
)
if [[ "$DRY_RUN" -eq 1 ]]; then
  RSYNC_FLAGS+=(--dry-run)
fi

echo "=== MLB Ops warehouse sync ==="
echo "  local:  $SRC"
echo "  remote: ${VPS_USER}@${VPS_HOST}:${DEST}"
echo "  ssh:    port ${VPS_PORT}, key ${VPS_KEY}"
echo ""

if [[ "$SKIP_SSH_CHECK" -eq 0 ]]; then
  run_ssh_with_timeout "SSH preflight" 25 \
    ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_HOST}" 'echo connected'
fi

run_ssh_with_timeout "prepare remote directory" 25 \
  ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_HOST}" "mkdir -p '${DEST}'"

echo "→ rsync warehouse (this is the long step — progress below)..."
rsync "${RSYNC_FLAGS[@]}" -e "$RSYNC_SSH" "$SRC" "${VPS_USER}@${VPS_HOST}:${DEST}"

echo ""
echo "=== Local source counts ==="
du -sh "$SRC"
echo "RAW:     $(find "$SRC" -path '*/raw/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
echo "PITCHES: $(find "$SRC" -path '*/pitches_enriched/*' -type f 2>/dev/null | wc -l | tr -d ' ')"

echo ""
echo "=== VPS destination counts ==="
run_ssh_with_timeout "verify remote counts" 60 \
  ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_HOST}" bash -s -- "$VERIFY_PATH" <<'REMOTE'
set -euo pipefail
path="$1"
du -sh "$path"
echo "RAW:     $(find "$path" -path '*/raw/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
echo "PITCHES: $(find "$path" -path '*/pitches_enriched/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
REMOTE

echo ""
echo "Done. No Docker restart required."
echo "Check: curl -s http://100.111.41.78/api/backend/system/readiness | python3 -m json.tool"
