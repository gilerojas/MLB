#!/usr/bin/env bash
# Pull MLB warehouse from Google Drive directly onto the VPS (no Mac rsync).
# Run ON the VPS (Hostinger browser terminal, etc.)
#
# One-time rclone setup:
#   Mac:  cat ~/.rclone.conf          ← config is HERE on Mac (not ~/.config/rclone/)
#   VPS:  mkdir -p ~/.config/rclone && nano ~/.config/rclone/rclone.conf
#         (paste Mac output, save)
#
# Usage on VPS:
#   bash /srv/mlbops/app/deploy/vps_pull_warehouse_from_drive.sh 2026
#
# Env overrides:
#   RCLONE_REMOTE              default mallitalytics
#   GDRIVE_WAREHOUSE_PATH      default MLB/warehouse/mlb
#   MLBOPS_VPS_WAREHOUSE       default /srv/mlbops/warehouse/mlb

set -euo pipefail

SEASON="${1:-2026}"
REMOTE="${RCLONE_REMOTE:-mallitalytics}"
REMOTE_BASE="${GDRIVE_WAREHOUSE_PATH:-MLB/warehouse/mlb}"
LOCAL_BASE="${MLBOPS_VPS_WAREHOUSE:-/srv/mlbops/warehouse/mlb}"

REMOTE_PATH="${REMOTE}:${REMOTE_BASE}/${SEASON}"
LOCAL_PATH="${LOCAL_BASE%/}/${SEASON}"

if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone not found. Install: apt-get install -y rclone" >&2
  exit 1
fi

if ! rclone listremotes 2>/dev/null | grep -q "^${REMOTE}:"; then
  echo "rclone remote '${REMOTE}:' not configured." >&2
  echo "Run: rclone config   (same remote name as your Mac: mallitalytics)" >&2
  exit 1
fi

mkdir -p "$LOCAL_PATH"

echo "=== VPS warehouse pull from Drive ==="
echo "  remote: ${REMOTE_PATH}"
echo "  local:  ${LOCAL_PATH}"
echo ""

rclone sync "${REMOTE_PATH}" "${LOCAL_PATH}" \
  --transfers 8 \
  --stats 30s \
  --stats-one-line

echo ""
echo "=== counts ==="
du -sh "$LOCAL_PATH"
echo "RAW:     $(find "$LOCAL_PATH" -path '*/raw/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
echo "PITCHES: $(find "$LOCAL_PATH" -path '*/pitches_enriched/*' -type f 2>/dev/null | wc -l | tr -d ' ')"
echo ""
echo "Done. No Docker restart required."
