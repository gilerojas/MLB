#!/usr/bin/env bash
# Pull mlbops data from Google Drive into the local repo (cache mirror).
# Requires rclone remote (same as CI): default RCLONE_REMOTE=mallitalytics
#
# Usage: from MLB repo root (your real path, not a placeholder):
#   cd ~/Desktop/Mallitalytics_VS/MLB   # example
#   ./scripts/pull_mlbops_from_drive.sh
#
# After pull: cd mlbops && uvicorn api.main:app --port 8000 --reload
# If port 8000 is busy, use: lsof -nP -iTCP:8000 -sTCP:LISTEN

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE="${RCLONE_REMOTE:-mallitalytics}"
WAREHOUSE_REMOTE_PATH="${GDRIVE_WAREHOUSE_PATH:-MLB/warehouse/mlb}"
INTEL_REMOTE_PATH="${GDRIVE_INTEL_SNAPSHOTS_PATH:-MLB/morning_intel/snapshots}"

cd "$REPO_ROOT"
mkdir -p data/warehouse/mlb data morning_intel/snapshots

echo "→ rclone sync ${REMOTE}:${WAREHOUSE_REMOTE_PATH} → data/warehouse/mlb"
rclone sync "${REMOTE}:${WAREHOUSE_REMOTE_PATH}" data/warehouse/mlb --transfers 8 \
  --stats 30s --stats-one-line 2>&1

HUB_DB_REMOTE="${REMOTE}:MLB/hub/hub.db"
echo "→ hub.db: ${HUB_DB_REMOTE}"
if rclone lsf "$HUB_DB_REMOTE" 2>/dev/null | grep -q .; then
  rclone copyto "$HUB_DB_REMOTE" data/hub.db 2>&1
  echo "   copied hub.db"
else
  echo "   (skip) no file at MLB/hub/hub.db on Drive yet — run Morning Intel CI once or copyto manually"
fi

echo "→ intel snapshots: ${REMOTE}:${INTEL_REMOTE_PATH}"
if rclone lsf "${REMOTE}:${INTEL_REMOTE_PATH}" 2>/dev/null | head -1 | grep -q .; then
  rclone sync "${REMOTE}:${INTEL_REMOTE_PATH}" morning_intel/snapshots --transfers 4 \
    --stats 30s --stats-one-line 2>&1
  echo "   synced snapshots"
else
  echo "   (skip) folder not on Drive yet — create MLB/morning_intel/snapshots/ or run updated morning_intel workflow"
fi

echo ""
echo "=== mlbops links (API must be running) ==="
echo "  API root:    http://127.0.0.1:8000/"
echo "  Health:      http://127.0.0.1:8000/health"
echo "  Paths check: http://127.0.0.1:8000/system/paths"
echo "  Swagger:     http://127.0.0.1:8000/docs"
echo "  Hub (Next):  http://127.0.0.1:3000  — run: cd mlbops/hub && npm run dev"
echo ""
echo "Inspect API: curl -s http://127.0.0.1:8000/system/paths | python3 -m json.tool"
