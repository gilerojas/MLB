#!/usr/bin/env bash
# Push local warehouse → Google Drive (default: add/update only, **never delete** on Drive).
#
# Requires: rclone remote (default RCLONE_REMOTE=mallitalytics) with access to GDRIVE_WAREHOUSE_PATH.
#
# From MLB repo root:
#   ./scripts/push_warehouse_to_drive.sh
#
# Default uses `rclone copy`: uploads new and changed files under data/warehouse/mlb.
# Extra files that exist only on Drive are **left alone** (safe when local is behind or incomplete).
#
# Logging:
#   Default: -v (per-file lines: Copied/skipped), full stats blocks (not one-line) every 5s, local + remote
#   `RCLONE_WAREHOUSE_PUSH_QUIET=1` — minimal output (use if too noisy for huge trees).
#   `RCLONE_WAREHOUSE_PUSH_DRY_RUN=1` — show what *would* transfer, no upload.
#   `RCLONE_WAREHOUSE_SKIP_REMOTE_SIZE=1` — skip the final `rclone size` on Drive (faster for huge trees).
#
# When the run says "There was nothing to transfer" and 0 B: Drive already matches your local
# files (for copy), so there are no "Copied" lines—nothing was wrong, there was no upload to show.
# Use DRY_RUN=1 to list what *would* go up if you add or change local files.
#
# Optional — full mirror (can **delete** on Drive; same idea as GHA’s rclone sync):
#   RCLONE_WAREHOUSE_PUSH_MODE=sync ./scripts/push_warehouse_to_drive.sh

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REMOTE="${RCLONE_REMOTE:-mallitalytics}"
WAREHOUSE_REMOTE_PATH="${GDRIVE_WAREHOUSE_PATH:-MLB/warehouse/mlb}"
MODE="${RCLONE_WAREHOUSE_PUSH_MODE:-copy}"
REMOTE_PATH="${REMOTE}:${WAREHOUSE_REMOTE_PATH}"
QUIET="${RCLONE_WAREHOUSE_PUSH_QUIET:-0}"
DRY="${RCLONE_WAREHOUSE_PUSH_DRY_RUN:-0}"
SKIP_REM="${RCLONE_WAREHOUSE_SKIP_REMOTE_SIZE:-0}"

cd "$REPO_ROOT"
if [ ! -d data/warehouse/mlb ]; then
  echo "error: data/warehouse/mlb missing — run from repo root" >&2
  exit 1
fi

case "$MODE" in
  copy|sync) ;;
  *)
    echo "error: RCLONE_WAREHOUSE_PUSH_MODE must be 'copy' or 'sync' (got: $MODE)" >&2
    exit 1
    ;;
esac

echo ""
echo "=== Local warehouse (source) ==="
rclone size data/warehouse/mlb 2>&1 | sed 's/^/  /' || true

RCLONE_ARGS=( "$MODE" data/warehouse/mlb "$REMOTE_PATH" --transfers 8 --checkers 8 --stats 5s )
if [ "$QUIET" = "1" ]; then
  RCLONE_ARGS+=( --stats-one-line --stats 30s )
else
  RCLONE_ARGS+=( -v )
fi
if [ "$DRY" = "1" ]; then
  RCLONE_ARGS+=( --dry-run )
  echo ""
  echo "=== Dry run: what would be uploaded/updated (no changes on Drive) ==="
else
  echo ""
  echo "=== rclone $MODE → ${REMOTE_PATH} ==="
  echo "  (If bytes actually upload, you will see Transferred: … > 0 and per-file INFO lines. If"
  echo "   everything already matched, rclone says 'nothing to transfer' — that is normal.)"
fi

RCLONE_LOG=$(mktemp)
trap 'rm -f "$RCLONE_LOG"' EXIT

rclone "${RCLONE_ARGS[@]}" 2>&1 | tee "$RCLONE_LOG"

echo ""
if [ "$DRY" = "1" ]; then
  echo "Dry run done. Re-run without RCLONE_WAREHOUSE_PUSH_DRY_RUN=1 to upload."
  exit 0
fi

# Plain-English result (rclone’s own line is easy to miss)
echo "=== What happened (read this) ==="
if grep -q "There was nothing to transfer" "$RCLONE_LOG" 2>/dev/null; then
  echo "  · Rclone: 0 bytes uploaded — local data/warehouse/mlb already matches Google Drive for"
  echo "    every file it compared, so there was no upload 'movement' (nothing new or changed to push)."
  echo "  · Per-file 'Copied' lines only appear when rclone actually sends or updates a file."
  echo "  · To list what would upload after you add data (from repo root):"
  echo "      RCLONE_WAREHOUSE_PUSH_DRY_RUN=1 ./scripts/push_warehouse_to_drive.sh"
else
  LAST=$(grep "Transferred:" "$RCLONE_LOG" 2>/dev/null | tail -1 | sed 's/^[[:space:]]*//')
  if [ -n "$LAST" ]; then
    echo "  · $LAST"
  else
    echo "  · See the rclone block above for Transferred/Checks."
  fi
fi
if grep -q "Duplicate object found in destination" "$RCLONE_LOG" 2>/dev/null; then
  echo "  · NOTICE: 'Duplicate object in destination' = two Drive files with the same path/name."
  echo "    Rclone will not overwrite one of them; clean duplicates in the Drive web UI if needed."
fi
echo ""

if [ "$SKIP_REM" = "1" ]; then
  echo "=== Skipped remote size (RCLONE_WAREHOUSE_SKIP_REMOTE_SIZE=1) ==="
else
  echo "=== Remote folder after $MODE (size on Drive; may take a while; includes files not on this machine) ==="
  rclone size "$REMOTE_PATH" 2>&1 | sed 's/^/  /' || echo "  (rclone size remote failed — check remote path / auth)"
fi

if [ "$MODE" = copy ]; then
  echo ""
  echo "Done. New/changed files are on Drive; nothing was deleted on the remote (copy mode)."
else
  echo ""
  echo "Done. Drive mirrored to local for this path (remote extras may be removed in sync mode)."
fi
