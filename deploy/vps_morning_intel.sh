#!/usr/bin/env bash
# Morning Intel on the VPS, reading the live warehouse in place.
#
# The GitHub Actions job pulled ~1,500 parquet files from Drive on every run and
# took 13-19 minutes to do it. The VPS already holds the warehouse the Hub and API
# read, so running here removes the transfer entirely and removes Drive as a
# source of truth for the briefing.
#
# State (snapshots, scouting ledger, priors) lives under the repo bind mount, so
# it survives container rebuilds — the persistence check is worthless if the
# snapshot history disappears with the image.
#
# Cron (VPS, UTC). After the 07:00 AST ingest (11:00 UTC):
#   30 11 * * * /srv/mlbops/app/deploy/vps_morning_intel.sh --collect-only
#   0  12 * * * /srv/mlbops/app/deploy/vps_morning_intel.sh
#
# --collect-only runs just the scouting collector, so reporting is already in the
# ledger by the time the briefing reads it. Run it more often for a denser
# ledger; the briefing itself never calls out to a collector.
set -euo pipefail

APP_DIR="${MLBOPS_APP_DIR:-/srv/mlbops/app}"
ENV_FILE="${MLBOPS_ENV_FILE:-/srv/mlbops/env/mlbops.env}"
LOG_DIR="${MLBOPS_LOG_DIR:-/srv/mlbops/logs}"
SEASON="${MLB_SEASON:-$(date -u +%Y)}"

COLLECT_ONLY=0
EXTRA_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --collect-only) COLLECT_ONLY=1 ;;
    *) EXTRA_ARGS+=("$arg") ;;
  esac
done

mkdir -p "$LOG_DIR"
cd "$APP_DIR"

# The warehouse is mounted at /data/warehouse/mlb; everything else defaults to
# paths under /app, which is the repo bind mount.
compose_exec() {
  docker compose --env-file "$ENV_FILE" exec -T \
    -e MLB_WAREHOUSE_ROOT=/data/warehouse/mlb \
    api "$@"
}

echo "=== morning intel start $(date -Is) season=$SEASON ==="

echo "--- scouting collector ---"
compose_exec python /app/morning_intel/collect_scouting.py --season "$SEASON" --hours 24 \
  | tee -a "$LOG_DIR/morning_intel.log"

if [ "$COLLECT_ONLY" -eq 1 ]; then
  echo "=== collect-only complete $(date -Is) ==="
  exit 0
fi

echo "--- briefing ---"
compose_exec python /app/morning_intel/morning_intel.py \
  --season "$SEASON" --stage all --skip-queue "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}" \
  | tee -a "$LOG_DIR/morning_intel.log"

echo "=== morning intel end $(date -Is) ==="
