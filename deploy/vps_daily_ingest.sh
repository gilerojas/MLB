#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${MLBOPS_APP_DIR:-/srv/mlbops/app}"
ENV_FILE="${MLBOPS_ENV_FILE:-/srv/mlbops/env/mlbops.env}"
LOG_DIR="${MLBOPS_LOG_DIR:-/srv/mlbops/logs}"
LAST_DAYS="${MLBOPS_INGEST_LAST_DAYS:-2}"
WORKERS="${MLBOPS_INGEST_WORKERS:-2}"
DELAY="${MLBOPS_INGEST_DELAY:-0.25}"
SEASON="${MLBOPS_INGEST_SEASON:-$(date +%Y)}"

mkdir -p "$LOG_DIR"
cd "$APP_DIR"

echo "=== mlbops ingest start $(date -Is) season=${SEASON} last_days=${LAST_DAYS} workers=${WORKERS} ==="

docker compose --env-file "$ENV_FILE" run --rm api \
  python /app/src/ingestion/load_mlb_warehouse.py \
    --warehouse /data/warehouse/mlb \
    --season "$SEASON" \
    --game-type R \
    --last-days "$LAST_DAYS" \
    --workers "$WORKERS" \
    --delay "$DELAY" \
    --refresh-schedule

docker compose --env-file "$ENV_FILE" exec -T api \
  python - <<'PY'
from pathlib import Path
from api.paths import get_warehouse_dir

wh = get_warehouse_dir()
print(f"warehouse={wh}")
print(f"exists={Path(wh).is_dir()}")
PY

echo "=== mlbops ingest end $(date -Is) ==="
