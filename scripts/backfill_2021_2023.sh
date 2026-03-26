#!/usr/bin/env bash
# Full warehouse backfill for 2021–2023 (all stages). Compressed JSON via ensure_raw.
# Run from repo root:  bash scripts/backfill_2021_2023.sh
# Or:  nohup bash scripts/backfill_2021_2023.sh >> logs/backfill_2021_2023.log 2>&1 &
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
LOG="${ROOT}/logs/backfill_2021_2023.log"
mkdir -p "${ROOT}/logs"
exec >>"$LOG" 2>&1
echo "=== $(date -u)  START backfill 2021–2023 ==="
for season in 2021 2022 2023; do
  echo "--- Season $season ---"
  python src/ingestion/load_mlb_warehouse.py \
    --season "$season" \
    --all-stages \
    --workers 3 \
    --delay 0.25
done
echo "=== Gap-fill: pitches_enriched from existing raws (missed Statcast) ==="
python src/ingestion/load_mlb_warehouse.py \
  --from-raw \
  --years 2021 2022 2023 \
  --workers 3 \
  --delay 0.25
echo "=== $(date -u)  DONE backfill 2021–2023 + gap-fill ==="
