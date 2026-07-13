# Agent Warehouse Context

This project's local Mac warehouse is not the production source of truth.

## Current Rule

- Treat `data/warehouse/mlb` on the Mac as stale by default.
- For current 2026 data investigations, use the VPS warehouse first.
- Production warehouse:
  - VPS host path: `/srv/mlbops/warehouse/mlb`
  - Docker container path: `/data/warehouse/mlb`
- Live MLB Ops at `http://100.111.41.78` reads the VPS warehouse.
- Google Drive is an archive/sync source, not the live runtime path.

## Why This Exists

Agents have repeatedly wasted time scanning the local warehouse for current data, then discovered it was outdated. That is the wrong first move for current-season card, ingest, Statcast, `pitches_enriched`, dashboard, Queue, Insights, or Pitching Index issues.

## Default Investigation Path

1. SSH to VPS:
   `ssh -i /Users/gilrojasb/Desktop/Hermes/id_ed25519 -p 2222 root@2.24.123.57`
2. Work from app dir:
   `cd /srv/mlbops/app`
3. Run checks inside the API container:
   `docker compose --env-file /srv/mlbops/env/mlbops.env exec -T api ...`
4. Use `/data/warehouse/mlb` as the warehouse root inside the container.

## Targeted Ingest

If a current date is missing or stale, run targeted ingest on the VPS:

```bash
docker compose --env-file /srv/mlbops/env/mlbops.env run --rm api \
  python /app/src/ingestion/load_mlb_warehouse.py \
  --warehouse /data/warehouse/mlb \
  --season 2026 \
  --game-type R \
  --dates YYYY-MM-DD \
  --workers 2 \
  --delay 0.25 \
  --refresh-schedule \
  --force
```

Use local warehouse only when the task is explicitly local/offline or after a verified sync.
