# MLB Ops Codex Notes

## VPS / Dev Workflow

- The VPS app at `http://100.111.41.78` runs through Docker Compose from `/srv/mlbops/app`.
- `hub` is a built Next.js image, not a live source bind mount in the normal VPS setup. Frontend changes copied to the VPS do not appear after browser refresh or container restart unless `hub` is rebuilt.
- During active frontend development, prefer enabling the existing hub dev workflow if appropriate; otherwise rebuild only what is needed and verify the new image contains the changed UI strings.
- Do not print or summarize secrets from Compose/env output.

## Warehouse Source Of Truth

- Assume the local Mac warehouse at `data/warehouse/mlb` is stale unless the user explicitly says it was just refreshed. Do not spend time debugging current-season missing data against the local warehouse first.
- For current 2026 production data, the active warehouse is on the VPS: host path `/srv/mlbops/warehouse/mlb`, container path `/data/warehouse/mlb`.
- The live Hub/API read the VPS warehouse, not the Mac mirror and not Google Drive directly. If the app reports missing `pitches_enriched` files, inspect or rerun ingest on the VPS.
- Preferred current-data checks:
  - SSH: `ssh -i /Users/gilrojasb/Desktop/Hermes/id_ed25519 -p 2222 root@2.24.123.57`
  - App dir: `cd /srv/mlbops/app`
  - Container command pattern: `docker compose --env-file /srv/mlbops/env/mlbops.env exec -T api ...`
  - Warehouse path inside container: `/data/warehouse/mlb`
- For a stale/missing current date, run a targeted VPS ingest before local investigation. Example:
  `docker compose --env-file /srv/mlbops/env/mlbops.env run --rm api python /app/src/ingestion/load_mlb_warehouse.py --warehouse /data/warehouse/mlb --season 2026 --game-type R --dates YYYY-MM-DD --workers 2 --delay 0.25 --refresh-schedule --force`
- Only use the local Mac warehouse for dev/offline tests, historical spot checks, or after explicitly syncing from Drive/VPS.

## Avoid Hanging Work

- Before running expensive local reads/builds, remember this repo has previously had local filesystem stalls around `mlbops/api/routers/insights.py`. If simple reads hang, stop using local filesystem reads for that file and inspect the VPS copy instead.
- Do not stack repeated builds, broad greps, or long-running API calls when one is already in progress. Poll the existing session, check process state, or kill the stuck process deliberately.
- Use timeouts for verification commands that can scan large build folders, load full-season Statcast, or traverse `.next`.
- When a command goes quiet, distinguish normal build phases from actual hangs by checking Docker/process status in a separate lightweight command.
- End the turn only after all started sessions are complete or intentionally stopped.

## Verification Pattern

- For API changes: compile the touched Python modules, restart `api` if needed, check `/health`, then run a narrow endpoint smoke test.
- For Insights changes: verify both backend JSON and frontend built assets. A browser refresh alone is not proof that the image changed.
- For memory work: check `docker stats --no-stream` after warming the endpoint once.
