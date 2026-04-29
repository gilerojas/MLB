# Warehouse: Google Drive as source of truth

## How it fits together

| Layer | Role |
|--------|------|
| **Google Drive** (`MLB/warehouse/mlb/…`) | Canonical store. GitHub Actions **daily_ingest** pulls → ingests → pushes here. |
| **Your machine** (`data/warehouse/mlb` or `MLB_WAREHOUSE_DIR`) | **Local mirror** — a cache the hub and Python scripts read. Not a second source of truth. |
| **Rclone** | The only “bridge” between Drive and disk (same as CI). |

The app does **not** call the Drive HTTP API when you open a page or generate a card. It reads **files**. So “scaling with Drive” means: **keep Drive authoritative**, and **refresh the mirror** on a schedule or path you control.

## Option A — Scheduled mirror (recommended for a dev laptop)

After CI has written new parquets to Drive, pull them down:

```bash
./scripts/pull_mlbops_from_drive.sh
```

Automate it — see `jobs/crontab.example` (section “Warehouse mirror from Drive”). Pick a time **after** your daily ingest finishes (workflow default: `0 11 * * *` UTC ≈ morning US ET).

## Option B — On demand (Hub)

**Settings → Sync from Google Drive** runs the same rclone script via `POST /system/sync-drive`. Use when you sit down to work and want the latest without waiting for cron.

## Option C — No duplicate copy: Drive File Stream path

If Google Drive for Desktop mounts the repo’s warehouse folder, set in `mlbops/.env`:

```env
MLB_WAREHOUSE_DIR=/full/path/to/.../MLB/warehouse/mlb
```

The hub still reads “local” paths, but the OS reads through the mount (network-backed). You trade disk space for latency and offline behavior.

## Option D — Server / team

Same ideas: a small VM or always-on Mac runs cron `pull_mlbops_from_drive.sh`, serves the hub, and `MLB_WAREHOUSE_DIR` points at that machine’s mirror. Everyone hits the hub URL; one mirror per environment.

## Pitcher cards and Insights

They need **`pitches_enriched` parquets** under the mirror path. Those appear in Drive after **daily ingest** (or manual `load_mlb_warehouse`) has run and CI (or you) has **synced to Drive**. If Drive has the file but your laptop errors “no parquet”, the mirror is stale — pull again (A, B, or C with a visible mount).

## Env vars (reference)

| Variable | Purpose |
|----------|---------|
| `MLB_WAREHOUSE_DIR` | Root of the mirror (default: `{repo}/data/warehouse/mlb`) |
| `RCLONE_REMOTE` | rclone remote name (default `mallitalytics`) |
| `GDRIVE_WAREHOUSE_PATH` | Path on remote (default `MLB/warehouse/mlb`) |

See also: `mlbops/api/paths.py`, `.github/workflows/daily_ingest.yml`.
