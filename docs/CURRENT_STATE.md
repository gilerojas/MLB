# MLB Ops — Current State (read this first)

**Last updated:** July 2026
**Audience:** AI agents, new contributors, future-you resuming work.

This file is the **authoritative description of how the project works today**. If another doc disagrees with this one, **trust this file** unless `TRACKING.md` explicitly says otherwise.

---

## What this repo is

Two systems in one repository:

| System | Purpose | Key paths |
|--------|---------|-----------|
| **MLB Warehouse** | Ingest MLB Stats API + Statcast into parquet/json | `src/ingestion/`, `data/warehouse/mlb/` (Mac dev mirror) |
| **MLB Ops (mlbops)** | Mallitalytics content hub — find angles, generate cards, queue, post to X | `mlbops/api/`, `mlbops/hub/`, `scripts/*_daily.py`, `morning_intel/` |

The brand goal: **data-driven baseball content on X** — visuals + stat-led posts, manual voice first, AI redraft secondary.

---

## Production runtime (VPS) — this is the real app

**MLB Ops production does not run on the Mac.** Phase 1 is live on a Hostinger VPS.

| Item | Value |
|------|-------|
| **Hub URL** | `http://100.111.41.78` (Tailscale) |
| **App root on server** | `/srv/mlbops/app` |
| **Env file** | `/srv/mlbops/env/mlbops.env` |
| **Warehouse** | `/srv/mlbops/warehouse/mlb` |
| **Generated PNGs** | `/srv/mlbops/outputs` |
| **Queue / state DB** | **Postgres** (Docker), not SQLite |
| **Runtime flag** | `MLBOPS_RUNTIME=vps`, `MLBOPS_DB_BACKEND=postgres` |

Supabase is legacy planning material only. It is not part of the runtime, database, authentication, storage, or deployment path.

Stack: `docker compose` → **postgres** + **api** (FastAPI) + **hub** (Next.js production).

Deploy after code changes:

```bash
./deploy/ship.sh "what changed"
# or: ./deploy/sync_app_to_vps.sh
```

Full ops guide: [`deploy/README.md`](../deploy/README.md).

The VPS copy is **not a git repo** — deploy is rsync from Mac, not `git pull` on the server.

---

## Mac role (development + sync only)

The Mac is **not** the production server. It is used for:

- Editing code in this repo
- Local dev (`./start_hub.sh` — optional, SQLite fallback)
- Pushing deploys to VPS (`deploy/ship.sh`)
- Warehouse mirror under `data/warehouse/mlb/` (dev reference; production warehouse lives on VPS)
- rclone sync from Google Drive when refreshing the Mac mirror or seeding VPS

**Do not assume** the Mac is awake, synced, or running the hub for daily posting.

---

## Data layers

```
Google Drive (MLB/warehouse/mlb)     ← CI ingest pushes here (canonical archive)
        ↓ rclone
VPS /srv/mlbops/warehouse/mlb        ← production reads this
        ↓ (optional mirror)
Mac data/warehouse/mlb                 ← local dev / backup mirror only

morning_intel/snapshots/               ← daily JSON intel (on VPS in production)
outputs/                               ← generated card PNGs (on VPS in production)
Postgres (mlbops DB)                   ← queue, watchlist, live events, metrics (production)
data/hub.db                            ← SQLite — local dev / legacy import only
```

Google Drive is **not** in the live VPS runtime path. VPS pulls warehouse via rclone (`deploy/vps_pull_warehouse_from_drive.sh`) or Mac rsync (`deploy/sync_warehouse_to_vps.sh`).

---

## mlbops architecture (current)

```
Browser (Mac or phone via Tailscale)
    → Next.js Hub (mlbops/hub) — password session, /api/backend proxy
    → FastAPI (mlbops/api) — private service-token boundary; cards, queue, intel, leaderboards, live, fantasy
    → Postgres — queue state, watchlist, audit, performance
    → /srv/mlbops/warehouse/mlb — parquets + raw feeds
    → /srv/mlbops/outputs — PNGs served via FastAPI /static
    → X/Twitter API — posting from queue
```

Card generation calls Python in `scripts/` and `src/` (pitcher/batter cards, HR tracker, probables, etc.).

---

## Agent session checklist

1. Read **`TRACKING.md`** (repo root) — what's done, broken, backlog.
2. Read **this file** — production vs dev vs history.
3. For deploy/SSH/warehouse: **`deploy/README.md`**.
4. For product/workflow (tabs, tweet types): **`docs/MLBOPS_OVERVIEW.md`**.
5. For data pipeline / parquet schema: root **`README.md`** + `docs/FEED_*.md`.

After hub/api/deploy changes: update `TRACKING.md` and run `./deploy/ship.sh`.

---

## Critical facts (things that have burned us)

- `player_name` in `pitches_enriched` parquets = **batter** name, not pitcher.
- Pitcher names come from `players_registry.json` in the warehouse.
- `MLB_WAREHOUSE_DIR` overrides warehouse path in API and card scripts.
- Schedule page is a Next.js **Server Component** — no client `onClick`/`onError` handlers.
- No emojis in hub UI.
- Production DB is **Postgres**; references to `data/hub.db` as the live queue DB are **outdated** unless explicitly about local dev or one-time import.
- Supabase is **legacy/unused**. Do not propose or implement Supabase unless the owner explicitly reopens that architecture decision.
- FastAPI production routes require `MLBOPS_API_SERVICE_TOKEN`; only `/health` and generated `/static/*` assets are public at the API layer.

---

## Historical / obsolete docs

These describe **past** architecture or **rejected/superseded** plans. **Do not treat them as current state.**

| Doc | Why obsolete |
|-----|----------------|
| [`docs/archive/SECURE_TRAVEL_HUB.md`](archive/SECURE_TRAVEL_HUB.md) | Mac + Tailscale Serve as production; replaced by VPS |
| [`docs/archive/mlbops_operating_model.md`](archive/mlbops_operating_model.md) | Supabase control-plane plan; production uses VPS Postgres |
| [`docs/MLBOPS_UPSCALE_GUIDE.md`](MLBOPS_UPSCALE_GUIDE.md) | May 2026 growth roadmap; many SQLite/local assumptions — use for feature ideas only |
| [`docs/mlbops_vps_production_spec.md`](mlbops_vps_production_spec.md) | Migration spec; Phase 1 deployed, some items still in progress |
| [`docs/progress/`](progress/) | Session logs from upscale work — historical record |

Obsolete local scripts (dev only, not production path):

- `./scripts/start_mlbops_travel.sh` — local travel mode; production is VPS Tailscale URL.

---

## Still accurate reference docs

| Doc | Topic |
|-----|-------|
| [`deploy/README.md`](../deploy/README.md) | VPS deploy, SSH, warehouse sync, health checks |
| [`docs/MLBOPS_OVERVIEW.md`](MLBOPS_OVERVIEW.md) | Hub tabs, daily tweet types, queue workflow |
| [`docs/WAREHOUSE_DRIVE_WORKFLOW.md`](WAREHOUSE_DRIVE_WORKFLOW.md) | Drive ↔ mirror sync (Mac dev + VPS pull) |
| [`docs/PITCHING_CARD.md`](PITCHING_CARD.md) | Pitcher card pipeline |
| [`docs/FEED_VS_PITCHES_ENRICHED.md`](FEED_VS_PITCHES_ENRICHED.md) | Data model |
| Root [`README.md`](../README.md) | Warehouse ingest CLI |
