# Documentation index

**Agents: start here → [`CURRENT_STATE.md`](CURRENT_STATE.md)** then [`TRACKING.md`](../TRACKING.md) at repo root.

This index separates **current** docs from **historical** ones so agents do not confuse past architecture with production.

---

## Current — read these for how things work today

| Doc | What it covers |
|-----|----------------|
| [**CURRENT_STATE.md**](CURRENT_STATE.md) | **Authoritative** production vs dev, VPS paths, data layers, agent checklist |
| [**MLBOPS_OVERVIEW.md**](MLBOPS_OVERVIEW.md) | Product: hub tabs, daily tweet types, queue workflow |
| [**deploy/README.md**](../deploy/README.md) | VPS deploy, Tailscale URL, SSH, warehouse sync, env secrets |
| [**WAREHOUSE_DRIVE_WORKFLOW.md**](WAREHOUSE_DRIVE_WORKFLOW.md) | Google Drive ↔ warehouse mirror (CI, Mac dev, VPS pull) |
| [**mlbops_vps_production_spec.md**](mlbops_vps_production_spec.md) | VPS target architecture (Phase 1 live; some items still open) |

## Data pipeline reference

| Doc | What it covers |
|-----|----------------|
| [Root README.md](../README.md) | Warehouse ingest CLI, folder structure |
| [FEED_VS_PITCHES_ENRICHED.md](FEED_VS_PITCHES_ENRICHED.md) | Raw feed vs enriched parquet |
| [FEED_VS_PITCHES_ENRICHED_COLUMNS.md](FEED_VS_PITCHES_ENRICHED_COLUMNS.md) | Column mapping |
| [FEED_LIVE_STRUCTURE.md](FEED_LIVE_STRUCTURE.md) | feed/live JSON shape |
| [PITCHING_CARD.md](PITCHING_CARD.md) | Pitcher card generation |
| [STORAGE_STRATEGY.md](STORAGE_STRATEGY.md) | Retention / scaling |

## Planning & growth (ideas — verify against CURRENT_STATE)

| Doc | Note |
|-----|------|
| [MLBOPS_UPSCALE_GUIDE.md](MLBOPS_UPSCALE_GUIDE.md) | May 2026 upscale roadmap; **not** runtime truth |
| [Growth_Strategy_X.md](Growth_Strategy_X.md) | X growth strategy |
| [progress/](progress/) | Session-by-session upscale logs |

---

## Historical / obsolete — do not use as runtime truth

Moved to [`archive/`](archive/) with explanation in [`archive/README.md`](archive/README.md).

| Doc | Superseded by |
|-----|----------------|
| [archive/SECURE_TRAVEL_HUB.md](archive/SECURE_TRAVEL_HUB.md) | VPS + `deploy/README.md` |
| [archive/mlbops_operating_model.md](archive/mlbops_operating_model.md) | VPS Postgres (`CURRENT_STATE.md`) |

Stub files remain at old paths (`SECURE_TRAVEL_HUB.md`) pointing here so broken links redirect agents correctly.