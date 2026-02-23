# MLB Warehouse

Pipeline for ingesting MLB game data: **feed/live** (raw) and **pitches_enriched** (Statcast + `play_id` join). Supports full-season backfill by stage (spring training, regular season, All-Star, playoffs) and schedule-only exports for publishing.

## Features

- **Schedule**: Fetch and export season schedules (JSON + CSV) for any year and stage.
- **Raw feed**: Download MLB Stats API `feed/live` per game; stored as `game_{pk}_{date}_feed_live.json.gz` (no indent) to save space.
- **Pitches enriched**: Merge Statcast (pybaseball) with feed `play_id`; output Parquet with a curated column set.
- **Stages**: Spring training (S), regular (R), All-Star (A), Wild Card (F), Division (D), Championship (L), World Series (W).
- **Deduplication**: Skips games that already have raw and enriched output; use `--force` to overwrite.
- **Progress**: Optional progress bar; parallel workers for enriched step (configurable).

## Setup

```bash
python -m venv mlb_env
source mlb_env/bin/activate   # Windows: mlb_env\Scripts\activate
pip install -r requirements.txt
```

Data is written under `data/warehouse/mlb/` (see [Structure](#warehouse-structure)); this folder is gitignored.

## Usage

### Schedule only (no game data)

Creates the season folder and saves schedule JSON + CSV for posting:

```bash
# Spring training only
python -m src.ingestion.load_mlb_warehouse --schedule-only --season 2026 --game-type S

# All stages for a season
python -m src.ingestion.load_mlb_warehouse --schedule-only --season 2026 --all-stages
```

Outputs under `data/warehouse/mlb/{year}/`:

- `schedule_{stage}.json` — full game list per stage
- `schedule_post.csv` — flat table: date, game_time, away_team, home_team, venue, game_pk, stage

### Full backfill (fetch raw + build enriched)

```bash
# One season, all stages (S, R, A, F, D, L, W)
python -m src.ingestion.load_mlb_warehouse --season 2025 --all-stages

# Single stage
python -m src.ingestion.load_mlb_warehouse --season 2025 --game-type R

# Optional: parallel workers and delay (default: 3 workers, 0.25s delay)
python -m src.ingestion.load_mlb_warehouse --season 2025 --all-stages --workers 4 --delay 0.2
```

### From existing raw only

Process only games that already have raw JSON (no feed download):

```bash
python -m src.ingestion.load_mlb_warehouse --from-raw --years 2025
```

### Audit stages before backfill

Check what each `gameType` returns and how it maps to folders:

```bash
python scripts/audit_stages.py --season 2025
```

## Warehouse structure

```
data/warehouse/mlb/
{year}/
  schedule_spring_training.json
  schedule_post.csv
  spring_training/
    raw/          game_{pk}_{date}_feed_live.json.gz
    pitches_enriched/   game_{pk}_{date}_pitches_enriched.parquet
  regular_season/
    raw/
    pitches_enriched/
  all_star/
  playoffs/
    wild_card/
    division/
    championship/
    world_series/
```

## Options (backfill)

| Option | Description |
|--------|-------------|
| `--warehouse` | Base path (default: `data/warehouse/mlb`) |
| `--season` | Year for schedule fetch |
| `--game-type` | S, R, A, F, D, L, W (default: R) |
| `--all-stages` | Fetch all stages for the season |
| `--from-raw` | Only process existing raw files |
| `--years` | With `--from-raw`: list of years to scan |
| `--force` | Overwrite existing enriched output |
| `--workers` | Parallel workers for enriched step (default: 3) |
| `--delay` | Seconds to wait before each Statcast call (default: 0.25) |
| `--quiet` | Disable progress bar |

## Storage

Raw feed files are stored as gzipped JSON (no indent) to reduce size. To compress existing `.json` files to `.json.gz` and remove originals:

```bash
python scripts/compress_raw_to_gz.py --dry-run   # preview
python scripts/compress_raw_to_gz.py             # run
```

See [docs/STORAGE_STRATEGY.md](docs/STORAGE_STRATEGY.md) for scaling and retention options.

## License

MIT
