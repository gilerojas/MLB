# Phase 0 — Data vs space “formula”

This is the recommended **default stack** for Mallitalytics: full workflow fidelity, smallest reasonable disk footprint, and **player bios** always available without repeating them in every game file.

## 1. Where the bytes go (one `feed_live`)

Typical split (minified JSON, see `notebooks/inspect_feed_size_and_efficiency.ipynb`):

| Block | ~Share | Role |
|--------|--------|------|
| `liveData.plays.allPlays` | **~63%** | Pitch-by-pitch truth; required for `play_id` merge + replay |
| `liveData.boxscore` | **~26%** | Game stats, officials, lineup context |
| `gameData.players` | **~7%** | **Bios** (name, age, position, bat/pitch, strike zone, etc.) — **duplicated per game** |
| Rest | ~1% | teams, venue, weather, linescore, … |

**Implication:** You cannot shrink raw dramatically without either (a) dropping pitch detail, or (b) moving big subtrees elsewhere. The wins are **compression**, **dedupe of bios**, and **querying from Parquet** instead of opening full JSON in daily jobs.

## 2. Recommended formula (tiers)

### Tier A — Always (baseline)

1. **`pitches_enriched` Parquet** per game  
   - Small (~order of **100×** smaller than uncompressed raw for pitch-level work).  
   - Primary surface for cards, exports, Statcast-backed metrics.

2. **Raw `feed_live` as `game_*_feed_live.json.gz`**  
   - Minified JSON + gzip (~**70–85%** saving vs pretty-printed or plain `.json`).  
   - Audit, re-merge, and anything that still needs full feed structure.

3. **One `players_registry.json` per season** at `{warehouse}/{season}/players_registry.json`  
   - Built by merging `gameData.players` on each ingest (see `src/ingestion/player_registry.py`).  
   - **Eliminates repeated ~7% × N games** for bios lookups (digest, API, cards copy).

### Tier B — Optional (more aggressive)

4. **Slim per-game JSON** (only after notebook/proof)  
   - Strip `gameData.players` from the stored per-game blob *if* you always join bios from `players_registry.json`.  
   - Extra savings ≈ **7% × N games** of raw; not enabled by default in the loader.

5. **Cold storage / delete old raw**  
   - Keep all Parquet; move or delete raw for seasons you rarely reprocess (re-fetch from MLB if needed).

### Tier C — Not recommended unless you accept risk

6. **No raw, only Parquet**  
   - Max space saving; re-enrichment depends on API + Statcast forever.

## 3. Order-of-magnitude sanity check

- **Raw only, many seasons:** ~**5–6 GB / season** uncompressed JSON historically → **~1 GB / season** with `.json.gz` (typical).  
- **Parquet:** ~**250 MB / season** ballpark for enriched pitches.  
- **Registry:** one JSON per season, **MB scale** for thousands of unique player IDs.

## 4. Code touchpoints

| Piece | Location |
|--------|-----------|
| Write `.json.gz` + update registry | `src/ingestion/load_mlb_warehouse.py` → `ensure_raw` |
| Registry merge | `src/ingestion/player_registry.py` |
| Inspect sizes | `notebooks/inspect_feed_size_and_efficiency.ipynb` (§7 multi-season table) |
| Older strategy notes | `docs/STORAGE_STRATEGY.md` |

## 5. One-line summary

**Keep:** `pitches_enriched` + `feed_live.json.gz` + `{season}/players_registry.json`.  
**Use Parquet for analytics; use registry for bios; use gzip raw for audit — that is the best default formula for data fidelity vs space.**
