# MLB Warehouse — Storage Strategy

## Current footprint (2 seasons: 2024 + 2025)

| Layer | Per season (approx) | 2 seasons | Main driver |
|-------|---------------------|-----------|-------------|
| **raw** (feed_live JSON) | ~5 GB | ~10 GB | ~2,950 files × ~1.7 MB/game |
| **pitches_enriched** (Parquet) | ~250 MB | ~500 MB | Already compact |
| **Schedules / manifests** | &lt; 1 MB | negligible | — |

**Total today:** ~12.6 GB for two seasons. Raw dominates (~95% of size).

---

## Scaling projection

- **Linear (raw kept as-is):** ~6 GB/season → 10 seasons ≈ **60 GB**, 20 seasons ≈ **120 GB**.
- **If raw is compressed or dropped after enrichment:** ~250 MB/season → 20 seasons ≈ **5 GB**.

So the main lever is **what you do with raw**, not pitches_enriched.

---

## Strategy options (from least to most aggressive)

### 1. **Compress raw on disk (recommended baseline)**

- After writing `game_*_feed_live.json`, gzip it and remove the uncompressed file (or write `.json.gz` from the start).
- Pipeline change: open with `gzip.open(raw_path, "rt", encoding="utf-8")` when the path ends in `.gz` (and keep supporting `.json` for backward compatibility).
- **Savings:** typically 70–85% on JSON (e.g. 5 GB → ~1 GB per season).
- **Trade-off:** Slightly more CPU when re-running enrichment; no loss of data.

### 2. **Write raw without pretty-print**

- Current code uses `json.dump(feed, f, indent=2, ensure_ascii=False)`, which inflates size.
- Switch to `json.dump(feed, f, ensure_ascii=False)` (no indent).
- **Savings:** often 30–50% on raw JSON with no behavior change.
- **Trade-off:** Raw files are harder to inspect by eye; trivial to apply.

### 3. **Compress-after-enrich (keep raw only while needed)**

- After successfully generating `pitches_enriched` for a game, gzip the corresponding raw file (or move it to an “archive” tree and compress there).
- Optionally **delete** raw after N days or after a full-season validation, if you are comfortable re-fetching from the API for rare re-runs.
- **Savings:** Same as (1) if you compress; 100% for that game if you delete.
- **Trade-off:** Re-enriching from raw requires either decompressing or re-downloading.

### 4. **Tiered retention**

- **Hot (local disk):** Current season raw (optionally compressed) + all seasons of pitches_enriched.
- **Cold (cloud or external disk):** Older seasons’ raw as `.json.gz` in S3/GCS or a USB/network drive; delete from laptop.
- **Savings:** Bounds local growth (e.g. cap at ~2–3 seasons of raw locally).

### 5. **Don’t keep raw long-term (most aggressive)**

- Keep only **pitches_enriched** (and schedules). Delete (or never write) raw once enrichment succeeds.
- Re-fetch feed from MLB API when you need to re-run enrichment.
- **Savings:** ~5 GB/season of raw eliminated.
- **Trade-off:** Re-enrichment depends on API availability and rate limits; no offline raw backup.

---

## Recommended path

1. **Short term (low risk)**  
   - **1 + 2:** Write raw without indent and compress to `.json.gz` (or gzip after write).  
   - Update the loader to support reading both `.json` and `.json.gz`.  
   - Run a one-off script to compress existing raw files.  
   - **Effect:** Same 2 seasons drop from ~10 GB raw to ~1–2 GB without losing data.

2. **As you add more seasons**  
   - **3 + 4:** Keep raw (compressed) only for the current (and maybe previous) season on your main machine; move or delete older raw.  
   - Keep **all** pitches_enriched locally (they’re small).

3. **Optional**  
   - If you never re-use raw except for initial enrichment, consider **5** for past seasons (delete raw after validated enrichment) and keep **1+2** only for the current season.

---

## Implementation (done)

- **Write raw:** New raw is saved as `game_{pk}_{date}_feed_live.json.gz` with no indent (`ensure_raw` in `load_mlb_warehouse.py`).
- **Read raw:** `process_pitches_enriched` and `find_raw_files` support both `.json` and `.json.gz`; `.gz` is preferred when both exist.
- **One-off compression:** Run `python scripts/compress_raw_to_gz.py [--warehouse data/warehouse/mlb]` to gzip existing `*_feed_live.json` and remove originals. Use `--dry-run` to preview.
