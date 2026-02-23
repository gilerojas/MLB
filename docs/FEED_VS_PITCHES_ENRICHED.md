# feed/live JSON vs pitches_enriched Parquet

Comparison for the same game (e.g. All-Star 2025: game_778566_20250715).

## How the pipeline works

| Step | Source | What is used |
|------|--------|----------------|
| 1 | **feed/live** | Only join keys + `play_id`: `game_pk`, `inning`, `at_bat_index`, `pitch_number`, `play_id` |
| 2 | **Statcast** (pybaseball) | All pitch-level columns (speed, spin, location, outcomes, etc.) |
| 3 | Merge | Left join Statcast on (game_pk, inning, at_bat_index, pitch_number); feed adds `play_id` |
| 4 | Output | Parquet keeps only `COLUMNS_TO_KEEP` (schema in `mlb_warehouse_schema.py`) |

So today **pitches_enriched is almost entirely Statcast**. The feed is used only to attach `play_id` for traceability. Anything in the feed that is not in Statcast (or has a different name/definition) is currently **not** in the parquet.

---

## Pitch count

- **feed/live**: 295 pitches (allPlays → playEvents with `isPitch` and `pitchData`)
- **pitches_enriched**: 295 rows  
→ Counts match; no pitches dropped by the join.

---

## What the feed has per pitch (that we do not pull)

### 1. **Timing** (feed-only in our output)

| Feed path | Type | In parquet? |
|----------|------|--------------|
| `startTime` | ISO8601 (pitch release) | No |
| `endTime` | ISO8601 (ball at plate / event) | No |
| `pitchData.plateTime` | float (seconds to plate) | No (Statcast has `effective_speed` but not time) |

Useful for: pace-of-play, time between pitches, plate time analysis.

### 2. **Strike zone (per-pitch from feed)**

| Feed path | Type | In parquet? |
|----------|------|--------------|
| `pitchData.strikeZoneTop` | float | Parquet has `sz_top` (from Statcast; may be at-bat level) |
| `pitchData.strikeZoneBottom` | float | Parquet has `sz_bot` |
| `pitchData.strikeZoneWidth` | float | No |
| `pitchData.strikeZoneDepth` | float | No |

Feed gives **per-pitch** zone; Statcast’s `sz_top`/`sz_bot` may be at-bat or game level.

### 3. **Umpire / call metadata** (feed-only)

| Feed path | Type | In parquet? |
|----------|------|--------------|
| `pitchData.typeConfidence` | float (e.g. 0.94) | No |
| `details.call.code` | string (e.g. "F", "S", "B") | Parquet has `type`: S/B/X and `description` from Statcast (different taxonomy) |
| `details.call.description` | string ("Foul", "Swinging Strike") | Parquet has `description` from Statcast |
| `details.code` | string | No (single-letter code) |
| `details.hasReview` | bool | No |

### 4. **Explicit booleans** (feed; parquet can derive from type/description)

| Feed path | In parquet? |
|----------|--------------|
| `details.isStrike` | Can derive from `type` / `description` |
| `details.isBall` | Can derive |
| `details.isInPlay` | Can derive from `type` / `events` |
| `details.isOut` | Can derive from `events` |

### 5. **Break/movement (feed vs Statcast)**

Feed `pitchData.breaks`: `breakAngle`, `breakLength`, `breakY`, `breakVertical`, `breakVerticalInduced`, `breakHorizontal`, `spinRate`, `spinDirection`.  
Parquet has Statcast’s `pfx_x`, `pfx_z`, `release_spin_rate`, `spin_axis` (and similar). Definitions and units can differ; feed break metrics are **not** currently in the parquet.

### 6. **Raw coordinates (feed)**

Feed `pitchData.coordinates`: `pX`, `pZ`, `pfxX`, `pfxZ`, `x`, `y`, `x0`, `y0`, `z0`, `vX0`, `vY0`, `vZ0`, `aX`, `aY`, `aZ`.  
Parquet has Statcast’s `plate_x`, `plate_z`, `pfx_x`, `pfx_z`, `release_pos_*`. So we have plate and release from Statcast; feed’s full trajectory/velocity vectors are not in the parquet (and COLUMNS_AUDIT explicitly drops raw physics vectors).

### 7. **Other feed-only**

| Feed path | In parquet? |
|----------|--------------|
| `details.ballColor` / `trailColor` | No (viz only) |
| `index` (event index in play) | No |
| `count.outs` (at pitch time) | Parquet has `outs_when_up` from Statcast |

---

## Summary: what you’re “missing” from the JSON

1. **plateTime** — time for ball to reach plate (good for stuff/approach analysis).  
2. **startTime / endTime** — per-pitch timestamps (pace, sequencing).  
3. **typeConfidence** — umpire call confidence.  
4. **strikeZoneWidth / strikeZoneDepth** — if you want per-pitch zone dimensions.  
5. **details.call.code** — feed’s single-letter call code (can differ slightly from Statcast `type`).  
6. **details.hasReview** — whether the call was reviewed.  
7. **Break metrics from feed** — if you want feed’s break angle/length/etc. in addition to Statcast.

---

## Recommendation

- **Keep** using Statcast as the main source for stuff and outcomes (it’s the standard for public analysis).  
- **Add** from the feed only what you need and that Statcast doesn’t provide:
  - **High value, low cost:** `plateTime`, `startTime` (and optionally `endTime`), `typeConfidence`, `details.call.code`, `details.hasReview`.
  - **Optional:** `strikeZoneWidth`, `strikeZoneDepth`, feed break metrics if you want to compare or prefer feed definitions.

Implementation: extend `extract_play_ids_from_feed()` (or add a parallel extraction) to build one row per pitch with these extra fields, then merge that with Statcast on (game_pk, inning, at_bat_index, pitch_number) and add the new columns to the parquet (and to `COLUMNS_TO_KEEP` if you want them in the curated output).
