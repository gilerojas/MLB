## Mallitalytics Daily Pitcher Card

The **Mallitalytics Daily Pitcher Card** is a single-game visualization built from a Statcast `pitches_enriched` parquet. It summarizes how a pitcher attacked hitters, what the ball did in flight, and how much damage they allowed, in a modern dashboard layout suitable for social sharing and automated reporting.

- **Script**: `scripts/mallitalytics_daily_card.py`  
- **Primary input**: one `*_pitches_enriched.parquet` file filtered to a single pitcher  
- **Primary output**: `outputs/pitching_cards/pitcher_card_<name>_<YYYY-MM-DD>[ _dark].png`

### Visual layout (top → bottom)

- **Header**  
  - Name, handedness, age, height/weight, team logo, neutralized MLB headshot.  
  - Game line: date and opponent.  
  - Box line: `IP, H, R, K, BB, HR` plus **Zone%**, **total whiffs**, **CSW%**, and **game-level GB%**.

- **Hard contact panel (left)**  
  - Catcher’s-view plot of all pitches over the plate with the strike zone grid.  
  - Hard-contact balls in play highlighted; option to overlay KDE density for “damage zones.”  
  - Shows whether the pitcher clustered damage in specific locations.

- **Movement profile (center)**  
  - Movement plotted in inches (`pfx_x_in`, `pfx_z_in`) with **arm/glove side normalized**: arm side is always on the correct side for the pitcher’s handedness.  
  - Per-pitch-type clouds with labeled centroids (pitch abbreviations).  
  - Optional **arm-angle ray** (degrees) when `arm_angle` is available.

- **Pitch tendencies by situation (right)**  
  - Horizontal bars for key count buckets: first pitch, pitcher ahead, two-strike, even, hitter ahead, full count.  
  - Each bar is a stacked distribution of pitch types; a badge highlights the **primary pitch** in each situation.

- **Pitch legend strip**  
  - Row of color-coded “pills” showing each pitch type, its abbreviation, and full name (e.g., `FF` – 4-Seam Fastball).

- **Arsenal table (bottom)**  
  - One row per pitch type plus an **All** row.  
  - Columns: `Pitch`, `#`, `Pitch%`, `Velo`, `Spin`, `Ext.`, `HB`, `IVB`, `Chase%`, `Whiff%`, `Str%`, `BS75+%`, `xwOBA*`.  
  - Rate columns are computed with **pitches as the denominator** so rows and the All line are consistent.  
  - `BS75+%` is the share of swings with bat speed ≥ 75 mph (no minimum swing count; `--` only when no tracked bat-speed data).

- **Footer**  
  - Short explanations for `xwOBA*`, hard contact, and `BS75+%`.  
  - Branding: `@Mallitalytics`, data sources.

### League-standard grading and benchmarks

To avoid per-game scales where everything looks “maxed out,” the card uses a fixed, league-wide benchmark JSON:

- **Benchmarks file**: `config/pitch_metric_benchmarks_2024.json`  
- **Built by**: `scripts/build_pitch_metric_benchmarks.py` (scans warehouse `pitches_enriched` files)

The benchmark file contains percentiles (p5, p20, p40, p60, p80, p95) for:

- `velocity_mph` – mean **release_speed** by (game, pitcher, pitch_type)  
- `whiff_per_pitch` – **whiff / pitches** by (game, pitcher, pitch_type)  
- `chase_per_pitch` – **chase / pitches** by (game, pitcher, pitch_type)  
- `strike_per_pitch` – **strikes / pitches** by (game, pitcher, pitch_type)  
- `xwoba_allowed` – mean **estimated_woba_using_speedangle** by (game, pitcher, pitch_type)

When rendering:

- The card infers the **season year** from `game_date` and loads `pitch_metric_benchmarks_<season>.json` if present.  
- For **Velo, Chase%, Whiff%, Str%, xwOBA*** the color gradients are anchored to league cutpoints (roughly p20 → p80, falling back to per-game ranges if no JSON is available).  
- All highlighted metrics keep the same **amber-style gradient**; only the mapping into that gradient is changed by the benchmarks.

### How the script works (high level)

`scripts/mallitalytics_daily_card.py` follows this pipeline:

1. **Load & filter**  
   - `load_game(parquet_path, pitcher_id)` reads the parquet and filters rows to the chosen pitcher.  
   - The script supports both `pitcher` and `pitcher_id` column names.

2. **Feature engineering**  
   - `process_pitches(df)` derives flags and helper fields:  
     - Swing / whiff / chase / in-zone / out-of-zone / strike flags.  
     - Movement in inches, balls in play, ground balls, hard-hit balls, “damage” balls (hard hit or high xwOBA).  
   - All **rate metrics** (Whiff%, Chase%, Str%, Zone%) are defined with **pitches as denominator**.

3. **Game summary & arsenal**  
   - `compute_box_score(df)` produces box stats and game-level metrics (Zone%, CSW%, GB%, BS75+%).  
   - `group_arsenal(df)` aggregates by `pitch_type` and computes usage, velo, movement, spin, extension, rates, GB% on balls in play, hard-hit%, and `fast_swing_pct` (BS75+%).

4. **Rendering**  
   - `render_card(parquet_path, pitcher_id, output_path)` pulls everything together, lays out the Matplotlib grids, and writes the PNG to `outputs/pitching_cards/`.

### CLI usage (script entry points)

From the repo root:

- **Default card (configured game & pitcher)**  

```bash
python scripts/mallitalytics_daily_card.py            # light theme
python scripts/mallitalytics_daily_card.py --dark     # dark / analytics theme
```

- **Specific pitchers on a specific date**  

```bash
python scripts/mallitalytics_daily_card.py \
  --pitchers 663460,690953,701542 \
  --date yesterday

python scripts/mallitalytics_daily_card.py \
  --pitchers 663460 \
  --date 2025-03-22
```

- **Random showcase game from the warehouse**  

```bash
python scripts/mallitalytics_daily_card.py --random
python scripts/mallitalytics_daily_card.py --random --dark
```

The card script is designed to be **batchable and AI-friendly**: it has a stable CLI, well-defined inputs/outputs, and compact, structured panels that can be interpreted or captioned automatically.

