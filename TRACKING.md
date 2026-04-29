# Mallitalytics MLB — Project Tracker

Living doc shared between Claude Code and Cursor.
**Update this file when work completes, bugs are found, or new tasks are added.**

---

## Done

### Infra rename: **mlbops** (Apr 2026)

- **`MalliOps/` → `mlbops/`** — FastAPI + Next stack lives under `mlbops/` (distinct from fantasy **MalliHub**).
- **Environment variables:** `MALLIOPS_*` → **`MLBOPS_*`** (`MLBOPS_HUB_PORT`, `MLBOPS_ALLOW_INTEL_RUN`, `MLBOPS_UVICORN_RELOAD`, `MLBOPS_STRICT_CORS`, tweet/redraft caps, etc.). Update `mlbops/.env` from keys in `mlbops/.env.example`; old names are no longer read.
- **Scripts:** `scripts/start_mlbops.sh`, `scripts/pull_mlbops_from_drive.sh`; **`./start_hub.sh`** remains the usual entrypoint from repo root.
- **Theme:** `localStorage` key **`mlbops-theme`**; legacy **`malliops-theme`** is read once on load and migrated (see `ThemeToggle.tsx` + `layout.tsx` beforeInteractive script).

### Hub brand mark in shell (Apr 2026)

- **`mlbops/hub/components/MalliBrandMark.tsx`** — SVG bar chart + trend + orange accent (fixed brand colors).
- **`mlbops/hub/components/HubTopBar.tsx`** — Logo + Mallitalytics + `MLB V2.0` headline, optional `ROOT / …` breadcrumb on `xl+`, brand links to `/`.
- **`mlbops/hub/components/NavSidebar.tsx`** — Desktop rail has **no** repeat of logo/title (headline only in `HubTopBar`); mobile sticky bar keeps text home link only.

### Hub readability + Schedule logos + Statcast Insights (Apr 2026)

- **`globals.css`** — `html` base `118%`, plus `1600px` / `2200px` breakpoints (`125%` / `132%`) so type scales on large monitors.
- **Typography sweep** — Replaced fixed `text-[9–13px]` pixel classes with `text-xs` / `text-sm` across hub components and pages; leaderboards table body uses `text-base 2xl:text-lg`.
- **Page shells** — Main hub routes use `max-w-[1800px] mx-auto px-8 2xl:px-12` where applicable (dashboard, insights, intel, schedule, cards, watchlist, leaderboards).
- **Team logos** — `schedule/page.tsx` `TeamLogo`: light tile (`bg-white/95`), no `opacity-80` on SVGs. Dashboard `page.tsx` wraps MLB static logos the same way.
- **`GET /insights/statcast`** — New bundles: `bs75_leaders`, `pitch_rv100_best`, `pitch_rv100_worst`, `batter_xwoba`, `batter_luck`; `_WANT_COLS` adds `bat_speed`, `delta_run_exp`, `pitch_name`.
- **`app/insights/page.tsx`** — New Statcast tiles + batter luck two-column section; RV/100 + BS75 row under pitching.

### Batter card — parquet path + API + Intel queue (Apr 2026)

- **`scripts/batter_card_daily.py`** — `--parquet` + `--batter` (no `feed_live`), `find_parquet_for_batter_on_date`, `parse_batter_game_from_parquet`, `--batters/--date` falls back to parquet when raw feeds are missing, `--output-suffix`, `generate_batter_card(..., feed_path= | parquet_path=)`, Card JSON **`schema_version` 2** with `box`, `pa_log`, `batted_balls`, `recent_context`, `batter_tweet_context`, `source_parquet`.
- **`mlbops/api/routers/cards.py`** — `POST /cards/batter` accepts `feed_path`, `parquet_path`, or `game_date`; always passes `--output-suffix` to avoid PNG collisions.
- **`mlbops/api/intel_standouts.py`** — Batter rows include optional `parquet_path` when `game_{pk}_{date}_pitches_enriched.parquet` exists under the warehouse.
- **`IntelStandoutsPanel.tsx`** — Queue enabled when `feed_path` **or** `parquet_path` is present; POST body sends whichever is available.

### Morning intel — start / BBE / PA windows + watchlist pulse (Apr 2026)

- **`src/pitcher_recent.py`** / **`src/batter_recent.py`** — shared pools for last N starts and BBE / PA pitch-row windows.
- **`morning_intel/morning_intel.py`** — `run_intel` parquet lookback uses `BASELINE_MAX_CAL_DAYS` only; anomalies via `detect_pitcher_anomalies(df, anchor)` / `detect_batter_anomalies(df, anchor)` (no `WINDOW_DAYS` loop); `build_watchlist_pulse` uses the same windows as anomalies; digest + queued card tweets use `window_label` with legacy `window_days` fallback; `enrich_pitch_features` coerces `at_bat_number`.
- **Hub** — `intel/page.tsx` `anomalyBlurb`, dashboard `Top Anomalies`, `WatchlistClient` column **Pulse** + fallback from `watchlist_pulse_detail.pulse_summary`.
- **Intel transactions + logos** — Snapshot JSON includes `transactions_detail` (`line`, `team_id` from MLB Stats `toTeam`/`fromTeam` when the side is a big-league club); hub Intel page uses it so each transaction row shows the same 28px team logo treatment as Milestone watch (placeholder tile when no MLB id).

### Card copy + redraft — headline events, sample gates, batter parity (Apr 2026)

- **`scripts/batter_card_daily.py`** — Parquet PA rows use `home_score`/`away_score` deltas for runs/RBI proxy; `_derive_notable_batter_events` (grand slam, multi-HR, big RBI); snapshot adds `notable_game_events`, richer `batter_tweet_context` (`hero_headline`, EV/bat speed hooks).
- **`scripts/mallitalytics_daily_card.py`** — `_derive_notable_pitcher_events` (no-hitter through 8 / first hit late, one-hit deep outings); stricter pitch-type xwOBA in `_derived_pitcher_tweet_context` (`MIN_PITCHES_FOR_XWOBA_BEAT` + `MIN_BIP_FOR_XWOBA_BEAT`); `best_xwoba_pitch` / `worst` include `n_pitches`, `n_bip`.
- **`mlbops/api/routers/queue.py`** — Split redraft prompts (`_prompt_pitcher_card`, `_prompt_batter_card`, `_prompt_generic`); batter personas + `_compute_batter_standout_signals` + `_batter_redraft_beat_sheet`; `_pitch_row_xwoba_supported` gates pitcher xwOBA signals; `notable_game_events` → `must_mention` signals; `_sanitize_redraft_output` for batter char band; `_get_recent_batter_tweets`.
- **Follow-up (Apr 2026)** — Contrarian persona no longer nudges bogus “ERA going out” on pitch types; pitcher/batter redraft prompts require honoring `notable_game_events` in the open and ban ERA in/out unless both sides are real ERA fields. Pitcher no-hit bid detection scans any pitch in the PA for hit events and treats all hits in the 9th+ as a through-8 no-hit bid. Batter `pa_log` adds `bases_loaded` / feed `grand slam` text + Statcast join so grand slams populate `notable_game_events` when PA RBI is thin.
- **Batter feed RBI** — `parse_batter_game` no longer trusts `result.rbi` before summing: it merges Statcast `home_score`/`away_score` deltas per PA (when parquet joins exist), applies a grand-slam floor when bases are loaded or the feed says “grand slam”, then sets game `rbi` from the sum of `pa_log` so the card line and JSON stay consistent.
- **`mlbops/api/paths.py`** + **`mlbops/api/main.py`** — `get_redraft_batter_tweet_target_range` + `/system/paths` keys `redraft_batter_tweet_min/max`.
- **`mlbops/api/routers/cards.py`** — Default queue tweets from card JSON (`_default_*_tweet_from_meta`) when hero/notable events exist.
- **`mlbops/.env.example`** — `MLBOPS_REDRAFT_BATTER_TWEET_MIN/MAX` documented.
- **`jobs/daily_card_generator.py`**, **`IntelStandoutsPanel.tsx`** — Label-first card prefixes removed from default `tweet_text` (name | date | tags).

### mlbops Hub — Stitch 2 UI (frontend-only)

- **Design system:** `mlbops/hub/app/globals.css` — Stitch palette on `data-theme="dark"` (`#09141f` background, surface tokens, outline `#a68b7f`), Inter + Space Grotesk + JetBrains Mono via `layout.tsx`, Material Symbols link, zero default radius, dot grid (`.hub-grid-bg`), `.article-card` hover, optional `surface-container` / tertiary theme colors.
- **Shell:** Fixed `176px` sidebar + `HubTopBar` (`LIVE` → `/`, `SCHEDULE`, `TRANSACTIONS` → `/intel`); `NavSidebar` Material icons, existing route labels preserved; `ThemeToggle` contrast row on desktop.
- **Pages reskinned:** Dashboard (`app/page.tsx`), Intel + `IntelStandoutsPanel`, Schedule game cards + date rail, Queue + `QueueClient` (toolbar, split layout, footer strip); light header/padding pass on Cards, Insights, Leaders, Watchlist, Settings.

### Notebooks — Fast swing (BS75+) study

- `notebooks/fast_swing_outcome_study.ipynb` — defines BS75+% / `fast_swing_pct` like `mallitalytics_daily_card.py` (tracked swings, 75 mph threshold); loads 2025–2026 regular-season `pitches_enriched` via `MLB_WAREHOUSE_DIR` (optional subsample env vars); compares fast vs slower tracked swings; by–pitch-type tables + correlations + plots; takeaways for card copy

### Card headshots — black silo backgrounds

- **Cause:** `_neutralize_headshot_background` only replaced **green/teal** MLB studio backdrops. Newer CDN silos (incl. some Mets assets) use **black/charcoal**, which never matched and stayed black on light cards.
- **Fix:** `src/mlb_headshot.py` — composite **RGBA** onto white (avoids black halos from alpha), keep green mask, then **Pillow `floodfill`** from edge seeds for dark neutral backdrops connected to the border. Wired into `mallitalytics_daily_card.py`, `batter_card_daily.py`, `batter_card_seasonal.py`.

### Pitcher card — Chase%, Whiff%, BS75+% denominators

- **Chase%**, **Whiff%**, and **BS75+%** in the arsenal table now use **swings** as the denominator (game box BS75+ matches: fast bat speed swings ÷ all swings). **Str%** and **Zone%** stay **per pitch**. `build_pitch_metric_benchmarks.py` outputs `whiff_per_swing`, `chase_per_swing`, `fast_swing_per_swing`; existing `pitch_metric_benchmarks_*.json` without those keys still color via legacy `whiff_per_pitch` / `chase_per_pitch` until regenerated.
- `notebooks/fast_swing_outcome_study.ipynb` — repo-root setup now handles both notebook execution (no `__file__`) and exported/script execution via `try: Path(__file__) ... except NameError: Path.cwd()` (including pasted `bat_speed_continuous` and `fast_swing_by_count` cells); `fast_swing_by_count` `prep()` `_bucket` row indexing fixed (`balls_int` / `strikes_int`, not `balls` / `strikes`)

### Queue — Twitter media OAuth (code 32)

- `hub/lib/twitter.ts` — media INIT/FINALIZE no longer duplicate form params in URL query + body (OAuth signature mismatch → “Could not authenticate you”)

### Queue / X Premium (long posts)

- `MLBOPS_TWEET_MAX_CHARS` in `mlbops/api/paths.py` (default 10000) — cards, queue PATCH/redraft, Claude prompt
- `GET /system/paths` includes `tweet_max_chars`; Queue UI counter + Post button use it
- Hub `PATCH /api/queue/[id]/tweet-text` slices with same env; `start_hub.sh` sources `mlbops/.env` so Next sees the var

### HR tracker

- PNG lists all HRs (scaled layout); tweet script defaults to top 5 lines + “(+N more on card)” via `hr_tracker_daily` / `image_gen`
- `image_gen.py` — fixed bottom cutoff (`row_h * nrows` could exceed canvas when `max(38,…)` forced tall rows); stack name/stat/meta with measured line heights (no EV vs venue overlap); tighter 2-col threshold for 20+ HR slates; slightly larger footer reserve
- HR table card (`render_hr_tracker_image`): English copy; `last_name_with_generational_suffix` so Robert Jr. / Tatis Jr. don’t render as bare Jr.; reads `ev_mph`/`distance_ft`/`team_abbrev` from pipeline; brand hex via `mallitalytics_style`
- `image_gen.py` — EV and DIST. use per-day min/max heat (smoothstep + muted gold→orange), not a hard ~110 mph cliff to gray; small cleanups (specific `except`, empty-slate save mkdir)
- `image_gen.py` — batter column uses neutral `slate` (no EV-based tint); heat only on DIST./EV

### Launch station — Games of Day (visual)

- `scripts/games_of_day_board.py` — same data + layout as probables (facescans, W-L, ERA) with header `GAMES OF DAY // DATE`; queue tweet is ET slate lines (`MLB slate for Apr 8 #Mallitalytics` + matchup list + footer; `(+N more on the card)` if over cap)
- Hub Queue quick generate — `AbortSignal.timeout(180s)` + safe JSON parse for long `/cards/*` runs (probables headshots can take a minute; avoids opaque `TypeError: Failed to fetch` when the browser aborts or the body is non-JSON)

### Launch station — Probables board

- `src/probables_board/` — schedule + `probablePitcher` hydrate, batched season pitching stats (W-L, ERA), PIL table 1200×675 (charcoal / burnt orange header)
- `scripts/probables_board_daily.py` — `--format all` prints `--- Tweet ---` + `Image:` path (for API stdout parsing)
- `POST /cards/probables-board` → queue row `content_type=probables_board` with PNG + short tweet (“W-L and ERA on the card”)
- Hub Queue quick generate: **Probables board** button; Launch icons use symbols (● ◆ ▶) per no-emoji UI rule
- `src/probables_board/render.py` — every row now uses MLB headshots, rounded matchup cards, soft shadow/panel depth, compact W-L / ERA pills, and a less linear two-column slate layout

### Card snapshot JSON (queue / AI redraft)

- `scripts/mallitalytics_daily_card.py` — opponent (`vs` on card / JSON): **majority defensive team** from `inning_topbot` (Top = home pitches, Bottom = away pitches), case-insensitive; **MLB boxscore** home/away abbrevs when parquet teams missing or duplicate; one **boxscore HTTP** per card shared with official line; sanity flip if opponent matched pitcher team while bio matches a side
- `scripts/mallitalytics_daily_card.py` — `from __future__ import annotations` + `date` import so Python 3.9 / older mlbops interpreters do not SyntaxError on `int | None` / `tuple[...]` hints at `_parquet_game_pk_date`
- `scripts/mallitalytics_daily_card.py` — after save, prints `--- Card JSON ---` … `--- End Card JSON ---` on stdout; writes `{stem}_card.json` beside the PNG (`outputs/pitching_cards/`)
- `scripts/batter_card_daily.py` — same pattern (`outputs/batter_cards/`)
- Snapshot includes `schema_version` **2** for pitcher cards, `card_type`, ids, `player_name`, game/opponent, `box`+`arsenal`; plus `**recent_outings**`, `**outing_context**`, `**recent_prior_summary**`, `**pitch_tendencies_by_situation**` (count-state mix aligned with the tendencies panel), `**pitcher_tweet_context**` (top-2/3 usage share, primary pitch line, game BS75+ rate, form_hints, optional tendency_highlight, best/worst xwOBA pitch with n>=5), and optional **`season_pitching_stats`** (MLB Stats API cumulative season line at card generation: ERA, WHIP, IP, games, etc.; may lag same-day games)
- `mlbops/api/routers/cards.py` — parses stdout → `insert_queue_item(..., meta=, player_name=, game_pk= when present)`
- `mlbops/api/routers/queue.py` — redraft: **short-first** `pitcher_card` + `batter_card` paths (targets `MLBOPS_REDRAFT_PITCHER_*` / `MLBOPS_REDRAFT_BATTER_*`, default 220–320); pitcher **beat sheet** + personas; batter **beat sheet** + `_BATTER_PERSONAS` + headline-event `must_mention` signals; `get_redraft_max_tokens()` (default 320); `_sanitize_redraft_output` trims runaway posts per card type; `GET /system/paths` exposes redraft bands + max_tokens
- `mlbops/api/paths.py` — `truncate_tweet_text_to_cap()` word-safe cap (used by queue PATCH, redraft, card tweets); hub `tweetMaxChars.ts` mirrors for PATCH tweet-text
- `scripts/mallitalytics_daily_card.py` — pitch tendencies panel: coerce/clip/round `balls`/`strikes` before bucketing (raw >3 balls / >2 strikes or NaN used to drop rows from the `n=` sums vs total pitches); optional **OTHER** row when any pitches still sit outside the six standard masks; JSON `situation_key` `other_count`
- `GET /system/paths` includes `redraft_meta_max_chars`; `.env.example` documents the env var

### mlbops Hub — themes (MALLITALYTICS_BRAND.md)

- `hub/app/globals.css` — CSS variables for **dark** (charcoal `#1A2530`, off-white text, burnt orange accent `#E8712B`, slate/muted gold/forest/light-green/soft-red) and **light** (warm cream `#EDE8E0`, dark teal text `#2C3E50`, same accent)
- `data-theme` on `<html>` — default dark; `localStorage` key `mlbops-theme`; `beforeInteractive` script in `layout.tsx` to avoid flash
- `ThemeToggle` + sidebar placement; hub pages refactored from raw hex to semantic tokens (`bg-background`, `text-accent`, `border-border`, `bg-info`, etc.)

### mlbops Hub (FastAPI + Next.js)

- **Queue “Failed to fetch” while Schedule seemed fine** — Schedule is an RSC (server fetch → API, no CORS). Queue is `use client` (browser fetch → API → needs CORS). FastAPI now merges `MLBOPS_HUB_PORT` into origins and, unless `MLBOPS_STRICT_CORS=1`, allows `http://127.0.0.1|localhost` with any port via `allow_origin_regex`.
- **Next.js dev crash → browser -102 / connection refused** — terminal showed `TypeError: api.createContextKey is not a function`; fixed with direct dependency `@opentelemetry/api` (^1.9) in `mlbops/hub/package.json` (correct OTel API resolution on Node 23+). If the hub URL fails, check the `start_hub.sh` terminal output first.
- Full hub under `mlbops/` — FastAPI port 8000, Next.js port 3000
- Startup: `./scripts/start_mlbops.sh` or `./start_hub.sh`
- `start_hub.sh` — if port 3000 is already in use (another local Next app), mlbops Next may fail to bind while the script still prints `:3000` → browser shows the *wrong* app; script now preflights with `lsof` and exits with the conflicting PID; optional `MLBOPS_HUB_PORT` + auto CORS append for non-3000
- NavSidebar: Briefing, Intel, Watchlist, Leaders, Insights, Schedule, Cards, Queue, Settings
- Path config in `mlbops/api/paths.py` — all paths env-overridable; `get_warehouse_dir()` ignores doc placeholders still left in `MLB_WAREHOUSE_DIR` (`path/to/your`, `path/to/local` + `mirror`) and uses `{repo_root}/data/warehouse/mlb` so Leaders/Insights work without editing `.env` after copy-paste from examples
- **`safe_is_dir()`** (`paths.py`) — warehouse/snapshot `is_dir()` checks treat Google Drive File Stream `TimeoutError` / `OSError` as “not available” so `/briefing`, `/system/paths`, leaderboards, etc. do not 500 when the mount is slow; prefer local `data/warehouse/mlb` after rclone for reliable reads
- **Card scripts + Drive** — `scripts/mallitalytics_daily_card.py` and `scripts/batter_card_daily.py` use the same env rules as `api.paths` (strip quotes, ignore doc placeholders) plus `_safe_is_dir` / `_safe_exists_warehouse` so File Stream `stat` timeouts do not traceback; globs wrapped in `OSError`/`TimeoutError`; hub `/leaderboards` fetch uses a 120s client timeout with a clearer timeout message
- **FastAPI `load_dotenv(..., override=True)`** — `mlbops/.env` overrides shell exports so a leftover `MLB_WAREHOUSE_DIR` in `~/.zshrc` cannot point the API at Google Drive after you fix `.env`; **`safe_non_empty_file()`** in `paths.py` + leaderboards CSV / parquet glob / stat paths avoid 500s when a Drive path still times out

### Pages

- `/` Briefing — KPIs, anomalies, milestones, roster moves, probables
- `/intel` — last 7d snapshots with anomaly feed
- `/queue` — card approval/rejection/redraft workflow, Twitter posting
- `/leaderboards` — batting/pitching leaders, season + sort filters
- `/watchlist` — player watchlist editor synced to JSON + DB
- `/schedule` — today's games with team logos (MLB static CDN), score, status badges
- `/cards` — player search → game log → generate card inline → preview → queue link
- `/insights` — Statcast analytics dashboard
- `/settings` — Drive sync button, notifications test, watchlist preview

### Insights → Launch station

- Each insight tile header includes a **Tweet** control → `POST /queue/insight-draft` inserts `content_type=insight_tile` (text-only; `game_date` = local as-of YYYY-MM-DD; `meta_json` has `insight_key`, rows, optional `pitcher_role`) for redraft/post from Queue
- **SQLite migration:** older `data/hub.db` had a `content_type` CHECK that omitted `insight_tile` / `probables_board` / `text_only`. Run once: `cd mlbops && ../mlb_env/bin/python -m api.db.migrate_queue_content_types` (`mlbops/api/db/migrate_queue_content_types.py`)

### Insights Page (`/insights`)

- Boxscore section: HR, OPS, K, ERA tiles from `/leaderboards`
- Pitching arsenal: fastball whiff%, hardest throwers, chase%, spin rate (from Statcast parquets)
- Contact quality: barrel%, exit velocity
- Luck & regression: lucky / unlucky pitchers (xwOBA vs wOBA delta)
- Value bars, 8 rows per tile, no emojis, season picker
- Graceful fallback banner if Statcast parquets missing

### Statcast Backend (`mlbops/api/routers/insights.py`)

- Bundles: fastball_whiff, hardest_throwers, pitcher_luck, exit_velocity, barrel_leaders, spin_rate, chase_kings
- Pitcher names via `data/warehouse/mlb/{season}/players_registry.json`
- Batter names via `player_name` col in pitches_enriched (confirmed = batter name, not pitcher)
- In-process cache keyed to file fingerprint — auto-invalidates on new games
- `min_bip=8`, `min_pitches=20` defaults (early-season friendly)

### Drive Integration

- `POST /system/sync-drive` endpoint (runs `scripts/pull_mlbops_from_drive.sh`)
- Drive sync button in `/settings` with inline output log

### Cards script warehouse path fix

- `scripts/batter_card_daily.py` — `_warehouse_root()` now respects `MLB_WAREHOUSE_DIR` env var
- `scripts/mallitalytics_daily_card.py` — `--pitchers` branch now uses `MLB_WAREHOUSE_DIR` env var
- `scripts/mallitalytics_daily_card.py` — `--pitchers` / `--date` no longer runs `warehouse_root.rglob("*pitches_enriched.parquet")` (was multi-minute on large mirrors); only `{year}/{stage}/pitches_enriched/game_*_{date}_pitches_enriched.parquet`

### Schedule improvements

- Multi-day date navigation tabs (yesterday, today, +4 days) via URL `?date=YYYY-MM-DD`
- Probable pitchers per game card (from `probablePitcher` hydration in schedule API)
- Team W-L records shown under each team name
- Scheduled start time shown in score column for unstarted games

### Insights page cleanup

- Removed all value/progress bars — stats have different dimensions
- Unicode section symbols replacing emojis
- Boxscore **pitching** tiles (K, ERA): **All / SP / RP** toggle — `GET /leaderboards/pitching?pitcher_role=all|starter|reliever` filters by `games_started` vs relief apps (`games - games_started`); parquet rollup sums `games` + `games_started` across teams; response `pitcher_role_filter_supported`; Insights UI splits batting vs pitching and puts toggle in pitching panel header; sublabels hide SP/RP wording when unsupported
- Insights page **section order**: all **batting** (boxscore HR/OPS → Statcast contact) first; then **pitching** block (border separator, boxscore K/ERA panel → arsenal → luck)
- **SP/RP leaderboard filter without parquet** — `boxscore_aggregate.aggregate_boxscore_by_player_team` (and `aggregate_boxscore_from_raw`) now sum `gamesPlayed` / `gamesStarted` from each `feed_live` game into `games` / `games_started`; `season_exports.player_team_boxscore_rows` passes them through; `_rollup_pitching_by_player` already sums them; leaderboard live cache key version bumped (`_PITCHING_LIVE_CACHE_VER`) so FastAPI picks up new columns after deploy
- **Insights SP/RP for all pitching** — `leaderboards.pitching_pitcher_ids_for_role` + `GET /insights/statcast?pitcher_role=` filters pitch-level DataFrame before pitcher bundles (whiff, velo, chase, spin, luck); batters unchanged; hub passes `pitcher_role` from one control above boxscore + Statcast pitching

### Intel page

- **Game standouts** — `GET /intel/daily-standouts?window=yesterday|7d|14d|month&limit=…` prefers **`{year}/regular_season/raw`** `feed_live` finals; **if no feeds match the window**, fills from **MLB Stats API** (schedule + per-game boxscore, regular `R` + Final only, boxscore cap ~220). Response includes **`data_source`** (`warehouse`|`api`), **`source_note`**, **`api_boxscores_fetched`**. API rows have `feed_path: null` — **pitcher** Queue still works; **batter** Queue needs synced feeds (hub disables Queue + shows “—”). Served via `run_in_threadpool`.
- Intel feed — `/intel/snapshots?limit=1&days=120` (latest snapshot only; no stacked past days). Watchlist pulse uses the same.
- **Watchlist pulse** — snapshot field `watchlist_pulse_detail`: per-player cards (last-7d Statcast lines + **OPS or ERA this season vs prior season** via MLB Stats API); hub falls back to plain `watchlist_pulse` strings on older JSON
- Milestone watch — `milestones_detail` + logos; expanded season marks (2B, 3B, batter SO, pitcher K/SV/CG, HR, H); **round-robin by stat** (SV/CG/K/3B/2B/bK/HR/H) so one category does not dominate; pitching HR/hits-allowed excluded from rules; card rows + stat chips
- **Pitcher HR in milestone watch** — `should_skip_hitting_milestones`: MLB **primary position** P/SP/RP + PA under 80 skips batting chase rows; fallback **≥0.1 IP or ≥1 game pitched** + low PA; **≥80 PA** never skips (two-way / real hitter)
- Anomalies block — human-readable metric labels (pitch mix, whiff/chase, xwOBA, velo), plain-language blurbs, `max-h` scroll, single-column layout when only pitcher or only batter anomalies
- Team logos in transaction rows — static team name → ID map, extracted from description text

### Briefing / home load time

- `/briefing` warehouse freshness — replaced full-tree `rglob` with bounded `{year}/{stage}/pitches_enriched/*.parquet` scan (matches insights layout); avoids multi-minute blocks on large mirrors
- Hub `/` — `fetch(/briefing)` uses `AbortSignal.timeout(60s)` so the page surfaces an error instead of loading indefinitely

### Cards / Anthropic / Intel UI

- `GET /cards/players/{id}/games` — feed_live index uses `{season}/{stage}/raw/` only (no full warehouse `rglob`); fixes multi-minute hang after player search (e.g. Ohtani)
- Anthropic: set `ANTHROPIC_API_KEY` in `mlbops/.env` (FastAPI + morning_intel both load it)
- Intel page — removed duplicate "Probable pitchers" rail (Schedule page is canonical)

### API responsiveness (event loop)

- Heavy sync routes moved to `starlette.concurrency.run_in_threadpool` so pandas / `requests` do not block other requests: `/insights/statcast`, `/leaderboards/*`, `/schedule/*`, `/briefing`, `/cards/players/*`
- Hub `getApiBase()` — browser uses page hostname + `:8000` when `NEXT_PUBLIC_FASTAPI_URL` unset, but maps **localhost → 127.0.0.1** for the API host (avoids IPv6 `::1` vs uvicorn `0.0.0.0:8000` “Failed to fetch”); server defaults to `127.0.0.1:8000` or `FASTAPI_BASE_URL`
- Queue page — `fetchSummary` / `fetchItems` catch network errors and show a banner when FastAPI is down (not only CORS)
- **Second pass — anything still sync blocked the whole API:** `/intel/snapshots` (especially `include_body=true` JSON reads), `/intel/snapshots/{anchor}`, `POST /intel/run`, `POST /system/sync-drive`, all `/queue/*` (incl. Claude redraft), `/watchlist/*`, `POST /cards/*` (subprocess card scripts)
- `start_hub.sh`: set `MLBOPS_UVICORN_RELOAD=0` to disable `--reload` for less overhead while debugging slowness

**Why it felt “everything loading”:** Uvicorn serves all routes on one asyncio loop. Any **synchronous** disk I/O, `subprocess.run`, or HTTP client call in a `def` route handler **blocks every other request** until it returns. The Intel hub page alone called `/intel/snapshots?include_body=true` (large JSON parses) while Insights fired five parallel calls — one blocking handler stalled the rest.

---

## Known Bugs / Needs Verification

- **Pitcher luck tiles** — groupby bug fixed. Needs visual confirmation in UI.
- **Pitching arsenal tiles** — same fix. Needs confirmation all 4 tiles populate.
- **Batter names (exit velo / barrel)** — `player_name` col = batter name, now mapped correctly. Needs confirmation no more "ID 572233" rows.
- **ERA leaders "No data"** — `min_ip=8` filter may be too high for 2026 early season. Lower if needed.
- **Cards parquet error fixed** — verify by generating a card from the UI with real warehouse data.
- **Intermittent POST /cards/pitcher 500** — mitigations: unique `--output-suffix` per request (no concurrent overwrite of same PNG); `_extract_saved_path` uses last `→ Saved:` line; `_image_url` resolves paths for symlink/realpath mismatch; `insert_queue_item` retries on SQLite locked/busy.
- **Pitcher card POST 500 with no stderr** — `mallitalytics_daily_card.py` could exit 0 after skipping every pitcher (under MIN_PITCHES in all parquets for that date), so FastAPI saw no PNG. Script now `sys.exit(1)` with stderr hints; `/cards/pitcher` includes stdout/stderr tails when PNG is missing after exit 0.
- **Schedule probable pitchers** — verify API returns `probablePitcher` for upcoming games.

---

## Backlog

### Insights

- Batter luck tile — xwOBA vs wOBA for batters (over/underperformers)
- K% leaders (batters) — most strikeout-prone
- WHIP / BB9 pitching control tiles
- Time-range filter (7d / 14d / season) using game_date in parquets
- "Refresh" button per tile without full page reload
- Link from insight row → generate card for that player

### Cards

- **Card JSON `recent_outings`** — fill from warehouse (last N pitcher/batter games) for trend copy; optional pre-aggregates table later. Early 2026: usually `[]` or one prior row.
- Season picker (currently hardcodes current year)
- Bulk generate: select multiple games at once

### Schedule

- Past dates navigation (currently only today + next 4 days; add backward nav)

### Leaderboards

- Column picker (hide/show)
- Player name → generate card link

### General

- Mobile nav is crowded with 8 items — consider collapsible or icons
- Loading states for server components (Briefing, Schedule)

---

## Architecture Notes

### Pipeline order (ops)

1. **Local mirror** — Sync from Google Drive (Hub Settings, or `scripts/pull_mlbops_from_drive.sh`) so `data/.last_drive_sync` updates; `MLB_WAREHOUSE_DIR` points at the tree that holds `{season}/{stage}/pitches_enriched/`.
2. **Enriched pitch files** — If parquets for a game date are missing, run the ingest path your mirror uses (e.g. CI `daily_ingest` or local scripts) so `game_*_YYYYMMDD_pitches_enriched.parquet` exists; card scripts reject dates with no files on disk.
3. **Intel** — `morning_intel/morning_intel.py` writes `morning_intel/snapshots/intel_YYYY-MM-DD.json` (or trigger **Regenerate** in the hub when `MLBOPS_ALLOW_INTEL_RUN=1`).
4. **Post** — Queue / Cards / X use the same local paths; **`GET /system/readiness`** (FastAPI) and the **Pipeline readiness** block on the dashboard summarize this in one pass.

- `player_name` in `pitches_enriched` parquets = **batter** name (Statcast convention)
- Pitcher names → `data/warehouse/mlb/{season}/players_registry.json`
- **Google Drive vs local:** Drive is the **canonical** store; the API and Python scripts only read **files on disk** under `MLB_WAREHOUSE_DIR` (default `data/warehouse/mlb`). Nothing pulls from Drive over the network per request — use rclone (`pull_mlbops_from_drive.sh` or Hub Settings → Sync) or point `MLB_WAREHOUSE_DIR` at a Drive File Stream mount path.
- **Pitcher cards** require `pitches_enriched` parquets for that game date; the MLB Stats API game log in the UI is not enough to render the card.
- **Queue `meta_json`** stores the card snapshot for redraft; optional sidecar `*_card.json` next to the PNG matches it for disk backup / debugging.
- **Scaling with Drive:** see `docs/WAREHOUSE_DRIVE_WORKFLOW.md` — mirror refresh via cron (`jobs/crontab.example`), Hub Settings → Sync, or `MLB_WAREHOUSE_DIR` on a Drive File Stream path. CI (`daily_ingest.yml`) already pushes the canonical warehouse to Drive.
- Hub reads from local mirror; use Settings → Sync to refresh
- No emojis in hub UI — user preference