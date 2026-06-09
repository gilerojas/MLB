# Session 4: Fantasy Streamer Matrix Backend

Date: 2026-05-13
Status: Implemented

## Goal

Add the first fantasy-useful backend product from the upscale roadmap: a Streamer Matrix that can surface pitcher streaming candidates without turning Mallitalytics into generic fantasy advice.

This session is backend-first. The hub UI and queue integration are intentionally reserved for Session 5.

## Why This Comes Next

Sessions 1 through 3 built the editorial foundation:

- Queue metadata.
- Primary KPI tracking.
- Growth analytics.
- Priority scoring and queue decision support.

Session 4 starts adding a new content product that can feed the queue later:

```text
Which pitchers are worth watching or streaming today, and why?
```

## What Was Built

### Fantasy Service

Added `mlbops/api/services/fantasy_service.py`.

The service builds a daily streamer matrix from:

- local MLB schedule data in `data/warehouse/mlb/{season}/schedule_post.csv`;
- optional MLB Stats API probables when available;
- recent local Statcast pitch files in `regular_season/pitches_enriched`;
- player names from `players_registry.json`.

When confirmed probables are unavailable, the service labels candidates as `projected_rotation` and infers likely starters from recent first pitchers by team. This is intentionally transparent so we do not confuse projections with confirmed probables.

### Fantasy Endpoint

Added:

```text
GET /fantasy/streamers
```

Supported query params:

- `game_date`: optional `YYYY-MM-DD`; defaults to today.
- `season`: optional season year.
- `limit`: number of rows to return.
- `include_live_probables`: try MLB Stats API probables first, then fall back to local projection.

### Streamer Row Fields

Each row returns:

- `pitcher`
- `player_id`
- `team`
- `team_name`
- `opponent`
- `opponent_name`
- `game_date`
- `game_pk`
- `venue`
- `home_away`
- `probable_status`
- `stream_score`
- `k_upside`
- `ratio_risk`
- `opponent_k_profile`
- `opponent_power_risk`
- `confidence`
- `league_fit`
- `note`
- `factor_scores`
- `sample`

### Scoring Model

The v1 streamer score is heuristic and review-first.

Inputs include:

- pitcher K rate;
- pitcher whiff rate;
- walk, hit, HR, and xwOBA risk;
- recent workload sample;
- opponent strikeout profile;
- opponent power risk;
- confidence from probable status and sample size.

The output is designed for editorial review:

- high score means worth reviewing for a fantasy or pitcher-watch post;
- high `k_upside` means strikeout chase angle;
- high `ratio_risk` means dangerous streamer even if strikeout upside is present;
- high `opponent_power_risk` means a blow-up path exists;
- `league_fit` keeps the recommendation framed by fantasy depth.

## Files Changed

- `docs/progress/SESSION_04_FANTASY_STREAMER_MATRIX.md`
- `docs/progress/UPSCALE_ROADMAP.md`
- `mlbops/api/main.py`
- `mlbops/api/routers/fantasy.py`
- `mlbops/api/services/fantasy_service.py`

## Checks Run

- `./mlb_env/bin/python -m compileall mlbops/api`
- FastAPI TestClient check:

```text
GET /fantasy/streamers?game_date=2026-05-13&season=2026&limit=3&include_live_probables=false
```

Result:

- HTTP 200.
- Returned streamer rows.
- Confirmed top rows include score, factor scores, sample sizes, league fit, and notes.

## Important Product Notes

- `probable_status = probable` means the row used MLB Stats API probable data.
- `probable_status = projected_rotation` means the row is inferred from recent local game data.
- `probable_status = unknown` means there was a scheduled team but no useful pitcher candidate.
- Session 4 does not create queue items yet.
- Session 4 does not add a Fantasy hub page yet.

## Follow-Ups

Session 5 should make this actionable in the hub:

- Add `/fantasy` page.
- Show Streamer Matrix rows in a table.
- Add filters for league fit, score, and risk.
- Add a queue action from a streamer row.
- Queue fantasy posts with:
  - pillar: `matchup_edge` or future `fantasy_streamer`;
  - primary KPI: `bookmarks`;
  - source module: `fantasy_service`;
  - manual review still required.
