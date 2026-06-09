# Session 1: Upscale Foundation

Date: 2026-05-11
Status: Implemented

## Goal

Start upgrading MLB Ops from a content assistant into a measurable Mallitalytics publishing cockpit.

The first session should make new queue items easier to classify, prioritize, and evaluate after posting. This follows the upscale guide's principle:

```text
Ship content daily. Refactor behind the workflow. Measure every post.
```

## Why This Comes First

The app already has working queue, cards, live events, manual review, and X posting. The missing foundation is measurement.

Before building Fantasy v1 or deeper automation, MLB Ops needs to know:

- Which content pillar a post belongs to.
- What behavior the post is trying to drive.
- Whether the post worked after publishing.
- Which formats deserve more volume next week.

## Session 1 Scope

1. Add content taxonomy definitions.
   - Define the first Mallitalytics content pillars.
   - Define supported hook types.
   - Define intended KPI values.
   - Keep names stable enough for analytics grouping.

2. Add queue metadata normalization.
   - Add metadata fields for `content_pillar`, `hook_type`, `intended_kpi`, `priority_score`, `campaign`, `source_module`, `manual_or_ai`, and `experiment_tag`.
   - Keep existing queue behavior intact.
   - Use safe defaults when older generators do not provide the new fields.

3. Add database migration coverage.
   - Add the new queue metadata fields to SQLite in a repeatable migration.
   - Add a `post_performance` table for manual analytics entry.
   - Do not require Postgres or any hosted database.

4. Add backend analytics foundation.
   - Add helpers or a service for creating and updating post-performance rows.
   - Calculate basic rates from manual metrics.
   - Add API routes for entering and reading post-performance data.

5. Add minimal hub visibility.
   - Show queue metadata in the selected queue item detail.
   - Add a lightweight manual performance entry path for posted items.
   - Keep the Growth dashboard for the next session unless the backend work is small enough to finish cleanly.

6. Document completed work.
   - Update this file at the end of the session with files changed, tests run, and follow-ups.

## Proposed Content Values

Initial content pillars:

- `probables`
- `pitcher_to_watch`
- `player_card`
- `leaderboard_watch`
- `statcast_signal`
- `hr_tracker`
- `buy_sell`
- `matchup_edge`
- `live_event`
- `text_only`

Initial hook types:

- `hidden_edge`
- `what_changed`
- `one_chart_one_takeaway`
- `signal_vs_noise`
- `box_score_missed`
- `bookmark_utility`
- `debate_prompt`
- `rare_air`
- `live_reaction`

Initial intended KPIs:

- `bookmarks`
- `replies`
- `reposts`
- `profile_visits`
- `follows`
- `impressions`

## Non-Goals

- Do not build Fantasy v1 in Session 1.
- Do not automate X metrics ingestion yet.
- Do not replace existing card scripts.
- Do not remove manual review before posting.
- Do not redesign the whole hub UI.

## Acceptance Criteria

Session 1 is complete when:

- New queue items can carry normalized content metadata.
- Existing queue items still load and post normally.
- A posted item can receive manual performance metrics.
- Basic calculated rates are stored or returned by the backend.
- Queue detail exposes the new metadata clearly enough for daily review.
- This progress file lists what changed and what remains.

## Verification Plan

Run checks appropriate to the files touched:

- FastAPI import or smoke check.
- SQLite migration check against `data/hub.db`.
- Queue list/detail API check.
- Manual performance create/update API check.
- Hub TypeScript build if frontend types/components change.

## Progress Log

### 2026-05-11

- Created `docs/progress/` to track upscale sessions.
- Created this Session 1 plan.
- Added first-class queue metadata columns and backfilled existing queue rows.
- Added Mallitalytics content taxonomy defaults for content pillar, hook type, KPI, and priority.
- Added `post_performance` storage for manual post metrics.
- Added FastAPI analytics routes for listing, reading, and upserting post-performance rows.
- Added queue metadata visibility in the hub detail panel.
- Added a manual metrics entry panel for posted queue items.
- Added priority sorting to the queue.
- Ran the Session 1 migration against `data/hub.db`.

## End-of-Session Notes

- Completed work:
  - Queue items now carry normalized `content_pillar`, `hook_type`, `intended_kpi`, `priority_score`, `campaign`, `source_module`, `manual_or_ai`, and `experiment_tag` fields.
  - New queue inserts from FastAPI normalize metadata through `api.services.content_taxonomy`.
  - Quick manual posts from the hub now store Session 1 metadata values directly.
  - AI redrafts now mark the first-class `manual_or_ai` column as `ai`.
  - Manual post-performance metrics can be saved through `/analytics/performance/{queue_item_id}`.
  - Posted queue items expose the manual metrics form in the hub.

- Files changed:
  - `data/schema.sql`
  - `mlbops/api/db/database.py`
  - `mlbops/api/db/migrate_upscale_session1.py`
  - `mlbops/api/main.py`
  - `mlbops/api/routers/analytics.py`
  - `mlbops/api/routers/queue.py`
  - `mlbops/api/services/analytics_service.py`
  - `mlbops/api/services/content_taxonomy.py`
  - `mlbops/hub/app/api/queue/quick-post/route.ts`
  - `mlbops/hub/components/QueueClient.tsx`
  - `mlbops/hub/lib/db.ts`

- Checks run:
  - `./mlb_env/bin/python mlbops/api/db/migrate_upscale_session1.py`
  - `../mlb_env/bin/python -c "from api.main import app; ..."`
  - `../mlb_env/bin/python -m compileall api`
  - `../mlb_env/bin/python` queue metadata spot check
  - `../mlb_env/bin/python` analytics rate calculation spot check
  - `../mlb_env/bin/python` FastAPI TestClient check for `GET /analytics/performance`
  - `npm run build`
  - Isolated temporary-DB integration test for queue metadata + performance upsert/read/list
  - Live API check for `GET /queue?sort_by=priority_score`
  - Live API check for `GET /analytics/performance`
  - Live API write check for `PUT /analytics/performance/{queue_item_id}` using a temporary queue item, then cleaned up the test rows
  - Headless browser test for `/queue`: login redirect, login, priority sort option, metadata panel, and posted-item manual metrics panel

- Follow-ups:
  - Build the Growth dashboard page using `post_performance`.
  - Add per-pillar and per-hook analytics summaries.
  - Add a weekly growth report generator.
  - Consider adding editable queue metadata controls after the default taxonomy has been used for a few posting days.
