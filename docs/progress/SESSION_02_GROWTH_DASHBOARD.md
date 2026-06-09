# Session 2: Growth Dashboard

Date: 2026-05-12
Status: Implemented

## Goal

Turn the Session 1 tracking foundation into a usable Growth dashboard.

Session 1 made queue items classifiable and post metrics storable. Session 2 should make that information visible enough to answer:

```text
What is working for Mallitalytics, and what should we post more of?
```

This follows the upscale guide's Week 1 target: basic growth tracking, manual analytics, and a Growth tab.

## Why This Comes Next

The app can now store:

- Content pillar.
- Hook type.
- Primary KPI.
- Priority score.
- Source module.
- Manual vs AI status.
- Manual performance metrics.

But those fields are only useful if the hub summarizes them. Session 2 should convert raw rows into decisions:

- Which content pillar is driving bookmarks?
- Which hook type is driving replies or reposts?
- Which posted items still need metrics entered?
- Which formats should get more volume this week?

## Session 2 Scope

1. Add Growth navigation.
   - Add a `Growth` item to the hub sidebar and mobile nav.
   - Create a new `/growth` page.
   - Keep the existing Dashboard and Queue behavior unchanged.

2. Add analytics summary endpoints.
   - Extend `mlbops/api/routers/analytics.py` beyond raw performance CRUD.
   - Add endpoints for:
     - Overall summary.
     - Performance by content pillar.
     - Performance by hook type.
     - Performance by content type.
     - Top posts by bookmarks, replies, reposts, follows, and engagement.
     - Posted queue items missing performance metrics.

3. Build Growth dashboard v1.
   - Show top-level KPI cards:
     - Posts tracked.
     - Posted without metrics.
     - Impressions.
     - Bookmarks.
     - Replies.
     - Reposts.
     - Follows.
     - Average engagement rate.
   - Show rate cards:
     - Bookmarks per 1,000 impressions.
     - Replies per 1,000 impressions.
     - Reposts per 1,000 impressions.
     - Follows per 1,000 impressions.
   - Show compact tables:
     - Performance by pillar.
     - Performance by hook type.
     - Performance by content type.
     - Top posts.
     - Posted without metrics.

4. Clarify KPI language from Session 1.
   - Rename Queue UI label from `KPI` to `Primary KPI`.
   - Keep storing the existing `intended_kpi` field.
   - Growth must always show all metrics, regardless of primary KPI.
   - Treat primary KPI as the intended goal of the post, not the only success measure.

5. Add queue-to-growth loop.
   - Growth page should list posted items without `post_performance`.
   - Each missing-metrics row should include enough context to find the item in Queue:
     - queue ID.
     - title/player name.
     - content type.
     - posted date.
     - primary KPI.
   - Link each row to `/queue` for now; deep-linking to a specific queue item can come later if the Queue page supports it.

6. Document completed work.
   - Update this file at the end of implementation.
   - Record files changed, checks run, and remaining follow-ups.

## Backend Shape

Recommended new endpoint:

```text
GET /analytics/growth-summary?days=30
```

Return one payload that the Growth page can render without coordinating many client requests:

```text
summary
by_pillar
by_hook_type
by_content_type
top_posts
missing_metrics
```

Use `days=30` as the default because it is long enough to compare formats but still reflects current-season momentum.

All summary rows should include:

- posts.
- impressions.
- likes.
- replies.
- reposts.
- quote tweets.
- bookmarks.
- profile visits.
- follows.
- engagement rate.
- bookmarks per 1,000 impressions.
- replies per 1,000 impressions.
- reposts per 1,000 impressions.
- follows per 1,000 impressions.

## Growth Page Design

The Growth page should feel like an operating dashboard, not a marketing page.

Use a compact layout:

- Header: `Growth cockpit`.
- Small subline: last 30 days, manual metrics.
- KPI cards in a dense grid.
- Tables below the cards.
- No oversized hero section.
- No decorative graphics.

The dashboard should make empty states useful:

- If no performance rows exist, show `No post metrics entered yet`.
- Still show `Posted without metrics` so the next action is obvious.
- Do not block the page just because manual metrics are sparse.

## Non-Goals

- Do not automate X metrics in Session 2.
- Do not build Fantasy v1 yet.
- Do not add account-level daily follower tracking yet unless the Growth dashboard is finished early.
- Do not build the weekly report generator yet.
- Do not introduce charts unless the tables/cards are already working cleanly.
- Do not change posting or approval behavior.

## Acceptance Criteria

Session 2 is complete when:

- The hub has a visible `Growth` nav item.
- `/growth` loads successfully.
- Growth page reads real analytics data from FastAPI.
- Dashboard shows overall summary, rate metrics, grouped performance, top posts, and posted-without-metrics.
- Queue metadata label reads `Primary KPI`.
- Empty and sparse-data states are readable.
- Session 2 progress notes are updated.

## Verification Plan

Run checks appropriate to the files touched:

- FastAPI import/smoke check.
- FastAPI TestClient check for `GET /analytics/growth-summary`.
- SQL summary spot check using existing `data/hub.db`.
- Hub `npm run build`.
- Browser check for `/growth`:
  - login redirect still works.
  - Growth nav appears.
  - `/growth` renders KPI cards.
  - missing metrics section appears.
  - no console errors from the Growth page.

## Progress Log

### 2026-05-12

- Created this Session 2 plan.
- Added `GET /analytics/growth-summary?days=30`.
- Added the `Growth` nav item.
- Built `/growth` as the first Growth cockpit page.
- Added top-level KPI cards, rate cards, grouped performance tables, top posts, and posted-without-metrics.
- Renamed the Queue metadata label from `KPI` to `Primary KPI`.

## End-of-Session Notes

- Completed work:
  - Growth dashboard now reads one FastAPI payload with summary, by-pillar, by-hook, by-content-type, top-post, missing-metrics, and queue-health data.
  - Growth page handles sparse data cleanly. If no manual metrics exist yet, it still shows posted items missing metrics.
  - Queue UI now says `Primary KPI`, clarifying that intended KPI is the post's main goal, not the only metric tracked.
  - Growth page uses all saved metrics regardless of primary KPI.

- Files changed:
  - `docs/progress/SESSION_02_GROWTH_DASHBOARD.md`
  - `mlbops/api/routers/analytics.py`
  - `mlbops/api/services/analytics_service.py`
  - `mlbops/hub/app/growth/page.tsx`
  - `mlbops/hub/components/NavSidebar.tsx`
  - `mlbops/hub/components/QueueClient.tsx`

- Checks run:
  - `../mlb_env/bin/python` FastAPI TestClient check for `GET /analytics/growth-summary?days=30`
  - `../mlb_env/bin/python -m compileall api`
  - `npm run build`
  - Headless browser check for `/growth`: login, Growth page render, KPI cards, grouped tables, and posted-without-metrics section

- Follow-ups:
  - Enter real post metrics for recent posts so the Growth page has meaningful top-post and grouped-performance data.
  - Add account-level daily metrics later if needed.
  - Add weekly report generation in a later session.
  - Add X metrics automation only after confirming which X API metrics are available for the account.
