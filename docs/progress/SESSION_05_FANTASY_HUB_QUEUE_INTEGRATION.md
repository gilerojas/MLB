# Session 5: Fantasy Hub and Queue Integration

Date: 2026-05-19
Status: Started

## Goal

Make the Fantasy Streamer Matrix visible and actionable inside the hub.

Session 4 created the backend endpoint, but it did not add a screen to the product. Session 5 starts by turning that API into a workflow:

```text
Streamer Matrix -> review candidate -> send to Queue -> manual approval/posting
```

## What Was Built

### Fantasy Navigation

Added `Fantasy` to the hub sidebar and mobile nav.

Route:

```text
/fantasy
```

### Fantasy Page

Added `mlbops/hub/app/fantasy/page.tsx`.

The page shows:

- target date selector;
- row limit selector;
- local/live probable mode toggle;
- candidate count;
- season;
- streamer cards;
- stream score;
- K upside;
- ratio risk;
- opponent K profile;
- opponent power risk;
- confidence;
- league fit;
- probable status;
- sample sizes.

### Queue Integration

Added:

```text
POST /queue/fantasy-streamer-draft
```

The Fantasy page can now send a streamer candidate to Queue as a draft.

Queued fantasy items use:

- `content_type`: `fantasy_streamer`
- `content_pillar`: `fantasy_streamer`
- `hook_type`: `bookmark_utility`
- `intended_kpi`: `bookmarks`
- `source_module`: `fantasy_service`
- `campaign`: `daily_mlb`

Manual review remains required. The action only creates a draft.

### Database Migration

Added `mlbops/api/db/migrate_fantasy_streamer.py`.

The migration expands the `content_queue.content_type` check constraint to allow:

```text
fantasy_streamer
```

Updated `data/schema.sql` to match.

## Files Changed

- `data/schema.sql`
- `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`
- `mlbops/api/db/migrate_fantasy_streamer.py`
- `mlbops/api/routers/queue.py`
- `mlbops/api/services/content_taxonomy.py`
- `mlbops/hub/app/fantasy/page.tsx`
- `mlbops/hub/components/NavSidebar.tsx`

## Checks Run

- `./mlb_env/bin/python -m mlbops.api.db.migrate_fantasy_streamer`
- `./mlb_env/bin/python -m compileall mlbops/api`
- FastAPI TestClient:
  - `GET /fantasy/streamers`
  - `POST /queue/fantasy-streamer-draft`
  - verified queued metadata;
  - deleted the temporary test draft.
- `npm run build`
- Browser verification:
  - `/fantasy` route loads after login;
  - Fantasy nav item appears;
  - Streamer Matrix renders candidate cards;
  - candidate cards show `Send to queue`;
  - live API check returned HTTP 200.

## Remaining Session 5 Work

- Confirm the Queue detail view displays fantasy metadata correctly.
- Add deeper filters later if needed:
  - league fit;
  - minimum stream score;
  - maximum ratio risk;
  - probable status.
