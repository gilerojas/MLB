# Session 3: Content Scoring and Queue Decisions

Date: 2026-05-13
Status: Implemented

## Goal

Turn queue priority from a static default into a useful editorial decision score.

Session 1 made every post classifiable. Session 2 made performance visible in Growth. Session 3 should make the Queue smarter before publishing by answering:

```text
Which draft is most worth posting, and why?
```

This follows the upscale guide's build order after the Growth dashboard:

```text
7. Add content scoring service.
8. Add queue priority display and sorting.
```

Priority display and sorting already exist in a basic form, but the score itself is still mostly a default by content type. Session 3 should make it explainable and adjustable.

## Why This Comes Next

Right now the metadata panel is useful but shallow:

- `Priority` is a default value by content type.
- `Primary KPI` is assigned by type, not by the actual post angle.
- `Experiment` exists but cannot be set from the UI.
- Queue review still depends mostly on human judgment without a structured reason.

Session 3 should make the queue feel like an editorial cockpit:

- Show why a draft is high or low priority.
- Let the user correct metadata before posting.
- Store scoring reasons for later learning.
- Keep manual review as the final decision.

## Session 3 Scope

1. Add `content_scoring.py`.
   - Add a backend service under `mlbops/api/services/`.
   - Use the guide's scoring factors:
     - Timeliness: 25%.
     - Statistical strength: 20%.
     - Audience relevance: 20%.
     - Visual clarity: 15%.
     - Reply/bookmark potential: 10%.
     - Fantasy relevance: 10%.
   - Return:
     - `priority_score`.
     - `primary_kpi`.
     - `recommended_pillar`.
     - `reason`.
     - factor-level scores.

2. Add score explanation storage.
   - Store score details in `meta_json` under a stable key such as `content_score`.
   - Keep the first-class `priority_score`, `content_pillar`, and `intended_kpi` columns in sync with the score result.
   - Do not add a new table unless needed.

3. Add scoring API routes.
   - Add an endpoint to score or rescore a queue item:

```text
POST /queue/{item_id}/score
```

   - The endpoint should:
     - read the queue item and metadata.
     - run the scoring service.
     - update queue metadata.
     - return the score explanation.

4. Add metadata editing in Queue.
   - Let the user edit:
     - content pillar.
     - hook type.
     - primary KPI.
     - priority score.
     - campaign.
     - experiment tag.
   - Keep inputs compact in the existing metadata block.
   - Use dropdowns for pillar, hook, and primary KPI.
   - Use a numeric input or stepper for priority score.
   - Use text inputs for campaign and experiment.

5. Add score explanation UI.
   - Add a small `Why this priority?` section in Queue detail.
   - Show:
     - factor scores.
     - final score.
     - short reason.
   - Add a `Rescore` button for draft items.
   - Do not rescore posted items automatically.

6. Add Queue filters.
   - Add lightweight filters for:
     - pillar.
     - primary KPI.
   - Keep existing status tabs and sort dropdown.
   - Default view should remain usable on mobile.

7. Document completed work.
   - Update this file after implementation.
   - Record files changed, checks run, and follow-ups.

## Scoring Defaults

The v1 scoring service should be heuristic and transparent, not AI-dependent.

Suggested starting logic:

- Timeliness:
  - live events and same-day game content score high.
  - stale game dates score lower.
- Statistical strength:
  - posts with structured stat metadata, player cards, and Statcast/live event details score higher.
- Audience relevance:
  - named players, recognizable teams, HR/live events, probables, and pitcher cards score higher.
- Visual clarity:
  - items with an image/card score higher.
  - text-only items score lower unless they are timely.
- Reply/bookmark potential:
  - probables, leaderboards, matchup edges, debate prompts, and useful pitcher posts score higher.
- Fantasy relevance:
  - pitcher-to-watch, probables, matchup edge, buy/sell, and streamer-style items score higher.

The score should be explainable even if it is imperfect. The goal is decision support, not replacing judgment.

## UI Copy Changes

Keep the vocabulary clear:

- Use `Primary KPI`, not `KPI`.
- Use `Priority`, but show `Why this priority?`.
- Use `Experiment tag`, not just `Experiment`, if space allows.

## Non-Goals

- Do not build Fantasy v1 in Session 3.
- Do not automate X metrics.
- Do not make AI scoring required.
- Do not remove manual approval.
- Do not rewrite existing card-generation scripts.
- Do not overfit scoring to current performance data yet; manual metrics are still sparse.

## Acceptance Criteria

Session 3 is complete when:

- Queue items can be rescored from the backend.
- Rescoring updates `priority_score`, `content_pillar`, and `intended_kpi` when appropriate.
- Score details are stored in `meta_json`.
- Queue detail shows a clear score explanation.
- Queue metadata can be edited from the UI.
- Queue can be filtered by pillar and primary KPI.
- Existing queue posting flow still works.
- Session 3 progress notes are updated.

## Verification Plan

Run checks appropriate to the files touched:

- FastAPI import/smoke check.
- FastAPI TestClient check for `POST /queue/{id}/score`.
- DB spot check that score details land in `meta_json`.
- Queue API list check with pillar/KPI filters if filters are backend-driven.
- Hub `npm run build`.
- Browser check for `/queue`:
  - metadata edit controls render.
  - `Rescore` works for a draft.
  - score explanation appears.
  - filters can narrow the visible queue.
  - posting buttons remain unchanged.

## Progress Log

### 2026-05-13

- Created this Session 3 plan.
- Added heuristic content scoring service.
- Added queue taxonomy and score endpoints.
- Added backend metadata patching for pillar, hook, primary KPI, priority, campaign, and experiment tag.
- Added Queue filters for pillar and primary KPI.
- Added editable Queue metadata controls for draft items.
- Added `Why this priority?` score explanation and `Rescore` action.

## End-of-Session Notes

- Completed work:
  - `content_scoring.py` scores queue items using timeliness, statistical strength, audience relevance, visual clarity, reply/bookmark potential, and fantasy relevance.
  - `POST /queue/{item_id}/score` stores score details in `meta_json.content_score` and syncs `priority_score`, `content_pillar`, and `intended_kpi`.
  - `GET /queue/taxonomy` exposes valid pillar, hook, and primary KPI values to the hub.
  - Queue list supports filtering by `content_pillar` and `intended_kpi`.
  - Draft queue items can be edited directly in the metadata panel.
  - Draft queue items can be rescored from the detail panel.
  - Posted items remain read-only for metadata in the UI.

- Files changed:
  - `docs/progress/SESSION_03_CONTENT_SCORING_QUEUE_DECISION.md`
  - `mlbops/api/db/database.py`
  - `mlbops/api/routers/queue.py`
  - `mlbops/api/services/content_scoring.py`
  - `mlbops/hub/components/QueueClient.tsx`

- Checks run:
  - FastAPI TestClient check for `GET /queue/taxonomy`
  - FastAPI TestClient check for `POST /queue/{item_id}/score`
  - FastAPI TestClient check for metadata `PATCH /queue/{item_id}`
  - `../mlb_env/bin/python -m compileall api`
  - `npm run build`
  - Headless browser check for `/queue`: filters, editable metadata controls, `Why this priority?`, `Rescore`, score explanation, and unchanged posting controls
  - Temporary browser-test draft was deleted after verification

- Follow-ups:
  - Use the scoring model for a few daily queue reviews before changing weights.
  - Consider deep-linking Growth missing-metrics rows to specific Queue items.
  - Build Session 4 Fantasy Streamer Matrix backend next, per the roadmap.
