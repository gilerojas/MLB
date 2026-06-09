# MLB Ops Upscale Roadmap

Last updated: 2026-05-13

This is the working roadmap for upgrading MLB Ops into the Mallitalytics publishing cockpit described in `docs/MLBOPS_UPSCALE_GUIDE.md`.

Use this file before starting any new session. It exists so we do not lose the thread while the project grows.

## Operating Principles

- Ship content daily; refactor behind the workflow.
- Keep manual review before posting to X.
- Keep SQLite for now.
- Do not rewrite working card scripts unless there is a specific reason.
- Add analytics before automation.
- Document every session in `docs/progress/`.
- Each session should leave the app usable.

## Current Status

| Session | Status | Outcome |
|---|---|---|
| Session 1: Upscale Foundation | Implemented | Queue metadata, primary KPI, priority, manual performance storage, analytics CRUD, manual metrics entry. |
| Session 2: Growth Dashboard | Implemented | `/growth`, summary analytics, grouped performance, top posts, missing metrics, Growth nav. |
| Session 3: Content Scoring and Queue Decisions | Implemented | Heuristic scoring, rescore endpoint, score explanation, editable metadata, pillar/KPI filters. |
| Session 4: Fantasy v1 Backend | Implemented | Streamer Matrix service and `/fantasy/streamers` endpoint. |
| Session 5: Fantasy Hub and Queue Integration | Started | `/fantasy` page, nav entry, and queue draft action from streamer rows. |

## Session Roadmap

### Session 3 — Content Scoring and Queue Decisions

Goal: make the Queue better at deciding what is worth posting.

Build:

- `content_scoring.py`.
- `POST /queue/{item_id}/score`.
- `Why this priority?` score explanation.
- Editable queue metadata:
  - pillar.
  - hook.
  - primary KPI.
  - priority.
  - campaign.
  - experiment tag.
- Queue filters by pillar and primary KPI.

Exit criteria:

- Drafts can be rescored.
- Score explanation is stored in `meta_json`.
- Queue priority is no longer only a static default.
- Posting flow remains unchanged.

### Session 4 — Fantasy v1 Backend: Streamer Matrix

Goal: add the first fantasy-useful content product without turning Mallitalytics into generic fantasy advice.

Build:

- `fantasy_service.py`.
- Streamer Matrix model.
- `/fantasy/streamers` endpoint.
- Scores using probables, pitcher quality, opponent profile, matchup risk, and confidence.

Initial fields:

- pitcher.
- player ID.
- team.
- opponent.
- game date.
- probable status.
- stream score.
- K upside.
- ratio risk.
- opponent K profile.
- opponent power risk.
- confidence.
- league fit.
- note.

Exit criteria:

- API returns usable streamer candidates.
- Streamer scoring is explainable.
- No hub UI required yet unless backend work finishes early.

Status: Implemented in `docs/progress/SESSION_04_FANTASY_STREAMER_MATRIX.md`.

### Session 5 — Fantasy Hub and Queue Integration

Goal: make fantasy candidates actionable in the hub.

Build:

- New Fantasy section/page.
- Streamer Matrix table or tiles.
- Queue action from fantasy rows.
- Fantasy queue items with proper metadata:
  - pillar: `matchup_edge` or future `fantasy_streamer`.
  - primary KPI: `bookmarks`.
  - source module: `fantasy_service`.

Exit criteria:

- Fantasy posts can be queued.
- Fantasy posts are separately visible in Growth by pillar/source.
- Manual review remains required.

Status: Started in `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.

### Session 6 — Card Generation Service Layer

Goal: start thinning routers and wrapping existing scripts.

Build:

- `card_generation_service.py`.
- Stable helpers around existing scripts.
- Normalized generation result:
  - image path.
  - image URL.
  - tweet text.
  - card metadata.
  - source module.

Exit criteria:

- Existing card scripts still work.
- At least one card route uses the service wrapper.
- No behavior regression in Queue or Cards.

### Session 7 — Experiments and Weekly Review

Goal: turn Growth into a weekly learning loop.

Build:

- Lightweight experiment tracking.
- Make `experiment_tag` useful in Growth.
- Weekly growth review generator.
- Review output should include:
  - total posts.
  - top posts by bookmarks.
  - top posts by replies.
  - top posts by follows.
  - best pillar.
  - worst pillar.
  - best hook.
  - what to double down on.
  - what to pause.
  - next week's experiments.

Exit criteria:

- A weekly review can be generated from saved metrics.
- Experiments can be grouped and compared.

### Session 8 — Growth Optimization Filters

Goal: make Growth more useful for decisions.

Build:

- Growth filters:
  - date window.
  - pillar.
  - hook type.
  - primary KPI.
  - content type.
- Best/worst indicators.
- Last 7 days vs prior 7 days comparison if there is enough data.

Exit criteria:

- Growth page can answer what to post more of this week.
- Low-performing formats are visible enough to pause or adjust.

### Session 9 — Reliability and Hardening

Goal: make the system safer before more scale.

Build:

- DB backup script.
- Migration runner.
- API smoke tests.
- Improved health dashboard.
- Operational metrics:
  - queue drafts.
  - queue posted.
  - queue rejected.
  - generation failures.
  - failed post rate.
  - warehouse freshness.
  - last intel snapshot.

Exit criteria:

- Backups are easy to run.
- Smoke tests cover core API routes.
- System health is visible.

### Session 10 — X Metrics Automation

Goal: reduce manual metrics entry where X API access allows it.

Build:

- `x_metrics_service.py`.
- Store X post IDs and sync status.
- Sync available metrics.
- Preserve manual overrides.
- Track last sync time.

Exit criteria:

- Metrics automation works for available X API fields.
- Manual entry remains the fallback.
- Missing or restricted metrics fail gracefully.

### Session 11 — Daily Operating Rhythm and Packaging

Goal: document the daily Mallitalytics operating system.

Build:

- Daily workflow doc.
- Morning checklist.
- Posting rhythm.
- Metrics entry rhythm.
- Weekly review rhythm.
- May retrospective template.

Exit criteria:

- A future session can run Mallitalytics daily without reconstructing the workflow from memory.

## Recommended Order

Do not skip Session 3. The Queue should become better at choosing posts before Fantasy v1 adds more candidates.

Recommended sequence:

```text
Session 3: Content scoring + editable queue metadata
Session 4: Fantasy Streamer Matrix backend
Session 5: Fantasy hub + queue integration
Session 6: Card generation service layer
Session 7: Experiments + weekly review
Session 8: Growth optimization filters
Session 9: Reliability + hardening
Session 10: X metrics automation
Session 11: Daily operating rhythm
```

## Before Starting Any Session

Read:

- `docs/MLBOPS_UPSCALE_GUIDE.md`
- this roadmap
- the latest completed `docs/progress/SESSION_*.md`

Then verify:

- current git status.
- whether MLB Ops is running.
- whether the requested session depends on metrics data being entered first.

## Current Next Step

Proceed with Session 4: Fantasy v1 Backend: Streamer Matrix.
