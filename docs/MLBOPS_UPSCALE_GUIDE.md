# MLB Ops Upscale Guide

## Purpose

This document is the working guide for upgrading `mlbops` from a local Mallitalytics content assistant into a more structured, measurable, and scalable growth platform for MLB content on X.

The goal is not to pause the account and rebuild everything from scratch. The goal is to improve the engineering structure while continuing to publish daily and learn from performance.

> Ship content daily. Refactor behind the workflow. Measure every post. Add one new dimension at a time.

---

## Strategic Context

Mallitalytics is a baseball analytics brand focused on making MLB data visual, useful, sharp, and accessible. The voice should remain warm but analytical: not Bloomberg-cold, not ESPN-loud.

Positioning:

> Data-driven baseball without empty opinions.

`mlbops` is the operating system behind that positioning. It should help identify angles, generate visuals, manage queue drafts, publish intentionally, and track what works.

The current priority is accelerated execution during May because the MLB season is already underway. The platform must become more useful immediately, not after a long rebuild cycle.

---

## Current System Summary

`mlbops` lives inside the `gilerojas/MLB` repository.

Current major components:

- `mlbops/api/` — FastAPI backend.
- `mlbops/hub/` — Next.js browser hub.
- `data/hub.db` — SQLite database for queue, watchlist, notifications, and operational state.
- `data/warehouse/mlb/` — local MLB warehouse mirror.
- `outputs/` — generated PNG cards and boards served by FastAPI.
- `scripts/` — existing production scripts for cards, probables, HR tracker, games of the day, and related assets.
- `morning_intel/` — daily intel generation and snapshots.
- `jobs/` — scheduled/operational scripts.

The existing system already supports:

- Dashboard.
- Queue.
- Cards.
- Schedule.
- Intel.
- Watchlist.
- Leaderboards.
- Insights.
- Live events.
- Static generated assets.
- Manual-first queue review.
- Optional AI redraft.
- X publishing workflow.

---

## North Star

By the end of May, `mlbops` should become a measurable publishing cockpit for Mallitalytics.

Target state:

```text
Current MLB Ops = content assistant
May MLB Ops = measurable publishing cockpit
June MLB Ops = scalable Mallitalytics growth engine
```

The platform should help answer:

- What should I post today?
- Why is this post worth publishing?
- Which content pillar does it belong to?
- What KPI is it targeting?
- Did it work?
- What should I double down on next week?

---

## Core Value Proposition

### Audience-facing Mallitalytics value

Mallitalytics helps serious baseball fans, fantasy players, and data-curious MLB followers understand the season through clear visual analysis, daily matchup insights, and data-driven storytelling.

### Internal MLB Ops value

MLB Ops helps turn raw MLB data into publishable content by coordinating:

1. Data freshness.
2. Signal discovery.
3. Visual generation.
4. Editorial queue review.
5. Publishing.
6. Performance tracking.
7. Weekly learning.

---

## May Execution Goals

By May 31, the app should support:

| Capability | Target |
|---|---:|
| Daily content anchors | 3–5 repeatable formats |
| Queue metadata | Pillar, hook type, intended KPI, priority |
| Growth tracking | Basic post-performance table and dashboard |
| Engineering structure | Service layer started |
| Fantasy expansion | Fantasy v1 live |
| Editorial review | Daily queue review |
| Learning loop | Weekly performance review |
| Reliability | Health checks, backups, safer scripts |

---

## Content Strategy for X

The account should grow through named, repeatable, useful content products.

### Primary daily anchors

| Product | Purpose | Primary KPI |
|---|---|---|
| Probables Board | Daily habit and bookmark utility | Bookmarks |
| Pitcher to Watch | Matchup/fantasy/scout curiosity | Bookmarks, replies |
| Player Card of the Day | Brand identity and visual proof | Follows, bookmarks |
| One Chart, One Takeaway | Simple insight distribution | Reposts |
| HR Tracker / Stat of the Night | Timely event relevance | Reposts, follows |

### Weekly pillars

| Product | Purpose | Primary KPI |
|---|---|---|
| Leaderboard Watch | Bookmarkable reference content | Bookmarks |
| Statcast Signal | Early breakout/regression detection | Profile visits, follows |
| Buy/Sell with Data | Fantasy/debate engine | Replies |
| Under the Hood | Educational authority | Bookmarks, follows |
| Matchup Edge | Game-day utility | Bookmarks, replies |
| Breakout Watch | Prospect/dynasty audience | Follows, bookmarks |
| Why This Is Happening | Deeper explanation | Bookmarks, profile visits |

### Editorial rules

- One post, one idea.
- One card, one takeaway.
- No naked stat dumps.
- Every visual needs a one-sentence narrative.
- Use accessible translations for advanced metrics.
- Use percentiles when possible.
- Prioritize replies, bookmarks, reposts, and profile visits over likes.
- Keep AI redraft secondary; Mallitalytics voice remains human-led.

### Brand language

Reusable phrases:

- `Today's hidden edge:`
- `What changed:`
- `One chart. One takeaway.`
- `Signal vs. noise.`
- `What the box score will not tell you.`
- `The matchup hiding in plain sight.`
- `Bookmark this for later.`

---

## Technical Upscale Principles

### 1. No full rewrite

Do not replace working scripts unless necessary. Existing scripts already generate useful outputs. The correct path is to wrap them with cleaner service interfaces.

### 2. Routers should get thinner

FastAPI routers should handle HTTP concerns. Business logic should move into service modules.

### 3. Every post should become a data object

A post is not just text and an image. It should carry metadata that allows learning.

Required metadata:

```text
content_type
content_pillar
hook_type
intended_kpi
priority_score
campaign
source_module
manual_or_ai
experiment_tag
player_id
team_id
game_date
x_post_id
performance_metrics
```

### 4. Analytics before automation

Start with manual metric entry if X API automation is not ready. A basic analytics loop now is more valuable than a perfect automated system later.

### 5. SQLite is acceptable for now

Keep SQLite while `mlbops` is local and mostly single-user. Consider Postgres later if the platform becomes hosted, multi-user, or more concurrent.

### 6. Fantasy should be integrated carefully

Fantasy should not make Mallitalytics generic. The framing is:

> Fantasy-useful baseball analysis, not generic fantasy advice.

---

## Proposed Engineering Structure

Add or evolve toward this structure:

```text
mlbops/
├── api/
│   ├── routers/
│   ├── services/
│   │   ├── analytics_service.py
│   │   ├── card_generation_service.py
│   │   ├── content_scoring.py
│   │   ├── fantasy_service.py
│   │   ├── queue_service.py
│   │   └── x_metrics_service.py
│   ├── schemas/
│   │   ├── analytics.py
│   │   ├── content.py
│   │   ├── fantasy.py
│   │   └── queue.py
│   ├── db/
│   │   ├── database.py
│   │   ├── migrations/
│   │   └── migrate.py
│   └── main.py
├── hub/
│   ├── app/
│   ├── components/
│   └── lib/
└── docs/
    ├── MLBOPS_UPSCALE_GUIDE.md
    ├── CONTENT_PILLARS.md
    └── ANALYTICS_SCHEMA.md
```

---

## Recommended Service Modules

### `content_scoring.py`

Purpose: score generated content candidates before or during queue creation.

Suggested scoring factors:

| Factor | Weight |
|---|---:|
| Timeliness | 25% |
| Statistical strength | 20% |
| Audience relevance | 20% |
| Visual clarity | 15% |
| Reply/bookmark potential | 10% |
| Fantasy relevance | 10% |

Expected output:

```json
{
  "priority_score": 84,
  "primary_kpi": "bookmarks",
  "recommended_pillar": "probables",
  "reason": "Daily slate utility with fantasy relevance."
}
```

### `queue_service.py`

Purpose: centralize queue creation, metadata normalization, status updates, and validation.

Responsibilities:

- Insert queue items.
- Validate metadata.
- Apply default content pillar.
- Apply default KPI.
- Normalize AI/manual flags.
- Attach scoring output.

### `card_generation_service.py`

Purpose: wrap existing card scripts behind stable Python functions.

Responsibilities:

- Build subprocess commands.
- Run scripts.
- Parse stdout.
- Extract generated PNG paths.
- Extract card JSON.
- Build static URLs.
- Return normalized generation result.

### `analytics_service.py`

Purpose: store and summarize performance metrics.

Responsibilities:

- Create/update post performance rows.
- Calculate rates.
- Summarize by pillar.
- Summarize by content type.
- Summarize by hook type.
- Generate weekly review data.

### `fantasy_service.py`

Purpose: compute fantasy-useful content signals.

Initial focus:

- Pitcher Streamer Matrix.
- Buy/Sell candidates.
- Matchup Edge.

### `x_metrics_service.py`

Purpose: eventually pull X metrics or normalize manual metrics.

Responsibilities:

- Store X post IDs.
- Sync metrics where possible.
- Accept manual overrides.
- Track last sync time.

---

## Database Expansion

### Current database

The current SQLite database should remain the operating database during May.

### New tables to add

#### `post_performance`

Tracks individual post performance.

```text
id
queue_item_id
x_post_id
posted_at
content_type
content_pillar
hook_type
impressions
likes
replies
reposts
quote_tweets
bookmarks
profile_visits
follows
engagement_rate
bookmark_rate
reply_rate
repost_rate
follows_per_1000_impressions
notes
created_at
updated_at
```

#### `daily_account_metrics`

Tracks account-level daily growth.

```text
id
date
followers_total
followers_delta
profile_visits
total_posts
total_impressions
total_replies
total_bookmarks
total_reposts
top_post_id
notes
created_at
updated_at
```

#### `content_experiments`

Tracks A/B tests and weekly experiments.

```text
id
experiment_name
hypothesis
start_date
end_date
variant_a
variant_b
primary_kpi
status
winner
notes
created_at
updated_at
```

### Migration discipline

Add migration scripts under:

```text
mlbops/api/db/migrations/
```

Add a simple migration runner:

```text
mlbops/api/db/migrate.py
```

Do not make schema changes manually without a migration file.

---

## Hub Improvements

### Add `Growth` tab

Purpose: show whether Mallitalytics is growing and what is driving it.

Minimum sections:

1. Today overview.
2. This week overview.
3. Best posts.
4. Performance by pillar.
5. Performance by content type.
6. Performance by hook type.
7. Queue health.
8. Experiments.

Minimum KPIs:

- Posts today.
- Impressions.
- Replies.
- Reposts.
- Bookmarks.
- Profile visits.
- Follows.
- Follows per 1,000 impressions.
- Bookmarks per 1,000 impressions.
- Replies per 1,000 impressions.

### Add analytics entry form

Purpose: allow manual post metrics entry before X API metrics are automated.

Fields:

```text
queue_item_id
x_post_id
impressions
likes
replies
reposts
quote_tweets
bookmarks
profile_visits
follows
notes
```

### Add queue metadata visibility

Queue item detail should display:

- Content pillar.
- Hook type.
- Intended KPI.
- Priority score.
- Campaign.
- AI-assisted status.
- Experiment tag.

---

## Fantasy v1

Fantasy should be launched as one practical product first.

Recommended first product:

## Pitcher Streamer Matrix

Why this first:

- It connects naturally to Probables.
- It supports fantasy users without changing the brand.
- It is bookmarkable.
- It can reuse pitcher, schedule, and opponent data.
- It creates a daily decision-support product.

### Streamer Matrix fields

```text
pitcher
player_id
team
opponent
game_date
probable_status
stream_score
k_upside
ratio_risk
opponent_k_profile
opponent_power_risk
confidence
league_fit
note
```

### Streamer score model

| Component | Weight |
|---|---:|
| Opponent weakness | 25% |
| Pitcher recent form | 20% |
| Strikeout upside | 20% |
| Ratio safety | 15% |
| Pitch matchup edge | 10% |
| Win/QS context | 5% |
| Ballpark/weather risk | 5% |

### Fantasy content formats

1. Streamer Matrix.
2. Buy/Sell with Data.
3. Matchup Edge.

Do not build full fantasy SaaS during May. Build the smallest fantasy layer that generates posts and measurable audience response.

---

## Metrics Framework

### Post-level metrics

Track per post:

```text
content_pillar
content_type
hook_type
primary_kpi
impressions
likes
replies
reposts
quote_tweets
bookmarks
profile_visits
follows
```

Calculate:

```text
engagement_rate
bookmarks_per_1000_impressions
replies_per_1000_impressions
reposts_per_1000_impressions
follows_per_1000_impressions
```

### Account-level metrics

Track daily:

```text
followers_total
followers_delta
profile_visits
total_posts
total_impressions
total_replies
total_bookmarks
total_reposts
```

### Operational metrics

Track platform health:

```text
queue_drafts
queue_posted
queue_rejected
generation_failures
failed_post_rate
warehouse_freshness
last_drive_sync
last_intel_snapshot
average_generation_time
```

---

## Weekly Review Ritual

Every week, generate a short review:

```text
Mallitalytics Weekly Growth Review — YYYY-MM-DD
```

Include:

1. Total posts.
2. Follower delta.
3. Top 5 posts by bookmarks.
4. Top 5 posts by replies.
5. Top 5 posts by follows.
6. Best content pillar.
7. Worst content pillar.
8. Best hook type.
9. What to double down on.
10. What to stop doing.
11. Next week's experiments.

Decision rules:

- If a pillar drives bookmarks, keep it as utility content.
- If a pillar drives replies, use it for discussion and visibility.
- If a pillar drives follows, make it a recurring anchor.
- If a pillar has low impressions and low engagement after repeated tests, pause it.

---

## May Roadmap

## Week 1 — Stabilize and Instrument

Goal: make every queue item measurable.

Deliverables:

- Create this guide.
- Add content pillar taxonomy.
- Add queue metadata schema.
- Add `content_scoring.py`.
- Add `post_performance` table.
- Add manual analytics entry route.
- Add basic Growth dashboard.

Content output:

- Daily Probables.
- Daily Pitcher to Watch.
- Daily Player Card.
- 2 Leaderboard Watch posts.
- 1 Buy/Sell test.

Success criteria:

- 100% of new queue items carry pillar and intended KPI.
- Manual performance tracking works.
- Growth tab shows basic data.

---

## Week 2 — Launch Fantasy v1

Goal: add the first new audience dimension.

Deliverables:

- Add `fantasy_service.py`.
- Add Streamer Matrix data model.
- Add `/fantasy/streamers` endpoint.
- Add Fantasy section in hub.
- Add queue action from fantasy tiles.
- Start extracting card generation logic into service layer.

Content output:

- Streamer Matrix 3–5x/week.
- Buy/Sell with Data 1x/week.
- Matchup Edge 2x/week.
- Continue daily anchors.

Success criteria:

- Fantasy posts can be generated and queued.
- Fantasy posts are tracked separately in analytics.
- First fantasy performance comparison is possible.

---

## Week 3 — Optimize Growth

Goal: use performance data to double down.

Deliverables:

- Add per-pillar analytics.
- Add hook-type analytics.
- Add experiment tracker.
- Add priority sorting in Queue.
- Add dashboard filters by pillar and KPI.
- Add weekly report generator.

Content output:

- Keep top 3 performing pillars.
- Reduce low-performing formats.
- Run 1 tentpole post or thread.

Success criteria:

- Queue can be sorted by priority.
- Best pillar and best hook type are visible.
- Next week can be planned from actual data.

---

## Week 4 — Harden and Package

Goal: make the system stable enough for June scaling.

Deliverables:

- Add DB backup script.
- Add migration runner.
- Add API smoke tests.
- Improve system health dashboard.
- Add May retrospective report.
- Document daily operating rhythm.

Content output:

- Weekly recap thread.
- Updated pinned post.
- Fantasy recap.
- Best-of-May Mallitalytics thread.

Success criteria:

- Growth dashboard live.
- Fantasy v1 live.
- Analytics loop live.
- Service layer started.
- System reliable enough to support June expansion.

---

## Build Order for Codex / Claude Code

Use this order when instructing code agents:

```text
1. Add content pillar definitions.
2. Add queue metadata normalization.
3. Add post_performance migration.
4. Add analytics service.
5. Add manual analytics API routes.
6. Add Growth dashboard page.
7. Add content scoring service.
8. Add queue priority display and sorting.
9. Add fantasy service skeleton.
10. Add Streamer Matrix endpoint.
11. Add fantasy hub section.
12. Extract card generation helpers into service module.
13. Add weekly growth report generator.
14. Add DB backup script.
15. Add API smoke tests.
```

Important guardrails:

- Do not break existing card scripts.
- Do not remove existing queue behavior.
- Do not require Postgres yet.
- Do not automate posting without manual review.
- Do not make AI the default voice.
- Add tests or smoke checks when changing routing or database behavior.

---

## Suggested Codex / Claude Code Prompt

Use this prompt when asking a coding agent to implement changes:

```text
You are working inside the gilerojas/MLB repository. The app to upscale is mlbops/. Read docs/MLBOPS_UPSCALE_GUIDE.md before making changes.

Goal: improve MLB Ops into a measurable Mallitalytics publishing cockpit without breaking the existing daily content workflow.

Rules:
- Preserve current FastAPI routes unless explicitly changing them.
- Preserve existing scripts and wrap them with services instead of rewriting them.
- Keep SQLite for now.
- Add migrations for database changes.
- Keep manual review before X posting.
- Track content metadata: content_pillar, hook_type, intended_kpi, priority_score, campaign, source_module, manual_or_ai, experiment_tag.
- Add analytics progressively and keep the app usable after each change.

Start with the next smallest safe step from the build order.
```

---

## Business Direction

Do not try to monetize software immediately.

Recommended business ladder:

```text
Audience
→ Trust
→ Recurring content products
→ Paid insight layer
→ Sponsored/partner opportunities
→ Optional creator-facing tooling later
```

Most likely early monetization paths:

1. Sponsorships around baseball/fantasy content.
2. Paid fantasy newsletter or reports.
3. Premium Streamer Matrix / Buy-Sell / Matchup Edge product.
4. Later: creator-facing content operations tool if demand appears.

The business asset is not just code. It is the combination of:

```text
data pipelines
+ visual storytelling
+ editorial workflow
+ growth analytics
+ audience trust
```

---

## Final Operating Philosophy

May is not the month to perfect MLB Ops.

May is the month to make MLB Ops dangerous.

The platform should help Mallitalytics publish faster, learn faster, and expand intelligently without losing its voice.

The work should always serve the loop:

```text
Find signal
→ Shape story
→ Generate asset
→ Queue draft
→ Review manually
→ Publish
→ Track performance
→ Learn weekly
```

That is the scalable foundation.
