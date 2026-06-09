# MLB Ops Overview

MLB Ops is the Mallitalytics operating hub for turning daily MLB data into publishable X content. It combines the local warehouse, FastAPI services, a Next.js browser hub, generated visual cards, and a queue-based posting workflow.

The app is designed as a personal content assistant: it helps find angles, generate visual assets, draft or edit post copy, and publish through a controlled queue. The goal is not to fully automate the voice. The goal is to make daily posting faster while still keeping final judgment, edits, and posting decisions in the hub.

## Core Flow

The daily flow is:

1. Sync or refresh MLB data into the local warehouse.
2. Use the hub to inspect dashboard, intel, schedule, leaderboards, live events, and cards.
3. Generate or manually write content into the queue.
4. Review the draft copy, card image, and source context.
5. Optionally use AI redraft as a secondary assist, not the default voice.
6. Approve and post to X from the queue.
7. Track posted volume and streak consistency.

The hub runs locally and can be accessed privately through Tailscale while traveling. See [Secure Travel Hub](SECURE_TRAVEL_HUB.md) for the secure browser setup.

## Main Components

- **Next.js Hub**: The browser UI for dashboard, intel, cards, queue, settings, watchlist, and live events.
- **FastAPI API**: Backend service for queue data, card generation, leaderboards, intel, schedule, watchlist, and generated static assets.
- **SQLite Hub DB**: Local queue database for drafts, posted items, metadata, posting state, and audit logs.
- **Warehouse Data**: Local MLB warehouse files used by cards, leaderboards, intel, and schedules.
- **Generated Assets**: PNG boards/cards served from the local FastAPI static output path.
- **Queue**: The launch station where drafts are edited, redrafted, rejected, or posted.

## Hub Tabs

The hub is organized around a set of operating tabs. Each tab has a different job in the publishing workflow: some are for monitoring, some are for finding angles, some are for generating assets, and the queue is where posts actually get reviewed and published.

### Dashboard

The Dashboard is the daily command center. It gives a fast read on whether the system is ready and what kind of content opportunities are available.

What it offers:

- Games today and yesterday.
- Current queue draft count.
- Warehouse freshness status.
- Morning intel run controls.
- Pipeline readiness checks.
- Today's games summary.
- Hitters to watch from the latest intel snapshot.
- System pipeline status.
- Queue counts by status.
- Recent roster moves.

How it supports posting:

- Confirms whether the data is fresh enough to trust.
- Shows whether there are pending drafts that need review.
- Points you toward schedule, intel, and queue work without needing to open every tab first.
- Helps decide whether the day starts with a slate post, an intel post, or queue cleanup.

### Live

The Live tab is the in-game event scanner. It is built for moments that happen during games rather than only after the slate is complete.

What it offers:

- Live game scan by date.
- Event detection for home runs, multi-HR games, no-hit bids, strikeout milestones, cycle watches, finals, and debuts.
- Event status filters: new, queued, dismissed, and all.
- One-click queue creation from detected live events.
- Dismiss controls for events that are not worth posting.

How it supports posting:

- Creates spontaneous post opportunities while games are happening.
- Helps avoid relying only on morning-after content.
- Feeds live-event drafts into the queue so they can still be reviewed before posting.
- Supports quicker reaction posts without manually searching the full slate.

### Intel

The Intel tab is the narrative and signal feed. It uses morning intel snapshots and standout detection to surface players, transactions, milestones, watchlist signals, and Statcast changes.

What it offers:

- Latest morning intel snapshot.
- Daily standout panel.
- Pitcher and batter anomaly sections.
- Transaction summaries.
- Milestone watch.
- Watchlist pulse.
- Raw JSON access for deeper inspection.
- Morning intel regeneration controls.

How it supports posting:

- Finds stat-led and story-led angles that are not obvious from the box score alone.
- Feeds leaderboard/stat observations into the queue.
- Helps identify which players deserve a card, a manual post, or a watchlist update.
- Supports daily X posts around transactions, milestones, and emerging player signals.

### Watchlist

The Watchlist tab manages the players MLB Ops should keep paying attention to across daily runs.

What it offers:

- Editable player watchlist.
- Player ID, name, team, position, active flag, priority, and notes.
- Saves to both `jobs/player_watchlist.json` and the hub database.
- Shows recent watchlist pulse from intel snapshots when available.

How it supports posting:

- Keeps recurring content targets organized.
- Helps morning intel know which players matter most to your account.
- Makes it easier to build continuity across posts instead of only reacting to one-day outliers.
- Lets you prioritize players who should get extra review after games.

### Leaders

The Leaders tab is a sortable, boxscore-backed leaderboard surface.

What it offers:

- Batting and pitching leaderboards.
- Season selectors.
- Batting sort presets such as OPS, AVG, OBP, SLG, HR, RBI, SB, barrel %, xwOBA, hard-hit %, and sweet-spot %.
- Pitching sort presets such as ERA, K/9, WHIP, BB/9, FIP, K/BB ratio, whiff %, and Stuff+.
- Minimum PA and IP filters.
- Refresh controls.

How it supports posting:

- Finds quick stat observations for X.
- Gives context before posting a card or player take.
- Helps turn leaderboards into manual queue drafts or insight posts.
- Supports posts like "leaderboard stat of the day" without needing to generate a visual asset.

### Insights

The Insights tab is the richer analytics layer. It combines boxscore leader tiles with Statcast and contact-quality views.

What it offers:

- Batting sections for home run leaders, OPS leaders, barrel %, exit velocity, xwOBA on contact, lucky hitters, and unlucky hitters.
- Pitching sections for strikeout leaders, ERA leaders, fastball whiff %, hardest throwers, chase rate, spin rate, BS75+%, best pitches by RV/100, worst pitches by RV/100, and pitcher luck.
- Season selector.
- Pitcher role filter for all, starters, and relievers.
- Queue buttons on insight tiles where supported.

How it supports posting:

- Produces stat-led content that can be posted throughout the day.
- Helps explain why a player is interesting beyond traditional box score stats.
- Finds regression/luck angles for hitters and pitchers.
- Supports posts about contact quality, pitch quality, bat speed, whiffs, chase, and arsenal performance.

### Schedule

The Schedule tab is the game slate view.

What it offers:

- Date navigation.
- Team logos and matchup cards.
- Game status, venue, time, score, records, and probable pitchers.
- Quick read on finals, live games, pregame state, and scheduled games.

How it supports posting:

- Helps decide when to post Probables Board or Games of the Day.
- Gives the context needed for pregame slate posts.
- Helps identify games that may deserve live monitoring or postgame card generation.
- Supports matchup awareness before creating pitcher cards.

### Cards

The Cards tab is the player card generator.

What it offers:

- Player search.
- Pitcher and batter card modes.
- Game history for the selected player.
- Boxscore stat previews for each game.
- Card generation into the queue.
- Generated image preview and tweet text.

How it supports posting:

- Generates Pitching Cards and Batter Cards from actual games.
- Turns notable player performances into visual assets.
- Pushes generated content into the queue for review before posting.
- Supports the current daily focus on pitching-card posts.

### Queue

The Queue tab is the launch station. It is the most important tab for publishing because nothing should go to X without passing through this review layer.

What it offers:

- Mobile-first **Write Now** composer for spontaneous manual posts.
- Character counter.
- Manual quick-post draft creation.
- Prompt nudges like "What surprised you?" and "One sentence before checking stats."
- Streak stats: posts today, current streak, weekly total, longest streak, and manual ratio.
- Quick generation toolbar for Games of Day, Probables Board, HR Tracker, and Player Cards.
- Draft status tabs: draft, all, approved, posted, rejected, failed.
- Sort controls.
- Draft detail view with source metadata and image preview.
- Tweet text editing.
- AI redraft options behind "Need a nudge?"
- Save, reject, and approve/post actions.

How it supports posting:

- Centralizes final review and avoids accidental publishing.
- Makes manual posting faster from phone or laptop.
- Keeps AI assistance secondary instead of making it the default voice.
- Supports spontaneous posts and structured generated posts in one place.
- Tracks consistency so posting becomes a repeatable habit.

### Settings

The Settings tab is the operations and maintenance panel.

What it offers:

- Google Drive warehouse sync.
- Sync log output.
- Test morning digest notifications through email/WhatsApp.
- Read-only watchlist preview.
- Links to edit the watchlist.
- Quick command references for running the hub, morning intel, and daily cards.

How it supports posting:

- Keeps the data pipeline healthy.
- Lets you refresh local files before generating content.
- Confirms notification credentials.
- Provides operational commands without needing to remember every script.

## Current Daily Tweet Types

These are the main daily post formats currently being published through MLB Ops.

### Pitching Cards

Pitching cards are player-specific visual posts for notable pitcher outings.

Typical workflow:

1. Find a pitcher from the daily slate, intel page, or cards page.
2. Generate a pitcher card from the relevant game.
3. Review the card image and default tweet draft in the queue.
4. Redraft only if needed.
5. Approve and post to X.

Purpose:

- Highlight standout pitching performances.
- Turn box score, pitch mix, whiffs, CSW, command, velo, and context into a clean visual post.
- Give followers a fast, readable summary of why the outing mattered.

### Intel Leaderboard Stats

Intel leaderboard posts come from the hub's intel and insights surfaces. These are stat-led posts based on notable leaderboard positions, outliers, or daily/season-to-date signals.

Typical workflow:

1. Review intel, insights, or leaderboards.
2. Identify a stat worth posting.
3. Send the stat/insight into the queue or write a manual post from the queue composer.
4. Review the copy and post.

Purpose:

- Surface players showing meaningful statistical signals.
- Post more than just game recap content.
- Create compact stat observations that can be posted throughout the day.

### Probables Board

The probables board is the daily slate board with games of the day and probable pitchers.

Typical workflow:

1. Generate the Probables Board from the queue quick generation toolbar.
2. Review the generated image and tweet text.
3. Redraft lightly if needed.
4. Approve and post.

Purpose:

- Preview the daily MLB slate.
- Show matchups and probable starters in one visual.
- Give the account a daily schedule/context post before or around game windows.

### Games Of The Day

Games of the Day is a slate-oriented post that highlights selected matchups from the daily schedule.

Typical workflow:

1. Generate Games of Day from the queue quick generation toolbar.
2. Review the board and copy.
3. Post if the slate angle is strong.

Purpose:

- Call attention to the games most worth watching.
- Add context around matchups beyond individual player cards.
- Support a more varied daily posting mix.

### HR Tracker

The HR Tracker is a daily home run summary board.

Typical workflow:

1. Generate HR Tracker from the queue quick generation toolbar.
2. Review the visual and generated post text.
3. Approve and post after edits if needed.

Purpose:

- Summarize the home run activity from the relevant slate.
- Provide a repeatable visual post format.
- Create a simple daily content anchor around power production.

## Queue And Posting Behavior

The queue is the final decision point before anything reaches X.

Queue actions:

- **Save draft**: Preserve a manual or generated idea for later.
- **Need a nudge?**: Show prompts or AI redraft options when stuck.
- **Redraft**: Use Claude or Grok to rewrite copy, marked as AI-assisted metadata.
- **Reject**: Remove weak drafts from the active posting flow.
- **Approve and post**: Publish to X and mark the item as posted.

The current queue is intentionally manual-first. The mobile Write Now composer makes it easy to post spontaneous observations without needing to generate a card or ask AI for copy.

## Daily Operating Rhythm

A practical daily rhythm:

1. **Morning or pre-slate**: Check schedule, intel, and probables.
2. **Before games**: Post Probables Board or Games of the Day if useful.
3. **After notable outings**: Generate Pitching Cards.
4. **Throughout the day**: Use Write Now for quick manual thoughts or leaderboard observations.
5. **After HR activity is available**: Generate HR Tracker.
6. **End of session**: Review queue, post the best drafts, reject the rest.

The system is built to support both routine daily anchors and more spontaneous posting from phone or laptop.

## Secure Travel Access

The travel setup keeps the app private and low-cost:

```bash
./scripts/start_mlbops_travel.sh
tailscale serve --bg 3001
```

Access path:

```text
Browser -> Tailscale Serve -> Next.js hub -> authenticated backend proxy -> FastAPI
```

Do not expose FastAPI directly and do not use Tailscale Funnel for this app.

## Current Priorities

The current MLB Ops goal is to support a consistent daily posting system with enough flexibility to avoid a repetitive workflow.

The content mix should balance:

- Visual daily anchors: Pitching Cards, Probables Board, Games of the Day, HR Tracker.
- Stat-led posts: Intel leaderboard observations.
- Manual quick thoughts: Spontaneous posts from the Write Now composer.
- AI assistance only when helpful: redraft is secondary, not the main voice.
