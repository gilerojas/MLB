# Codebase Structure

**Analysis Date:** 2026-07-10

## Directory Layout

```text
MLB/
├── .agents/skills/               # Repository-local agent/design guidance
├── .github/workflows/            # Scheduled ingest, card, and intel automation
├── .planning/codebase/           # GSD repository maps
├── assets/                       # Brand assets and fonts used by renderers
├── config/                       # Versioned analytics/rendering configuration
├── data/warehouse/mlb/           # Local warehouse mirror; game data mostly ignored
├── deploy/                       # VPS deploy, sync, ingest, and health tooling
├── docs/                         # Current operations/product docs plus archive/history
│   ├── archive/                  # Superseded architecture documentation
│   └── progress/                 # Historical implementation/session records
├── jobs/                         # Scheduled Python content/reporting jobs
├── mlbops/
│   ├── api/
│   │   ├── db/                   # Database adapter, schemas, and migrations
│   │   ├── live/                 # Live-feed fetch, detection, and text generation
│   │   ├── routers/              # FastAPI endpoint modules by feature
│   │   └── services/             # Reusable API domain services
│   └── hub/
│       ├── app/                  # Next.js pages, layout, and route handlers
│       │   └── api/              # Authenticated Hub-side control-plane routes
│       ├── components/           # Shared React UI and feature clients
│       ├── lib/                  # API, DB, auth, posting, notification utilities
│       └── public/               # Hub static public assets
├── morning_intel/                # Daily intelligence pipeline and snapshots
├── notebooks/                    # Exploratory Jupyter analyses
├── outputs/                      # Generated cards, boards, reports; ignored runtime data
├── research/                     # Topic-scoped reproducible studies and publications
├── scripts/                      # Executable cards, boards, exports, studies, and ops tools
├── src/
│   ├── fantasy_streamer/         # Fantasy projection image renderer
│   ├── games_of_day/             # Slate fetch/render package
│   ├── hr_tracker/               # Home-run extraction/render package
│   ├── ingestion/                # Warehouse ingest/schema/export package
│   ├── insight_tiles/            # Insight tile image renderer
│   ├── pitching_performances/    # Pitching index/MalliScore render logic
│   └── probables_board/          # Probables fetch/render/story package
├── supabase/                     # Draft/unused control-plane schema artifacts
├── Dockerfile.api                # FastAPI production image
├── docker-compose.yml            # VPS service topology
├── requirements.txt              # Shared Python runtime dependencies
└── start_hub.sh                  # Local FastAPI + Next.js launcher
```

## Directory Purposes

**`mlbops/api/`:**
- Purpose: Production Python web service for analytics, content generation, queue operations, and data access.
- Contains: FastAPI composition in `mlbops/api/main.py`, runtime path policy in `mlbops/api/paths.py`, feature routers, services, database adapters, and live-event logic.
- Key files: `mlbops/api/main.py`, `mlbops/api/paths.py`, `mlbops/api/db/database.py`, `mlbops/api/db/schema_postgres.sql`

**`mlbops/api/routers/`:**
- Purpose: Organize HTTP contracts by feature domain.
- Contains: One `APIRouter` module per surface: `analytics.py`, `briefing.py`, `cards.py`, `fantasy.py`, `insights.py`, `intel.py`, `leaderboards.py`, `live.py`, `queue.py`, `schedule.py`, `system_readiness.py`, and `watchlist.py`.
- Key files: `mlbops/api/routers/cards.py`, `mlbops/api/routers/queue.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/api/routers/insights.py`

**`mlbops/api/services/`:**
- Purpose: Hold reusable calculations that do not belong to HTTP routing or persistence.
- Contains: Growth/performance analytics, queue taxonomy/scoring, fantasy streamer projection, and pitcher dashboard assembly.
- Key files: `mlbops/api/services/analytics_service.py`, `mlbops/api/services/content_taxonomy.py`, `mlbops/api/services/content_scoring.py`, `mlbops/api/services/fantasy_service.py`, `mlbops/api/services/pitcher_dashboard.py`

**`mlbops/api/db/`:**
- Purpose: Own relational persistence contracts for production Postgres and local SQLite.
- Contains: Shared Python adapter, canonical production schema, one-time import/migration scripts, and feature migrations.
- Key files: `mlbops/api/db/database.py`, `mlbops/api/db/schema_postgres.sql`, `mlbops/api/db/migrate_upscale_session1.py`, `mlbops/api/db/migrate_live_events.py`

**`mlbops/api/live/`:**
- Purpose: Separate live MLB feed retrieval, deterministic event detection, and social text construction from HTTP endpoints.
- Contains: `fetch.py`, `detect.py`, and `text.py`, composed by `mlbops/api/routers/live.py`.
- Key files: `mlbops/api/live/fetch.py`, `mlbops/api/live/detect.py`, `mlbops/api/live/text.py`

**`mlbops/hub/app/`:**
- Purpose: Define the Next.js App Router page tree and server route tree.
- Contains: Root shell/error/loading files; feature folders for cards, fantasy, growth, insights, intel, leaderboards, live, login, queue, schedule, settings, and watchlist; authenticated server endpoints under `mlbops/hub/app/api/`.
- Key files: `mlbops/hub/app/layout.tsx`, `mlbops/hub/app/page.tsx`, `mlbops/hub/app/globals.css`, `mlbops/hub/app/api/backend/[...path]/route.ts`

**`mlbops/hub/components/`:**
- Purpose: Store reusable shell components and stateful feature clients shared by pages.
- Contains: Navigation/top bar, queue client, live scanner, watchlist editor, schedule cards, intel panels, pipeline readiness, and theme controls.
- Key files: `mlbops/hub/components/QueueClient.tsx`, `mlbops/hub/components/NavSidebar.tsx`, `mlbops/hub/components/HubTopBar.tsx`, `mlbops/hub/components/LiveEventsClient.tsx`

**`mlbops/hub/lib/`:**
- Purpose: Centralize server/client infrastructure utilities for the Hub.
- Contains: FastAPI URL/CSRF helpers, dual-backend database access, signed-session security, Twitter, Resend, Twilio, tweet length rules, and heat-scale utilities.
- Key files: `mlbops/hub/lib/api.ts`, `mlbops/hub/lib/db.ts`, `mlbops/hub/lib/security.ts`, `mlbops/hub/lib/twitter.ts`

**`src/`:**
- Purpose: Provide importable Python domain packages shared by API routers, jobs, and executable scripts.
- Contains: Warehouse ingestion, recent-player metrics, branded styling/headshots, slate/card render packages, fantasy projections, insight tiles, and pitching performance logic.
- Key files: `src/ingestion/load_mlb_warehouse.py`, `src/mallitalytics_style.py`, `src/pitcher_recent.py`, `src/batter_recent.py`, `src/arm_angle.py`

**`src/ingestion/`:**
- Purpose: Own the warehouse's fetch, schema, registry, raw aggregation, and season export logic.
- Contains: Main ingest CLI, game-type/stage schema, player registry merge, boxscore aggregation, and season-level export builders.
- Key files: `src/ingestion/load_mlb_warehouse.py`, `src/ingestion/mlb_warehouse_schema.py`, `src/ingestion/player_registry.py`, `src/ingestion/boxscore_aggregate.py`, `src/ingestion/season_exports.py`

**`scripts/`:**
- Purpose: Expose operator- and subprocess-facing executable workflows.
- Contains: Daily/seasonal card generation, slate boards, HR tracking, pitching index, exports, validation, Drive sync, database import/export, research runners, and local launch wrappers.
- Key files: `scripts/mallitalytics_daily_card.py`, `scripts/batter_card_daily.py`, `scripts/batter_card_seasonal.py`, `scripts/pitching_performances_daily.py`, `scripts/probables_board_daily.py`, `scripts/games_of_day_board.py`

**`morning_intel/`:**
- Purpose: Build the daily narrative layer over warehouse, MLB API, watchlist, and queue data.
- Contains: Main intelligence pipeline, notification wrapper, operational README, and generated JSON snapshots.
- Key files: `morning_intel/morning_intel.py`, `morning_intel/morning_digest.py`, `morning_intel/README.md`, `morning_intel/snapshots/`

**`jobs/`:**
- Purpose: Hold recurring business jobs that are broader than a single renderer or API request.
- Contains: Daily watchlist card selection, weekly social performance reporting, and cron examples.
- Key files: `jobs/daily_card_generator.py`, `jobs/weekly_report.py`, `jobs/crontab.example`

**`deploy/`:**
- Purpose: Operate the VPS application and production data volumes.
- Contains: Application sync/build wrappers, Hub dev override, daily ingest, warehouse sync/pull/verify/audit tools, and deployment documentation.
- Key files: `deploy/README.md`, `deploy/sync_app_to_vps.sh`, `deploy/vps_hub_dev.sh`, `deploy/vps_daily_ingest.sh`, `deploy/vps_verify_warehouse.sh`

**`.github/workflows/`:**
- Purpose: Run cloud automation against the Google Drive archive and generated content pipelines.
- Contains: Daily warehouse ingest, card generation, and post-ingest morning intelligence workflows.
- Key files: `.github/workflows/daily_ingest.yml`, `.github/workflows/generate_cards.yml`, `.github/workflows/morning_intel.yml`

**`research/`:**
- Purpose: Organize reproducible topic-specific analysis outside the serving path.
- Contains: A folder per study with scripts, generated data, images, README, publication brief, and optional article/thread drafts; `research/study/starter_projection_gsd/` adds durable experiment state and verification artifacts.
- Key files: `research/Arm angle variability and deception/README.md`, `research/Deception contributors/README.md`, `research/FF usage in 2 strikes situations/README.md`, `research/Tunnel score/README.md`, `research/study/starter_projection_gsd/README.md`

**`docs/`:**
- Purpose: Document current runtime, product workflows, warehouse contracts, card behavior, and historical decisions.
- Contains: Authoritative current-state docs at the top level, superseded documents under `docs/archive/`, and historical session records under `docs/progress/`.
- Key files: `docs/CURRENT_STATE.md`, `docs/MLBOPS_OVERVIEW.md`, `docs/WAREHOUSE_DRIVE_WORKFLOW.md`, `docs/PITCHING_CARD.md`, `docs/archive/`

**`data/warehouse/mlb/`:**
- Purpose: Mirror the warehouse layout used by ingestion and local/offline readers.
- Contains: Season/stage schedule metadata plus ignored raw feed, enriched parquet, and player registry artifacts.
- Key files: `data/warehouse/mlb/README.md`, `data/warehouse/mlb/2025/season_context.md`, `data/warehouse/mlb/2026/season_context.md`

**`supabase/`:**
- Purpose: Preserve draft control-plane schema work that is not the current production database architecture.
- Contains: SQL drafts and an empty seed placeholder.
- Key files: `supabase/schema_drafts/001_mlbops_control_plane.sql`, `supabase/seed/.gitkeep`

## Key File Locations

**Entry Points:**
- `mlbops/api/main.py`: FastAPI application composition root.
- `mlbops/hub/app/layout.tsx`: Hub root layout and shell.
- `mlbops/hub/proxy.ts`: Hub session gate for page navigation.
- `src/ingestion/load_mlb_warehouse.py`: Warehouse CLI and ingest pipeline.
- `morning_intel/morning_intel.py`: Daily intelligence CLI.
- `jobs/daily_card_generator.py`: Automated watchlist card job.
- `jobs/weekly_report.py`: Social performance reporting job.
- `start_hub.sh`: Local dual-service launcher.
- `Dockerfile.api`: FastAPI production process definition.
- `mlbops/hub/Dockerfile`: Next.js production build/process definition.

**Configuration:**
- `requirements.txt`: Shared Python analytics/rendering dependencies.
- `mlbops/api/requirements-api.txt`: FastAPI-specific dependencies.
- `mlbops/hub/package.json`: Next.js runtime and scripts.
- `mlbops/hub/tsconfig.json`: Strict TypeScript and `@/*` path alias configuration.
- `mlbops/hub/next.config.ts`: Build concurrency/timeouts and current type-check behavior.
- `config/pitch_metric_benchmarks_2024.json`: Versioned pitch-metric reference data.
- `src/ingestion/mlb_warehouse_schema.py`: Warehouse stage mapping and retained parquet columns.
- `mlbops/api/paths.py`: Runtime filesystem path policy.
- `docker-compose.yml`: VPS service and volume topology; treat environment values as sensitive and do not copy them into source docs.

**Core Logic:**
- `mlbops/api/routers/`: HTTP feature orchestration.
- `mlbops/api/services/`: Reusable API domain logic.
- `mlbops/api/db/database.py`: Python persistence boundary.
- `mlbops/hub/lib/db.ts`: Hub persistence boundary.
- `mlbops/hub/lib/security.ts`: Session, CSRF, rate-limit, and audit boundary.
- `src/`: Shared analytics, ingestion, and rendering modules.
- `scripts/`: Executable content and data workflows.

**Testing:**
- `test_parse.py`: Root-level feed parsing check.
- `test_parquet.py`: Root-level parquet read check.
- `test_malli_score_v2.py`: Untracked MalliScore V2 verification script.
- `research/*/scripts/`: Study-local reproducible analysis scripts; these are verification runners, not application unit tests.
- `scripts/validate_daily_ingest.py`: End-to-end daily warehouse validation.
- `scripts/validate_season_warehouse.py`: Season warehouse validation.
- `deploy/vps_verify_warehouse.sh`: Production warehouse smoke verification.

**Documentation:**
- `README.md`: Repository split and warehouse usage.
- `docs/CURRENT_STATE.md`: Authoritative production-vs-local architecture.
- `docs/MLBOPS_OVERVIEW.md`: Product workflow and Hub surface ownership.
- `deploy/README.md`: VPS layout, deploy, sync, and health operations.
- `MALLITALYTICS_BRAND.md`: Brand contract used by content renderers.
- `mlbops/hub/AGENTS.md`: Required Next.js implementation guidance.

## Naming Conventions

**Files:**
- Use lowercase `snake_case.py` for Python modules and executable scripts, as in `mlbops/api/services/fantasy_service.py` and `scripts/probables_board_daily.py`.
- Use `page.tsx`, `layout.tsx`, `loading.tsx`, `route.ts`, and `global-error.tsx` according to Next.js App Router conventions under `mlbops/hub/app/`.
- Use `PascalCase.tsx` for reusable React components, as in `mlbops/hub/components/QueueClient.tsx` and `mlbops/hub/components/ScheduleGameCard.tsx`.
- Use uppercase topic names for repository operating documents, as in `docs/CURRENT_STATE.md` and `MALLITALYTICS_BRAND.md`; study-local files use descriptive lowercase names under `research/`.
- Name warehouse artifacts `game_{game_pk}_{YYYYMMDD}_feed_live.json.gz` and `game_{game_pk}_{YYYYMMDD}_pitches_enriched.parquet` according to `src/ingestion/load_mlb_warehouse.py`.
- Name database migrations `migrate_<feature>.py` under `mlbops/api/db/`, as in `mlbops/api/db/migrate_live_events.py`.
- Do not create duplicate suffix files such as `mlbops/hub/lib/db 2.ts` or `src/pitching_performances/malli_score 2.py`; update the canonical `db.ts` or `malli_score.py` after reconciling user work.

**Directories:**
- Use a feature/domain noun for API and shared package directories, as in `mlbops/api/live/`, `src/probables_board/`, and `src/fantasy_streamer/`.
- Mirror URL route segments with App Router folders under `mlbops/hub/app/`, such as `mlbops/hub/app/leaderboards/` and `mlbops/hub/app/api/queue/`.
- Use one self-contained topic directory per research study under `research/`, with `scripts/`, `data/`, and `images/` where applicable.
- Use MLB stage names from `src/ingestion/mlb_warehouse_schema.py` for warehouse partitions under `data/warehouse/mlb/{season}/`.

## Where to Add New Code

**New Hub Feature:**
- Primary page: `mlbops/hub/app/<feature>/page.tsx`
- Shared/stateful UI: `mlbops/hub/components/<FeatureName>.tsx`
- Browser/server API helper: `mlbops/hub/lib/`
- Privileged Hub operation: `mlbops/hub/app/api/<feature>/route.ts`
- FastAPI-backed browser operation: call `/api/backend/<path>` through `mlbops/hub/lib/api.ts`
- Tests/verification: add focused TypeScript/React coverage beside the selected test framework when introduced; currently verify through build/type checks and local Hub workflows defined in `mlbops/hub/package.json`.

**New FastAPI Feature:**
- HTTP contract: `mlbops/api/routers/<feature>.py`
- Router registration: `mlbops/api/main.py`
- Domain calculation: `mlbops/api/services/<feature>_service.py`
- Shared persistence: `mlbops/api/db/database.py` and, when used by Hub handlers, `mlbops/hub/lib/db.ts`
- Schema/migration: `mlbops/api/db/schema_postgres.sql` and `mlbops/api/db/migrate_<feature>.py`
- Tests/verification: add a focused API test module in a new repository-level `tests/` tree, then follow the API health/smoke sequence documented in `AGENTS.md`.

**New Warehouse Field or Dataset:**
- Stage/schema constants: `src/ingestion/mlb_warehouse_schema.py`
- Ingest/join/write logic: `src/ingestion/load_mlb_warehouse.py`
- Aggregation/export logic: `src/ingestion/boxscore_aggregate.py` or `src/ingestion/season_exports.py`
- Validation: `scripts/validate_daily_ingest.py`, `scripts/validate_season_warehouse.py`, and `COLUMNS_AUDIT.md`
- Documentation: `README.md` and the relevant data contract under `docs/`.

**New Card or Board:**
- Reusable fetch/transform/render package: `src/<feature>/`
- CLI composition: `scripts/<feature>_daily.py` or another descriptive `snake_case.py`
- API generation endpoint: `mlbops/api/routers/cards.py` for established card workflows, or a dedicated router if the feature has multiple operations.
- Hub trigger/view: `mlbops/hub/app/cards/page.tsx`, `mlbops/hub/components/QueueClient.tsx`, or a feature page under `mlbops/hub/app/`.
- Generated assets: resolved output directory from `mlbops/api/paths.py`, never a committed source directory.

**New Queue or Content Taxonomy Behavior:**
- Normalization/defaults: `mlbops/api/services/content_taxonomy.py`
- Scoring: `mlbops/api/services/content_scoring.py`
- Python queue API: `mlbops/api/routers/queue.py`
- Hub interaction: `mlbops/hub/components/QueueClient.tsx`
- Privileged post action: `mlbops/hub/app/api/queue/`
- Persistence: both `mlbops/api/db/database.py` and `mlbops/hub/lib/db.ts` when the field crosses runtimes.

**New Live Event Type:**
- Feed parsing/detection: `mlbops/api/live/detect.py`
- Tweet text: `mlbops/api/live/text.py`
- HTTP lifecycle: `mlbops/api/routers/live.py`
- UI: `mlbops/hub/components/LiveEventsClient.tsx`
- Persistence migration/schema: `mlbops/api/db/`.

**New Scheduled Automation:**
- Business job: `jobs/<job_name>.py`
- Local/VPS schedule example: `jobs/crontab.example` or `deploy/`
- Cloud workflow: `.github/workflows/<job_name>.yml`
- Reusable logic: import from `src/` or `mlbops/api/services/` rather than embedding it only in workflow YAML.

**New Research Study:**
- Study package: `research/<Topic>/`
- Reproducible runner: `research/<Topic>/scripts/<study_name>.py`
- Evidence: `research/<Topic>/data/` and `research/<Topic>/images/`
- Interpretation: `research/<Topic>/README.md` and `research/<Topic>/publication_brief.md`
- Production promotion: extract validated reusable code into `src/` or `mlbops/api/services/`; do not import research artifacts directly from a serving path.

**Utilities:**
- Shared baseball/data helper: `src/`
- API-only helper: `mlbops/api/services/` or the relevant `mlbops/api/live/` package
- Hub-only helper: `mlbops/hub/lib/`
- Shared React component: `mlbops/hub/components/`
- Operator-only maintenance command: `scripts/` or `deploy/` depending on whether it changes application data or VPS infrastructure.

## Special Directories

**`.planning/codebase/`:**
- Purpose: GSD-generated repository reference maps consumed by planning/execution workflows.
- Generated: Yes, by mapping workflows.
- Committed: Project-dependent; files are not excluded by `.gitignore`.

**`.agents/skills/`:**
- Purpose: Repository-local frontend, visual design, image generation, brand, and output-completeness guidance.
- Generated: No.
- Committed: Yes; skill manifests are tracked under `.agents/skills/*/SKILL.md`.

**`data/warehouse/mlb/`:**
- Purpose: Local mirror of stage-partitioned schedules, raw feed/live, enriched parquet, and registries.
- Generated: Mostly yes, by `src/ingestion/load_mlb_warehouse.py` and sync workflows.
- Committed: Only selected README/context/schedule files; game data and most JSON/parquet artifacts are excluded by `.gitignore`.

**`outputs/`:**
- Purpose: Runtime cards, boards, CSVs, and other generated publishing artifacts.
- Generated: Yes, by `scripts/`, `src/` renderers, and API generation routes.
- Committed: No; excluded by `.gitignore` and `.dockerignore`, and mounted separately in production per `deploy/README.md`.

**`morning_intel/snapshots/`:**
- Purpose: Daily intelligence JSON consumed by FastAPI and Hub intel surfaces.
- Generated: Yes, by `morning_intel/morning_intel.py`.
- Committed: Only `.gitkeep`; snapshot JSON is excluded by `.gitignore`.

**`mlbops/hub/.next/` and `mlbops/hub/node_modules/`:**
- Purpose: Next.js build output and installed JavaScript dependencies.
- Generated: Yes, by npm/Next.js.
- Committed: No; excluded by `mlbops/hub/.gitignore`, root `.gitignore`, and `.dockerignore`.

**`research/*/data/` and `research/*/images/`:**
- Purpose: Study-local evidence and publishable visual outputs.
- Generated: Yes, by the corresponding `research/*/scripts/` runner.
- Committed: Mixed; tracked CSV/Markdown evidence may be committed, while image formats are generally excluded by root `.gitignore`.

**`docs/archive/` and `docs/progress/`:**
- Purpose: Preserve superseded architecture and historical session records without overriding current truth.
- Generated: No.
- Committed: Mixed in the current worktree; current guidance remains `docs/CURRENT_STATE.md` and `deploy/README.md`.

**`supabase/`:**
- Purpose: Preserve a schema draft from an earlier control-plane direction.
- Generated: No.
- Committed: Yes, but not part of the current Postgres-on-VPS runtime described in `docs/CURRENT_STATE.md`.

**`.claude/worktrees/`, `graphify-out/`, caches, and virtual environments:**
- Purpose: Local agent workspaces, analysis indexes, tool caches, and installed runtimes.
- Generated: Yes.
- Committed: No; excluded by `.gitignore` or maintained outside the production source path.

---

*Structure analysis: 2026-07-10*
