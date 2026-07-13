<!-- refreshed: 2026-07-10 -->
# Architecture

**Analysis Date:** 2026-07-10

## System Overview

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                    Operators and automation                              │
├───────────────────────┬──────────────────────┬───────────────────────────┤
│ Browser over Tailscale│ GitHub Actions       │ CLI / cron / research     │
│ `mlbops/hub/app/`     │ `.github/workflows/` │ `jobs/`, `scripts/`,      │
│                       │                      │ `research/`, `notebooks/` │
└───────────┬───────────┴───────────┬──────────┴──────────────┬────────────┘
            │                       │                         │
            ▼                       │                         │
┌───────────────────────────────────┐                         │
│ Next.js Hub and control plane     │                         │
│ `mlbops/hub/`                     │                         │
│ UI, session/CSRF, API proxy, X    │                         │
└───────────┬───────────────────────┘                         │
            │ `/api/backend/*`                                │
            ▼                                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ FastAPI application and Python orchestration                            │
│ `mlbops/api/` → `src/`, `scripts/`, `morning_intel/`                    │
└───────────┬──────────────────────────┬───────────────────────┬───────────┘
            │                          │                       │
            ▼                          ▼                       ▼
┌──────────────────────┐  ┌────────────────────────┐  ┌────────────────────┐
│ Queue/control state  │  │ MLB warehouse          │  │ Generated artifacts│
│ Postgres production  │  │ `/data/warehouse/mlb`  │  │ `/outputs`,        │
│ SQLite local dev     │  │ local mirror under     │  │ snapshots, reports │
│ `mlbops/api/db/`     │  │ `data/warehouse/mlb/`  │  │ `outputs/`,        │
│ `mlbops/hub/lib/db.ts`│ │                        │  │ `morning_intel/`   │
└──────────────────────┘  └────────────────────────┘  └────────────────────┘
            │                          ▲                       │
            ▼                          │                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ External services: MLB Stats API, Statcast/pybaseball, Google Drive,    │
│ X API, Anthropic/Grok, Resend, Twilio                                   │
│ Call sites: `src/ingestion/`, `mlbops/api/`, `mlbops/hub/lib/`,         │
│ `.github/workflows/`, `morning_intel/morning_intel.py`                  │
└──────────────────────────────────────────────────────────────────────────┘
```

The repository contains two connected systems: the MLB warehouse in `src/ingestion/` and `data/warehouse/mlb/`, and the Mallitalytics content operations application in `mlbops/`, `jobs/`, `morning_intel/`, and the daily scripts in `scripts/`. Production runs the Hub, API, and Postgres on the VPS; the current production contract is documented in `docs/CURRENT_STATE.md` and `deploy/README.md`.

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Next.js Hub UI | Operator-facing dashboard, schedule, intel, insights, cards, queue, live events, fantasy, growth, settings, and watchlist surfaces | `mlbops/hub/app/` |
| Hub control plane | Password session, CSRF, same-origin checks, rate limits, API proxying, direct queue actions, notifications, and X posting | `mlbops/hub/proxy.ts`, `mlbops/hub/app/api/`, `mlbops/hub/lib/security.ts`, `mlbops/hub/lib/twitter.ts` |
| FastAPI composition root | Loads runtime paths, mounts generated assets, configures CORS, and registers domain routers | `mlbops/api/main.py` |
| FastAPI routers | HTTP contracts for analytics, briefing, cards, fantasy, insights, intel, leaderboards, live events, queue, schedule, readiness, and watchlist | `mlbops/api/routers/` |
| API services | Domain calculations for analytics, taxonomy, scoring, fantasy projections, and pitcher dashboards | `mlbops/api/services/` |
| Queue persistence | Shared SQLite/Postgres compatibility helpers used by Python routes and jobs | `mlbops/api/db/database.py` |
| Hub persistence | Async SQLite/Postgres helpers used by Next.js route handlers for posting, auditing, streaks, and quick posts | `mlbops/hub/lib/db.ts` |
| Warehouse ingestion | Fetches schedules and feed/live, joins Statcast to feed play IDs, maintains player registry, and writes raw/parquet artifacts | `src/ingestion/load_mlb_warehouse.py`, `src/ingestion/mlb_warehouse_schema.py`, `src/ingestion/player_registry.py` |
| Reusable analytics/rendering | Shared data extraction, metrics, branded rendering, and slate/card primitives | `src/` |
| Card and board executables | CLI entry points for pitcher/batter cards, daily boards, trackers, exports, and analyses | `scripts/` |
| Morning intelligence | Scans warehouse data, builds anomalies/milestones/watchlist pulse, optionally drafts content/cards, writes snapshots, and sends notifications | `morning_intel/morning_intel.py` |
| Scheduled jobs | Watchlist-driven card generation and weekly performance reporting | `jobs/daily_card_generator.py`, `jobs/weekly_report.py` |
| Research workbenches | Reproducible exploratory studies, publication briefs, datasets, figures, and durable experiment state | `research/`, `notebooks/` |
| Deployment and CI | Builds and deploys the VPS app, syncs warehouse data, runs ingest/intel automation, and verifies health | `Dockerfile.api`, `mlbops/hub/Dockerfile`, `deploy/`, `.github/workflows/` |

## Pattern Overview

**Overall:** Modular monorepo with a two-runtime web application, filesystem data lake, shared relational control plane, and script-oriented batch pipelines.

**Key Characteristics:**
- Keep browser-facing authentication and privileged posting operations in the Next.js control plane under `mlbops/hub/app/api/`; browser API traffic defaults to the authenticated catch-all proxy in `mlbops/hub/app/api/backend/[...path]/route.ts`.
- Keep analytics, warehouse reads, generation orchestration, and long synchronous Python work in FastAPI routers/services under `mlbops/api/`, offloading blocking work with `run_in_threadpool` as demonstrated by `mlbops/api/routers/cards.py` and `mlbops/api/routers/leaderboards.py`.
- Keep reusable analytics and rendering logic in importable modules under `src/`; reserve `scripts/` for executable composition and CLI concerns.
- Treat the production warehouse mounted at `/data/warehouse/mlb` as the live read model; `data/warehouse/mlb/` is the local mirror selected through `mlbops/api/paths.py`.
- Treat Postgres as production queue/control state and SQLite `data/hub.db` as the local-development fallback; both Python and TypeScript adapters must preserve the same schema and behavior through `mlbops/api/db/database.py` and `mlbops/hub/lib/db.ts`.
- Treat `research/` and `notebooks/` as offline workbenches, not production runtime dependencies; promote reusable conclusions into `src/`, `mlbops/api/services/`, or a production script only after validation.

## Layers

**Presentation Layer:**
- Purpose: Render operational workflows and collect operator actions.
- Location: `mlbops/hub/app/`, `mlbops/hub/components/`, `mlbops/hub/app/globals.css`
- Contains: Next.js App Router pages, Server Components, Client Components, route loading/error states, navigation, and page-specific view models.
- Depends on: `mlbops/hub/lib/api.ts`, Hub route handlers under `mlbops/hub/app/api/`, and FastAPI JSON contracts under `mlbops/api/routers/`.
- Used by: Browser sessions entering through `mlbops/hub/proxy.ts`.

**Hub Control Plane:**
- Purpose: Enforce owner access and perform server-side privileged actions.
- Location: `mlbops/hub/proxy.ts`, `mlbops/hub/app/api/`, `mlbops/hub/lib/`
- Contains: Signed session cookies, CSRF enforcement, rate limiting, audit logging, protected FastAPI proxying, database helpers, X posting, email, and WhatsApp clients.
- Depends on: Shared Postgres/SQLite state through `mlbops/hub/lib/db.ts` and internal FastAPI through `mlbops/hub/app/api/backend/[...path]/route.ts`.
- Used by: Client Components through `getApiBase()` and `secureFetch()` in `mlbops/hub/lib/api.ts`.

**HTTP Application Layer:**
- Purpose: Validate requests, map domain operations to HTTP, and shape JSON responses.
- Location: `mlbops/api/main.py`, `mlbops/api/routers/`
- Contains: FastAPI composition, Pydantic request models, endpoint handlers, static output serving, and route-specific orchestration.
- Depends on: `mlbops/api/services/`, `mlbops/api/db/`, `mlbops/api/live/`, `src/`, `scripts/`, warehouse files, and external MLB endpoints.
- Used by: Hub Server Components, Hub API proxy, local clients, and operational smoke checks.

**Domain and Analysis Layer:**
- Purpose: Implement reusable calculations and content semantics independent of HTTP rendering.
- Location: `mlbops/api/services/`, `mlbops/api/live/`, `src/`
- Contains: Leader scoring, content taxonomy, fantasy projections, live event detection, boxscore aggregation, Statcast transforms, branded image renderers, and card support utilities.
- Depends on: pandas/NumPy, filesystem datasets, MLB response shapes, and shared style assets in `src/mallitalytics_style.py` and `assets/`.
- Used by: FastAPI routers, `jobs/`, `morning_intel/`, and `scripts/`.

**Persistence and Data Layer:**
- Purpose: Store control state and provide read-optimized baseball datasets.
- Location: `mlbops/api/db/`, `mlbops/hub/lib/db.ts`, `data/warehouse/mlb/`, `morning_intel/snapshots/`, `outputs/`
- Contains: Postgres/SQLite schema and adapters, queue/watchlist/live/audit/performance records, raw feed JSON, enriched parquet, player registries, schedule exports, snapshots, and generated PNG/CSV files.
- Depends on: Runtime path resolution in `mlbops/api/paths.py`, production volume mounts documented in `deploy/README.md`, and schemas under `mlbops/api/db/schema_postgres.sql`.
- Used by: Both web runtimes and all batch/reporting flows.

**Batch and Operations Layer:**
- Purpose: Materialize data, generate artifacts, schedule recurring work, sync storage, and deploy services.
- Location: `src/ingestion/`, `scripts/`, `jobs/`, `morning_intel/`, `deploy/`, `.github/workflows/`
- Contains: CLI parsers, subprocess composition, thread pools, rclone/rsync wrappers, GitHub Actions, Docker builds, health checks, and data audits.
- Depends on: External APIs, Google Drive archive, local or mounted warehouse paths, and production services.
- Used by: GitHub Actions, VPS cron/manual operators, and FastAPI generation endpoints.

## Data Flow

### Primary Browser Request Path

1. `mlbops/hub/proxy.ts:66` validates the signed owner session and redirects unauthenticated page requests to `/login`.
2. Server Components call FastAPI directly with the internal URL from `mlbops/hub/lib/api.ts:10`; Client Components resolve the same helper to `/api/backend` at `mlbops/hub/lib/api.ts:17`.
3. `mlbops/hub/app/api/backend/[...path]/route.ts:8` requires a session for reads, CSRF for writes, strips browser credentials, and forwards the request to FastAPI.
4. `mlbops/api/main.py:75` dispatches to a router under `mlbops/api/routers/`; blocking pandas, filesystem, network, and subprocess operations run in Starlette's thread pool in route modules such as `mlbops/api/routers/cards.py:429`.
5. Routers read Postgres/SQLite via `mlbops/api/db/database.py:137`, read warehouse/snapshot files via `mlbops/api/paths.py:50`, or invoke domain modules under `mlbops/api/services/` and `src/`.
6. JSON returns through the proxy; generated images are served by FastAPI's `/static` mount configured at `mlbops/api/main.py:73`.

### Card Generation and Publishing Flow

1. A Hub page submits a protected write through `secureFetch()` in `mlbops/hub/lib/api.ts:55` to a card endpoint registered in `mlbops/api/routers/cards.py:49`.
2. The card router validates input, executes a script such as `scripts/mallitalytics_daily_card.py` or `scripts/batter_card_daily.py`, extracts the generated path/metadata, and inserts a draft with `mlbops/api/db/database.py:159`.
3. The queue UI in `mlbops/hub/components/QueueClient.tsx` loads and edits the shared draft through FastAPI queue endpoints in `mlbops/api/routers/queue.py`.
4. Approval reaches `mlbops/hub/app/api/queue/[id]/approve/route.ts:10`, which enforces session/CSRF/rate limits, uploads media and posts through `mlbops/hub/lib/twitter.ts`, then records posted or failed state through `mlbops/hub/lib/db.ts`.
5. Audit and performance metadata are stored by `mlbops/hub/lib/security.ts:161`, `mlbops/hub/lib/db.ts`, and analytics endpoints in `mlbops/api/routers/analytics.py`.

### Warehouse Ingestion Flow

1. GitHub Actions or a VPS/manual command starts `src/ingestion/load_mlb_warehouse.py:458`; workflow composition lives in `.github/workflows/daily_ingest.yml` and VPS wrappers live in `deploy/vps_daily_ingest.sh`.
2. The loader fetches schedule rows from MLB Stats API at `src/ingestion/load_mlb_warehouse.py:119`, filters final games, and fetches feed/live concurrently at `src/ingestion/load_mlb_warehouse.py:675`.
3. `ensure_raw()` writes compressed feed JSON and merges player registry data at `src/ingestion/load_mlb_warehouse.py:225`.
4. `process_pitches_enriched()` joins pybaseball Statcast pitches to feed play IDs and writes curated parquet columns using `src/ingestion/load_mlb_warehouse.py:280` and `src/ingestion/mlb_warehouse_schema.py`.
5. GitHub Actions copy the materialized warehouse to the Google Drive archive through `.github/workflows/daily_ingest.yml`; production reads the separately synchronized VPS mount described in `docs/CURRENT_STATE.md`.

### Morning Intelligence Flow

1. Automation or `POST /intel/run` in `mlbops/api/routers/intel.py:180` launches `morning_intel/morning_intel.py`.
2. `run_intel()` at `morning_intel/morning_intel.py:1413` reads enriched parquet, computes pitcher/batter anomalies, hydrates MLB people/transaction/schedule context, and builds milestone/watchlist sections.
3. Optional LLM drafting and card subprocesses create queue items through `morning_intel/morning_intel.py:1292` and `morning_intel/morning_intel.py:1304`.
4. The job writes `morning_intel/snapshots/intel_YYYY-MM-DD.json` at `morning_intel/morning_intel.py:1594` and optionally sends notifications.
5. FastAPI serves snapshots and computed standouts through `mlbops/api/routers/intel.py`; the Hub renders them in `mlbops/hub/app/intel/page.tsx` and `mlbops/hub/components/IntelStandoutsPanel.tsx`.

**State Management:**
- UI-local state uses React hooks inside Client Components such as `mlbops/hub/components/QueueClient.tsx` and `mlbops/hub/app/insights/page.tsx`; Server Components fetch uncached request data directly in pages such as `mlbops/hub/app/page.tsx`.
- Durable operational state belongs in Postgres production or SQLite local development through `mlbops/api/db/database.py` and `mlbops/hub/lib/db.ts`.
- Analytics datasets and generated artifacts remain filesystem state under the resolved warehouse, `morning_intel/snapshots/`, and `outputs/` paths selected by `mlbops/api/paths.py`.
- Process-local caches and locks are acceptable only for recomputable acceleration, as used by `mlbops/api/routers/leaderboards.py`, `mlbops/api/routers/insights.py`, and `mlbops/api/services/fantasy_service.py`.

## Key Abstractions

**Resolved Runtime Paths:**
- Purpose: Decouple local repository paths from production volume mounts.
- Examples: `mlbops/api/paths.py`, `scripts/mallitalytics_daily_card.py`, `scripts/batter_card_daily.py`
- Pattern: Read path overrides from process environment, reject placeholder values, fall back to repository-relative local paths, and guard cloud-backed filesystem operations.

**Queue Item:**
- Purpose: Shared lifecycle record for generated and manual content from draft through posted/rejected/failed states.
- Examples: `mlbops/api/db/database.py`, `mlbops/hub/lib/db.ts`, `mlbops/api/services/content_taxonomy.py`, `mlbops/hub/components/QueueClient.tsx`
- Pattern: Relational core columns plus normalized taxonomy fields and extensible `meta_json`; all producers insert through shared helpers.

**Router plus Sync Worker:**
- Purpose: Keep async HTTP handlers responsive while retaining synchronous pandas/filesystem/script implementations.
- Examples: `mlbops/api/routers/cards.py`, `mlbops/api/routers/queue.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/api/routers/briefing.py`
- Pattern: Private `_operation_sync()` function performs work; thin `async` endpoint delegates with `run_in_threadpool`.

**Warehouse Game Artifact:**
- Purpose: Make every game independently discoverable and recomputable.
- Examples: `src/ingestion/load_mlb_warehouse.py`, `src/ingestion/mlb_warehouse_schema.py`, `src/ingestion/boxscore_aggregate.py`
- Pattern: Stage-partitioned raw feed and enriched parquet share `game_{game_pk}_{YYYYMMDD}_...` naming; season-level registries and schedule exports provide context.

**Renderer Package plus CLI:**
- Purpose: Separate reusable fetch/transform/render primitives from executable output orchestration.
- Examples: `src/probables_board/` with `scripts/probables_board_daily.py`, `src/games_of_day/` with `scripts/games_of_day_board.py`, `src/hr_tracker/` with `scripts/hr_tracker_daily.py`
- Pattern: Importable package exposes domain functions; script resolves arguments, paths, output formats, and stdout metadata consumed by FastAPI.

## Entry Points

**FastAPI Service:**
- Location: `mlbops/api/main.py`
- Triggers: `uvicorn api.main:app`, `Dockerfile.api`, or `start_hub.sh`.
- Responsibilities: Load configuration, resolve outputs, configure CORS/static files, register routers, and expose health/system operations.

**Next.js Hub:**
- Location: `mlbops/hub/app/layout.tsx`, `mlbops/hub/app/page.tsx`, `mlbops/hub/proxy.ts`
- Triggers: `npm run dev`, `npm run build`, or `npm run start` from `mlbops/hub/package.json`.
- Responsibilities: Render the application shell/routes, protect navigation, proxy FastAPI, and host privileged route handlers.

**Warehouse CLI:**
- Location: `src/ingestion/load_mlb_warehouse.py`
- Triggers: `python -m src.ingestion.load_mlb_warehouse`, GitHub Actions, or VPS ingest wrappers.
- Responsibilities: Fetch schedules/raw feeds, build enriched parquet, update registries, and refresh schedule exports.

**Morning Intel CLI:**
- Location: `morning_intel/morning_intel.py`
- Triggers: GitHub Actions, FastAPI `POST /intel/run`, or manual CLI.
- Responsibilities: Compute daily signals, populate queue drafts/cards, write snapshots, and send digest notifications.

**Daily and Weekly Jobs:**
- Location: `jobs/daily_card_generator.py`, `jobs/weekly_report.py`
- Triggers: Cron/manual job execution described by `jobs/crontab.example`.
- Responsibilities: Select watchlist performers, generate queue content, refresh post metrics, and deliver reports.

**Research Runners:**
- Location: `research/*/scripts/`, `scripts/run_starter_projection_experiment.py`, `notebooks/`
- Triggers: Manual reproducible study runs or notebook exploration.
- Responsibilities: Produce study-local datasets, figures, briefs, and evidence; do not serve production requests.

## Architectural Constraints

- **Threading:** FastAPI endpoints execute synchronous I/O and pandas work through `run_in_threadpool` in `mlbops/api/routers/`; ingestion uses bounded `ThreadPoolExecutor` pools in `src/ingestion/load_mlb_warehouse.py`. Do not execute long CPU/filesystem/subprocess work directly on the async event loop.
- **Global state:** Module-level caches/locks exist in `mlbops/api/routers/leaderboards.py`, `mlbops/api/routers/insights.py`, and `mlbops/api/services/fantasy_service.py`; Hub rate limiting is process-local in `mlbops/hub/lib/security.ts`. Treat these as per-process accelerators/guards, never authoritative state.
- **Dual database access:** Python and TypeScript independently translate SQLite-oriented SQL for Postgres in `mlbops/api/db/database.py` and `mlbops/hub/lib/db.ts`. Any schema/query change must work in both adapters and against `mlbops/api/db/schema_postgres.sql`.
- **Filesystem source of truth:** Production analytics require the VPS warehouse mounted at `/data/warehouse/mlb` and outputs mounted at `/outputs`, per `deploy/README.md`; local `data/warehouse/mlb/` may be stale and is not production proof.
- **Process working directory:** Python API imports assume Uvicorn starts from `mlbops/`, while scripts often assume repository root or inject it into `sys.path`; preserve the launch contracts in `Dockerfile.api`, `start_hub.sh`, and route subprocess commands.
- **Private-network trust boundary:** FastAPI does not provide the Hub's session layer; production exposure must remain behind the protected Hub/private network described in `docs/CURRENT_STATE.md`. Browser writes should use `mlbops/hub/app/api/backend/[...path]/route.ts`, not a public direct API URL.
- **Built frontend:** Production Hub uses the image built by `mlbops/hub/Dockerfile`; source synchronization alone does not update `next start` unless using the explicit development override in `deploy/vps_hub_dev.sh`.
- **Next.js version contract:** Before changing Hub framework behavior, follow `mlbops/hub/AGENTS.md` and inspect the installed Next.js documentation under `mlbops/hub/node_modules/next/dist/docs/`.
- **Generated/ignored data:** Warehouse game files, snapshots, outputs, caches, secrets, and local databases are intentionally excluded by `.gitignore` and `.dockerignore`; code must recreate or mount them rather than import them into source control.

## Anti-Patterns

### Bypassing the Hub Control Plane

**What happens:** A Client Component calls a direct FastAPI URL for a mutating action instead of resolving `/api/backend` and using CSRF-aware helpers from `mlbops/hub/lib/api.ts`.
**Why it's wrong:** FastAPI routes do not enforce the Hub's signed session, CSRF, rate-limit, and audit controls implemented in `mlbops/hub/lib/security.ts` and `mlbops/hub/app/api/backend/[...path]/route.ts`.
**Do this instead:** Use `getApiBase()` plus `secureFetch()` from `mlbops/hub/lib/api.ts`, or add a narrowly privileged Hub route handler under `mlbops/hub/app/api/` for actions such as X posting.

### Adding a Third Database Path

**What happens:** A feature opens SQLite/Postgres directly from a router or component with new SQL translation rules.
**Why it's wrong:** Production and local behavior already depend on synchronized adapters in `mlbops/api/db/database.py` and `mlbops/hub/lib/db.ts`; another path causes schema and transaction drift.
**Do this instead:** Extend the appropriate shared adapter, update `mlbops/api/db/schema_postgres.sql`, and add a migration under `mlbops/api/db/` when persisted structure changes.

### Putting Reusable Logic in Large Route or Page Files

**What happens:** Analytics, parsing, or rendering logic grows inside files such as `mlbops/api/routers/queue.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/hub/components/QueueClient.tsx`, or `mlbops/hub/app/insights/page.tsx`.
**Why it's wrong:** HTTP/view concerns become inseparable from calculation logic, and script/job reuse requires duplication.
**Do this instead:** Move Python calculations to `mlbops/api/services/` or `src/`, and move reusable Hub behavior to `mlbops/hub/components/` or `mlbops/hub/lib/`; leave route/page files responsible for orchestration and presentation.

### Treating Local Warehouse Data as Current Production Data

**What happens:** Current-season behavior is diagnosed against `data/warehouse/mlb/` on the Mac.
**Why it's wrong:** The live API reads the VPS mount documented in `docs/CURRENT_STATE.md`, while the local mirror may be stale, partial, or cloud-placeholder backed.
**Do this instead:** Verify or ingest `/data/warehouse/mlb` through the VPS workflow in `deploy/README.md`; use local warehouse files only for offline development and historical studies.

### Promoting Research Artifacts Directly into Runtime

**What happens:** A production route imports notebooks, generated CSVs, or study-local helpers from `research/`.
**Why it's wrong:** Research packages document exploratory assumptions and may depend on partial local data, as stated in `research/*/README.md`.
**Do this instead:** Validate the method, extract stable code into `src/` or `mlbops/api/services/`, and make production data/path assumptions explicit.

## Error Handling

**Strategy:** Validate at HTTP/CLI boundaries, convert expected failures into typed responses or explicit exit states, record publishing failures durably, and allow batch work to continue per game where practical.

**Patterns:**
- FastAPI routers raise `HTTPException` for invalid input, missing artifacts, disabled operations, and upstream failures in `mlbops/api/routers/`; async handlers delegate blocking failures through `run_in_threadpool`.
- Hub route handlers return `NextResponse.json` with explicit status codes and audit failed writes through `mlbops/hub/lib/security.ts`; X failures persist queue status/error text in `mlbops/hub/app/api/queue/[id]/approve/route.ts`.
- Database context managers commit on success, rollback on exceptions, and close connections in `mlbops/api/db/database.py:137`; SQLite queue insertion retries lock/busy errors in `mlbops/api/db/database.py:198`.
- Ingestion catches per-game raw and enrichment failures, reports them, and continues remaining futures in `src/ingestion/load_mlb_warehouse.py:682` and `src/ingestion/load_mlb_warehouse.py:741`.
- Path probes catch `OSError`/`TimeoutError` for cloud-backed filesystems in `mlbops/api/paths.py`; research scripts document skipping unavailable dataless files under `research/*/README.md`.

## Cross-Cutting Concerns

**Logging:** Services use stdout/stderr and Uvicorn/container logs in `mlbops/api/` and `scripts/`; Hub route handlers use server `console.error` and durable security audit rows through `mlbops/hub/lib/security.ts`; operational logs and health checks are organized by `deploy/`.

**Validation:** FastAPI uses Pydantic models and `Query` constraints in `mlbops/api/routers/`; Hub handlers validate request bodies/IDs before persistence in `mlbops/hub/app/api/`; ingestion validates game finality, names, dates, and existing artifacts in `src/ingestion/load_mlb_warehouse.py`.

**Authentication:** The Hub signs a 12-hour owner session cookie and enforces CSRF/same-origin checks in `mlbops/hub/lib/security.ts`; `mlbops/hub/proxy.ts` protects application routes, while FastAPI relies on the private deployment/proxy boundary described in `docs/CURRENT_STATE.md`.

---

*Architecture analysis: 2026-07-10*
