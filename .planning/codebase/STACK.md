# Technology Stack

**Analysis Date:** 2026-07-10

## Languages

**Primary:**
- Python 3.11 in CI and production containers - Warehouse ingestion, FastAPI services, analytics, card rendering, scheduled jobs, and operational scripts in `src/`, `mlbops/api/`, `scripts/`, `jobs/`, and `morning_intel/`; runtime pins are in `Dockerfile.api` and `.github/workflows/daily_ingest.yml`.
- TypeScript 5.x - Next.js App Router pages, route handlers, database adapters, authentication, and external-service clients in `mlbops/hub/`; strict checking is configured in `mlbops/hub/tsconfig.json`.

**Secondary:**
- JavaScript/ES modules - PostCSS configuration in `mlbops/hub/postcss.config.mjs`.
- Bash - Local startup, rclone sync, VPS deployment, Docker operations, and scheduled ingest wrappers in `scripts/` and `deploy/`.
- SQL - Postgres schema and migrations in `mlbops/api/db/schema_postgres.sql`, `mlbops/api/db/migrate_*.py`, and `supabase/schema_drafts/001_mlbops_control_plane.sql`.
- CSS - Tailwind v4 entry styles and repository-specific Hub styling in `mlbops/hub/app/globals.css`.

## Runtime

**Environment:**
- Python 3.11 slim Debian image in production via `Dockerfile.api`; GitHub-hosted ingest and intel workflows also select Python 3.11 in `.github/workflows/daily_ingest.yml` and `.github/workflows/morning_intel.yml`.
- Node.js 22 Bookworm slim image for the production Hub via `mlbops/hub/Dockerfile`; the inspected workstation runtime is Node 22.22.3.
- Local Python environments are not uniform: `mlb_env.nosync/pyvenv.cfg` records Python 3.13.3 while the inspected host `python3` is 3.14.6. Use Python 3.11 for production parity.
- Docker Compose runs the production API, Hub, and Postgres services as documented in `docs/CURRENT_STATE.md` and `deploy/README.md`.

**Package Manager:**
- Python: pip; dependencies are range-constrained rather than locked in `requirements.txt` and `mlbops/api/requirements-api.txt`.
- Node: npm 10.9.8 on the inspected workstation; production uses `npm ci` in `mlbops/hub/Dockerfile`.
- Lockfile: `mlbops/hub/package-lock.json` is present at lockfile version 3; no Python lockfile is present.

## Frameworks

**Core:**
- FastAPI `>=0.110.0` - HTTP API, OpenAPI docs, CORS, static PNG serving, and routers under `mlbops/api/main.py` and `mlbops/api/routers/`.
- Uvicorn `>=0.29.0` with standard extras - ASGI server launched by `Dockerfile.api`.
- Next.js `16.2.1` - App Router Hub UI, server components, route handlers, middleware-style proxy, and backend proxy under `mlbops/hub/app/` and `mlbops/hub/proxy.ts`.
- React and React DOM `19.2.4` - Client and server UI components in `mlbops/hub/components/` and `mlbops/hub/app/`.
- Tailwind CSS `^4` with `@tailwindcss/postcss` `^4` - Hub styling configured by `mlbops/hub/postcss.config.mjs` and `mlbops/hub/app/globals.css`.

**Testing:**
- No repository-wide Python or TypeScript test runner is configured. Standalone Python checks live at `test_malli_score_v2.py`, `test_parquet.py`, and `test_parse.py`.
- No Jest, Vitest, Playwright, pytest, coverage, lint, or test script is declared in `mlbops/hub/package.json` or a root test configuration file.

**Build/Dev:**
- Next CLI `16.2.1` - `next dev --webpack`, optional `next dev --turbopack`, `next build`, and `next start` scripts in `mlbops/hub/package.json`.
- TypeScript `^5` - Strict no-emit checking through `mlbops/hub/tsconfig.json`; production Next builds currently set `typescript.ignoreBuildErrors` in `mlbops/hub/next.config.ts`, so run `npx tsc --noEmit` separately when validating types.
- Docker - Reproducible API and Hub images from `Dockerfile.api` and `mlbops/hub/Dockerfile`.
- GitHub Actions - Warehouse ingest, morning intel, and manual card generation in `.github/workflows/`.

## Key Dependencies

**Critical:**
- pandas `>=2.2` root / `>=2.0` API and NumPy `>=2.0` - Tabular aggregation and Statcast analysis throughout `src/`, `scripts/`, `morning_intel/`, and `mlbops/api/services/`.
- pyarrow `>=14.0` - Parquet reads and writes for the warehouse in `src/ingestion/load_mlb_warehouse.py`, `mlbops/api/services/pitcher_dashboard.py`, and analytics scripts.
- pybaseball `>=2.2` - Baseball Savant Statcast retrieval, imported lazily by `src/ingestion/load_mlb_warehouse.py`.
- requests `>=2.28` - MLB Stats API, notification, CDN, and xAI HTTP calls across Python services and jobs.
- Pillow `>=10.0`, Matplotlib `>=3.8`, and Seaborn `>=0.13` - Branded card and chart rendering in `src/*/render.py` and `scripts/*card*.py`.
- Anthropic SDK `>=0.40` - Claude-assisted queue redrafts and morning intel drafts in `mlbops/api/routers/queue.py` and `morning_intel/morning_intel.py`.
- `better-sqlite3` `^12.8.0`, `pg` `^8.16.3`, and psycopg binary `>=3.2` - Dual SQLite/Postgres adapters in `mlbops/hub/lib/db.ts` and `mlbops/api/db/database.py`.
- `oauth-1.0a` `^2.2.6` - X/Twitter OAuth 1.0a signing for media upload and posting in `mlbops/hub/lib/twitter.ts`.
- Resend `^6.9.4` and Twilio `^5.13.0` - Hub-side email and WhatsApp notification clients in `mlbops/hub/lib/resend.ts` and `mlbops/hub/lib/twilio.ts`.

**Infrastructure:**
- python-dotenv `>=1.0` - Local environment-file loading by `mlbops/api/main.py`, `morning_intel/morning_intel.py`, and `jobs/weekly_report.py`; environment files must remain uncommitted.
- `@opentelemetry/api` `^1.9.1` - Declared in `mlbops/hub/package.json`, but no active instrumentation import was detected.
- rclone - Google Drive archive synchronization in `.github/workflows/daily_ingest.yml`, `.github/workflows/morning_intel.yml`, `scripts/pull_mlbops_from_drive.sh`, and `deploy/vps_pull_warehouse_from_drive.sh`.
- curl - Installed in both application images and used for X/Twitter I/O by `mlbops/hub/lib/twitter.ts`.
- scikit-learn, SciPy, and joblib - Required by projection/research scripts such as `scripts/run_starter_projection_experiment.py`, `scripts/prielipp_col_case_study.py`, and `scripts/batter_card_daily.py`, but not declared in `requirements.txt`; run these only in an environment that supplies them.

## Configuration

**Environment:**
- Keep runtime configuration outside source control. Production reads `/srv/mlbops/env/mlbops.env` through Docker Compose as documented in `deploy/README.md`; local examples exist at `mlbops/.env.example`, `mlbops/hub/.env.local.example`, and `jobs/.env.example` and their contents were not inspected for this map.
- Use `MLBOPS_DB_BACKEND=postgres` plus `DATABASE_URL` in production; omit or set SQLite mode for local development. Both adapters are implemented in `mlbops/api/db/database.py` and `mlbops/hub/lib/db.ts`.
- Set path controls through `MLB_WAREHOUSE_DIR`, `MLBOPS_OUTPUTS_DIR`, `MLB_INTEL_SNAPSHOTS_DIR`, and `MLB_REPO_ROOT`; resolution logic is centralized in `mlbops/api/paths.py`.
- Configure Hub-to-API routing with `FASTAPI_BASE_URL` or `INTERNAL_API_URL`; browser clients default to the authenticated `/api/backend` proxy through `mlbops/hub/lib/api.ts`. Production also requires the same strong `MLBOPS_API_SERVICE_TOKEN` in Hub and FastAPI.
- Configure session, app password, notifications, X/Twitter, and AI providers only through environment variables consumed by `mlbops/hub/lib/security.ts`, `mlbops/hub/lib/twitter.ts`, `mlbops/hub/lib/resend.ts`, `mlbops/hub/lib/twilio.ts`, and `mlbops/api/routers/queue.py`.

**Build:**
- `Dockerfile.api` builds the Python API from both requirements files and sets `PYTHONPATH=/app` plus headless Matplotlib.
- `mlbops/hub/Dockerfile` uses `npm ci`, builds with `next build`, and runs `next start` on port 3000.
- `mlbops/hub/next.config.ts` limits static generation concurrency and allows known local/VPS development origins.
- `docker-compose.hub-dev.yml` overrides only the Hub for bind-mounted Next development while retaining production API, database, warehouse, and output services.

## Platform Requirements

**Development:**
- macOS or Linux with Git, Python, Node/npm, and the system libraries needed by Pillow/Matplotlib and `better-sqlite3`.
- Use Python 3.11 for production parity, install `requirements.txt`, then install Hub packages from `mlbops/hub/package-lock.json` with `npm ci`.
- A local warehouse mirror under `data/warehouse/mlb` is optional and stale by default; refresh it through `scripts/pull_mlbops_from_drive.sh` before current-season data work.
- Run the API on port 8000 and Hub on port 3000 through `scripts/start_mlbops.sh`, or use the documented Docker Hub dev override in `deploy/vps_hub_dev.sh`.

**Production:**
- Hostinger VPS reachable privately through Tailscale, with app files at `/srv/mlbops/app`, as defined by `docs/CURRENT_STATE.md` and `deploy/README.md`.
- Docker Compose services: Postgres, FastAPI API, and production Next.js Hub. The Hub is published through the VPS/Tailscale address; Postgres remains on the container network.
- Persistent host storage: `/srv/mlbops/postgres`, `/srv/mlbops/warehouse/mlb`, `/srv/mlbops/outputs`, and `/srv/mlbops/logs`.
- Deploy with rsync/SSH via `deploy/sync_app_to_vps.sh`; the VPS app directory is not a Git checkout.

---

*Stack analysis: 2026-07-10*
