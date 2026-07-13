# External Integrations

**Analysis Date:** 2026-07-10

## APIs & External Services

**Baseball Data:**
- MLB Stats API - Schedule, live feed, box scores, linescores, player/team metadata, transactions, probable pitchers, and game logs.
  - SDK/Client: Python `requests` and native `fetch`; central call sites include `src/ingestion/mlb_warehouse_schema.py`, `src/ingestion/load_mlb_warehouse.py`, `mlbops/api/live/fetch.py`, `mlbops/api/routers/schedule.py`, `mlbops/api/routers/cards.py`, and `morning_intel/morning_intel.py`.
  - Auth: None; public endpoints under `https://statsapi.mlb.com/api/v1` and `/api/v1.1`.
- Baseball Savant/Statcast - Per-game pitch data merged with MLB feed `play_id` values into Parquet.
  - SDK/Client: `pybaseball.statcast_single_game` in `src/ingestion/load_mlb_warehouse.py`.
  - Auth: None.
- MLB static media CDN - Team logos and player headshots used by Hub pages and generated cards.
  - SDK/Client: Browser/native `fetch` or image URLs in `mlbops/hub/app/page.tsx`, `mlbops/hub/components/ScheduleGameCard.tsx`, `src/mlb_headshot.py`, and card renderers.
  - Auth: None.
- ESPN image CDN and FlagCDN - Fallback team marks and country flags for card rendering.
  - SDK/Client: Python `requests` in `scripts/batter_card_daily.py`, `scripts/batter_card_seasonal.py`, and `src/insight_tiles/render.py`.
  - Auth: None.

**Publishing:**
- X/Twitter API v2 and media upload API v1.1 - Upload PNG media, publish approved posts, and retrieve public metrics.
  - SDK/Client: Custom OAuth 1.0a signing with `oauth-1.0a`, Node `crypto`, system `curl`, and native `fetch` in `mlbops/hub/lib/twitter.ts`; Python metrics retrieval also exists in `jobs/weekly_report.py`.
  - Auth: `TWITTER_API_KEY`, `TWITTER_API_SECRET`, `TWITTER_ACCESS_TOKEN`, `TWITTER_ACCESS_TOKEN_SECRET`, and `TWITTER_BEARER_TOKEN`.

**AI Services:**
- Anthropic Claude - Secondary-assist queue redrafts and morning-intel tweet draft generation.
  - SDK/Client: Python `anthropic.Anthropic` in `mlbops/api/routers/queue.py` and `morning_intel/morning_intel.py`.
  - Auth: `ANTHROPIC_API_KEY`; model selection through `ANTHROPIC_MODEL`.
- xAI Grok - Optional queue redraft provider using the OpenAI-compatible chat-completions endpoint.
  - SDK/Client: Python `requests` in `mlbops/api/routers/queue.py`.
  - Auth: `X_API_KEY`; model selection through `GROK_MODEL`.

**Notifications:**
- Resend - Morning digest, weekly report, and ingest-failure email delivery.
  - SDK/Client: `resend` Node SDK in `mlbops/hub/lib/resend.ts`; direct HTTPS calls in `morning_intel/morning_intel.py`, `jobs/weekly_report.py`, and `.github/workflows/daily_ingest.yml`.
  - Auth: `RESEND_API_KEY`, with sender/recipient configuration in `RESEND_FROM_EMAIL` and `RESEND_TO_EMAIL`.
- Twilio WhatsApp - Morning and weekly digest delivery.
  - SDK/Client: `twilio` Node SDK in `mlbops/hub/lib/twilio.ts`; direct REST calls in `morning_intel/morning_intel.py` and `jobs/weekly_report.py`.
  - Auth: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM`, and `TWILIO_WHATSAPP_TO`.

**Archive Synchronization:**
- Google Drive - Canonical warehouse archive and CI exchange point for warehouse files, intel snapshots, and the legacy SQLite Hub database.
  - SDK/Client: rclone CLI in `.github/workflows/daily_ingest.yml`, `.github/workflows/morning_intel.yml`, `scripts/pull_mlbops_from_drive.sh`, and `deploy/vps_pull_warehouse_from_drive.sh`; the live application does not call the Drive HTTP API during requests.
  - Auth: GitHub Actions uses `RCLONE_CONFIG_BASE64` or `GDRIVE_SERVICE_ACCOUNT` plus `GDRIVE_FOLDER_ID`; local/VPS runs use an external rclone configuration. Remote/path selection uses `RCLONE_REMOTE`, `GDRIVE_WAREHOUSE_PATH`, and `GDRIVE_INTEL_SNAPSHOTS_PATH`.

## Data Storage

**Databases:**
- PostgreSQL - Production control-plane database for content queue, watchlist, live events, performance metrics, notification history, and security audit events.
  - Connection: `MLBOPS_DB_BACKEND=postgres` and `DATABASE_URL`.
  - Client: psycopg 3 adapter in `mlbops/api/db/database.py` and `pg.Pool` in `mlbops/hub/lib/db.ts`; schema source is `mlbops/api/db/schema_postgres.sql` with migration helpers under `mlbops/api/db/`.
- SQLite - Local-development and legacy-import fallback at `data/hub.db`, using WAL mode.
  - Connection: default when `MLBOPS_DB_BACKEND` is absent; optional Hub path override through `HUB_DB_PATH`.
  - Client: Python standard-library `sqlite3` in `mlbops/api/db/database.py` and `better-sqlite3` in `mlbops/hub/lib/db.ts`.
- Supabase - Not an active runtime integration. `supabase/schema_drafts/001_mlbops_control_plane.sql` and `scripts/export_hub_db_for_supabase.py` are schema/export artifacts; current production uses VPS PostgreSQL per `docs/CURRENT_STATE.md`.

**File Storage:**
- VPS filesystem is the live store: warehouse data at `/srv/mlbops/warehouse/mlb`, generated assets at `/srv/mlbops/outputs`, snapshots under the deployed app, and logs at `/srv/mlbops/logs`; container paths are `/data/warehouse/mlb`, `/outputs`, and `/logs` as documented in `deploy/README.md`.
- Warehouse formats are gzipped MLB feed JSON, Parquet pitch/game artifacts, schedule JSON/CSV, and player registry JSON produced by `src/ingestion/load_mlb_warehouse.py` and `src/ingestion/season_exports.py`.
- FastAPI exposes generated images from the configured outputs directory at `/static` in `mlbops/api/main.py`; queue rows store file paths and public image URLs.
- Google Drive is an archive/synchronization layer, not live request-path storage. Production reads only the VPS mirror, as specified in `docs/WAREHOUSE_DRIVE_WORKFLOW.md`.

**Caching:**
- No external cache service is used.
- Process-local bounded caches support expensive Insights and leaderboard reads in `mlbops/api/routers/insights.py` and `mlbops/api/routers/leaderboards.py`, controlled by `MLB_INSIGHTS_STATCAST_CACHE_MAX`, `MLB_INSIGHTS_RESPONSE_CACHE_MAX`, and `MLB_LEADERBOARD_CACHE_MAX`.
- SQLite uses WAL mode in both database adapters. Next and browser data calls generally request `no-store` where freshness matters through `mlbops/hub/lib/api.ts` and route/page call sites.

## Authentication & Identity

**Auth Provider:**
- Custom single-owner password session; no external identity provider is used.
  - Implementation: `mlbops/hub/app/api/auth/login/route.ts` validates a configured password or SHA-256 hash through `mlbops/hub/lib/security.ts`, then issues a 12-hour HMAC-SHA256 signed `mlbops_session` HTTP-only cookie.
  - Session secrets: `MLBOPS_SESSION_SECRET` or `SESSION_SECRET`; production requires at least 32 characters.
  - Password configuration: `MLBOPS_APP_PASSWORD_SHA256` / `APP_PASSWORD_SHA256`, or the plain-password fallbacks `MLBOPS_APP_PASSWORD` / `APP_PASSWORD`.
  - Request protection: CSRF tokens, same-origin checks, per-process IP rate buckets, and database-backed audit events in `mlbops/hub/lib/security.ts`.
  - Enforcement: `mlbops/hub/proxy.ts` protects Hub pages and API routes; `mlbops/hub/app/api/backend/[...path]/route.ts` removes browser cookies and proxies authenticated requests to FastAPI.

## Monitoring & Observability

**Error Tracking:**
- No hosted error-tracking service is detected. `@opentelemetry/api` is declared in `mlbops/hub/package.json` but no active instrumentation is present.

**Logs:**
- FastAPI/Uvicorn and Next.js write container stdout/stderr; deployment and operations inspect these with Docker Compose as documented in `deploy/README.md`.
- Scheduled VPS ingest writes under `/srv/mlbops/logs` through `deploy/vps_daily_ingest.sh`; readiness reads `MLBOPS_DAILY_INGEST_LOG` in `mlbops/api/routers/system_readiness.py`.
- Application security actions are persisted in `security_audit_log` by `mlbops/hub/lib/security.ts` and `mlbops/hub/lib/db.ts`.
- Health and readiness endpoints are `/health`, `/system/readiness`, and `/system/paths` in `mlbops/api/main.py` and `mlbops/api/routers/system_readiness.py`.
- GitHub Actions failures create repository issues and optionally send a Resend email from `.github/workflows/daily_ingest.yml`.

## CI/CD & Deployment

**Hosting:**
- Hostinger VPS running Docker Compose, reached privately through Tailscale at the Hub URL documented in `docs/CURRENT_STATE.md`.
- Production services are Next.js Hub, FastAPI API, and PostgreSQL. Persistent warehouse, outputs, database, and logs use `/srv/mlbops/*` host paths from `deploy/README.md`.
- Application deployment is Mac-to-VPS rsync over SSH followed by targeted Docker image rebuild/restart in `deploy/sync_app_to_vps.sh`; `deploy/ship.sh` wraps commit, push, and deploy.
- Active frontend development can switch only the Hub to bind-mounted Next dev mode through `deploy/vps_hub_dev.sh` and `docker-compose.hub-dev.yml`.

**CI Pipeline:**
- GitHub Actions daily ingest - Scheduled at 11:00 UTC plus manual dispatch; installs Python 3.11 and rclone, ingests/validates warehouse data, syncs Google Drive, and reports failures in `.github/workflows/daily_ingest.yml`.
- GitHub Actions morning intel - Runs after a successful daily ingest or manually; pulls Drive artifacts, runs Claude/card/notification work, then pushes snapshots and the legacy SQLite database in `.github/workflows/morning_intel.yml`.
- GitHub Actions manual cards - Uses a self-hosted runner and an existing local Python environment to run daily or HR card generation in `.github/workflows/generate_cards.yml`.
- No general lint, unit-test, type-check, image-build, or deployment workflow is defined under `.github/workflows/`; production deployment is script-driven.

## Environment Configuration

**Required env vars:**
- Core production database: `MLBOPS_DB_BACKEND`, `DATABASE_URL`, `MLBOPS_RUNTIME`.
- Core paths: `MLB_WAREHOUSE_DIR`, `MLBOPS_OUTPUTS_DIR`; optional `MLB_INTEL_SNAPSHOTS_DIR`, `MLB_REPO_ROOT`, and `MLBOPS_DAILY_INGEST_LOG`.
- Hub/API routing: `FASTAPI_BASE_URL` or `INTERNAL_API_URL`; optional browser overrides `NEXT_PUBLIC_FASTAPI_URL` / `NEXT_PUBLIC_API_URL` and public Hub URL `NEXT_PUBLIC_HUB_URL`.
- Hub authentication: `MLBOPS_SESSION_SECRET` and one of `MLBOPS_APP_PASSWORD_SHA256` or `MLBOPS_APP_PASSWORD`; production cookie behavior is controlled by `MLBOPS_SECURE_COOKIES`.
- X/Twitter posting: `TWITTER_API_KEY`, `TWITTER_API_SECRET`, `TWITTER_ACCESS_TOKEN`, `TWITTER_ACCESS_TOKEN_SECRET`; metrics also require `TWITTER_BEARER_TOKEN`.
- AI redraft/intel: `ANTHROPIC_API_KEY` with optional `ANTHROPIC_MODEL`; optional Grok uses `X_API_KEY` and `GROK_MODEL`.
- Notifications: `RESEND_API_KEY`, `RESEND_FROM_EMAIL`, `RESEND_TO_EMAIL`, `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM`, `TWILIO_WHATSAPP_TO`.
- Drive CI: `RCLONE_CONFIG_BASE64` or `GDRIVE_SERVICE_ACCOUNT` plus `GDRIVE_FOLDER_ID`; path controls include `RCLONE_REMOTE`, `GDRIVE_WAREHOUSE_PATH`, and `GDRIVE_INTEL_SNAPSHOTS_PATH`.

**Secrets location:**
- Production secrets are stored in the external VPS environment file at `/srv/mlbops/env/mlbops.env`, loaded with Docker Compose and never committed, per `deploy/README.md`.
- CI secrets are GitHub repository secrets referenced by `.github/workflows/daily_ingest.yml` and `.github/workflows/morning_intel.yml`.
- Local development uses ignored environment files associated with the examples at `mlbops/.env.example`, `mlbops/hub/.env.local.example`, and `jobs/.env.example`, plus an external rclone configuration. Do not read or commit their populated counterparts.

## Webhooks & Callbacks

**Incoming:**
- No public third-party webhook receiver is detected.
- Browser-originated Next route handlers under `mlbops/hub/app/api/` handle login/logout, CSRF, queue actions, notifications, watchlist access, and authenticated FastAPI proxying; these are application endpoints rather than external-provider callbacks.
- GitHub Actions `workflow_run` triggers the Morning Intel workflow after Daily Ingest completion in `.github/workflows/morning_intel.yml`.

**Outgoing:**
- X/Twitter post and media-upload requests originate from `mlbops/hub/lib/twitter.ts` after an explicit queue approval in `mlbops/hub/app/api/queue/[id]/approve/route.ts`.
- Resend and Twilio notifications originate from `mlbops/hub/app/api/notify/route.ts`, `morning_intel/morning_intel.py`, `jobs/weekly_report.py`, and ingest-failure handling in `.github/workflows/daily_ingest.yml`.
- MLB Stats API and Statcast requests originate from ingestion, live-event, schedule, card, fantasy, leaderboard, and intel modules; no callback is expected.
- rclone performs bidirectional archive synchronization between local/CI/VPS filesystems and Google Drive; the production request path does not depend on a Drive callback.

---

*Integration audit: 2026-07-10*
