# Codebase Concerns

**Analysis Date:** 2026-07-10

## Tech Debt

**Oversized orchestration modules:**
- Issue: Core HTTP, UI, and card-generation behavior is concentrated in files ranging from roughly 1,000 to 2,500 lines. Routing, persistence, prompt construction, data shaping, subprocess control, and presentation logic are frequently mixed in one module.
- Files: `mlbops/api/routers/queue.py`, `mlbops/api/routers/cards.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/hub/components/QueueClient.tsx`, `mlbops/hub/app/insights/page.tsx`, `scripts/batter_card_daily.py`, `scripts/batter_card_seasonal.py`, `scripts/mallitalytics_daily_card.py`, `morning_intel/morning_intel.py`
- Impact: Narrow changes require understanding unrelated behavior, merge conflicts are likely, and focused unit tests are difficult to write without importing large dependency graphs.
- Fix approach: Extract pure domain functions first, then move I/O into explicit adapters. Split `queue.py` by drafting/scoring/redrafting/CRUD, split `cards.py` by card family, and split large React files into data hooks plus bounded view components without changing endpoint contracts.

**Parallel database implementations and manual schema evolution:**
- Issue: Python and TypeScript each implement SQLite/Postgres compatibility, while schema changes are represented as independent one-off scripts with duplicated full-table definitions and no migration ledger or enforced order.
- Files: `mlbops/api/db/database.py`, `mlbops/hub/lib/db.ts`, `mlbops/api/db/migrate_queue_content_types.py`, `mlbops/api/db/migrate_live_events.py`, `mlbops/api/db/migrate_pitching_index.py`, `mlbops/api/db/migrate_fantasy_streamer.py`, `mlbops/api/db/migrate_upscale_session1.py`, `mlbops/api/db/schema_postgres.sql`
- Impact: SQLite and Postgres behavior can drift, a migration can silently omit a newer content type or column, and deployment correctness depends on operator knowledge.
- Fix approach: Adopt one ordered migration system for both backends, record applied versions, generate schema from a canonical source, and add migration tests that upgrade representative old schemas to the current schema.

**Duplicate conflict-copy files:**
- Issue: Files with a ` 2` suffix duplicate or preserve older implementations beside active modules. One MalliScore copy is byte-identical, while the database and package initializer copies differ behaviorally.
- Files: `src/pitching_performances/malli_score 2.py`, `src/pitching_performances/__init__ 2.py`, `mlbops/hub/lib/db 2.ts`
- Impact: Search results are ambiguous, editors and scripts can target the wrong implementation, and stale code can be mistaken for a supported fallback.
- Fix approach: Confirm the canonical versions, preserve any genuinely needed changes through a normal diff, then remove conflict copies and add a repository check rejecting filenames matching conflict-copy patterns.

**Fixed-season defaults spread across runtime surfaces:**
- Issue: The current season is hard-coded independently in API defaults, Hub selectors, workflows, deployment scripts, and analysis scripts.
- Files: `mlbops/api/routers/insights.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/hub/app/insights/page.tsx`, `mlbops/hub/app/leaderboards/page.tsx`, `.github/workflows/daily_ingest.yml`, `.github/workflows/morning_intel.yml`, `deploy/vps_pull_warehouse_from_drive.sh`, `deploy/vps_verify_warehouse.sh`, `src/ingestion/load_mlb_warehouse.py`
- Impact: Annual rollover requires coordinated edits; missed locations return stale or empty data while the ingest writes a different season.
- Fix approach: Derive the operational season from one server-side configuration value or the requested date, expose available warehouse seasons from the API, and populate UI selectors dynamically.

**Python environments are not reproducible:**
- Issue: Python dependencies use open-ended minimum versions and there is no lock file or constraints file. The API and batch requirements overlap but are maintained separately.
- Files: `requirements.txt`, `mlbops/api/requirements-api.txt`, `Dockerfile.api`, `.github/workflows/daily_ingest.yml`, `.github/workflows/morning_intel.yml`
- Impact: CI, local development, and container rebuilds can resolve different package versions; breaking changes in pandas, pyarrow, pybaseball, FastAPI, or SDK clients can enter without a source change.
- Fix approach: Generate a reviewed lock/constraints set for each supported Python version, derive the API subset from the same source, and make CI install from the locked artifacts.

## Known Bugs

**Single-player card workflow ignores its input:**
- Symptoms: The manual workflow advertises `player_id`, but the generation command never reads that input and always runs the full daily generator for non-HR requests.
- Files: `.github/workflows/generate_cards.yml`, `jobs/daily_card_generator.py`
- Trigger: Dispatch `Generate Cards (Manual)` with a specific `player_id` and `card_type=auto`.
- Workaround: Run the appropriate card script directly with the requested player identifier; the workflow itself has no single-player branch.

**Authenticated visits to the login page are not redirected:**
- Symptoms: A valid session can still render `/login`; the intended redirect to `/queue` is unreachable because `/login` returns early as a public path.
- Files: `mlbops/hub/proxy.ts`, `mlbops/hub/app/login/page.tsx`
- Trigger: Open `/login` while holding a valid `mlbops_session` cookie.
- Workaround: Navigate directly to `/queue` or another protected route.

**Live-events SQLite migration is incompatible with the metadata-expanded queue schema:**
- Symptoms: `INSERT INTO content_queue_new SELECT * FROM content_queue` fails with a column-count mismatch when queue metadata columns exist but `live_event` is absent. Its replacement table also omits the metadata columns and newer content types.
- Files: `mlbops/api/db/migrate_live_events.py`, `mlbops/api/db/migrate_upscale_session1.py`, `mlbops/api/db/migrate_pitching_index.py`, `mlbops/api/db/migrate_fantasy_streamer.py`
- Trigger: Apply the live-events migration to a SQLite database that has already received the upscale metadata migration but does not allow `live_event`.
- Workaround: Apply migrations only in the known compatible order or use a reviewed current-schema migration that names every copied column explicitly.

**Interrupted parquet writes can be treated as complete forever:**
- Symptoms: A partial or corrupt enriched parquet remains present and later ingest runs skip it solely because the path exists.
- Files: `src/ingestion/load_mlb_warehouse.py`
- Trigger: Terminate the process or exhaust disk space during `DataFrame.to_parquet()`.
- Workaround: Re-run the affected date with `--force` after deleting or identifying the corrupt file.

## Security Considerations

**FastAPI has no application authentication boundary:**
- Risk: Any client that can reach FastAPI directly can invoke mutating card, queue, watchlist, live-event, redraft, sync, and analytics endpoints without the Next.js session or CSRF checks. The Hub proxy protects only requests routed through `/api/backend`.
- Files: `mlbops/api/main.py`, `mlbops/api/routers/cards.py`, `mlbops/api/routers/queue.py`, `mlbops/api/routers/watchlist.py`, `mlbops/api/routers/live.py`, `mlbops/hub/app/api/backend/[...path]/route.ts`, `mlbops/hub/lib/security.ts`
- Current mitigation: Browser traffic defaults to the authenticated Next.js proxy, and selected operational endpoints also require feature flags.
- Recommendations: Bind FastAPI to a private container/network interface, enforce firewall rules, and add service authentication or shared authorization dependencies to every sensitive FastAPI route so network topology is not the only control.

**Operational filesystem paths are exposed by an API response:**
- Risk: `/system/paths` returns absolute repository, warehouse, snapshot, and output paths. Direct API reachability exposes deployment layout that can aid further attacks or leak operator-specific paths.
- Files: `mlbops/api/main.py`, `mlbops/api/paths.py`
- Current mitigation: Access through the Hub proxy requires a session.
- Recommendations: Return readiness booleans and logical identifiers to normal clients; place absolute paths behind an administrator-only diagnostic endpoint and protect FastAPI itself.

**Rate limiting trusts client-controlled forwarding data and retains buckets indefinitely:**
- Risk: A caller can vary `x-forwarded-for` to bypass per-IP limits and grow the process-global `Map` without bound. Limits also reset on restart and are not shared across Next.js replicas.
- Files: `mlbops/hub/lib/security.ts`, `mlbops/hub/app/api/auth/login/route.ts`, `mlbops/hub/app/api/backend/[...path]/route.ts`
- Current mitigation: Per-action, per-IP counters limit ordinary traffic in a single process.
- Recommendations: Accept forwarding headers only from a trusted proxy, use the platform-provided client address, expire stored keys, cap bucket cardinality, and move login/write limits to a shared store or edge control when running multiple replicas.

**Session revocation is client-side only:**
- Risk: Session tokens are self-contained and valid for 12 hours; logout clears the browser cookie but cannot invalidate a copied token before expiry.
- Files: `mlbops/hub/lib/security.ts`, `mlbops/hub/proxy.ts`, `mlbops/hub/app/api/auth/logout/route.ts`
- Current mitigation: Tokens are HMAC-signed, HTTP-only, same-site cookies with CSRF tokens, and production requires a sufficiently long secret in the Node route implementation.
- Recommendations: Store active session IDs or a revocation epoch server-side, rotate session IDs on sensitive changes, and consolidate signing/verification so `proxy.ts` and `security.ts` cannot diverge.

## Performance Bottlenecks

**Full-season parquet materialization in request processes:**
- Problem: Insights, fantasy, and leaderboard endpoints read many parquet files into pandas and concatenate full frames. Multiple bounded caches still retain several large DataFrames per process and per worker.
- Files: `mlbops/api/routers/insights.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/api/services/fantasy_service.py`, `mlbops/api/services/pitcher_dashboard.py`
- Cause: Aggregation is performed at request time over game-level files; cache invalidation is based on filesystem fingerprints rather than precomputed analytical tables.
- Improvement path: Build date/player/team rollups during ingest, query only required columns and partitions through DuckDB/Polars/pyarrow datasets, record cache memory metrics, and avoid duplicating caches across workers.

**Card subprocesses have no execution timeout or concurrency budget:**
- Problem: A stalled card script occupies a thread and child process indefinitely; concurrent requests can launch several CPU- and memory-heavy pandas/matplotlib jobs.
- Files: `mlbops/api/routers/cards.py`, `scripts/batter_card_daily.py`, `scripts/batter_card_seasonal.py`, `scripts/mallitalytics_daily_card.py`
- Cause: `_run_script()` calls `subprocess.run()` without `timeout`, while each endpoint delegates directly to Starlette's shared thread pool.
- Improvement path: Put generation behind a bounded job queue, persist job state, enforce per-card timeouts, terminate process groups on cancellation, and return a job identifier for polling.

**Large client components perform broad state and fetch orchestration:**
- Problem: Queue and Insights pages contain large render trees, numerous fetch paths, and many state transitions in single client bundles.
- Files: `mlbops/hub/components/QueueClient.tsx`, `mlbops/hub/app/insights/page.tsx`, `mlbops/hub/app/intel/page.tsx`
- Cause: Data loading, mutation handling, formatting, and every view mode are colocated.
- Improvement path: Split stable display sections into server-compatible components, isolate interactive leaves, centralize typed request hooks, and measure bundle size and render cost in CI.

## Fragile Areas

**Warehouse ingest and file publication:**
- Files: `src/ingestion/load_mlb_warehouse.py`, `src/ingestion/player_registry.py`, `src/ingestion/season_exports.py`, `.github/workflows/daily_ingest.yml`
- Why fragile: Raw gzip and parquet outputs are written directly to final paths, file existence is used as completion state, and schedule/export artifacts are separate writes. Only the player registry uses a temp-file replacement pattern.
- Safe modification: Preserve idempotent date-targeted ingest, publish every artifact through temp file plus atomic replace, validate gzip/parquet readability before marking success, and keep `--force` recovery behavior.
- Test coverage: No automated failure-injection tests cover interrupted writes, partial dates, late Statcast availability, or concurrent ingest workers.

**SQLite/Postgres compatibility translation:**
- Files: `mlbops/api/db/database.py`, `mlbops/hub/lib/db.ts`, `mlbops/api/db/schema_postgres.sql`
- Why fragile: SQL is translated with string replacements and two language-specific adapters; backend-specific date arithmetic, `RETURNING`, row types, constraints, and transaction behavior differ.
- Safe modification: Test every shared query against both engines, avoid adding SQL syntax that relies on ad hoc translation, and prefer explicit backend implementations for nontrivial statements.
- Test coverage: No database integration suite or migration matrix is present.

**External-service content pipeline:**
- Files: `morning_intel/morning_intel.py`, `mlbops/api/routers/queue.py`, `mlbops/hub/lib/twitter.ts`, `mlbops/hub/lib/resend.ts`, `mlbops/hub/lib/twilio.ts`, `jobs/daily_card_generator.py`, `jobs/weekly_report.py`
- Why fragile: MLB, AI, X, email, and WhatsApp calls are coordinated through broad exception handling and partially independent scripts; failure semantics vary between returning empty data, logging, queueing a failed item, and raising.
- Safe modification: Define typed provider adapters, classify retryable failures, attach correlation/job IDs, make retries idempotent, and never infer success from missing exceptions alone.
- Test coverage: Provider boundaries have no mocks or contract tests; posting, notification, and retry behavior is untested.

**2026 rollover and stage selection:**
- Files: `mlbops/api/routers/insights.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/hub/app/insights/page.tsx`, `mlbops/hub/app/leaderboards/page.tsx`, `.github/workflows/daily_ingest.yml`, `.github/workflows/morning_intel.yml`
- Why fragile: API defaults, UI options, and scheduled ingest configuration can disagree, and stage folder names are supplied as free-form strings in several endpoints.
- Safe modification: Resolve season/stage against a server-provided warehouse catalog and validate stage values against `src/ingestion/mlb_warehouse_schema.py`.
- Test coverage: No rollover test asserts that the first games of a new season appear in API responses and Hub selectors.

## Scaling Limits

**SQLite plus copied database snapshots:**
- Files: `mlbops/api/db/database.py`, `mlbops/hub/lib/db.ts`, `.github/workflows/morning_intel.yml`
- Current capacity: SQLite WAL and short retry loops support a single-machine, low-concurrency operator workflow; `.github/workflows/morning_intel.yml` also transfers `data/hub.db` as a whole-file snapshot.
- Limit: Multiple writers on different hosts cannot merge snapshots. In SQLite mode, a CI pull-run-push can overwrite changes made after its initial pull, and write contention grows as API, Hub, and jobs share the file.
- Scaling path: Use Postgres as the sole production control-plane store, keep SQLite local-only, migrate CI jobs to the same database, and replace whole-file synchronization with transactional records.

**Process-local caches and rate state:**
- Files: `mlbops/api/routers/insights.py`, `mlbops/api/routers/leaderboards.py`, `mlbops/api/services/fantasy_service.py`, `mlbops/hub/lib/security.ts`
- Current capacity: Caches and rate buckets work within one Python or Node process and are bounded only in selected pandas caches.
- Limit: Additional workers multiply memory use, produce inconsistent cache warmth and rate decisions, and cannot coordinate invalidation.
- Scaling path: Precompute warehouse aggregates, use a shared bounded cache only where measurements justify it, and move distributed rate/session state to a shared service.

**Generated asset filesystem:**
- Files: `mlbops/api/main.py`, `mlbops/api/paths.py`, `mlbops/api/routers/cards.py`
- Current capacity: Generated PNGs are served from one `OUTPUTS_DIR` mounted directly by FastAPI.
- Limit: Multiple API replicas do not share newly generated files unless they share a writable volume; static URLs depend on the generating host's filesystem state.
- Scaling path: Store generated assets in object storage with checksums and durable metadata, then serve immutable URLs independently of the API worker.

## Dependencies at Risk

**pybaseball and upstream Statcast behavior:**
- Risk: Ingest depends on `pybaseball.statcast_single_game()` and contains a specific recovery message for import failures involving PyGithub; no exact compatible versions are locked.
- Impact: Daily enriched parquet production can stop even when MLB feed retrieval still works.
- Migration plan: Pin a verified pybaseball dependency set, add a smoke test for imports and one fixture-backed transformation, and isolate the Statcast provider behind an adapter so an alternate source can be introduced.
- Files: `src/ingestion/load_mlb_warehouse.py`, `requirements.txt`

**Rapidly moving framework and SDK ranges:**
- Risk: FastAPI, pandas, pyarrow, anthropic, Resend, Twilio, React, and Next.js are central to runtime paths; Python packages are unconstrained above their minimum versions.
- Impact: Rebuilds can introduce API incompatibility, changed serialization, native-wheel issues, or altered model/provider behavior.
- Migration plan: Lock deployable dependency graphs, run API import/build/test gates on dependency updates, and update packages through reviewed automated pull requests.
- Files: `requirements.txt`, `mlbops/api/requirements-api.txt`, `mlbops/hub/package.json`, `mlbops/hub/package-lock.json`

## Missing Critical Features

**Automated migration runner and schema verification:**
- Problem: Migrations are invoked manually and there is no startup or deployment gate proving that the active database supports every route and content type.
- Blocks: Reliable unattended deployment and confident SQLite-to-Postgres parity.
- Files: `mlbops/api/db/migrate_queue_content_types.py`, `mlbops/api/db/migrate_live_events.py`, `mlbops/api/db/migrate_pitching_index.py`, `mlbops/api/db/migrate_fantasy_streamer.py`, `mlbops/api/db/migrate_upscale_session1.py`, `mlbops/api/db/schema_postgres.sql`

**Durable job lifecycle for long-running operations:**
- Problem: Card generation, intel generation, warehouse sync, and AI redrafts are request-coupled or script-coupled. The Postgres schema defines `job_runs`, but application code does not use it.
- Blocks: Cancellation, deduplication, retries, progress reporting, worker isolation, and safe horizontal scaling.
- Files: `mlbops/api/routers/cards.py`, `mlbops/api/routers/intel.py`, `mlbops/api/main.py`, `mlbops/api/db/schema_postgres.sql`

**Central observability and health depth:**
- Problem: `/health` reports only a static success object; logs are mostly free-form prints/console calls and do not prove database, warehouse, provider, disk, or queue health.
- Blocks: Automated detection of stale ingest, corrupt files, provider degradation, and stuck generation jobs.
- Files: `mlbops/api/main.py`, `mlbops/api/routers/system_readiness.py`, `morning_intel/morning_intel.py`, `mlbops/hub/lib/security.ts`

## Test Coverage Gaps

**API routes and authorization boundary:**
- What's not tested: Request validation, mutating endpoint authorization assumptions, CSRF proxying, direct FastAPI access, timeout behavior, and error response contracts.
- Files: `mlbops/api/main.py`, `mlbops/api/routers/cards.py`, `mlbops/api/routers/queue.py`, `mlbops/hub/app/api/backend/[...path]/route.ts`, `mlbops/hub/lib/security.ts`
- Risk: Security regressions and breaking API changes can ship without detection.
- Priority: High

**Database migrations and dual-backend queries:**
- What's not tested: Every migration order, upgrade from representative old SQLite schemas, current Postgres bootstrap, query translation, transaction rollback, and schema parity.
- Files: `mlbops/api/db/database.py`, `mlbops/hub/lib/db.ts`, `mlbops/api/db/migrate_live_events.py`, `mlbops/api/db/migrate_pitching_index.py`, `mlbops/api/db/migrate_fantasy_streamer.py`, `mlbops/api/db/schema_postgres.sql`
- Risk: Production startup or writes fail only after a schema-dependent feature is invoked.
- Priority: High

**Ingest correctness and recovery:**
- What's not tested: Feed parsing, Statcast merge keys, atomic publication, corrupt existing files, late data, schedule refresh, and rerun idempotency. Existing `test_parquet.py` and `test_parse.py` are print-driven scripts with fixed local data paths rather than assertions.
- Files: `src/ingestion/load_mlb_warehouse.py`, `src/ingestion/boxscore_aggregate.py`, `test_parquet.py`, `test_parse.py`
- Risk: Missing or malformed warehouse data propagates into every card, insight, and leaderboard.
- Priority: High

**Hub workflows and components:**
- What's not tested: Login redirect, queue edit/post/reject flows, loading/error/empty states, season selectors, responsive layout, and browser-to-proxy routing. The Hub has no `test` or `lint` script.
- Files: `mlbops/hub/package.json`, `mlbops/hub/proxy.ts`, `mlbops/hub/components/QueueClient.tsx`, `mlbops/hub/app/insights/page.tsx`, `mlbops/hub/app/leaderboards/page.tsx`
- Risk: Large UI changes rely on manual verification and can regress operator-critical actions.
- Priority: High

**Card metric and rendering logic:**
- What's not tested: Daily batter/pitcher extraction, tweet derivation, image output contracts, missing-column fallbacks, and card subprocess integration. Only the new MalliScore formula has assertion-based smoke coverage.
- Files: `scripts/batter_card_daily.py`, `scripts/batter_card_seasonal.py`, `scripts/mallitalytics_daily_card.py`, `mlbops/api/routers/cards.py`, `test_malli_score_v2.py`
- Risk: Statistical or visual regressions can publish incorrect content or make API generation fail after long processing.
- Priority: High

**CI quality gates:**
- What's not tested: No tracked workflow runs pytest, Python compilation/lint/type checks, TypeScript checks, Next.js builds, migration tests, or browser tests as a required gate.
- Files: `.github/workflows/daily_ingest.yml`, `.github/workflows/generate_cards.yml`, `.github/workflows/morning_intel.yml`, `mlbops/hub/package.json`
- Risk: Scheduled automation and deployable branches can execute unverified code.
- Priority: High

---

*Concerns audit: 2026-07-10*
