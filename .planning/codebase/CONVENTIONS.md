# Coding Conventions

**Analysis Date:** 2026-07-10

## Naming Patterns

**Files:**
- Use lowercase `snake_case.py` for Python modules and command scripts, as in `src/ingestion/player_registry.py`, `mlbops/api/services/content_scoring.py`, and `scripts/validate_daily_ingest.py`.
- Use `PascalCase.tsx` for reusable React components, as in `mlbops/hub/components/ScheduleGameCard.tsx` and `mlbops/hub/components/WatchlistClient.tsx`.
- Use framework-defined lowercase names for Next.js App Router files: `page.tsx`, `layout.tsx`, `loading.tsx`, `global-error.tsx`, and `route.ts`, under `mlbops/hub/app/`.
- Keep shared TypeScript utilities in descriptive camelCase files such as `mlbops/hub/lib/heatScale.ts`, `mlbops/hub/lib/security.ts`, and `mlbops/hub/lib/tweetMaxChars.ts`.
- Avoid duplicate-copy filenames containing spaces or numeric suffixes. Files such as `src/pitching_performances/malli_score 2.py`, `src/pitching_performances/__init__ 2.py`, and `mlbops/hub/lib/db 2.ts` are not normal module names and should not be used as examples for new code.

**Functions:**
- Use `snake_case` for Python functions and prefix module-private helpers with `_`, as in `_parse_pitching_season_split()` and `build_probable_rows_for_date()` in `src/probables_board/fetch.py`.
- Use `camelCase` for TypeScript helpers and event handlers, as in `getApiBase()` in `mlbops/hub/lib/api.ts`, `loadBoxscore()` in `mlbops/hub/components/ScheduleGameCard.tsx`, and `queueStreamer()` in `mlbops/hub/app/fantasy/page.tsx`.
- Use `PascalCase` for React components, including local leaf components such as `StatusBadge` and exported components such as `ScheduleGameCard` in `mlbops/hub/components/ScheduleGameCard.tsx`.
- Use uppercase HTTP method exports for Next.js route handlers, such as `GET`, `POST`, and `PATCH` in `mlbops/hub/app/api/`.

**Variables:**
- Use lowercase `snake_case` for Python locals and parameters; use leading underscores for module constants or helpers that are intentionally private, as in `_DEFAULT_MEANS` and `_weighted_z` in `src/pitching_performances/malli_score.py`.
- Use `UPPER_SNAKE_CASE` for Python and TypeScript constants shared across functions, as in `BASE_URL` in `src/probables_board/fetch.py`, `WRITE_METHODS` in `mlbops/hub/app/api/backend/[...path]/route.ts`, and `SESSION_COOKIE` in `mlbops/hub/lib/security.ts`.
- Use `camelCase` for TypeScript state, props, and locals, as in `gameDate`, `includeLiveProbables`, and `queueBusyId` in `mlbops/hub/app/fantasy/page.tsx`.
- Name booleans with state or predicate wording such as `using_postgres()` in `mlbops/api/db/database.py`, `isProbable` in `mlbops/hub/app/fantasy/page.tsx`, and `hasValidSession()` in `mlbops/hub/proxy.ts`.

**Types:**
- Use `PascalCase` for Python dataclasses, as in `OutingRawMetrics` and `LeagueNorms` in `src/pitching_performances/malli_score.py`.
- Use `PascalCase` for TypeScript `type` aliases and `interface` declarations, as in `StreamerPayload` in `mlbops/hub/app/fantasy/page.tsx` and `QueueItem` in `mlbops/hub/lib/db.ts`.
- Prefer precise unions for finite UI states, as in `ThemeMode` in `mlbops/hub/components/ThemeToggle.tsx` and `EventFilter` in `mlbops/hub/components/LiveEventsClient.tsx`.
- Use built-in generic forms (`list[T]`, `dict[str, T]`, `tuple[...]`, `X | None`) in typed Python modules such as `src/probables_board/fetch.py`; older `Optional[T]` remains common in database-facing code such as `mlbops/api/db/database.py`, so match the touched module.

## Code Style

**Formatting:**
- No repository-wide Python formatter configuration is present in the tracked root; format Python to the dominant PEP 8-like style visible in `src/pitching_performances/malli_score.py` and `scripts/validate_daily_ingest.py`: four-space indentation, blank lines between top-level definitions, and multiline calls with trailing commas.
- No Prettier configuration or script is present in `mlbops/hub/package.json`; match the established TypeScript style in `mlbops/hub/lib/api.ts` and `mlbops/hub/components/ScheduleGameCard.tsx`: two-space indentation, double quotes, semicolons, multiline JSX props, and trailing commas in multiline lists/calls.
- Keep Python source compatible with annotations by placing `from __future__ import annotations` immediately after the module docstring where used, as in `src/ingestion/player_registry.py` and `mlbops/api/services/content_scoring.py`.
- Prefer `pathlib.Path` over raw path concatenation in new Python code, following `src/ingestion/player_registry.py`, `scripts/validate_daily_ingest.py`, and `mlbops/api/paths.py`.
- Preserve the Hub's Tailwind utility-first styling and semantic design tokens from `mlbops/hub/app/globals.css`; avoid introducing a second component styling system in `mlbops/hub/app/` or `mlbops/hub/components/`.

**Linting:**
- No root Ruff, Black, isort, Flake8, mypy, ESLint, Biome, or Prettier configuration is detected in `requirements.txt`, `mlbops/api/requirements-api.txt`, or `mlbops/hub/package.json`.
- Treat strict TypeScript compilation as the principal static check because `strict: true` and `noEmit: true` are set in `mlbops/hub/tsconfig.json`.
- Do not rely on `npm run build` alone for type correctness: `mlbops/hub/next.config.ts` sets `typescript.ignoreBuildErrors: true`. Run `npx tsc --noEmit` from `mlbops/hub/` when changing TypeScript.
- Inline ESLint suppression appears only for deliberate framework exceptions, such as the raw MLB logo `<img>` in `mlbops/hub/components/ScheduleGameCard.tsx`; keep suppressions narrow and on the affected line.
- Local frontend instructions in `mlbops/hub/AGENTS.md` require consulting the installed Next.js 16 documentation under `mlbops/hub/node_modules/next/dist/docs/` before changing framework APIs or file conventions.

## Import Organization

**Order:**
1. In Python, place the module docstring and `from __future__ import annotations` first, then standard-library imports, third-party packages, and repository imports, following `src/probables_board/fetch.py` and `scripts/validate_daily_ingest.py`.
2. In executable Python scripts that need root imports, establish `_REPO_ROOT` and update `sys.path` before importing `src.*`, as in `scripts/validate_daily_ingest.py`; package modules under `src/` and `mlbops/api/` should use normal package imports instead.
3. In TypeScript, import Next/React and other external modules first, then `@/` aliases, then relative side-effect CSS imports, following `mlbops/hub/app/layout.tsx` and `mlbops/hub/app/fantasy/page.tsx`.
4. Use `import type` for type-only TypeScript dependencies where practical, as in `mlbops/hub/app/layout.tsx`, `mlbops/hub/app/intel/page.tsx`, and `mlbops/hub/components/QueueClient.tsx`.

**Path Aliases:**
- Use `@/*` for Hub-root imports; it maps to `./*` in `mlbops/hub/tsconfig.json`. Examples include `@/lib/api` and `@/components/NavSidebar`.
- Use `src.*` absolute imports for reusable analytics/ingestion packages, as in `src/probables_board/fetch.py` and `scripts/validate_daily_ingest.py`.
- API modules commonly import from `api.*` because the API runtime places `mlbops/` on the Python path, as in `mlbops/api/main.py` and `mlbops/api/services/content_scoring.py`; preserve this convention inside `mlbops/api/`.
- Use explicit relative imports only within package facades, as in `src/probables_board/__init__.py`.

## Error Handling

**Patterns:**
- Validate public inputs at the boundary and raise `HTTPException` with a specific status and user-facing detail in FastAPI routers, as in `mlbops/api/routers/fantasy.py` and `mlbops/api/routers/schedule.py`.
- Translate domain `ValueError` exceptions to HTTP 400 with exception chaining (`raise ... from exc`) in router layers, following `mlbops/api/routers/fantasy.py`.
- For external HTTP calls, set an explicit timeout and call `raise_for_status()`, as in `src/probables_board/fetch.py` and `mlbops/api/routers/schedule.py`; convert request failures to service-appropriate errors at the API boundary.
- For optional enrichment or fallback paths, return an empty collection only when loss of that data is intentionally nonfatal, as in standings fallbacks in `src/probables_board/story.py`. Do not silently swallow errors on required writes or required API responses.
- Protect database transactions with commit/rollback/finally-close behavior, following `get_db()` in `mlbops/api/db/database.py`.
- Write generated registries atomically through a temporary file and clean up on failure, following `merge_game_data_players_from_feed()` in `src/ingestion/player_registry.py`.
- In client components, set explicit loading/error state, check `res.ok`, throw an `Error` with useful response detail, and clear busy state in `finally`, following `mlbops/hub/app/fantasy/page.tsx` and `mlbops/hub/components/ScheduleGameCard.tsx`.
- In Next.js route handlers, return `NextResponse.json()` with explicit status codes for auth, validation, and rate-limit failures, following `mlbops/hub/lib/security.ts` and `mlbops/hub/app/api/auth/login/route.ts`.

## Logging

**Framework:** Console/standard output; no centralized Python logging framework is configured in `requirements.txt` or `mlbops/api/requirements-api.txt`.

**Patterns:**
- Use concise `print()` output and nonzero `SystemExit` results for operator-facing CLI scripts such as `scripts/validate_daily_ingest.py` and `scripts/build_pitch_metric_benchmarks.py`.
- Return structured JSON from API endpoints instead of printing request-level status in routers such as `mlbops/api/routers/fantasy.py`.
- Use `console.error()` only for nonfatal server-side operational failures that should not break the main request, as in audit logging in `mlbops/hub/lib/security.ts`.
- Record security-sensitive actions through `auditFromRequest()` and `logAuditEvent()` in `mlbops/hub/lib/security.ts` and `mlbops/hub/lib/db.ts`, not through ad hoc console messages.
- Never log environment values or secret material. Environment-dependent code in `mlbops/hub/lib/security.ts` and `mlbops/api/db/database.py` logs or raises only variable names and remediation text.

## Comments

**When to Comment:**
- Add comments for domain-specific baseball rules, compatibility boundaries, and operational hazards, as in completed-inning logic in `mlbops/api/live/detect.py`, SQLite/Postgres translation in `mlbops/api/db/database.py`, and hydration timing in `mlbops/hub/app/fantasy/page.tsx`.
- Keep comments adjacent to the non-obvious branch they explain; avoid narrating self-explanatory assignments in modules such as `mlbops/api/services/content_scoring.py`.
- Preserve prominent section comments only in long domain modules where they aid navigation, as in detector sections in `mlbops/api/live/detect.py`.

**JSDoc/TSDoc:**
- Use Python module docstrings to define module responsibility and function docstrings for public or mathematically non-obvious behavior, following `src/pitching_performances/malli_score.py` and `src/ingestion/player_registry.py`.
- Use TypeScript block comments for shared library contracts and environment-dependent behavior, as in `mlbops/hub/lib/api.ts` and `mlbops/hub/lib/db.ts`.
- React components generally rely on descriptive types and names instead of per-component JSDoc, as in `mlbops/hub/components/ScheduleGameCard.tsx`.

## Function Design

**Size:** Keep scoring, parsing, formatting, and fetch responsibilities in focused helpers, following `mlbops/api/services/content_scoring.py` and `src/probables_board/fetch.py`. Large page components such as `mlbops/hub/app/insights/page.tsx` and `mlbops/hub/components/QueueClient.tsx` are existing exceptions, not preferred templates for new features.

**Parameters:**
- Type public Python parameters and returns; use keyword-only parameters for optional tuning knobs, as in `refine_league_norms()` and `workload_scalar()` in `src/pitching_performances/malli_score.py`.
- Pass filesystem locations as `Path` objects in reusable Python functions, following `season_registry_path()` in `src/ingestion/player_registry.py`.
- Type React props inline for small local components and use named interfaces/types for shared or larger payloads, following `StatusBadge` in `mlbops/hub/app/fantasy/page.tsx` and `ScheduleGame` in `mlbops/hub/components/ScheduleGameCard.tsx`.
- Keep API boundary defaults and constraints explicit with FastAPI `Query`, as in `mlbops/api/routers/fantasy.py`.

**Return Values:**
- Return structured dictionaries/typed payloads from Python service functions, as in `score_queue_item()` in `mlbops/api/services/content_scoring.py`; keep field names stable because routers and Hub pages consume them directly.
- Return `None` or empty collections for documented absence, not for swallowed required-operation failures, following `src/probables_board/fetch.py` and `src/probables_board/story.py`.
- Have executable Python entry points return integer exit codes and terminate with `raise SystemExit(main())`, as in `scripts/validate_daily_ingest.py` and `scripts/export_season_drive_artifacts.py`.
- Use generic return types for shared TypeScript transport helpers, as in `fetchJson<T>()` in `mlbops/hub/lib/api.ts`.

## Module Design

**Exports:**
- Keep implementation helpers private with `_` prefixes and expose a small functional surface from Python packages. `src/probables_board/__init__.py` demonstrates an explicit `__all__` facade.
- Use named TypeScript exports for reusable components, types, and helpers, as in `mlbops/hub/components/ScheduleGameCard.tsx` and `mlbops/hub/lib/api.ts`.
- Use default exports for Next.js pages/layouts and single-purpose client entry components, as in `mlbops/hub/app/fantasy/page.tsx` and `mlbops/hub/components/QueueClient.tsx`.
- Keep FastAPI router objects module-local and register them centrally in `mlbops/api/main.py`.

**Barrel Files:**
- Python package facades are used selectively through `__init__.py`, notably `src/probables_board/__init__.py`; add a facade only when a package has a deliberate public API.
- No TypeScript barrel-file convention is established under `mlbops/hub/`; import directly from concrete `@/components/*` and `@/lib/*` modules as done in `mlbops/hub/app/layout.tsx`.
- The project-specific frontend skill guidance under `.agents/skills/` is design guidance, not an alternative module architecture; existing Hub conventions in `mlbops/hub/` remain authoritative for implementation work.

---

*Convention analysis: 2026-07-10*
