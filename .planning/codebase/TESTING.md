# Testing Patterns

**Analysis Date:** 2026-07-10

## Test Framework

**Runner:**
- Pytest-style tests are present only in the untracked root file `test_malli_score_v2.py`; it uses plain `test_*` functions and bare `assert` statements.
- Pytest is not declared in `requirements.txt` or `mlbops/api/requirements-api.txt`, and no `pytest.ini`, `pyproject.toml`, `setup.cfg`, `tox.ini`, or `conftest.py` is detected.
- The tracked files `test_parquet.py` and `test_parse.py` are executable diagnostic scripts with top-level I/O and printing, not assertion-based automated tests.
- No JavaScript test runner is configured in `mlbops/hub/package.json`; Jest, Vitest, React Testing Library, and Playwright configs are not detected under `mlbops/hub/`.
- Configuration: Not detected. Type/static configuration lives in `mlbops/hub/tsconfig.json`, and build behavior lives in `mlbops/hub/next.config.ts`.

**Assertion Library:**
- Python built-in `assert` through pytest-style discovery in `test_malli_score_v2.py`.
- FastAPI smoke checks use `fastapi.testclient.TestClient` by documented practice in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` through `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`, but the smoke harnesses are not committed.
- No frontend assertion library is present in `mlbops/hub/package.json`.

**Run Commands:**
```bash
./mlb_env/bin/python -m pytest test_malli_score_v2.py  # Run the available MalliScore unit tests when pytest is installed
./mlb_env/bin/python -m compileall mlbops/api src scripts  # Compile-check Python changes
cd mlbops/hub && npx tsc --noEmit                     # Strict TypeScript check
cd mlbops/hub && npm run build                        # Next.js production build
python scripts/validate_daily_ingest.py --season 2026 --game-type R --last-days 1  # Date-scoped warehouse validation
```
- `pytest` availability depends on the local `mlb_env/` because it is absent from `requirements.txt`; do not assume a clean install can run `test_malli_score_v2.py`.
- `npm run build` does not enforce TypeScript correctness because `typescript.ignoreBuildErrors` is enabled in `mlbops/hub/next.config.ts`; pair it with `npx tsc --noEmit`.

## Test File Organization

**Location:**
- The only assertion-based unit test file is root-level `test_malli_score_v2.py`, adjacent to repository-wide scripts rather than colocated with `src/pitching_performances/malli_score.py`.
- Tracked diagnostic scripts are root-level `test_parquet.py` and `test_parse.py` and depend on historical warehouse files under `data/warehouse/mlb/`.
- Operational validators live under `scripts/`, especially `scripts/validate_daily_ingest.py` and `scripts/validate_season_warehouse.py`.
- API smoke verification is documented per feature in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` through `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md` rather than maintained in a reusable test directory.
- Browser verification is documented in the same `docs/progress/` session files; no committed E2E suite or `mlbops/hub/tests/` directory exists.

**Naming:**
- Use `test_<behavior>()` for pytest functions, following `test_harmonic_mean_penalizes_lopsided_scores()` and `test_outputs_are_finite()` in `test_malli_score_v2.py`.
- Reserve `validate_<scope>.py` for executable operational validators with CLI arguments and exit codes, following `scripts/validate_daily_ingest.py` and `scripts/validate_season_warehouse.py`.
- Do not name exploratory print scripts `test_*.py` in new work; `test_parquet.py` and `test_parse.py` are legacy diagnostics and are misleading to test discovery.

**Structure:**
```text
test_malli_score_v2.py              # Root-level pytest-style unit tests; currently untracked
test_parquet.py                     # Tracked manual data diagnostic
test_parse.py                       # Tracked manual feed/parquet diagnostic
scripts/validate_daily_ingest.py    # CI/operator warehouse validator
scripts/validate_season_warehouse.py# Broader warehouse validator
docs/progress/SESSION_*.md           # Recorded API/build/browser smoke checks
```

## Test Structure

**Suite Organization:**
```python
def test_elite_process_bad_results_scores_moderate() -> None:
    norms = default_league_norms()
    elite_process = OutingRawMetrics(
        swstr_pct=18.0,
        called_strike_pct=24.0,
        chase_pct=36.0,
        xwoba_allowed=0.240,
        game_whip=0.90,
        earned_runs=6,
        home_runs=2,
        pitches=95,
        outs=18,
    )
    scored = malliscore_v2(elite_process, norms)
    assert scored["dominance_score"] > scored["run_prevention_score"]
    assert 0.0 <= scored["malli_score_v2"] <= 100.0
```
- This pattern comes from `test_malli_score_v2.py`: arrange a deterministic dataclass input, call one public function, and assert domain relationships and bounds.

**Patterns:**
- Keep unit tests deterministic and free of warehouse/network dependencies, following all four tests in `test_malli_score_v2.py`.
- Assert invariants and relationships, not only exact snapshots: score bounds, monotonic workload behavior, and finite numeric output are covered in `test_malli_score_v2.py`.
- Use exact assertions only for mathematically stable values such as `harmonic_mean(70.0, 70.0) == 70.0` in `test_malli_score_v2.py`.
- For FastAPI work, exercise the concrete route with `TestClient`, assert the HTTP status, and inspect response fields; examples are recorded for analytics, scoring, taxonomy, and fantasy endpoints in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` through `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- For database smoke checks, isolate writes in a temporary database and remove temporary rows after live checks, following the verification records in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` and `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- For frontend changes, verify both compilation/build and the actual authenticated route in a browser, following `docs/progress/SESSION_03_CONTENT_SCORING_QUEUE_DECISION.md` and `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.

## Mocking

**Framework:** Not detected in `requirements.txt`, `mlbops/api/requirements-api.txt`, or `mlbops/hub/package.json`.

**Patterns:**
```python
# Existing tests avoid mocks by targeting pure scoring functions.
raw = OutingRawMetrics(
    swstr_pct=11.0,
    called_strike_pct=18.0,
    chase_pct=28.0,
    xwoba_allowed=0.32,
    game_whip=1.2,
    earned_runs=2,
    home_runs=1,
    pitches=88,
    outs=21,
)
scored = malliscore_v2(raw, default_league_norms())
assert all(math.isfinite(value) for value in scored.values())
```
- The concrete no-mock pattern is in `test_malli_score_v2.py` and works because `src/pitching_performances/malli_score.py` separates pure scoring from feed/boxscore adapters.

**What to Mock:**
- Mock `requests.get()` at module boundaries when unit-testing MLB API clients such as `src/probables_board/fetch.py`, `mlbops/api/routers/schedule.py`, and `scripts/validate_daily_ingest.py`; provide realistic nested MLB payloads and assert timeout/error behavior.
- Replace database access with a temporary SQLite database when testing `mlbops/api/db/database.py` or queue routers under `mlbops/api/routers/queue.py`; the documented integration pattern is in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md`.
- Mock outbound notification/social clients when testing Next.js routes that import `mlbops/hub/lib/twitter.ts`, `mlbops/hub/lib/resend.ts`, or `mlbops/hub/lib/twilio.ts`.
- Freeze or inject dates when testing date-sensitive scoring in `mlbops/api/services/content_scoring.py` and date defaults in `mlbops/hub/app/fantasy/page.tsx`.

**What NOT to Mock:**
- Do not mock pure domain functions in `src/pitching_performances/malli_score.py`, `mlbops/api/services/content_scoring.py`, or detector functions in `mlbops/api/live/detect.py`; construct direct inputs and assert outputs.
- Do not mock the function under test's dataclasses or plain dictionaries; use representative values such as those in `test_malli_score_v2.py`.
- Do not replace database transaction behavior with a loose fake when validating commit/rollback semantics in `mlbops/api/db/database.py`; use an isolated real SQLite file.
- Do not treat a mocked component test as sufficient for Hub workflows spanning `mlbops/hub/app/api/backend/[...path]/route.ts` and FastAPI; retain a route-level smoke check.

## Fixtures and Factories

**Test Data:**
```python
raw = OutingRawMetrics(
    swstr_pct=18.0,
    called_strike_pct=24.0,
    chase_pct=36.0,
    xwoba_allowed=0.240,
    game_whip=0.90,
    earned_runs=6,
    home_runs=2,
    pitches=95,
    outs=18,
)
```
- Tests construct small dataclass values inline in `test_malli_score_v2.py`; no shared factory layer exists.
- API checks use temporary queue rows and representative endpoint query strings documented in `docs/progress/SESSION_03_CONTENT_SCORING_QUEUE_DECISION.md` and `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- Warehouse validators derive expected game sets from MLB schedule responses and compare them with filesystem artifacts in `scripts/validate_daily_ingest.py`.

**Location:**
- No `fixtures/`, `factories/`, or `conftest.py` is detected.
- Historical raw/parquet data referenced by `test_parquet.py` and `test_parse.py` lives under ignored `data/warehouse/mlb/` paths and is not portable to clean checkouts.
- Tracked sample JSON/CSV outputs under `outputs/` are artifacts rather than a formal fixture API; new tests should place minimal sanitized fixtures in a dedicated test directory instead of depending on operator outputs.

## Coverage

**Requirements:** None enforced. No coverage dependency, configuration, threshold, report, or CI upload is detected in `requirements.txt`, `mlbops/api/requirements-api.txt`, `mlbops/hub/package.json`, or `.github/workflows/`.

**View Coverage:**
```bash
./mlb_env/bin/python -m pytest --cov=src --cov=mlbops/api --cov-report=term-missing  # Requires pytest-cov; not declared
```
- Treat this as an optional local diagnostic only until `pytest` and `pytest-cov` are declared in a development requirements file.
- No equivalent frontend coverage command exists because `mlbops/hub/package.json` contains no test script or coverage tooling.

## Test Types

**Unit Tests:**
- Pure numeric scoring has focused unit coverage in `test_malli_score_v2.py` for harmonic mean behavior, workload monotonicity/caps, process-vs-result scoring, and finite outputs.
- The unit test file is untracked, so clean clones and CI do not receive this coverage.
- Pure service logic in `mlbops/api/services/content_scoring.py`, content normalization in `mlbops/api/services/content_taxonomy.py`, and detectors in `mlbops/api/live/detect.py` have no committed unit tests despite being suitable for direct dictionary-based cases.

**Integration Tests:**
- FastAPI integration checks use `TestClient` for specific endpoints and are recorded in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` through `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- Database integration checks use temporary SQLite state or temporary queue rows, then clean up, as recorded in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` and `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- The daily CI workflow invokes `scripts/validate_daily_ingest.py` from `.github/workflows/daily_ingest.yml`; this validates final-game raw feeds and enriched parquet presence against the live schedule.
- Production verification includes API health and readiness requests documented in `deploy/README.md` and follows the repository-level `AGENTS.md` verification pattern.

**E2E Tests:**
- No committed E2E framework or suite is used; `mlbops/hub/package.json` has no Playwright/Cypress dependency or script.
- Headless browser checks are performed per feature and recorded in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md`, `docs/progress/SESSION_03_CONTENT_SCORING_QUEUE_DECISION.md`, and `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- Browser verification must cover authentication redirects/login and the changed workflow, because `mlbops/hub/proxy.ts` protects application and API routes.

**Static and Build Checks:**
- Run Python compilation against touched packages, matching the `compileall` checks recorded throughout `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` through `docs/progress/SESSION_05_FANTASY_HUB_QUEUE_INTEGRATION.md`.
- Run strict TypeScript explicitly through `mlbops/hub/tsconfig.json`; do not infer success from the production build because `mlbops/hub/next.config.ts` ignores build-time type errors.
- Run `npm run build` from `mlbops/hub/package.json` to validate Next.js bundling, route generation, and server/client boundaries.

## Common Patterns

**Async Testing:**
```python
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)
response = client.get("/fantasy/streamers", params={
    "game_date": "2026-05-13",
    "season": 2026,
    "limit": 3,
    "include_live_probables": False,
})
assert response.status_code == 200
assert "streamers" in response.json()
```
- This is the reusable shape implied by the endpoint smoke check recorded in `docs/progress/SESSION_04_FANTASY_STREAMER_MATRIX.md`; no committed harness currently contains it.
- Use `TestClient` for async FastAPI endpoints because routers such as `mlbops/api/routers/fantasy.py` offload blocking work through `run_in_threadpool`.
- For Hub client fetch behavior in `mlbops/hub/app/fantasy/page.tsx` and `mlbops/hub/components/ScheduleGameCard.tsx`, test loading, non-OK response, success, and `finally` cleanup when frontend test tooling is introduced.

**Error Testing:**
```python
def test_workload_scalar_stays_within_caps() -> None:
    value = workload_scalar(110, 27)
    assert MIN_WORKLOAD_SCALAR <= value <= MAX_WORKLOAD_SCALAR
```
- Existing error-adjacent coverage in `test_malli_score_v2.py` emphasizes numeric bounds and finite values.
- Add boundary tests for invalid dates and IDs exposed by `mlbops/api/routers/schedule.py` and `mlbops/api/routers/fantasy.py`, asserting both status code and response detail.
- Add failure-path tests for malformed JSON, missing files, stale raw feeds, and non-played finals handled by `scripts/validate_daily_ingest.py`.
- Add transaction rollback tests for exceptions inside `get_db()` in `mlbops/api/db/database.py` and auth/CSRF rejection tests for `mlbops/hub/lib/security.ts` when a JS test runner is established.

## Verification Guidance

- For a Python pure-function change under `src/`, add or update a deterministic pytest case, run that case, and compile the touched module. `src/pitching_performances/malli_score.py` plus `test_malli_score_v2.py` is the current model.
- For a FastAPI endpoint change under `mlbops/api/`, compile touched modules, run a narrow `TestClient` request, start/restart the API if deployment is in scope, check `/health`, and smoke the concrete endpoint; this follows the root `AGENTS.md` and `docs/progress/` records.
- For a Hub change under `mlbops/hub/`, run `npx tsc --noEmit`, run `npm run build`, and exercise the authenticated route in a browser. Verify both backend JSON and built frontend assets for Insights work, per the root `AGENTS.md`.
- For ingestion changes under `src/ingestion/` or `scripts/`, use a date-scoped warehouse validation through `scripts/validate_daily_ingest.py`; current-season data verification belongs on the VPS warehouse according to the root `AGENTS.md`.
- Keep test writes isolated from `data/hub.db` and production warehouse paths. The temporary-database pattern recorded in `docs/progress/SESSION_01_UPSCALE_FOUNDATION.md` is the default for database integration checks.

---

*Testing analysis: 2026-07-10*
