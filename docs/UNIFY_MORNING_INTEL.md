# Unify the Morning Intel execution path

Task prompt for an engineering agent. Every fact below was verified on
2026-09-22 against the live VPS and GitHub Actions run `35787751888`. Do not
trust it blindly — re-verify anything you are about to depend on, and correct
this document where it has drifted.

---

## The problem in one line

Morning Intel runs on two hosts, each holding roughly half of the inputs it
needs, and they keep **separate snapshot histories** — which silently corrupts
the persistence logic that decides which findings are publishable.

## What each host has

| Input | GitHub Actions | VPS (`api` container) |
|---|---|---|
| Warehouse | Drive mirror, ~1,564 parquet pulled per run (6–7 min) | `/data/warehouse/mlb`, live, ingested 4×/day on cron |
| `mlb.com` RSS | works | **HTTP 403** — IP blocked, browser UA does not help |
| `statsapi.mlb.com` | works | works |
| Email secrets | `GMAIL_SMTP_USER`, `GMAIL_APP_PASSWORD`, `MORNING_INTEL_TO_EMAIL`, `RESEND_TO_EMAIL` | keys present in `/srv/mlbops/env/mlbops.env` but **0 chars** |
| `GLM_API_KEY` | set | set |
| `X_API_KEY` (Grok) | **not a GitHub secret at all** | set, with `GROK_MODEL` |
| Scouting ledger | absent — fresh checkout, file is gitignored | `morning_intel/scouting/ledger.jsonl`, 252 entries |
| Prior-season baselines | absent — gitignored | `morning_intel/priors/priors_2025.json`, copied by hand |
| Snapshot history | Drive `MLB/morning_intel/snapshots`, synced both ways | repo bind mount, **never synced anywhere** |
| rclone → Drive | yes | yes, remote `mallitalytics:` |

Neither host can currently produce a complete edition.

## The five shortcomings, in priority order

### 1. Two snapshot histories corrupt persistence (correctness bug, not cosmetic)

`morning_intel/persistence.py::build_history` counts how many prior snapshots
contain a signal, and `confirmed_streak` requires the window to have advanced.
That streak feeds `anomaly_score` (a ×1.6 multiplier at its ceiling) and
promotes sub-threshold findings into leads in `split_leads_and_watch`.

Actions reads and writes Drive. The VPS reads and writes its own bind mount.
A signal that genuinely repeated for four days across alternating hosts is
scored as new on both. Streaks are undercounted, so real repeats are suppressed
and the multiplicity control the feature exists to provide does not apply.

**This is the reason to unify.** Everything else is inconvenience.

### 2. Corroboration only ever fires on the VPS

`_scout.read_since()` on Actions returns `[]` because the ledger is gitignored
and the checkout is fresh. Consequences on that host:

- no lead ever gets the `+1.0` corroboration bonus in `anomaly_score`
- no lead shows a human hinge, which is the whole point of the ledger
- `_stories_from_ledger()` — the fallback for the news panel — returns nothing
- the collector cannot be run there anyway: `X_API_KEY` is not a GitHub secret

### 3. Priors are absent on Actions (verified, not theoretical)

Run `35787751888` printed:

```
No 2025 priors cached — early-season baselines are unblended.
```

The same anchor date on the VPS printed:

```
Blended 910 baselines with 2025 priors.
```

`priors_2025.json` is 192 KB and gitignored via `morning_intel/priors/*.json`.
Effect is small in September when baselines are thick, and large in April —
which is precisely the case the feature was built for.

### 4. `mlb.com` refuses the VPS

Verified: `https://www.mlb.com/feeds/news/rss.xml` returns 403 from inside the
`api` container, with and without a browser `User-Agent`. `statsapi.mlb.com`
returns 200 from the same container, so this is host-level blocking of
`mlb.com`, not general egress failure.

The current VPS behaviour is to note the reason in the pipeline notes and fall
back to the scouting ledger for the "Around the league" panel. That is honest
but it is not the same content, and it is the one capability Actions has that
the VPS cannot obtain by configuration alone.

### 5. Scheduling is split for no reason

VPS cron already runs the whole rest of the system:

```
30 2,6,10 * * *  vps_daily_ingest.sh
0  8 * * *       vps_daily_ingest.sh
0  7 * * *       vps_score_shadow_slates.sh
30 8 * * *       vps_predict_e7_slate.sh
15 7-14 * * *    vps_statcast_retry.sh
10 9 * * *       vps_publish_mallitalytics_public.sh
30 9 * * *       vps_publish_mallitalytics_starter_cards.sh
20 9 * * *       vps_publish_mallitalytics_season_profiles.sh
30 10 * * *      vps_verify_mallitalytics_public.sh
...
```

Morning Intel is the only component still in Actions, where it pays 6–7 minutes
per run to re-download a warehouse that already exists locally on the VPS.

## Constraints you must respect

- **No SSH key exists in GitHub secrets.** Actions cannot reach the VPS. Any
  design where Actions pushes to the VPS requires a new secret the user must
  create; do not assume it.
- **The VPS has rclone configured** (`mallitalytics:`), so Drive is a viable
  shared medium in both directions.
- **Container Python is 3.11.16.** A newer local Python will accept syntax that
  fails there — backslashes inside f-string expressions is the one that already
  bit this code. Always `py_compile` inside the container before declaring done.
- Warehouse inside the container is `/data/warehouse/mlb`; the repo is bind
  mounted at `/app`; env file is `/srv/mlbops/env/mlbops.env`.
- Never print or summarise secret values.
- `AGENTS.md` applies, including the Mallitalytics brand gate for anything
  user-facing and the "avoid hanging work" rules for long reads and builds.

## Goal

**One execution path, one state, one snapshot history.** A single scheduled run
produces a complete edition: live warehouse, RSS headlines, scouting ledger,
priors, persistence continuity, and delivered email.

## Recommended direction — validate before building

The VPS should be the single execution host. It holds the live warehouse, the
Grok key, the ledger, and it already schedules everything else. Actions should
keep only the capability the VPS cannot obtain: reaching `mlb.com`.

Proposed shape, in dependency order:

1. **Make the VPS able to send.** Populate `GMAIL_SMTP_USER`,
   `GMAIL_APP_PASSWORD`, `MORNING_INTEL_TO_EMAIL` in the env file, confirm they
   reach the container, and verify with one real send. Until this is done
   nothing else matters, because the VPS produces a briefing that never arrives.

2. **Make state shared and singular.** Have the VPS rclone the snapshot
   directory and the scouting ledger to and from Drive, the way the Actions job
   already does for snapshots. Decide explicitly whether Drive or the VPS disk
   is authoritative and write it down. Verify that `build_history` sees a
   continuous history across runs from either host.

3. **Solve RSS deliberately.** Options, in the order worth testing:
   - a thin Actions job whose only job is to fetch the feed, resolve player ids,
     and append tier-1 entries to the shared ledger on Drive — this inverts the
     dependency and reuses `collect_scouting.py` machinery
   - a news source reachable from the VPS that is good enough to replace it
   - accept the ledger-only panel and delete the RSS path
   Do not leave a silent empty section whichever way you go.

4. **Distribute priors.** Either commit `priors_*.json` (192 KB, and it is a
   real input rather than a build artifact) or have the VPS build them from its
   own warehouse once the prior season is present there. Note that the VPS
   currently holds only 2026, which is why the file was hand-copied.

5. **Reduce Actions to its residual role.** Either the news fetcher above, or a
   manual-dispatch fallback that can still produce an edition if the VPS is
   down. Remove the `workflow_run` trigger so two hosts never publish the same
   edition to two histories. The workflow file currently carries a comment
   saying it is superseded — make that true or remove the comment.

6. **One script, one entry point.** `deploy/vps_morning_intel.sh` currently runs
   the collector then the briefing. Fold the full sequence into it, add it to
   cron after the 11:00 UTC ingest, and make the Actions path call the same code
   with different flags rather than duplicating the sequence.

## Acceptance criteria

- A single scheduled invocation produces an edition containing: leads from the
  live warehouse, a populated news panel, at least one corroborated lead when
  the ledger has a matching claim, blended baselines when a prior season is
  available, and a delivered email. All verified from the run log.
- Two consecutive runs on consecutive days show non-zero `confirmed_streak` for
  at least one repeating signal, proving snapshot continuity.
- `morning_intel/snapshots/` contains one continuous history regardless of which
  host ran, with no duplicate edition for the same anchor date.
- The pipeline notes contain no "unavailable", "missing", or "not cached"
  warnings under normal operation.
- `py_compile` passes inside the `api` container for every touched module.
- `python -m unittest tests.test_morning_intel_signal` passes.
- No secret values appear in any log or committed file.

## Where the code is

| Path | Role |
|---|---|
| `morning_intel/morning_intel.py` | CLI, detection, ranking, render, send |
| `morning_intel/signal_stats.py` | standard errors, shrinkage, FDR |
| `morning_intel/persistence.py` | snapshot history, streaks |
| `morning_intel/scouting.py` | ledger, entity resolution, corroboration |
| `morning_intel/collect_scouting.py` | collector CLI (transactions, RSS, Grok) |
| `deploy/vps_morning_intel.sh` | VPS entry point |
| `.github/workflows/morning_intel.yml` | Actions entry point |
| `tests/test_morning_intel_signal.py` | regression tests |
| `morning_intel/README.md` | how a signal becomes a lead |

Work on branch `morning-intel-signal` (pushed, not merged) or a branch from it.
`main` still runs the old ranking code, so the daily email is the old design
until that branch merges.
