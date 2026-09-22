# Morning intel (Mallitalytics)

Everything for the daily intel pipeline lives **in this folder** (except the GitHub workflow file).

| Path | Purpose |
|------|---------|
| `morning_intel.py` | Main CLI — MLB news + scores + Statcast signals + GLM editorial + newsletter email |
| `signal_stats.py` | Standard errors, shrinkage, FDR — how a delta becomes a ranked finding |
| `persistence.py` | Reads prior snapshots so a signal that repeats outranks one that does not |
| `scouting.py` | Scouting ledger + news-to-player entity resolution |
| `collect_scouting.py` | Collector CLI — transactions, MLB.com, Grok → ledger |
| `morning_digest.py` | Deprecated stub (points you to `morning_intel.py`) |
| `snapshots/` | JSON data and HTML newsletter previews written each run |
| `scouting/ledger.jsonl` | Append-only sourced claims (gitignored) |
| `priors/` | Cached prior-season baselines (gitignored) |

## How a signal becomes a lead

Findings are ranked by **delta over its own standard error**, not by raw delta.
Ranking by size alone sorts toward whichever metric is noisiest at the smallest
sample — it previously filled the newsletter with pitch-mix swings on 50-pitch
windows and batters who happened to hit three barrels in ten balls in play.

1. **Detect** — each metric emits its own standard error. Rates use a pooled
   binomial; means regularize toward league dispersion measured from the
   warehouse; pitch mix uses *between-outing* spread, because a pitcher picks a
   plan per start rather than per pitch.
2. **Blend** — thin baselines are pulled toward the cached prior season by the
   metric's stabilization weight, so April needs no special-casing.
3. **Persist** — prior snapshots are read back and repeats are scored up. A
   false positive from noise rarely repeats three days running.
4. **Corroborate** — ledger claims from the last 14 days attach to the signal. A
   smaller move with a known mechanism outranks a larger unexplained one.
5. **Control** — Benjamini-Hochberg across the whole slate, because ~1,600
   comparisons run every morning and any fixed cutoff admits chance findings.
6. **Split** — a handful of leads (capped per metric family so one metric cannot
   monopolize them) and a watch list that graduates on repeat.

## Running

```bash
# briefing (local warehouse)
python morning_intel/morning_intel.py --dry-run --skip-notify --skip-claude

# on the VPS, next to the live warehouse — this is the production path
deploy/vps_morning_intel.sh

# scouting collector; safe to run more often than the briefing
python morning_intel/collect_scouting.py --hours 24 --dry-run

# prior-season baselines, once per season, wherever that season is complete
python morning_intel/morning_intel.py --season 2025 --build-priors
```

`priors_<season>.json` is a derived artifact and is gitignored. Build it where
the season is complete and copy it to `morning_intel/priors/` on the target
host — the VPS warehouse currently holds only the current season.

**Known constraint:** `mlb.com` refuses datacenter traffic, so the RSS headline
section is empty when the job runs on the VPS (`statsapi.mlb.com` is
unaffected). The reason appears in the pipeline notes rather than silently
emptying the section, and the scouting ledger still supplies reporting.

**Environment variables** (create `morning_intel/.env` — gitignored — or reuse `jobs/.env`; both are loaded)

| Variable | Purpose |
|----------|---------|
| `GLM_API_KEY` | GLM editorial brief and private tweet drafts |
| `GLM_MODEL` | Optional; default `glm-5.2` |
| `GLM_BASE_URL` | Optional; default `https://api.z.ai/api/coding/paas/v4` |
| `RESEND_API_KEY`, `RESEND_FROM_EMAIL`, `RESEND_TO_EMAIL` | Resend delivery (optional) |
| `GMAIL_SMTP_USER`, `GMAIL_APP_PASSWORD`, `MORNING_INTEL_TO_EMAIL` | Gmail SMTP delivery fallback; use a Google app password, never the account password |
| `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM`, `TWILIO_WHATSAPP_TO` | Digest WhatsApp |
| `PUBLIC_STATIC_BASE_URL` | Card image URLs in queue (e.g. `http://localhost:8000/static`) |

GitHub Actions: set the same values as **repository secrets** (see workflow file). The scheduled newsletter runs with `--skip-queue`; it does not copy or modify the legacy Drive `hub.db`.

**Run locally**

```bash
python morning_intel/morning_intel.py --dry-run --skip-notify --skip-claude
```

**GitHub Actions:** `.github/workflows/morning_intel.yml` (after daily warehouse ingest).

The briefing has no player-watchlist dependency. It scans league-wide news and qualified Statcast signals, then writes a private content notebook into the email.

**About `jobs/`:** That directory holds other automation (`daily_card_generator.py`, `weekly_report.py`, etc.). It is **not** tied to cron — only `crontab.example` documents optional local schedules. Production timing for intel is **GitHub Actions**, not your Mac.
