# Morning intel (Mallitalytics)

Everything for the daily intel pipeline lives **in this folder** (except the GitHub workflow file).

| Path | Purpose |
|------|---------|
| `morning_intel.py` | Main CLI — MLB news + scores + Statcast signals + Claude drafts + newsletter email |
| `morning_digest.py` | Deprecated stub (points you to `morning_intel.py`) |
| `snapshots/` | JSON data and HTML newsletter previews written each run |

**Environment variables** (create `morning_intel/.env` — gitignored — or reuse `jobs/.env`; both are loaded)

| Variable | Purpose |
|----------|---------|
| `ANTHROPIC_API_KEY` or `anthropic_api_key` | Claude tweet drafts |
| `ANTHROPIC_MODEL` | Optional; default `claude-sonnet-4-20250514` |
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
