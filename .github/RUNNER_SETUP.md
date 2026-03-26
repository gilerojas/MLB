# Self-Hosted Runner Setup

A self-hosted runner lets your Mac execute GitHub Actions jobs locally.
This means the warehouse data stays on your machine — no cloud storage needed.

## One-time setup (~5 minutes)

### 1. Register the runner on GitHub

1. Go to your repo on GitHub → **Settings** → **Actions** → **Runners**
2. Click **New self-hosted runner**
3. Select **macOS** → **ARM64** (for M-series Mac) or **x64**
4. Follow the shown commands — they download the runner agent and register it

When asked for runner labels, add: `mlb-mac`
(the workflow file uses `runs-on: self-hosted` which matches any self-hosted runner)

### 2. Start the runner as a background service

After registration, install it as a launchd service so it starts automatically on login:

```bash
cd ~/actions-runner
./svc.sh install
./svc.sh start
```

Check it's running:
```bash
./svc.sh status
```

### 3. Verify on GitHub

Go to **Settings → Actions → Runners** — your Mac should show as **Idle** (green dot).

---

## How it works

- **Daily ingest** (`daily_ingest.yml`) runs at 3:05 AM ET every day
  - Ingests yesterday's regular season games
  - Then runs `daily_card_generator.py` to queue draft cards
- **Manual card gen** (`generate_cards.yml`) — trigger from Actions tab anytime
  - Pick a date and card type

## Manual trigger (from GitHub UI)

1. Go to repo → **Actions** tab
2. Click the workflow you want
3. Click **Run workflow** (top right)
4. Fill in inputs → **Run**

## Monitoring

- Every run appears in **Actions** tab with logs
- GitHub emails you if a run fails (configure in Settings → Notifications)
- Green checkmark = ingestion succeeded

## Stopping the runner temporarily

```bash
cd ~/actions-runner
./svc.sh stop
```

## Troubleshooting

**Runner shows offline**: `./svc.sh start` from `~/actions-runner`

**pybaseball hangs**: The script uses `--quiet` flag in CI. If it still hangs,
check Statcast API availability. The `--last-days 1` flag limits scope.

**Wrong Python**: Workflows use `mlb_env/bin/python` — make sure the venv
exists at `MLB/mlb_env/` (it's gitignored, so recreate if you re-clone).
