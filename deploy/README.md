# MLB Ops VPS Deployment

Phase 1 runs MLB Ops privately on the VPS with Docker Compose. Google Drive is not used in the live runtime path.

The default Hub container uses Next production mode (`next start`). For active UI work, switch only the Hub to dev mode with `deploy/vps_hub_dev.sh`; API, Postgres, warehouse, and outputs stay unchanged.

## Server Layout

Create these directories on the VPS:

```bash
mkdir -p /srv/mlbops/{app,warehouse/mlb,outputs,postgres,logs,backups,env}
```

Runtime mounts:

- `/srv/mlbops/warehouse/mlb` -> `/data/warehouse/mlb`
- `/srv/mlbops/outputs` -> `/outputs`
- `/srv/mlbops/postgres` -> Postgres data
- `/srv/mlbops/logs` -> service logs

## Environment

Copy `deploy/vps.env.example` to `/srv/mlbops/env/mlbops.env` on the VPS and fill secrets:

```bash
cp deploy/vps.env.example /srv/mlbops/env/mlbops.env
chmod 600 /srv/mlbops/env/mlbops.env
```

Use a real `POSTGRES_PASSWORD`, `MLBOPS_SESSION_SECRET`, and `MLBOPS_APP_PASSWORD_SHA256`.

Hub login requires both secrets in `/srv/mlbops/env/mlbops.env`:

```bash
# Mac — pick a hub password and hash it
printf '%s' 'your-hub-password' | shasum -a 256

# Mac — session signing secret (32+ chars)
openssl rand -base64 48
```

Add to `mlbops.env`:

```bash
MLBOPS_APP_PASSWORD_SHA256=<sha256-from-above>
MLBOPS_SESSION_SECRET=<random-from-above>
MLBOPS_SECURE_COOKIES=0
```

Restart hub after editing: `docker compose --env-file /srv/mlbops/env/mlbops.env up -d hub`

If Unlock does nothing or loops back to login, check: `docker compose logs hub --tail 30`

## Deploy code changes (Mac → VPS)

**Agent default after code changes:** `./deploy/ship.sh "what changed"`  
(commit → push → `./deploy/sync_app_to_vps.sh`)

Manual deploy only:

```bash
./deploy/sync_app_to_vps.sh
```

Then hard-refresh the browser (`Cmd+Shift+R`).

Rsync only (no rebuild):

```bash
./deploy/sync_app_to_vps.sh --no-build
```

Defaults: Tailscale `100.111.41.78`, SSH port `2222`, key `~/Desktop/Hermes/id_ed25519`.

## Hub Dev Mode

Use this while MLB Ops is under active private development. It keeps the current Hub URL and only changes the Hub container command/volumes:

```bash
./deploy/vps_hub_dev.sh enable
```

Then normal UI edits are:

```bash
./deploy/vps_hub_dev.sh sync
```

Refresh the Hub after sync. No Hub image rebuild is needed for ordinary frontend edits.

Useful controls:

```bash
./deploy/vps_hub_dev.sh status
./deploy/vps_hub_dev.sh logs
./deploy/vps_hub_dev.sh disable
```

The dev override bind-mounts `mlbops/hub` only. `node_modules` and `.next` are Docker-owned Linux volumes, so Mac dependencies are never mounted into the Linux container.

**One-time alternative:** clone the GitHub repo on the VPS so `git pull` works:

```bash
cd /srv/mlbops
mv app app.bak
git clone YOUR_GITHUB_URL app
# restore env + compose paths; then git pull && docker compose ... up -d --build
```

## Start Services

From `/srv/mlbops/app`:

```bash
docker compose --env-file /srv/mlbops/env/mlbops.env up -d --build
docker compose --env-file /srv/mlbops/env/mlbops.env ps
```

The compose file binds only to localhost on the VPS:

- Hub: `127.0.0.1:3001`
- API: `127.0.0.1:8000`
- Postgres: container network only, not public

## Import Current Local State

After copying `data/hub.db` to the deployed app folder, run:

```bash
docker compose --env-file /srv/mlbops/env/mlbops.env run --rm api \
  python /app/scripts/import_hub_db_to_postgres.py --db /app/data/hub.db
```

This imports queue, watchlist, notification, metrics, live event, audit, and performance rows into Postgres.

## SSH Access

VPS connection defaults:

- Host: `2.24.123.57` (public) — **SSH/rsync use this IP on port 2222**
- Tailscale `100.111.41.78` — Hub/browser only (SSH is not open on the Tailscale IP)
- Port: `2222`
- User: `root`
- Key: `/Users/gilrojasb/Desktop/Hermes/id_ed25519`

**Mac SSH hangs?** See [`deploy/fix_vps_ssh.md`](fix_vps_ssh.md). Fastest workaround: **Tailscale SSH** (`tailscale ssh root@srv1698142`) — same network as the Hub.

Optional `~/.ssh/config` entry:

```sshconfig
Host mlbops-vps
  HostName 2.24.123.57
  User root
  Port 2222
  IdentityFile /Users/gilrojasb/Desktop/Hermes/id_ed25519
  IdentitiesOnly yes
```

## Private Access

**Tailscale (current):** open the Hub at `http://100.111.41.78` (Docker publishes Hub directly on the Tailscale IP).

**SSH tunnel (fallback):**

```bash
ssh -i /Users/gilrojasb/Desktop/Hermes/id_ed25519 -p 2222 \
  -L 3001:127.0.0.1:3001 -L 8000:127.0.0.1:8000 root@2.24.123.57
```

Then open `http://127.0.0.1:3001` locally.

## Warehouse Sync

Expected full **2026** mirror: ~884 raw feeds, ~830 pitches_enriched, ~140M.

### Option A — Mac rsync (when SSH works)

```bash
# 1. Refresh Mac from Drive
./scripts/pull_mlbops_from_drive.sh

# 2. Push 2026 to VPS (shows progress; SSH preflight first)
./deploy/sync_warehouse_to_vps.sh --season 2026

# 3. Verify
./deploy/vps_verify_warehouse.sh --remote 2026
```

### Option B — VPS pulls from Drive (when Mac SSH hangs)

Use this if `sync_warehouse_to_vps.sh` stalls on SSH. No Mac rsync needed.

**1. Mac — show rclone config** (config lives at `~/.rclone.conf`, not `~/.config/rclone/`):

```bash
cat ~/.rclone.conf
```

**2. Hostinger browser terminal** — paste into VPS config:

```bash
mkdir -p ~/.config/rclone
nano ~/.config/rclone/rclone.conf
```

Paste Mac output, save (`Ctrl+O`, Enter, `Ctrl+X`).

**3. Test + pull 2026:**

```bash
rclone lsd mallitalytics:MLB/warehouse/mlb/2026

rclone sync mallitalytics:MLB/warehouse/mlb/2026 /srv/mlbops/warehouse/mlb/2026 \
  --transfers 8 --stats 30s --stats-one-line
```

Or use the wrapper script:

```bash
bash /srv/mlbops/app/deploy/vps_pull_warehouse_from_drive.sh 2026
```

**4. Verify on VPS:**

```bash
du -sh /srv/mlbops/warehouse/mlb/2026
ls /srv/mlbops/warehouse/mlb/2026/regular_season/raw | wc -l
ls /srv/mlbops/warehouse/mlb/2026/regular_season/pitches_enriched | wc -l
```

Target: ~140M, ~884 raw, ~830 pitches.

### SSH hangs

If the sync script prints the header then sits silent, it is waiting on SSH (before rsync starts).

Try in order:

```bash
# 1. Unlock key if it has a passphrase
ssh-add --apple-use-keychain /Users/gilrojasb/Desktop/Hermes/id_ed25519

# 2. Test SSH alone (should return "connected" in ~5s)
ssh -i /Users/gilrojasb/Desktop/Hermes/id_ed25519 -p 2222 \
  -o GSSAPIAuthentication=no -o ConnectTimeout=15 \
  root@2.24.123.57 'echo connected'
```

If step 2 still hangs → use **Option B** (Drive pull on VPS).

The API container mounts `/srv/mlbops/warehouse/mlb` at `/data/warehouse/mlb`; no restart after sync.

## Health Checks

On the VPS:

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/system/readiness
```

Through the SSH tunnel, from the Mac:

```bash
curl -s http://127.0.0.1:8000/health
open http://127.0.0.1:3001
```

## Reboot Check

```bash
reboot
```

After SSH reconnect:

```bash
cd /srv/mlbops/app
docker compose --env-file /srv/mlbops/env/mlbops.env ps
docker compose --env-file /srv/mlbops/env/mlbops.env up -d
```
