# Fix Mac → VPS SSH (Hostinger)

Symptom: `nc` to port 2222 succeeds, but `ssh ... 'echo ok'` hangs forever (no banner, no passphrase prompt).

Hub/Tailscale (`http://100.111.41.78`) can work while public SSH is broken.

## Fastest workaround: Tailscale SSH

Skip public port 2222 entirely. Use the same Tailscale network as the Hub.

**On VPS (Hostinger browser terminal):**

```bash
tailscale version
tailscale set --ssh
systemctl restart tailscaled 2>/dev/null || true
```

If your tailnet uses ACLs, enable SSH in [Tailscale admin → Access controls](https://login.tailscale.com/admin/acls) or use the admin UI **Machines → … → Edit route settings → SSH**.

**On Mac:**

```bash
tailscale status          # find VPS name, e.g. srv1698142
tailscale ssh root@srv1698142
```

Add to `~/.ssh/config`:

```sshconfig
Host mlbops-vps
  HostName srv1698142
  User root
  ProxyCommand /usr/local/bin/tailscale ssh -- %h %p
```

Or use Tailscale's MagicDNS name from `tailscale status`.

Then rsync works:

```bash
./deploy/sync_warehouse_to_vps.sh --season 2026
# with: export MLBOPS_VPS_HOST=srv1698142  and ProxyCommand via ssh config
```

---

## Fix public SSH (Hostinger console)

Run on the VPS. Paste output if still stuck.

### 1. Confirm sshd is what answers 2222

```bash
ss -tlnp | grep -E ':22|:2222'
ssh -p 2222 -o ConnectTimeout=5 root@127.0.0.1 'echo local-ok'
```

- **local-ok prints** → sshd fine; problem is firewall / fail2ban / Hostinger edge.
- **local hang** → sshd misconfig on that port.

### 2. Fix slow DNS (most common “hang after Connection established”)

```bash
grep -i usedns /etc/ssh/sshd_config /etc/ssh/sshd_config.d/* 2>/dev/null

sudo tee /etc/ssh/sshd_config.d/99-mlbops.conf <<'EOF'
UseDNS no
GSSAPIAuthentication no
LoginGraceTime 30
EOF

sudo sshd -t && sudo systemctl restart ssh
```

Test from Mac again.

### 3. fail2ban / blocked IP

```bash
sudo fail2ban-client status 2>/dev/null
sudo fail2ban-client status sshd 2>/dev/null
sudo fail2ban-client status sshd-2222 2>/dev/null

# if your Mac IP is banned (check Hostinger or https://ifconfig.me on Mac):
sudo fail2ban-client set sshd unbanip YOUR_MAC_PUBLIC_IP
```

### 4. Hostinger + UFW firewall

```bash
sudo ufw status verbose
sudo iptables -L INPUT -n | head -30
```

In **hPanel → VPS → Security / Firewall**: allow **TCP 2222** (and 22 if used) from your IP or temporarily from anywhere while testing.

### 5. sshd logs while you connect from Mac

On VPS:

```bash
sudo journalctl -u ssh -f
```

From Mac, try SSH once. Watch for `Accepted`, `refused`, `banner`, or silence.

### 6. Verify Mac key is authorized on VPS

```bash
grep -n ed25519 /root/.ssh/authorized_keys
# Mac pubkey:
# ssh-keygen -y -f /Users/gilrojasb/Desktop/Hermes/id_ed25519
```

If missing, paste pubkey into `/root/.ssh/authorized_keys` from Hostinger console.

---

## Mac-side checklist

```bash
# key fingerprint (no secrets)
ssh-keygen -l -f /Users/gilrojasb/Desktop/Hermes/id_ed25519

# verbose connect — note last debug line before hang
ssh -vvv -i /Users/gilrojasb/Desktop/Hermes/id_ed25519 -p 2222 \
  -o GSSAPIAuthentication=no -o ConnectTimeout=15 \
  root@2.24.123.57
```

If hang is after `Connection established` with **no** `Remote protocol version` line → server-side (UseDNS / firewall / not sshd).

---

## After SSH works

```bash
ssh-add --apple-use-keychain /Users/gilrojasb/Desktop/Hermes/id_ed25519
./deploy/sync_warehouse_to_vps.sh --season 2026
```

Daily warehouse updates stay on cron (`deploy/vps_daily_ingest.sh`, last 2 days). Full season = one-time Drive pull or rsync from Mac.
