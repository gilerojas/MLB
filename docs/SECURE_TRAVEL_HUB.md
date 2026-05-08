# Secure Travel Hub

This document explains how to run the Mallitalytics MLB Ops hub as a private browser app for laptop and phone access while traveling.

## What This Setup Does

The hub runs locally on the trusted machine and is exposed privately through Tailscale Serve. This keeps the app low-cost and avoids deploying the SQLite database, generated cards, and X credentials to a paid cloud host.

Security and workflow changes included:

- Password login at `/login`.
- Signed HTTP-only session cookies.
- Protected Next.js proxy for app routes.
- CSRF protection on write actions.
- Authenticated `/api/backend/...` proxy so browser requests reach FastAPI through Next.js.
- Local-only travel launcher binding services to `127.0.0.1`.
- Rate limiting and audit logging for sensitive actions.
- Mobile-first `/queue` Write Now composer.
- Manual quick-post drafts.
- Posting streak stats.
- AI redrafts marked as `ai_assisted` in queue metadata.

## Run Locally

From the repo root:

```bash
./scripts/start_mlbops_travel.sh
```

The travel launcher starts:

- Hub: `http://127.0.0.1:3001`
- FastAPI: `http://127.0.0.1:8000`

Open the hub in a browser:

```text
http://127.0.0.1:3001
```

Login uses the password configured in `mlbops/.env`.

## Tailscale Access

After the hub is running locally, expose only the Next.js hub through Tailscale:

```bash
tailscale serve --bg 3001
```

Then open the Tailscale Serve URL from devices signed into the same Tailscale account.

Do not use Tailscale Funnel for this app. Funnel exposes the service publicly, which is not the intended security model.

## Password And Secrets

Runtime secrets live in `mlbops/.env`, not in this document.

Required security variables:

```bash
MLBOPS_APP_PASSWORD_SHA256=...
MLBOPS_SESSION_SECRET=...
MLBOPS_LOCAL_ONLY=1
MLBOPS_HUB_HOST=127.0.0.1
MLBOPS_API_HOST=127.0.0.1
MLBOPS_STRICT_CORS=1
MLBOPS_HUB_PORT=3001
```

Rotate the app password by replacing `MLBOPS_APP_PASSWORD_SHA256` with a SHA-256 hash:

```bash
printf '%s' 'new-password-here' | shasum -a 256
```

Generate a new session secret with:

```bash
openssl rand -base64 48
```

Keep `.env` permissions restricted:

```bash
chmod 600 mlbops/.env
```

## Posting Workflow

Use `/queue` as the main mobile posting surface.

- `Write Now` is for spontaneous manual drafts.
- `Save draft` creates a text-only queue item.
- `Need a nudge?` shows non-AI prompts first.
- AI redraft actions are secondary and marked in metadata.
- Streak stats show posts today, weekly total, current streak, longest streak, and manual ratio.

For generated content, the existing Cards, Intel, Live, and Queue workflows continue to work through the authenticated hub.

## Security Model

The intended access path is:

```text
Phone/Laptop browser
  -> Tailscale Serve private URL
  -> Next.js hub on 127.0.0.1:3001
  -> authenticated /api/backend proxy
  -> FastAPI on 127.0.0.1:8000
```

The browser should not talk directly to FastAPI. The FastAPI service is bound locally, and write actions are protected by the Next.js session and CSRF layer.

## Quick Checks

Unauthenticated hub pages should redirect to login:

```bash
curl -I http://127.0.0.1:3001/queue
```

Unauthenticated API requests should return `401`:

```bash
curl http://127.0.0.1:3001/api/queue/streaks
```

The app should be reachable after login at:

```text
http://127.0.0.1:3001
```

