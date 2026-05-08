#!/usr/bin/env bash
# Secure local-only launcher for Tailscale Serve.
#
# Usage:
#   ./scripts/start_mlbops_travel.sh
#   tailscale serve --bg 3000
#
# This binds FastAPI and Next to 127.0.0.1. Do not use Tailscale Funnel for this app.

set -euo pipefail

export MLBOPS_LOCAL_ONLY=1
export MLBOPS_UVICORN_RELOAD="${MLBOPS_UVICORN_RELOAD:-0}"
export MLBOPS_HUB_HOST=127.0.0.1
export MLBOPS_API_HOST=127.0.0.1
export MLBOPS_STRICT_CORS=1

exec "$(cd "$(dirname "$0")/.." && pwd)/start_hub.sh"

