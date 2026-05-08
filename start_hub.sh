#!/bin/bash
# mlbops — start FastAPI + Next.js hub together.
#
# Usage (repo root):
#   ./start_hub.sh
#   # or
#   ./scripts/start_mlbops.sh
#   # travel/private mode:
#   ./scripts/start_mlbops_travel.sh
#
# Then open: http://127.0.0.1:3000  (hub, or MLBOPS_HUB_PORT)  ·  API: :8000/docs
#
# If you see "Failed to fetch" in the browser, the API was not running or CORS
# blocked the hub origin — this script sets CORS for both localhost and 127.0.0.1.

set -e
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

# Next.js inherits the shell env; FastAPI also loads mlbops/.env via load_dotenv.
if [ -f mlbops/.env ]; then
  set -a
  # shellcheck disable=SC1091
  . mlbops/.env
  set +a
fi

export HUB_CORS_ORIGINS="${HUB_CORS_ORIGINS:-http://localhost:3000,http://127.0.0.1:3000}"
HUB_PORT="${MLBOPS_HUB_PORT:-3000}"
API_HOST="${MLBOPS_API_HOST:-0.0.0.0}"
HUB_HOST="${MLBOPS_HUB_HOST:-127.0.0.1}"
if [ "${MLBOPS_LOCAL_ONLY:-0}" = "1" ]; then
  API_HOST="127.0.0.1"
  HUB_HOST="127.0.0.1"
  export MLBOPS_STRICT_CORS="${MLBOPS_STRICT_CORS:-1}"
  export HUB_CORS_ORIGINS="${HUB_CORS_ORIGINS:-http://127.0.0.1:${HUB_PORT},http://localhost:${HUB_PORT}}"
fi
if [ "$HUB_PORT" != "3000" ]; then
  export HUB_CORS_ORIGINS="${HUB_CORS_ORIGINS},http://127.0.0.1:${HUB_PORT},http://localhost:${HUB_PORT}"
fi
# Allow hub UI to spawn morning_intel/morning_intel.py (disable in untrusted environments).
export MLBOPS_ALLOW_INTEL_RUN="${MLBOPS_ALLOW_INTEL_RUN:-1}"

echo "Starting mlbops…"

# FastAPI — cwd mlbops so package api resolves; inherits HUB_CORS_ORIGINS
mlb_env/bin/pip install -q -r mlbops/api/requirements-api.txt 2>/dev/null || true
# Default dev mode binds API to 0.0.0.0 so localhost/127.0.0.1 both work.
# Travel mode sets MLBOPS_LOCAL_ONLY=1 and binds API + hub to localhost only.
# Set MLBOPS_UVICORN_RELOAD=0 for a snappier API (no file watcher / extra process).
UVICORN_RELOAD_ARGS=(--reload)
if [ "${MLBOPS_UVICORN_RELOAD:-1}" = "0" ]; then
  UVICORN_RELOAD_ARGS=()
fi
(cd mlbops && ../mlb_env/bin/uvicorn api.main:app --host "$API_HOST" --port 8000 "${UVICORN_RELOAD_ARGS[@]}") &
FASTAPI_PID=$!
echo "  API   → http://127.0.0.1:8000  host $API_HOST  pid $FASTAPI_PID"

sleep 2

# If something else already owns this port, Next fails to bind but the script still
# told you to open :3000 — you then see the *other* app (e.g. another local hub).
if command -v lsof >/dev/null 2>&1; then
  if lsof -iTCP:"$HUB_PORT" -sTCP:LISTEN -P -n >/dev/null 2>&1; then
    echo ""
    echo "ERROR: Port $HUB_PORT is already in use. Another dev server is still running."
    echo "       Stop it first, or use: MLBOPS_HUB_PORT=3001 ./start_hub.sh"
    echo "       (CORS for that port is added automatically for non-3000 hubs.)"
    echo ""
    lsof -iTCP:"$HUB_PORT" -sTCP:LISTEN -P -n || true
    kill "$FASTAPI_PID" 2>/dev/null || true
    exit 1
  fi
fi

cd mlbops/hub
npm install --silent 2>/dev/null || true
echo "  Hub   → http://127.0.0.1:$HUB_PORT  host $HUB_HOST"
# On macOS with large repos, webpack/watchpack can hit EMFILE and drop route
# discovery; polling avoids file descriptor exhaustion in dev.
WATCHPACK_POLLING="${WATCHPACK_POLLING:-true}" \
npm run dev -- --hostname "$HUB_HOST" --port "$HUB_PORT" &
NEXTJS_PID=$!

echo ""
echo "Open http://127.0.0.1:$HUB_PORT — Ctrl+C stops both."
trap "kill $FASTAPI_PID $NEXTJS_PID 2>/dev/null; exit" INT TERM

wait
