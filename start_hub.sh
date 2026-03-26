#!/bin/bash
# Mallitalytics Hub — start both servers
# Usage: ./start_hub.sh
# Requires: mlb_env activated for FastAPI, node for Next.js

set -e
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

echo "Starting Mallitalytics Hub..."

# Start FastAPI (background)
mlb_env/bin/pip install -q -r api/requirements-api.txt 2>/dev/null || true
mlb_env/bin/uvicorn api.main:app --port 8000 --reload &
FASTAPI_PID=$!
echo "  FastAPI  → http://localhost:8000  (pid $FASTAPI_PID)"

# Wait a moment for FastAPI to boot
sleep 2

# Start Next.js hub (foreground — so Ctrl+C stops both)
cd hub
npm install --silent 2>/dev/null || true
echo "  Next.js  → http://localhost:3000"
npm run dev &
NEXTJS_PID=$!

echo ""
echo "Both servers running. Press Ctrl+C to stop."

# On exit, kill both
trap "kill $FASTAPI_PID $NEXTJS_PID 2>/dev/null; exit" INT TERM

wait
