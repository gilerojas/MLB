#!/usr/bin/env bash
# One command to start mlbops: FastAPI (8000) + Next hub (3000).
#
# New day — from MLB repo root:
#   ./scripts/start_mlbops.sh
#
# Optional first (fresh Drive mirror):
#   ./scripts/pull_mlbops_from_drive.sh
#
# Requires: ./mlb_env with FastAPI deps, Node/npm for mlbops/hub

exec "$(cd "$(dirname "$0")/.." && pwd)/start_hub.sh"
