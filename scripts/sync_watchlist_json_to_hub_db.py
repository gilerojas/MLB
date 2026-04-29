#!/usr/bin/env python3
"""
After editing jobs/player_watchlist.json by hand, push the same rows into hub.db
so GET /watchlist and the Hub UI match morning_intel (which reads the JSON file).

Usage (repo root):

  python scripts/sync_watchlist_json_to_hub_db.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "mlbops"))

from api.db.database import write_watchlist_json_and_sync_db  # noqa: E402


def main() -> int:
    path = ROOT / "jobs" / "player_watchlist.json"
    players = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(players, list):
        print("player_watchlist.json must be a JSON array", file=sys.stderr)
        return 1
    write_watchlist_json_and_sync_db(players)
    print(f"Synced {len(players)} players to hub.db + {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
