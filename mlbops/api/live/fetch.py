"""MLB Stats API helpers for live-game polling.

Endpoints used:
  - GET  /api/v1/schedule?sportId=1&date=YYYY-MM-DD&hydrate=linescore,probablePitcher
  - GET  /api/v1.1/game/{gamePk}/feed/live

All calls use a short timeout so a single hung request cannot block the FastAPI
loop (callers should still wrap in `run_in_threadpool`).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

_STATS_V1 = "https://statsapi.mlb.com/api/v1"
_STATS_V11 = "https://statsapi.mlb.com/api/v1.1"

_TIMEOUT = 10  # seconds
_SESSION = requests.Session()
# Avoid inherited proxy env vars (HTTP_PROXY/HTTPS_PROXY) that can break
# local/dev calls to statsapi.mlb.com with tunnel 403s.
_SESSION.trust_env = False


def _get_json(url: str, *, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    r = _SESSION.get(url, params=params, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def today_et() -> str:
    """Local ET date — MLB schedule is ET-anchored."""
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def get_schedule(date: Optional[str] = None) -> dict[str, Any]:
    """All MLB games for `date` with linescore + probable pitchers.

    The linescore hydrate gives us inning + outs + score without a per-game
    feed call, which is useful for the live-games summary.
    """
    d = date or today_et()
    url = f"{_STATS_V1}/schedule"
    params = {
        "sportId": 1,
        "date": d,
        "hydrate": "linescore,probablePitcher,team",
    }
    return _get_json(url, params=params)


def list_games(date: Optional[str] = None) -> list[dict[str, Any]]:
    """Flatten schedule payload → list of game dicts."""
    data = get_schedule(date)
    dates = data.get("dates") or []
    out: list[dict[str, Any]] = []
    for d in dates:
        for g in d.get("games") or []:
            out.append(g)
    return out


def is_live_state(abstract: str, detailed: str) -> bool:
    a = (abstract or "").lower()
    d = (detailed or "").lower()
    if a == "live":
        return True
    # "Manager challenge", "Umpire review" surface as Live too; "Delayed: Rain"
    # uses detailed == 'Delayed Start: Rain' but abstract stays Preview.
    if "progress" in d or "delayed" in d and "start" not in d:
        return True
    return False


def is_final_state(abstract: str) -> bool:
    return (abstract or "").lower() == "final"


def get_live_feed(game_pk: int) -> dict[str, Any]:
    """Full /feed/live payload for a single game."""
    url = f"{_STATS_V11}/game/{int(game_pk)}/feed/live"
    return _get_json(url)


def get_live_feed_diff(game_pk: int, start_timecode: str) -> dict[str, Any]:
    """Delta since `start_timecode` (format: 'YYYYMMDD_HHMMSS' UTC).

    Not currently used by the scan loop (we re-read full feed and rely on the
    SQLite dedupe_key to avoid duplicates), but kept here for future polling
    that wants to reduce bandwidth on long games.
    """
    url = f"{_STATS_V11}/game/{int(game_pk)}/feed/live/diffPatch"
    return _get_json(url, params={"startTimecode": start_timecode})
