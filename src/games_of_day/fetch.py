"""
MLB schedule for a calendar day → rows for the Games of Day board.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import requests

BASE_URL = "https://statsapi.mlb.com/api/v1"
SPORT_ID_MLB = 1

TEAM_ABBREV: dict[int, str] = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC", 113: "CIN",
    114: "CLE", 115: "COL", 116: "DET", 117: "HOU", 118: "KC", 119: "LAD",
    120: "WSH", 121: "NYM", 133: "OAK", 134: "PIT", 135: "SD", 136: "SEA",
    137: "SF", 138: "STL", 139: "TB", 140: "TEX", 141: "TOR", 142: "MIN",
    143: "PHI", 144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}


def _abbrev_team(team: dict) -> str:
    tid = (team or {}).get("id")
    if tid in TEAM_ABBREV:
        return TEAM_ABBREV[tid]
    name = (team or {}).get("name") or "???"
    return name[:3].upper()


def _game_time_et_12h_short(game_date_iso: str | None) -> str:
    """ET 12h, no ' ET' suffix (matches probables card chips)."""
    if not game_date_iso:
        return "TBD"
    try:
        dt = datetime.fromisoformat(game_date_iso.replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        h, m = et.hour, et.minute
        if h == 0:
            return f"12:{m:02d}a"
        if h < 12:
            return f"{h}:{m:02d}a"
        if h == 12:
            return f"12:{m:02d}p"
        return f"{h - 12}:{m:02d}p"
    except Exception:
        return "TBD"


def fetch_schedule_games(date_str: str) -> list[dict]:
    r = requests.get(
        f"{BASE_URL}/schedule",
        params={"sportId": SPORT_ID_MLB, "date": date_str},
        timeout=25,
    )
    r.raise_for_status()
    games: list[dict] = []
    for d in r.json().get("dates", []):
        games.extend(d.get("games", []))
    return games


def build_game_rows_for_date(date_str: str) -> list[dict]:
    """Sorted games with time_et, away_abbr, home_abbr."""
    games = fetch_schedule_games(date_str)
    games = sorted(games, key=lambda g: (g.get("gameDate") or ""))
    rows: list[dict] = []
    for g in games:
        teams = g.get("teams") or {}
        away = teams.get("away") or {}
        home = teams.get("home") or {}
        rows.append(
            {
                "time_et": _game_time_et_12h_short(g.get("gameDate")),
                "away_abbr": _abbrev_team(away.get("team") or {}),
                "home_abbr": _abbrev_team(home.get("team") or {}),
            }
        )
    return rows
