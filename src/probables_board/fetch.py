"""
Schedule probables + season W-L / ERA from MLB Stats API.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from src.hr_tracker.name_display import last_name_with_generational_suffix

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


def _game_time_et_12h(game_date_iso: str | None) -> str:
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
        params={
            "sportId": SPORT_ID_MLB,
            "date": date_str,
            "hydrate": "team,probablePitcher",
        },
        timeout=25,
    )
    r.raise_for_status()
    games: list[dict] = []
    for d in r.json().get("dates", []):
        games.extend(d.get("games", []))
    return games


def _parse_pitching_season_split(person: dict) -> tuple[int, int, str] | None:
    for block in person.get("stats") or []:
        g = (block.get("group") or {}).get("displayName", "")
        t = (block.get("type") or {}).get("displayName", "")
        if g != "pitching" or t != "season":
            continue
        for sp in block.get("splits") or []:
            st = sp.get("stat") or {}
            w = st.get("wins")
            l = st.get("losses")
            era = st.get("era")
            if w is None or l is None:
                continue
            era_s = str(era) if era is not None else "—"
            return (int(w), int(l), era_s)
    return None


def batch_pitching_season_stats(person_ids: list[int], season: int) -> dict[int, tuple[int, int, str]]:
    """Map personId -> (wins, losses, era string). Missing -> not in dict."""
    out: dict[int, tuple[int, int, str]] = {}
    if not person_ids:
        return out
    chunk_size = 45
    hydrate = f"stats(group=[pitching],type=[season],season={season},sportId=1)"
    for i in range(0, len(person_ids), chunk_size):
        chunk = person_ids[i : i + chunk_size]
        ids_s = ",".join(str(x) for x in chunk)
        r = requests.get(
            f"{BASE_URL}/people",
            params={"personIds": ids_s, "hydrate": hydrate},
            timeout=30,
        )
        r.raise_for_status()
        for person in r.json().get("people", []) or []:
            pid = person.get("id")
            if pid is None:
                continue
            parsed = _parse_pitching_season_split(person)
            if parsed:
                out[int(pid)] = parsed
    return out


def _pitcher_info(prob: dict, stats_map: dict[int, tuple[int, int, str]]) -> dict:
    if not prob or not prob.get("id"):
        return {
            "id": None,
            "name": "TBD",
            "wins": None,
            "losses": None,
            "era": "—",
            "record": "—",
            "summary": "TBD",
        }

    pid = int(prob["id"])
    name = last_name_with_generational_suffix(prob.get("fullName") or "?")
    tup = stats_map.get(pid)
    wins: int | None = None
    losses: int | None = None
    era = "—"
    if tup:
        wins, losses, era = tup
    record = f"{wins}-{losses}" if wins is not None and losses is not None else "—"
    return {
        "id": pid,
        "name": name,
        "wins": wins,
        "losses": losses,
        "era": era,
        "record": record,
        "summary": f"{name}  {record}  {era}" if record != "—" else f"{name}  —  —",
    }


def build_probable_rows_for_date(date_str: str) -> list[dict]:
    """One row per scheduled game, sorted by first pitch."""
    season = int(date_str[:4])
    games = fetch_schedule_games(date_str)
    games = sorted(games, key=lambda g: g.get("gameDate") or "")

    ids: list[int] = []
    for g in games:
        teams = g.get("teams") or {}
        for side in ("away", "home"):
            prob = (teams.get(side) or {}).get("probablePitcher") or {}
            pid = prob.get("id")
            if pid is not None:
                ids.append(int(pid))
    stats_map = batch_pitching_season_stats(list(dict.fromkeys(ids)), season)

    rows: list[dict] = []
    for g in games:
        teams = g.get("teams") or {}
        away = teams.get("away") or {}
        home = teams.get("home") or {}
        away_prob = away.get("probablePitcher") or {}
        home_prob = home.get("probablePitcher") or {}
        away_pitcher = _pitcher_info(away_prob, stats_map)
        home_pitcher = _pitcher_info(home_prob, stats_map)
        ateam = away.get("team") or {}
        hteam = home.get("team") or {}
        away_tid = ateam.get("id")
        home_tid = hteam.get("id")
        rows.append(
            {
                "time_et": _game_time_et_12h(g.get("gameDate")),
                "away_abbr": _abbrev_team(ateam),
                "home_abbr": _abbrev_team(hteam),
                "away_team_id": int(away_tid) if away_tid is not None else None,
                "home_team_id": int(home_tid) if home_tid is not None else None,
                "away_pitcher": away_pitcher,
                "home_pitcher": home_pitcher,
                "away_line": away_pitcher["summary"],
                "home_line": home_pitcher["summary"],
            }
        )
    return rows
