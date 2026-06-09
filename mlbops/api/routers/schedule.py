"""
Schedule endpoints — proxy MLB Stats API schedule.

GET /schedule/today         — today's games
GET /schedule/{date}        — games for a specific date (YYYY-MM-DD)
"""
from datetime import date, datetime

import requests
from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

router = APIRouter(prefix="/schedule", tags=["schedule"])

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
MLB_BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
MLB_LINESCORE_URL = "https://statsapi.mlb.com/api/v1/game/{game_pk}/linescore"
SPORT_ID = 1  # MLB


def _fetch_schedule(game_date: str) -> dict:
    try:
        resp = requests.get(
            MLB_SCHEDULE_URL,
            params={
                "sportId": SPORT_ID,
                "date": game_date,
                "hydrate": "team,venue,linescore,probablePitcher",
            },
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"MLB API error: {e}")


def _parse_games(raw: dict) -> list[dict]:
    games = []
    for day in raw.get("dates", []):
        for g in day.get("games", []):
            teams = g.get("teams", {})
            away = teams.get("away", {})
            home = teams.get("home", {})

            away_rec = away.get("leagueRecord", {})
            home_rec = home.get("leagueRecord", {})

            away_prob = away.get("probablePitcher", {})
            home_prob = home.get("probablePitcher", {})

            # scheduled start time (UTC ISO → show as-is; front end can format)
            game_time = g.get("gameDate")  # full ISO datetime

            games.append({
                "game_pk": g.get("gamePk"),
                "game_date": game_time,
                "status": g.get("status", {}).get("detailedState"),
                "away_team": away.get("team", {}).get("name"),
                "away_team_id": away.get("team", {}).get("id"),
                "away_score": away.get("score"),
                "away_wins": away_rec.get("wins"),
                "away_losses": away_rec.get("losses"),
                "away_probable": away_prob.get("fullName"),
                "home_team": home.get("team", {}).get("name"),
                "home_team_id": home.get("team", {}).get("id"),
                "home_score": home.get("score"),
                "home_wins": home_rec.get("wins"),
                "home_losses": home_rec.get("losses"),
                "home_probable": home_prob.get("fullName"),
                "venue": g.get("venue", {}).get("name"),
                "game_type": g.get("gameType"),
            })
    return games


def _bat_line(bat: dict) -> str:
    ab = int(bat.get("atBats") or 0)
    h = int(bat.get("hits") or 0)
    hr = int(bat.get("homeRuns") or 0)
    rbi = int(bat.get("rbi") or 0)
    r = int(bat.get("runs") or 0)
    bb = int(bat.get("baseOnBalls") or 0)
    return f"{ab}-{h} · {hr} HR · {rbi} RBI · {r} R · {bb} BB"


def _pitch_line(pit: dict) -> str:
    ip = pit.get("inningsPitched", "")
    k = int(pit.get("strikeOuts") or 0)
    h = int(pit.get("hits") or 0)
    er = int(pit.get("earnedRuns") or 0)
    bb = int(pit.get("baseOnBalls") or 0)
    return f"{ip} IP · {k} K · {h} H · {bb} BB · {er} ER"


def _ip_sort_key(ip: object) -> float:
    s = str(ip or "0")
    if "." not in s:
        return float(s)
    whole, frac = s.split(".", 1)
    return int(whole or 0) + int(frac or 0) / 3.0


def _player_lines(teams: dict, side: str, stat_key: str, *, min_ab: int = 1) -> list[dict]:
    team = teams.get(side) or {}
    players = team.get("players") or {}
    rows: list[dict] = []
    for player in players.values():
        if not isinstance(player, dict):
            continue
        stats = (player.get("stats") or {}).get(stat_key) or {}
        if stat_key == "batting":
            if int(stats.get("atBats") or 0) < min_ab:
                continue
            sort_key = int(stats.get("hits") or 0)
        else:
            ip = stats.get("inningsPitched")
            if not ip or str(ip) in ("0.0", "0"):
                continue
            sort_key = _ip_sort_key(ip)
        person = player.get("person") or {}
        name = person.get("fullName") or ""
        if not name:
            continue
        line_fn = _bat_line if stat_key == "batting" else _pitch_line
        rows.append({"name": name, "line": line_fn(stats), "sort": sort_key})
    rows.sort(key=lambda r: r["sort"], reverse=True)
    return [{"name": r["name"], "line": r["line"]} for r in rows[:8]]


def _fetch_boxscore(game_pk: int) -> dict:
    try:
        box_resp = requests.get(MLB_BOXSCORE_URL.format(game_pk=game_pk), timeout=12)
        box_resp.raise_for_status()
        line_resp = requests.get(MLB_LINESCORE_URL.format(game_pk=game_pk), timeout=12)
        line_resp.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"MLB API error: {e}")

    box = box_resp.json()
    linescore = line_resp.json()
    teams = box.get("teams") or {}
    away_team = (teams.get("away") or {}).get("team") or {}
    home_team = (teams.get("home") or {}).get("team") or {}

    innings_raw = linescore.get("innings") or []
    innings = [
        {
            "num": inn.get("num"),
            "away": (inn.get("away") or {}).get("runs"),
            "home": (inn.get("home") or {}).get("runs"),
        }
        for inn in innings_raw
    ]

    ls_teams = linescore.get("teams") or {}
    away_totals = ls_teams.get("away") or {}
    home_totals = ls_teams.get("home") or {}

    return {
        "game_pk": game_pk,
        "status": linescore.get("currentInningOrdinal") or box.get("status", {}).get("detailedState"),
        "detailed_state": box.get("status", {}).get("detailedState"),
        "away_team": away_team.get("name"),
        "home_team": home_team.get("name"),
        "away_abbrev": away_team.get("abbreviation"),
        "home_abbrev": home_team.get("abbreviation"),
        "linescore": {
            "innings": innings,
            "away": {
                "runs": away_totals.get("runs"),
                "hits": away_totals.get("hits"),
                "errors": away_totals.get("errors"),
            },
            "home": {
                "runs": home_totals.get("runs"),
                "hits": home_totals.get("hits"),
                "errors": home_totals.get("errors"),
            },
        },
        "away_batting": _player_lines(teams, "away", "batting"),
        "home_batting": _player_lines(teams, "home", "batting"),
        "away_pitching": _player_lines(teams, "away", "pitching", min_ab=0),
        "home_pitching": _player_lines(teams, "home", "pitching", min_ab=0),
    }


def _today_payload() -> dict:
    today = date.today().strftime("%Y-%m-%d")
    raw = _fetch_schedule(today)
    return {"date": today, "games": _parse_games(raw)}


@router.get("/today")
async def get_today():
    return await run_in_threadpool(_today_payload)


@router.get("/boxscore/{game_pk}")
async def get_boxscore(game_pk: int):
    if game_pk <= 0:
        raise HTTPException(status_code=400, detail="Invalid game_pk.")
    return await run_in_threadpool(_fetch_boxscore, game_pk)


def _by_date_payload(game_date: str) -> dict:
    raw = _fetch_schedule(game_date)
    return {"date": game_date, "games": _parse_games(raw)}


@router.get("/{game_date}")
async def get_by_date(game_date: str):
    try:
        datetime.strptime(game_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD format.")
    return await run_in_threadpool(_by_date_payload, game_date)
