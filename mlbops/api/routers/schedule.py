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


def _today_payload() -> dict:
    today = date.today().strftime("%Y-%m-%d")
    raw = _fetch_schedule(today)
    return {"date": today, "games": _parse_games(raw)}


@router.get("/today")
async def get_today():
    return await run_in_threadpool(_today_payload)


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
