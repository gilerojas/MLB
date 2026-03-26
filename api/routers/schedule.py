"""
Schedule endpoints — proxy MLB Stats API schedule.

GET /schedule/today         — today's games
GET /schedule/{date}        — games for a specific date (YYYY-MM-DD)
"""
from datetime import date, datetime

import requests
from fastapi import APIRouter, HTTPException

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
                "hydrate": "team,venue,linescore",
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
            games.append({
                "game_pk": g.get("gamePk"),
                "game_date": g.get("gameDate"),
                "status": g.get("status", {}).get("detailedState"),
                "away_team": teams.get("away", {}).get("team", {}).get("name"),
                "away_team_id": teams.get("away", {}).get("team", {}).get("id"),
                "away_score": teams.get("away", {}).get("score"),
                "home_team": teams.get("home", {}).get("team", {}).get("name"),
                "home_team_id": teams.get("home", {}).get("team", {}).get("id"),
                "home_score": teams.get("home", {}).get("score"),
                "venue": g.get("venue", {}).get("name"),
                "game_type": g.get("gameType"),
            })
    return games


@router.get("/today")
def get_today():
    today = date.today().strftime("%Y-%m-%d")
    raw = _fetch_schedule(today)
    return {"date": today, "games": _parse_games(raw)}


@router.get("/{game_date}")
def get_by_date(game_date: str):
    try:
        datetime.strptime(game_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD format.")
    raw = _fetch_schedule(game_date)
    return {"date": game_date, "games": _parse_games(raw)}
