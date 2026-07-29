"""MLB Stats API data for a daily probable-starter comparison."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

from src.ingestion.boxscore_aggregate import innings_to_float
from src.probables_board.fetch import TEAM_ABBREV

BASE_URL = "https://statsapi.mlb.com/api/v1"
SPORT_ID = 1


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ip_from_outs(outs: int) -> str:
    return f"{outs // 3}.{outs % 3}"


def _time_et(game_date: str | None) -> str:
    if not game_date:
        return "TBD"
    try:
        dt = datetime.fromisoformat(game_date.replace("Z", "+00:00"))
        local = dt.astimezone(ZoneInfo("America/New_York"))
        return local.strftime("%-I:%M %p ET")
    except (TypeError, ValueError):
        return "TBD"


def fetch_schedule(date_str: str) -> list[dict]:
    response = requests.get(
        f"{BASE_URL}/schedule",
        params={
            "sportId": SPORT_ID,
            "date": date_str,
            "hydrate": "team,venue,probablePitcher",
        },
        timeout=25,
    )
    response.raise_for_status()
    games: list[dict] = []
    for day in response.json().get("dates") or []:
        games.extend(day.get("games") or [])
    return games


def _probable(side: dict) -> dict:
    pitcher = side.get("probablePitcher") or {}
    team = side.get("team") or {}
    team_id = _integer(team.get("id"))
    return {
        "id": _integer(pitcher.get("id")) or None,
        "name": pitcher.get("fullName") or "TBD",
        "team_id": team_id or None,
        "team": TEAM_ABBREV.get(team_id, (team.get("name") or "???")[:3].upper()),
    }


def schedule_matchups(date_str: str) -> list[dict]:
    matchups: list[dict] = []
    for game in fetch_schedule(date_str):
        teams = game.get("teams") or {}
        away = _probable(teams.get("away") or {})
        home = _probable(teams.get("home") or {})
        matchups.append(
            {
                "game_pk": _integer(game.get("gamePk")) or None,
                "date": date_str,
                "game_time": _time_et(game.get("gameDate")),
                "venue": (game.get("venue") or {}).get("name") or "",
                "rescheduled_from_date": game.get("rescheduledFromDate"),
                "description": game.get("description") or "",
                "away": away,
                "home": home,
            }
        )
    return matchups


def _season_stat(pid: int, season: int) -> dict:
    response = requests.get(
        f"{BASE_URL}/people/{pid}/stats",
        params={"stats": "season", "group": "pitching", "season": season},
        timeout=20,
    )
    response.raise_for_status()
    for block in response.json().get("stats") or []:
        for split in block.get("splits") or []:
            stat = split.get("stat") or {}
            if stat:
                return stat
    return {}


def _game_log(pid: int, season: int, before_date: str) -> list[dict]:
    response = requests.get(
        f"{BASE_URL}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "pitching", "season": season},
        timeout=20,
    )
    response.raise_for_status()
    rows: list[dict] = []
    for block in response.json().get("stats") or []:
        for split in block.get("splits") or []:
            if str(split.get("gameType") or "").upper() != "R":
                continue
            if str(split.get("date") or "") >= before_date:
                continue
            stat = split.get("stat") or {}
            rows.append(
                {
                    "date": str(split.get("date") or ""),
                    "opponent": (split.get("opponent") or {}).get("name") or "",
                    "opponent_team": TEAM_ABBREV.get(
                        _integer((split.get("opponent") or {}).get("id")),
                        ((split.get("opponent") or {}).get("name") or "???")[:3].upper(),
                    ),
                    "is_home": bool(split.get("isHome")),
                    "game_pk": _integer((split.get("game") or {}).get("gamePk")) or None,
                    "games_started": _integer(stat.get("gamesStarted")),
                    "wins": _integer(stat.get("wins")),
                    "losses": _integer(stat.get("losses")),
                    "outs": _integer(stat.get("outs"))
                    or round(innings_to_float(str(stat.get("inningsPitched") or "0")) * 3),
                    "hits": _integer(stat.get("hits")),
                    "earned_runs": _integer(stat.get("earnedRuns")),
                    "walks": _integer(stat.get("baseOnBalls")),
                    "strikeouts": _integer(stat.get("strikeOuts")),
                    "batters_faced": _integer(stat.get("battersFaced")),
                    "pitches": _integer(stat.get("numberOfPitches")),
                }
            )
    return sorted(rows, key=lambda row: row["date"])


def _aggregate(rows: list[dict]) -> dict:
    outs = sum(_integer(row.get("outs")) for row in rows)
    hits = sum(_integer(row.get("hits")) for row in rows)
    earned_runs = sum(_integer(row.get("earned_runs")) for row in rows)
    walks = sum(_integer(row.get("walks")) for row in rows)
    strikeouts = sum(_integer(row.get("strikeouts")) for row in rows)
    batters_faced = sum(_integer(row.get("batters_faced")) for row in rows)
    wins = sum(_integer(row.get("wins")) for row in rows)
    losses = sum(_integer(row.get("losses")) for row in rows)
    return {
        "starts": sum(_integer(row.get("games_started"), 1) for row in rows),
        "outs": outs,
        "ip": _ip_from_outs(outs),
        "era": earned_runs * 27 / outs if outs else None,
        "whip": (hits + walks) * 3 / outs if outs else None,
        "k_bb_pct": (strikeouts - walks) * 100 / batters_faced if batters_faced else None,
        "strikeouts": strikeouts,
        "walks": walks,
        "earned_runs": earned_runs,
        "wins": wins,
        "losses": losses,
    }


def _rolling_era(rows: list[dict], window: int = 3) -> list[float]:
    values: list[float] = []
    for index in range(window - 1, len(rows)):
        summary = _aggregate(rows[index - window + 1 : index + 1])
        era = summary.get("era")
        if era is not None:
            values.append(round(float(era), 2))
    return values[-8:]


def fetch_pitcher_profile(
    pitcher: dict,
    *,
    season: int,
    before_date: str,
) -> dict:
    pid = _integer(pitcher.get("id"))
    if not pid:
        raise ValueError("A probable pitcher ID is required.")
    logs = _game_log(pid, season, before_date)
    start_logs = [row for row in logs if _integer(row.get("games_started")) >= 1]
    recent_rows = start_logs[-3:]
    season_summary = _aggregate(logs)
    profile = dict(pitcher)
    profile["season"] = {
        "record": f"{season_summary['wins']}-{season_summary['losses']}",
        "era": season_summary["era"],
        "whip": season_summary["whip"],
        "k_bb_pct": season_summary["k_bb_pct"],
        "ip": season_summary["ip"],
        "starts": season_summary["starts"],
        "strikeouts": season_summary["strikeouts"],
        "walks": season_summary["walks"],
    }
    profile["recent"] = _aggregate(recent_rows)
    profile["recent_outings"] = recent_rows
    profile["rolling_era"] = _rolling_era(start_logs)
    return profile


def _era_for_selection(pid: int, season: int) -> float:
    stat = _season_stat(pid, season)
    era = _number(stat.get("era"), 99.0)
    starts = _integer(stat.get("gamesStarted"))
    return era + (0.75 if starts < 5 else 0.0)


def choose_showdown_game(
    date_str: str,
    *,
    away_pitcher_id: int | None = None,
    home_pitcher_id: int | None = None,
    excluded_pairs: set[frozenset[str]] | None = None,
) -> dict:
    """Choose a complete matchup; explicit IDs override the quality ranking."""
    matchups = schedule_matchups(date_str)
    if away_pitcher_id and home_pitcher_id:
        for matchup in matchups:
            if (
                matchup["away"].get("id") == away_pitcher_id
                and matchup["home"].get("id") == home_pitcher_id
            ):
                return matchup
        raise ValueError("Requested probable-pitcher pairing is not on the selected date.")

    complete = [
        matchup
        for matchup in matchups
        if matchup["away"].get("id") and matchup["home"].get("id")
    ]
    excluded_pairs = excluded_pairs or set()
    eligible = [
        matchup
        for matchup in complete
        if frozenset(
            (
                str(matchup["away"]["name"]).strip().casefold(),
                str(matchup["home"]["name"]).strip().casefold(),
            )
        )
        not in excluded_pairs
    ]
    if eligible:
        complete = eligible
    if not complete:
        raise ValueError(f"No complete probable-pitcher matchup for {date_str}.")
    season = int(date_str[:4])
    scored: list[tuple[float, dict]] = []
    for matchup in complete:
        away_era = _era_for_selection(int(matchup["away"]["id"]), season)
        home_era = _era_for_selection(int(matchup["home"]["id"]), season)
        pair_score = max(away_era, home_era) * 0.65 + (away_era + home_era) * 0.175
        scored.append((pair_score, matchup))
    return min(scored, key=lambda item: item[0])[1]


def build_showdown(
    date_str: str,
    *,
    away_pitcher_id: int | None = None,
    home_pitcher_id: int | None = None,
    excluded_pairs: set[frozenset[str]] | None = None,
) -> dict:
    matchup = choose_showdown_game(
        date_str,
        away_pitcher_id=away_pitcher_id,
        home_pitcher_id=home_pitcher_id,
        excluded_pairs=excluded_pairs,
    )
    season = int(date_str[:4])
    return {
        **matchup,
        "away": fetch_pitcher_profile(
            matchup["away"],
            season=season,
            before_date=date_str,
        ),
        "home": fetch_pitcher_profile(
            matchup["home"],
            season=season,
            before_date=date_str,
        ),
    }
