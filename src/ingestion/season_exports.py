"""
Season-level CSV exports for warehouse / Drive: standings, team stats, player boxscore splits.
"""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .boxscore_aggregate import aggregate_boxscore_by_player_team
from .mlb_warehouse_schema import BASE_URL

STANDINGS_URL = f"{BASE_URL}/standings"


def _get_json(url: str, timeout: float = 60.0) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "MallitalyticsWarehouse/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_standings_regular_season(season: int) -> list[dict[str, Any]]:
    """Flat rows for team standings (AL/NL divisions + hydrated team/league/division)."""
    params = (
        f"leagueId=103,104&season={season}&standingsTypes=regularSeason"
        f"&sportId=1&hydrate=division,league,team"
    )
    data = _get_json(f"{STANDINGS_URL}?{params}")
    rows: list[dict[str, Any]] = []
    for rec in data.get("records") or []:
        div = rec.get("division") or {}
        league = rec.get("league") or {}
        for tr in rec.get("teamRecords") or []:
            team = tr.get("team") or {}
            lr = tr.get("leagueRecord") or {}
            streak = tr.get("streak") or {}
            rows.append(
                {
                    "season": season,
                    "team_id": team.get("id"),
                    "team_abbrev": team.get("abbreviation"),
                    "team_name": team.get("name"),
                    "league_id": league.get("id"),
                    "league_name": league.get("name"),
                    "division_id": div.get("id"),
                    "division_name": div.get("name"),
                    "games_played": tr.get("gamesPlayed"),
                    "wins": lr.get("wins"),
                    "losses": lr.get("losses"),
                    "ties": lr.get("ties"),
                    "pct": lr.get("pct"),
                    "division_rank": tr.get("divisionRank"),
                    "league_rank": tr.get("leagueRank"),
                    "sport_rank": tr.get("sportRank"),
                    "run_differential": tr.get("runDifferential"),
                    "runs_scored": tr.get("runsScored"),
                    "runs_allowed": tr.get("runsAllowed"),
                    "wild_card_games_back": tr.get("wildCardGamesBack"),
                    "division_games_back": tr.get("divisionGamesBack"),
                    "games_back": tr.get("gamesBack"),
                    "streak_code": streak.get("streakCode"),
                    "last_updated": tr.get("lastUpdated"),
                }
            )
    return rows


def _team_stat_row(season: int, group: str, payload: dict[str, Any]) -> dict[str, Any]:
    team = payload.get("team") or {}
    stat = payload.get("stat") or {}
    row: dict[str, Any] = {
        "season": season,
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "stat_group": group,
    }
    for k, v in stat.items():
        row[f"stat_{k}"] = v
    return row


def fetch_team_season_stats_rows(
    season: int,
    team_ids: list[int],
    *,
    disable_progress: bool = False,
    existing_rows: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """
    Hitting + pitching season totals per team (Stats API).

    If ``existing_rows`` is set, skips (team_id, stat_group) pairs already present
    (for resume after interrupt). Pass a copy if you mutate the list elsewhere.

    Returns ``(rows, completed)``; ``completed`` is False if the user interrupted with Ctrl+C.
    """
    rows: list[dict[str, Any]] = list(existing_rows) if existing_rows else []
    done: set[tuple[int, str]] = set()
    for r in rows:
        tid, g = r.get("team_id"), r.get("stat_group")
        if tid is not None and g:
            try:
                done.add((int(tid), str(g)))
            except (TypeError, ValueError):
                pass

    it_ids = team_ids
    if not disable_progress:
        from tqdm import tqdm

        it_ids = tqdm(team_ids, desc="Team season stats (API)", unit="team")

    try:
        for tid in it_ids:
            for group in ("hitting", "pitching"):
                if (int(tid), group) in done:
                    continue
                url = f"{BASE_URL}/teams/{tid}/stats?season={season}&group={group}&stats=season"
                try:
                    data = _get_json(url, timeout=45.0)
                except (HTTPError, URLError, TimeoutError, OSError) as e:
                    rows.append(
                        {
                            "season": season,
                            "team_id": tid,
                            "team_name": None,
                            "stat_group": group,
                            "_error": str(e),
                        }
                    )
                    done.add((int(tid), group))
                    continue
                stats = data.get("stats") or []
                if not stats:
                    done.add((int(tid), group))
                    continue
                splits = (stats[0].get("splits") or [])[:1]
                if not splits:
                    done.add((int(tid), group))
                    continue
                rows.append(_team_stat_row(season, group, splits[0]))
                done.add((int(tid), group))
            time.sleep(0.15)
    except KeyboardInterrupt:
        return rows, False
    return rows, True


def _batting_derived(agg: dict[str, Any]) -> dict[str, Any]:
    ab = int(agg.get("atBats") or 0)
    h = int(agg.get("hits") or 0)
    tb = int(agg.get("totalBases") or 0)
    bb = int(agg.get("baseOnBalls") or 0)
    hbp = int(agg.get("hitByPitch") or 0)
    sf = int(agg.get("sacFlies") or 0)
    avg = (h / ab) if ab else 0.0
    obp_denom = ab + bb + hbp + sf
    obp = (h + bb + hbp) / obp_denom if obp_denom > 0 else 0.0
    slg = tb / ab if ab > 0 else 0.0
    return {"avg": round(avg, 4), "obp": round(obp, 4), "slg": round(slg, 4), "ops": round(obp + slg, 4)}


def _pitching_derived(agg: dict[str, Any]) -> dict[str, Any]:
    ip = float(agg.get("ip") or 0.0)
    er = int(agg.get("earnedRuns") or 0)
    era = (er * 9.0 / ip) if ip > 0 else 0.0
    h = int(agg.get("hits") or 0)
    bb = int(agg.get("baseOnBalls") or 0)
    whip = ((h + bb) / ip) if ip > 0 else 0.0
    k = int(agg.get("strikeOuts") or 0)
    k9 = (k * 9.0 / ip) if ip > 0 else 0.0
    return {
        "era": round(era, 3),
        "whip": round(whip, 3),
        "k_per_9": round(k9, 3),
        "ip_outs": round(ip * 3, 2),
    }


def player_team_boxscore_rows(
    warehouse: Path,
    season: int,
    stage: str,
    *,
    disable_progress: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    bat_map, pit_map = aggregate_boxscore_by_player_team(
        warehouse, season, stage, disable_progress=disable_progress
    )
    bat_rows: list[dict[str, Any]] = []
    for (pid, tid), agg in sorted(bat_map.items(), key=lambda x: (x[0][1], x[0][0])):
        d = _batting_derived(agg)
        bat_rows.append(
            {
                "player_id": int(pid),
                "player_name": agg.get("name"),
                "team_id": tid,
                "team_abbrev": agg.get("team_abbrev"),
                "at_bats": agg.get("atBats"),
                "hits": agg.get("hits"),
                "doubles": agg.get("doubles"),
                "triples": agg.get("triples"),
                "home_runs": agg.get("homeRuns"),
                "rbi": agg.get("rbi"),
                "runs": agg.get("runs"),
                "walks": agg.get("baseOnBalls"),
                "strikeouts": agg.get("strikeOuts"),
                "hbp": agg.get("hitByPitch"),
                "sac_flies": agg.get("sacFlies"),
                "stolen_bases": agg.get("stolenBases"),
                "plate_appearances": agg.get("plateAppearances"),
                "total_bases": agg.get("totalBases"),
                **d,
            }
        )
    pit_rows: list[dict[str, Any]] = []
    for (pid, tid), agg in sorted(pit_map.items(), key=lambda x: (x[0][1], x[0][0])):
        d = _pitching_derived(agg)
        pit_rows.append(
            {
                "player_id": int(pid),
                "player_name": agg.get("name"),
                "team_id": tid,
                "team_abbrev": agg.get("team_abbrev"),
                "ip": round(float(agg.get("ip") or 0), 3),
                "earned_runs": agg.get("earnedRuns"),
                "runs": agg.get("runs"),
                "hits": agg.get("hits"),
                "walks": agg.get("baseOnBalls"),
                "strikeouts": agg.get("strikeOuts"),
                "batters_faced": agg.get("battersFaced"),
                "home_runs": agg.get("homeRuns"),
                "hbp": agg.get("hitByPitch"),
                "games": int(agg.get("games") or 0),
                "games_started": int(agg.get("gamesStarted") or 0),
                **d,
            }
        )
    return bat_rows, pit_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    for r in rows[1:]:
        for k in r:
            if k not in fieldnames:
                fieldnames.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
