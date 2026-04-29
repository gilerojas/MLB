"""
Aggregate boxscore batting/pitching from warehouse raw feed_live files.
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path
from typing import Any


def open_raw_feed(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def innings_to_float(ip_str: str) -> float:
    if not ip_str or ip_str in (".--", "-.--"):
        return 0.0
    s = str(ip_str).strip()
    if "." not in s:
        try:
            return float(s)
        except ValueError:
            return 0.0
    a, b = s.split(".", 1)
    try:
        whole = int(a)
        partial = int(b)
    except ValueError:
        return 0.0
    return whole + partial / 3.0


def find_stage_raw_paths(warehouse: Path, season: int, stage: str) -> list[Path]:
    base = warehouse / str(season) / stage / "raw"
    if not base.exists():
        return []
    by_key: dict[str, Path] = {}
    for raw_path in base.glob("game_*_feed_live.json*"):
        if not (
            raw_path.name.endswith(".json") or raw_path.name.endswith(".json.gz")
        ):
            continue
        name = raw_path.name
        stem = name[:-7] if name.endswith(".json.gz") else name[:-5]
        m = re.match(r"game_(\d+)_(\d+)_feed_live", stem)
        if not m:
            continue
        key = m.group(1) + "_" + m.group(2)
        if key not in by_key or (
            raw_path.name.endswith(".json") and not raw_path.name.endswith(".json.gz")
        ):
            by_key[key] = raw_path
    return sorted(by_key.values())


def _normalize_player_id(player_key: str) -> str | None:
    s = str(player_key).strip()
    if s.startswith("ID"):
        s = s[2:]
    try:
        return str(int(s))
    except ValueError:
        return None


BAT_KEYS = [
    "atBats",
    "hits",
    "homeRuns",
    "rbi",
    "runs",
    "strikeOuts",
    "baseOnBalls",
    "hitByPitch",
    "sacFlies",
    "doubles",
    "triples",
    "plateAppearances",
    "totalBases",
    "stolenBases",
]

PITCH_SUM_KEYS = [
    "strikeOuts",
    "earnedRuns",
    "runs",
    "hits",
    "baseOnBalls",
    "battersFaced",
    "homeRuns",
    "hitByPitch",
]


def aggregate_boxscore_from_raw(
    warehouse: Path, season: int, stage: str
) -> tuple[dict[str, dict], dict[str, dict]]:
    paths = find_stage_raw_paths(warehouse, season, stage)
    batting_totals: dict[str, dict] = {}
    pitching_totals: dict[str, dict] = {}

    for raw_path in paths:
        try:
            with open_raw_feed(raw_path) as f:
                feed = json.load(f)
        except Exception:
            continue

        game_data = feed.get("gameData", {})
        teams = game_data.get("teams", {})
        away = teams.get("away") or {}
        home = teams.get("home") or {}
        away_abbrev = away.get("abbreviation") or "?"
        home_abbrev = home.get("abbreviation") or "?"
        away_id = away.get("id")
        home_id = home.get("id")

        box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
        for side, abbrev, tid in [
            ("away", away_abbrev, away_id),
            ("home", home_abbrev, home_id),
        ]:
            players = (box.get(side) or {}).get("players") or {}
            for player_id, p in players.items():
                person = (p.get("person") or {})
                full_name = person.get("fullName") or f"ID {player_id}"
                stats = p.get("stats") or {}

                bat = stats.get("batting") or {}
                if bat and (bat.get("atBats") or 0) > 0:
                    agg = batting_totals.setdefault(
                        player_id,
                        {k: 0 for k in BAT_KEYS} | {"name": full_name, "team": abbrev},
                    )
                    for k in BAT_KEYS:
                        v = bat.get(k)
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                    agg["name"] = full_name
                    agg["team"] = abbrev
                    agg["team_id"] = tid

                pit = stats.get("pitching") or {}
                if pit and (pit.get("inningsPitched") or pit.get("outs", 0)):
                    ip_val = pit.get("inningsPitched")
                    if isinstance(ip_val, str):
                        ip_float = innings_to_float(ip_val)
                    else:
                        ip_float = float(ip_val or 0)
                    if ip_float <= 0:
                        continue
                    agg = pitching_totals.setdefault(
                        player_id,
                        {
                            "ip": 0.0,
                            "strikeOuts": 0,
                            "earnedRuns": 0,
                            "runs": 0,
                            "hits": 0,
                            "baseOnBalls": 0,
                            "battersFaced": 0,
                            "homeRuns": 0,
                            "hitByPitch": 0,
                            "games": 0,
                            "gamesStarted": 0,
                            "name": full_name,
                            "team": abbrev,
                            "team_id": tid,
                        },
                    )
                    agg["ip"] += ip_float
                    for k in PITCH_SUM_KEYS:
                        v = pit.get(k)
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                    gp = int(pit.get("gamesPlayed") or 0)
                    gs = int(pit.get("gamesStarted") or 0)
                    if gp <= 0 and ip_float > 0:
                        gp = 1
                    agg["games"] = agg.get("games", 0) + gp
                    agg["gamesStarted"] = agg.get("gamesStarted", 0) + gs
                    agg["name"] = full_name
                    agg["team"] = abbrev
                    agg["team_id"] = tid

    return batting_totals, pitching_totals


def aggregate_boxscore_by_player_team(
    warehouse: Path,
    season: int,
    stage: str,
    *,
    disable_progress: bool = False,
) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[tuple[str, int], dict[str, Any]]]:
    paths = find_stage_raw_paths(warehouse, season, stage)
    batting_totals: dict[tuple[str, int], dict[str, Any]] = {}
    pitching_totals: dict[tuple[str, int], dict[str, Any]] = {}

    if not disable_progress:
        from tqdm import tqdm

        paths = tqdm(paths, desc="Boxscore raw feeds", unit="game")

    for raw_path in paths:
        try:
            with open_raw_feed(raw_path) as f:
                feed = json.load(f)
        except Exception:
            continue

        game_data = feed.get("gameData", {})
        teams = game_data.get("teams", {})
        away = teams.get("away") or {}
        home = teams.get("home") or {}
        away_abbrev = away.get("abbreviation") or "?"
        home_abbrev = home.get("abbreviation") or "?"
        away_id = away.get("id")
        home_id = home.get("id")

        box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
        for side, abbrev, tid in [
            ("away", away_abbrev, away_id),
            ("home", home_abbrev, home_id),
        ]:
            if tid is None:
                continue
            try:
                tid_int = int(tid)
            except (TypeError, ValueError):
                continue
            players = (box.get(side) or {}).get("players") or {}
            for player_key, p in players.items():
                pid = _normalize_player_id(player_key)
                if pid is None:
                    continue
                key = (pid, tid_int)
                person = (p.get("person") or {})
                full_name = person.get("fullName") or f"ID {pid}"
                stats = p.get("stats") or {}

                bat = stats.get("batting") or {}
                if bat and (bat.get("atBats") or 0) > 0:
                    agg = batting_totals.setdefault(
                        key,
                        {k: 0 for k in BAT_KEYS}
                        | {
                            "name": full_name,
                            "team_abbrev": abbrev,
                            "team_id": tid_int,
                        },
                    )
                    for k in BAT_KEYS:
                        v = bat.get(k)
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                    agg["name"] = full_name
                    agg["team_abbrev"] = abbrev

                pit = stats.get("pitching") or {}
                if pit and (pit.get("inningsPitched") or pit.get("outs", 0)):
                    ip_val = pit.get("inningsPitched")
                    if isinstance(ip_val, str):
                        ip_float = innings_to_float(ip_val)
                    else:
                        ip_float = float(ip_val or 0)
                    if ip_float <= 0:
                        continue
                    agg = pitching_totals.setdefault(
                        key,
                        {
                            "ip": 0.0,
                            "strikeOuts": 0,
                            "earnedRuns": 0,
                            "runs": 0,
                            "hits": 0,
                            "baseOnBalls": 0,
                            "battersFaced": 0,
                            "homeRuns": 0,
                            "hitByPitch": 0,
                            "games": 0,
                            "gamesStarted": 0,
                            "name": full_name,
                            "team_abbrev": abbrev,
                            "team_id": tid_int,
                        },
                    )
                    agg["ip"] += ip_float
                    for k in PITCH_SUM_KEYS:
                        v = pit.get(k)
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                    gp = int(pit.get("gamesPlayed") or 0)
                    gs = int(pit.get("gamesStarted") or 0)
                    if gp <= 0 and ip_float > 0:
                        gp = 1
                    agg["games"] = agg.get("games", 0) + gp
                    agg["gamesStarted"] = agg.get("gamesStarted", 0) + gs
                    agg["name"] = full_name
                    agg["team_abbrev"] = abbrev

    return batting_totals, pitching_totals
