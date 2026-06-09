from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
import requests

from api.paths import get_warehouse_dir


_PITCH_DATA_CACHE: dict[tuple[int, str], tuple[tuple[float, int], tuple[dict, dict, dict]]] = {}
_MATRIX_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_CACHE_LOCK = Lock()
_PITCH_DATA_CACHE_MAX = 6
_MATRIX_CACHE_MAX = 12


MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

TEAM_ABBR_BY_NAME = {
    "Arizona Diamondbacks": "AZ",
    "Athletics": "ATH",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

WHIFF_DESCRIPTIONS = {"swinging_strike", "swinging_strike_blocked", "foul_tip", "missed_bunt"}
CSW_DESCRIPTIONS = {"called_strike", "swinging_strike", "swinging_strike_blocked", "foul_tip"}
SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "missed_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}
HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk", "hit_by_pitch"}
DEFAULT_LEAGUE_RATES = {
    "pa": 0,
    "k_rate": 0.225,
    "walk_rate": 0.085,
    "hit_rate": 0.225,
    "hr_rate": 0.032,
    "hard_hit_rate": 0.400,
    "xwoba": 0.320,
    "whiff_rate": 0.255,
    "csw_rate": 0.285,
}


@dataclass(frozen=True)
class StreamerGame:
    game_pk: int | None
    game_date: str
    team: str
    team_name: str
    opponent: str
    opponent_name: str
    venue: str | None
    is_home: bool
    probable_name: str | None = None
    probable_id: int | None = None


def _fingerprint(paths: list[Path]) -> tuple[float, int]:
    total_mtime = 0.0
    total_size = 0
    for path in paths:
        try:
            st = path.stat()
            total_mtime += st.st_mtime
            total_size += st.st_size
        except OSError:
            continue
    return total_mtime, total_size


def _trim_cache(cache: dict, max_entries: int) -> None:
    while len(cache) > max_entries:
        cache.pop(next(iter(cache)))


def _parquet_paths_for_window(warehouse: Path, target_date: date, lookback_days: int = 28) -> list[Path]:
    enriched_dir = warehouse / "regular_season" / "pitches_enriched"
    if not enriched_dir.is_dir():
        return []
    start_date = target_date - timedelta(days=lookback_days)
    paths: list[Path] = []
    for path in sorted(enriched_dir.glob("*.parquet")):
        file_date = _date_from_filename(path.name)
        if file_date and start_date <= file_date < target_date:
            paths.append(path)
    return paths


def get_streamer_matrix(
    game_date: str | None = None,
    season: int | None = None,
    limit: int = 30,
    include_live_probables: bool = True,
) -> dict[str, Any]:
    target_date = _parse_date(game_date)
    season = season or target_date.year
    limit = max(1, min(limit, 100))

    warehouse = get_warehouse_dir() / str(season)
    parquet_paths = _parquet_paths_for_window(warehouse, target_date)
    fp = _fingerprint(parquet_paths)
    matrix_key = (target_date.isoformat(), season, limit, include_live_probables, fp)
    with _CACHE_LOCK:
        cached = _MATRIX_CACHE.get(matrix_key)
        if cached is not None:
            return cached

    player_names = _load_player_names(warehouse / "players_registry.json")
    games = _load_games(warehouse, target_date, include_live_probables)
    pitch_stats, opponent_stats, starters = _load_recent_pitch_data(
        warehouse, target_date, player_names, parquet_paths=parquet_paths, fp=fp
    )
    league_stat = opponent_stats.get("_league", DEFAULT_LEAGUE_RATES)

    candidates = []
    for game in games:
        probable_id = game.probable_id
        probable_name = game.probable_name
        probable_status = "probable" if probable_name else "projected_rotation"

        if not probable_id:
            projected = _project_starter(game.team, starters, target_date)
            if projected:
                probable_id = projected["player_id"]
                probable_name = projected["pitcher"]

        if not probable_name:
            probable_name = f"{game.team} starter TBD"
            probable_status = "unknown"

        pitcher_stat = pitch_stats.get(int(probable_id or 0), {})
        opponent_stat = _select_opponent_split(opponent_stats, game.opponent, pitcher_stat.get("hand"), league_stat)
        candidates.append(
            _score_candidate(
                game=game,
                pitcher_id=probable_id,
                pitcher_name=probable_name,
                probable_status=probable_status,
                pitcher_stat=pitcher_stat,
                opponent_stat=opponent_stat,
                league_stat=league_stat,
            )
        )

    candidates.sort(key=lambda row: (row["projected_malli_score"], row["confidence"]), reverse=True)
    out = {
        "game_date": target_date.isoformat(),
        "season": season,
        "source": "local_warehouse_with_optional_live_probables",
        "notes": [
            "probable rows use MLB Stats API names when available",
            "projected_rotation rows infer a starter from recent first pitchers by team",
            "projected_malli_score uses recent pitcher form plus opponent offense split by pitcher hand",
            "stream_score is retained for legacy queue compatibility; the matrix is ranked by projected_malli_score",
        ],
        "count": min(len(candidates), limit),
        "streamers": candidates[:limit],
    }
    with _CACHE_LOCK:
        _MATRIX_CACHE[matrix_key] = out
        _trim_cache(_MATRIX_CACHE, _MATRIX_CACHE_MAX)
    return out


def _parse_date(raw: str | None) -> date:
    if not raw:
        return date.today()
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("game_date must be YYYY-MM-DD") from exc


def _load_player_names(path: Path) -> dict[int, str]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    names: dict[int, str] = {}
    for key, player in raw.items():
        if not isinstance(player, dict):
            continue
        player_id = _to_int(player.get("id") or key)
        full_name = player.get("fullName") or player.get("nameFirstLast")
        if player_id and full_name:
            names[player_id] = str(full_name)
    return names


def _load_games(warehouse: Path, target_date: date, include_live_probables: bool) -> list[StreamerGame]:
    games = _load_local_schedule_games(warehouse, target_date)
    if include_live_probables:
        live_games = _load_live_schedule_games(target_date)
        if live_games:
            return live_games
    return games


def _load_local_schedule_games(warehouse: Path, target_date: date) -> list[StreamerGame]:
    csv_path = warehouse / "schedule_post.csv"
    if csv_path.is_file():
        try:
            df = pd.read_csv(csv_path)
            day = df[df["date"].astype(str) == target_date.isoformat()].copy()
            games: list[StreamerGame] = []
            for _, row in day.iterrows():
                games.extend(_schedule_row_to_games(row.to_dict(), target_date.isoformat()))
            if games:
                return games
        except Exception:
            pass

    json_path = warehouse / "schedule_regular_season.json"
    if not json_path.is_file():
        return []
    try:
        raw = json.loads(json_path.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    games: list[StreamerGame] = []
    for game in raw if isinstance(raw, list) else []:
        if str(game.get("officialDate")) != target_date.isoformat():
            continue
        games.extend(_statsapi_game_to_streamer_games(game))
    return games


def _load_live_schedule_games(target_date: date) -> list[StreamerGame]:
    try:
        resp = requests.get(
            MLB_SCHEDULE_URL,
            params={
                "sportId": 1,
                "date": target_date.isoformat(),
                "hydrate": "team,venue,probablePitcher",
            },
            timeout=4,
        )
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException:
        return []

    games: list[StreamerGame] = []
    for day in raw.get("dates", []):
        for game in day.get("games", []):
            games.extend(_statsapi_game_to_streamer_games(game))
    return games


def _schedule_row_to_games(row: dict[str, Any], game_date: str) -> list[StreamerGame]:
    away_name = str(row.get("away_team") or "")
    home_name = str(row.get("home_team") or "")
    away = TEAM_ABBR_BY_NAME.get(away_name, away_name)
    home = TEAM_ABBR_BY_NAME.get(home_name, home_name)
    game_pk = _to_int(row.get("game_pk"))
    venue = row.get("venue")
    return [
        StreamerGame(game_pk, game_date, away, away_name, home, home_name, venue, False),
        StreamerGame(game_pk, game_date, home, home_name, away, away_name, venue, True),
    ]


def _statsapi_game_to_streamer_games(game: dict[str, Any]) -> list[StreamerGame]:
    teams = game.get("teams", {})
    away = teams.get("away", {})
    home = teams.get("home", {})
    away_team = away.get("team", {})
    home_team = home.get("team", {})
    away_name = away_team.get("name") or ""
    home_name = home_team.get("name") or ""
    away_probable = away.get("probablePitcher") or {}
    home_probable = home.get("probablePitcher") or {}
    official_date = game.get("officialDate") or str(game.get("gameDate", ""))[:10]
    game_pk = _to_int(game.get("gamePk"))
    venue = (game.get("venue") or {}).get("name")
    away_abbr = TEAM_ABBR_BY_NAME.get(away_name, away_name)
    home_abbr = TEAM_ABBR_BY_NAME.get(home_name, home_name)
    return [
        StreamerGame(
            game_pk,
            official_date,
            away_abbr,
            away_name,
            home_abbr,
            home_name,
            venue,
            False,
            away_probable.get("fullName"),
            _to_int(away_probable.get("id")),
        ),
        StreamerGame(
            game_pk,
            official_date,
            home_abbr,
            home_name,
            away_abbr,
            away_name,
            venue,
            True,
            home_probable.get("fullName"),
            _to_int(home_probable.get("id")),
        ),
    ]


def _load_recent_pitch_data(
    warehouse: Path,
    target_date: date,
    player_names: dict[int, str],
    lookback_days: int = 28,
    *,
    parquet_paths: list[Path] | None = None,
    fp: tuple[float, int] | None = None,
) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    season = int(warehouse.name) if warehouse.name.isdigit() else target_date.year
    cache_key = (season, target_date.isoformat())
    if fp is not None:
        with _CACHE_LOCK:
            hit = _PITCH_DATA_CACHE.get(cache_key)
            if hit is not None and hit[0] == fp:
                return hit[1]

    paths = parquet_paths if parquet_paths is not None else _parquet_paths_for_window(warehouse, target_date, lookback_days)
    if not paths:
        return {}, {}, {}

    start_date = target_date - timedelta(days=lookback_days)
    frames = []
    columns = [
        "game_pk",
        "game_date",
        "pitcher",
        "inning_topbot",
        "at_bat_number",
        "pitch_number",
        "p_throws",
        "home_team",
        "away_team",
        "zone",
        "description",
        "events",
        "estimated_woba_using_speedangle",
        "woba_value",
        "woba_denom",
        "launch_speed",
    ]
    for path in paths:
        try:
            frames.append(pd.read_parquet(path, columns=columns))
        except Exception:
            continue

    if not frames:
        return {}, {}, {}

    df = pd.concat(frames, ignore_index=True)
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    df = df[(df["game_date"] >= start_date) & (df["game_date"] < target_date)].copy()
    if df.empty:
        return {}, {}, {}

    df["pitcher"] = pd.to_numeric(df["pitcher"], errors="coerce").astype("Int64")
    df["pitcher_team"] = df.apply(
        lambda row: row["home_team"] if row.get("inning_topbot") == "Top" else row["away_team"],
        axis=1,
    )
    df["batter_team"] = df.apply(
        lambda row: row["away_team"] if row.get("inning_topbot") == "Top" else row["home_team"],
        axis=1,
    )
    df["is_pa_end"] = df["events"].notna()
    df["is_swing"] = df["description"].isin(SWING_DESCRIPTIONS)
    df["is_whiff"] = df["description"].isin(WHIFF_DESCRIPTIONS)
    df["is_csw"] = df["description"].isin(CSW_DESCRIPTIONS)
    df["is_k"] = df["events"].eq("strikeout")
    df["is_walk"] = df["events"].isin(WALK_EVENTS)
    df["is_hit"] = df["events"].isin(HIT_EVENTS)
    df["is_hr"] = df["events"].eq("home_run")
    df["is_hard"] = pd.to_numeric(df["launch_speed"], errors="coerce").ge(95)
    df["xwoba"] = pd.to_numeric(df["estimated_woba_using_speedangle"], errors="coerce")
    if "woba_value" in df.columns and "woba_denom" in df.columns:
        woba = pd.to_numeric(df["woba_value"], errors="coerce")
        denom = pd.to_numeric(df["woba_denom"], errors="coerce").fillna(0)
        df["pa_xwoba"] = df["xwoba"].where(df["xwoba"].notna(), woba.where(denom > 0))
    else:
        df["pa_xwoba"] = df["xwoba"]

    pitch_stats = _aggregate_pitcher_stats(df, player_names)
    opponent_stats = _aggregate_opponent_stats(df)
    starters = _detect_recent_starters(df, player_names)
    bundle = (pitch_stats, opponent_stats, starters)
    if fp is None:
        fp = _fingerprint(paths)
    with _CACHE_LOCK:
        _PITCH_DATA_CACHE[cache_key] = (fp, bundle)
        _trim_cache(_PITCH_DATA_CACHE, _PITCH_DATA_CACHE_MAX)
    return bundle


def _aggregate_pitcher_stats(df: pd.DataFrame, player_names: dict[int, str]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for pitcher_id, group in df.dropna(subset=["pitcher"]).groupby("pitcher"):
        pid = int(pitcher_id)
        pa = group[group["is_pa_end"]]
        swings = int(group["is_swing"].sum())
        whiffs = int(group["is_whiff"].sum())
        csw = int(group["is_csw"].sum())
        batted = group[group["is_pa_end"]].dropna(subset=["pa_xwoba"])
        hard_balls = group[group["launch_speed"].notna()]
        games = int(group["game_pk"].nunique())
        batters_faced = int(len(pa))
        pitches = int(len(group))
        out[pid] = {
            "player_id": pid,
            "pitcher": player_names.get(pid, str(pid)),
            "team": _mode(group["pitcher_team"]),
            "hand": _clean_hand(_mode(group["p_throws"])) or "R",
            "games": games,
            "batters_faced": batters_faced,
            "pitches": pitches,
            "bf_per_game": _rate(batters_faced, games),
            "pitches_per_game": _rate(pitches, games),
            "k_rate": _rate(pa["is_k"].sum(), batters_faced),
            "walk_rate": _rate(pa["is_walk"].sum(), batters_faced),
            "hit_rate": _rate(pa["is_hit"].sum(), batters_faced),
            "hr_rate": _rate(pa["is_hr"].sum(), batters_faced),
            "whiff_rate": _rate(whiffs, swings),
            "swstr_rate": _rate(whiffs, pitches),
            "csw_rate": _rate(csw, pitches),
            "hard_hit_rate": _rate(hard_balls["is_hard"].sum(), len(hard_balls)),
            "xwoba": _safe_mean(batted["pa_xwoba"]),
        }
    return out


def _aggregate_opponent_stats(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for team, group in df.groupby("batter_team"):
        splits = {"all": _offense_stat(str(team), group, "all")}
        for hand, sub in group.groupby("p_throws"):
            cleaned = _clean_hand(hand)
            if cleaned:
                splits[cleaned] = _offense_stat(str(team), sub, cleaned)
        out[str(team)] = splits
    out["_league"] = _offense_stat("_league", df, "all")
    return out


def _offense_stat(team: str, group: pd.DataFrame, split: str) -> dict[str, Any]:
    pa = group[group["is_pa_end"]]
    batters_faced = int(len(pa))
    swings = int(group["is_swing"].sum())
    whiffs = int(group["is_whiff"].sum())
    hard_balls = group[group["launch_speed"].notna()]
    return {
        "team": team,
        "split": split,
        "pa": batters_faced,
        "k_rate": _rate(pa["is_k"].sum(), batters_faced),
        "walk_rate": _rate(pa["is_walk"].sum(), batters_faced),
        "hit_rate": _rate(pa["is_hit"].sum(), batters_faced),
        "hr_rate": _rate(pa["is_hr"].sum(), batters_faced),
        "whiff_rate": _rate(whiffs, swings),
        "hard_hit_rate": _rate(hard_balls["is_hard"].sum(), len(hard_balls)),
        "xwoba": _safe_mean(pa["pa_xwoba"].dropna()),
    }


def _select_opponent_split(
    opponent_stats: dict[str, dict[str, Any]],
    opponent: str,
    pitcher_hand: Any,
    league_stat: dict[str, Any],
) -> dict[str, Any]:
    team_splits = opponent_stats.get(opponent) or {}
    hand = _clean_hand(pitcher_hand)
    split = team_splits.get(hand or "") if isinstance(team_splits, dict) else None
    fallback = "hand"
    if not split or int(split.get("pa") or 0) < 80:
        all_split = team_splits.get("all") if isinstance(team_splits, dict) else None
        if all_split:
            split = {**all_split, "requested_split": hand, "fallback": "team_all_hands"}
            fallback = "team_all_hands"
        else:
            split = {**league_stat, "team": opponent, "split": "all", "requested_split": hand, "fallback": "league"}
            fallback = "league"
    else:
        split = {**split, "requested_split": hand, "fallback": fallback}
    return split


def _detect_recent_starters(df: pd.DataFrame, player_names: dict[int, str]) -> dict[str, list[dict[str, Any]]]:
    starters: dict[str, list[dict[str, Any]]] = {}
    ordered = df.sort_values(["game_date", "game_pk", "inning_topbot", "at_bat_number", "pitch_number"])
    for (game_pk, team), group in ordered.groupby(["game_pk", "pitcher_team"], sort=False):
        first = group.dropna(subset=["pitcher"]).head(1)
        if first.empty:
            continue
        row = first.iloc[0]
        pid = int(row["pitcher"])
        starters.setdefault(str(team), []).append(
            {
                "player_id": pid,
                "pitcher": player_names.get(pid, str(pid)),
                "team": str(team),
                "game_pk": _to_int(game_pk),
                "last_start_date": row["game_date"].isoformat(),
            }
        )

    for team, rows in starters.items():
        rows.sort(key=lambda item: item["last_start_date"], reverse=True)
        seen: set[int] = set()
        unique_rows = []
        for row in rows:
            if row["player_id"] in seen:
                continue
            seen.add(row["player_id"])
            unique_rows.append(row)
        starters[team] = unique_rows[:8]
    return starters


def _project_starter(team: str, starters: dict[str, list[dict[str, Any]]], target_date: date) -> dict[str, Any] | None:
    candidates = starters.get(team, [])
    if not candidates:
        return None
    best = None
    best_score = -999
    for row in candidates:
        try:
            last_start = datetime.strptime(row["last_start_date"], "%Y-%m-%d").date()
        except ValueError:
            continue
        rest_days = (target_date - last_start).days
        rest_score = 100 - abs(rest_days - 5) * 18
        if rest_days < 3:
            rest_score -= 35
        if rest_score > best_score:
            best_score = rest_score
            best = row
    return best


def _score_candidate(
    game: StreamerGame,
    pitcher_id: int | None,
    pitcher_name: str,
    probable_status: str,
    pitcher_stat: dict[str, Any],
    opponent_stat: dict[str, Any],
    league_stat: dict[str, Any],
) -> dict[str, Any]:
    projected = _project_line(pitcher_stat, opponent_stat, league_stat)
    k_upside = _clamp_score(_scale_pct(pitcher_stat.get("k_rate"), 0.14, 0.34) * 0.55 + _scale_pct(pitcher_stat.get("whiff_rate"), 0.18, 0.38) * 0.45)
    ratio_safety = _clamp_score(
        _inverse_scale_pct(pitcher_stat.get("walk_rate"), 0.04, 0.13) * 0.28
        + _inverse_scale_pct(pitcher_stat.get("hit_rate"), 0.16, 0.30) * 0.24
        + _inverse_scale_pct(pitcher_stat.get("hr_rate"), 0.015, 0.055) * 0.22
        + _inverse_scale_number(pitcher_stat.get("xwoba"), 0.260, 0.390) * 0.26
    )
    opponent_k = _clamp_score(_scale_pct(opponent_stat.get("k_rate"), 0.17, 0.29))
    opponent_power = _clamp_score(
        _scale_pct(opponent_stat.get("hr_rate"), 0.020, 0.060) * 0.45
        + _scale_pct(opponent_stat.get("hard_hit_rate"), 0.32, 0.48) * 0.35
        + _scale_number(opponent_stat.get("xwoba"), 0.285, 0.360) * 0.20
    )
    workload = _clamp_score(_scale_number(pitcher_stat.get("batters_faced"), 20, 115))
    matchup_edge = _clamp_score(opponent_k * 0.65 + (100 - opponent_power) * 0.35)
    confidence = _confidence(probable_status, pitcher_stat, opponent_stat)
    stream_score = _clamp_score(
        k_upside * 0.27
        + ratio_safety * 0.22
        + matchup_edge * 0.23
        + workload * 0.10
        + confidence * 0.08
        + 50 * 0.10
    )
    ratio_risk = 100 - ratio_safety
    league_fit = _league_fit(stream_score, ratio_risk, k_upside, confidence)
    note = _candidate_note(
        pitcher_name,
        game,
        k_upside,
        ratio_risk,
        opponent_k,
        opponent_power,
        probable_status,
        projected,
        opponent_stat,
    )

    return {
        "pitcher": pitcher_name,
        "player_id": pitcher_id,
        "team": game.team,
        "team_name": game.team_name,
        "opponent": game.opponent,
        "opponent_name": game.opponent_name,
        "game_date": game.game_date,
        "game_pk": game.game_pk,
        "venue": game.venue,
        "home_away": "home" if game.is_home else "away",
        "probable_status": probable_status,
        "pitcher_hand": pitcher_stat.get("hand") or opponent_stat.get("requested_split") or "R",
        "projected_malli_score": round(projected["malli_score"], 1),
        "projected": {
            "ip": round(projected["ip"], 1),
            "pitches": round(projected["pitches"]),
            "batters_faced": round(projected["batters_faced"]),
            "k": round(projected["k"], 1),
            "bb": round(projected["bb"], 1),
            "h": round(projected["h"], 1),
            "hr": round(projected["hr"], 2),
            "er": round(projected["er"], 1),
            "whip": round(projected["whip"], 2),
            "k_pct": round(projected["k_rate"] * 100, 1),
            "bb_pct": round(projected["bb_rate"] * 100, 1),
            "k_minus_bb_pct": round(projected["kbb_rate"] * 100, 1),
            "swstr_pct": round(projected["swstr_rate"] * 100, 1),
            "csw_pct": round(projected["csw_rate"] * 100, 1),
            "xwoba_allowed": round(projected["xwoba"], 3),
        },
        "stream_score": round(stream_score),
        "k_upside": round(k_upside),
        "ratio_risk": round(ratio_risk),
        "opponent_k_profile": round(opponent_k),
        "opponent_power_risk": round(opponent_power),
        "confidence": round(confidence),
        "league_fit": league_fit,
        "note": note,
        "factor_scores": {
            "k_upside": round(k_upside),
            "ratio_safety": round(ratio_safety),
            "matchup_edge": round(matchup_edge),
            "opponent_k_profile": round(opponent_k),
            "opponent_power_risk": round(opponent_power),
            "recent_workload": round(workload),
            "projected_malli_score": round(projected["malli_score"], 1),
            "projected_k_pct": round(projected["k_rate"] * 100, 1),
            "projected_xwoba": round(projected["xwoba"], 3),
        },
        "sample": {
            "pitcher_batters_faced": pitcher_stat.get("batters_faced", 0),
            "opponent_pa": opponent_stat.get("pa", 0),
            "opponent_split": opponent_stat.get("split") or "all",
            "opponent_split_fallback": opponent_stat.get("fallback") or "hand",
        },
    }


def _project_line(pitcher_stat: dict[str, Any], opponent_stat: dict[str, Any], league_stat: dict[str, Any]) -> dict[str, float]:
    league = {**DEFAULT_LEAGUE_RATES, **{k: v for k, v in league_stat.items() if v is not None}}
    pitcher_bf = _blend_sample(pitcher_stat.get("bf_per_game"), 22.5, pitcher_stat.get("batters_faced"), 90)
    batters_faced = _clamp_number(pitcher_bf, 14.0, 29.0)
    pitches = _clamp_number(
        _blend_sample(pitcher_stat.get("pitches_per_game"), batters_faced * 3.9, pitcher_stat.get("pitches"), 350),
        55.0,
        105.0,
    )

    pitcher_k = _blend_sample(pitcher_stat.get("k_rate"), league["k_rate"], pitcher_stat.get("batters_faced"), 90)
    pitcher_bb = _blend_sample(pitcher_stat.get("walk_rate"), league["walk_rate"], pitcher_stat.get("batters_faced"), 90)
    pitcher_hit = _blend_sample(pitcher_stat.get("hit_rate"), league["hit_rate"], pitcher_stat.get("batters_faced"), 90)
    pitcher_hr = _blend_sample(pitcher_stat.get("hr_rate"), league["hr_rate"], pitcher_stat.get("batters_faced"), 90)
    pitcher_xwoba = _blend_sample(pitcher_stat.get("xwoba"), league["xwoba"], pitcher_stat.get("batters_faced"), 90)
    pitcher_swstr = _blend_sample(pitcher_stat.get("swstr_rate"), 0.115, pitcher_stat.get("pitches"), 350)
    pitcher_csw = _blend_sample(pitcher_stat.get("csw_rate"), league.get("csw_rate", 0.285), pitcher_stat.get("pitches"), 350)

    opp_pa = opponent_stat.get("pa")
    opp_k = _blend_sample(opponent_stat.get("k_rate"), league["k_rate"], opp_pa, 180)
    opp_bb = _blend_sample(opponent_stat.get("walk_rate"), league["walk_rate"], opp_pa, 180)
    opp_hit = _blend_sample(opponent_stat.get("hit_rate"), league["hit_rate"], opp_pa, 180)
    opp_hr = _blend_sample(opponent_stat.get("hr_rate"), league["hr_rate"], opp_pa, 180)
    opp_xwoba = _blend_sample(opponent_stat.get("xwoba"), league["xwoba"], opp_pa, 180)
    opp_hard = _blend_sample(opponent_stat.get("hard_hit_rate"), league["hard_hit_rate"], opp_pa, 180)

    k_rate = _clamp_number(pitcher_k + (opp_k - league["k_rate"]) * 0.35 + (pitcher_swstr - 0.115) * 0.25, 0.10, 0.42)
    bb_rate = _clamp_number(pitcher_bb * 0.70 + opp_bb * 0.30, 0.035, 0.16)
    hit_rate = _clamp_number(pitcher_hit * 0.68 + opp_hit * 0.32 + (opp_hard - league["hard_hit_rate"]) * 0.08, 0.14, 0.34)
    hr_rate = _clamp_number(pitcher_hr * 0.65 + opp_hr * 0.35 + (opp_xwoba - league["xwoba"]) * 0.06, 0.005, 0.085)
    xwoba = _clamp_number(pitcher_xwoba * 0.65 + opp_xwoba * 0.35 + (opp_hard - league["hard_hit_rate"]) * 0.08, 0.240, 0.430)
    swstr_rate = _clamp_number(pitcher_swstr + (opp_k - league["k_rate"]) * 0.08, 0.055, 0.230)
    csw_rate = _clamp_number(pitcher_csw + (k_rate - league["k_rate"]) * 0.06 - (bb_rate - league["walk_rate"]) * 0.04, 0.200, 0.390)
    kbb_rate = k_rate - bb_rate

    strikeouts = batters_faced * k_rate
    walks = batters_faced * bb_rate
    hits = batters_faced * hit_rate
    hr = batters_faced * hr_rate
    ip = _clamp_number(batters_faced / 4.25, 3.0, 7.0)
    er = _clamp_number((xwoba - 0.250) * 12.5 + hr * 0.75, 0.2, 6.5)
    whip = (walks + hits) / ip if ip > 0 else 0.0
    workload = min(1.0, max(0.55, math.sqrt(max(pitches, 1) / 85.0)))
    malli_score = (
        0.28 * _scale_number(csw_rate * 100, 22.0, 40.0)
        + 0.22 * _scale_number(swstr_rate * 100, 7.0, 24.0)
        + 0.30 * _inverse_scale_number(xwoba, 0.180, 0.460)
        + 0.20 * _scale_number(kbb_rate * 100, -5.0, 35.0)
    ) * workload

    return {
        "batters_faced": batters_faced,
        "pitches": pitches,
        "ip": ip,
        "k": strikeouts,
        "bb": walks,
        "h": hits,
        "hr": hr,
        "er": er,
        "whip": whip,
        "k_rate": k_rate,
        "bb_rate": bb_rate,
        "kbb_rate": kbb_rate,
        "swstr_rate": swstr_rate,
        "csw_rate": csw_rate,
        "xwoba": xwoba,
        "malli_score": _clamp_score(malli_score),
    }


def _candidate_note(
    pitcher_name: str,
    game: StreamerGame,
    k_upside: float,
    ratio_risk: float,
    opponent_k: float,
    opponent_power: float,
    probable_status: str,
    projected: dict[str, float],
    opponent_stat: dict[str, Any],
) -> str:
    status = "confirmed probable" if probable_status == "probable" else "rotation projection"
    strengths = []
    if k_upside >= 65:
        strengths.append("K upside")
    if opponent_k >= 62:
        strengths.append("opponent swing-and-miss")
    if ratio_risk <= 42:
        strengths.append("ratio safety")
    if not strengths:
        strengths.append("balanced profile")
    risk = "power risk elevated" if opponent_power >= 64 else "power risk manageable"
    split = opponent_stat.get("requested_split") or opponent_stat.get("split") or "all"
    fallback = opponent_stat.get("fallback")
    split_note = f"vs {split}HP split"
    if fallback == "team_all_hands":
        split_note = "team all-hands fallback"
    elif fallback == "league":
        split_note = "league fallback"
    return (
        f"{pitcher_name} vs {game.opponent}: projected MalliScore {projected['malli_score']:.1f}, "
        f"{projected['k']:.1f} K; {', '.join(strengths)}; {risk}. {status}; {split_note}."
    )


def _league_fit(stream_score: float, ratio_risk: float, k_upside: float, confidence: float) -> str:
    if stream_score >= 72 and confidence >= 58 and ratio_risk <= 58:
        return "12-team mixed"
    if stream_score >= 64 and k_upside >= 60:
        return "15-team / K chase"
    if stream_score >= 55:
        return "deep league only"
    return "watchlist"


def _confidence(probable_status: str, pitcher_stat: dict[str, Any], opponent_stat: dict[str, Any]) -> float:
    base = 74 if probable_status == "probable" else 52 if probable_status == "projected_rotation" else 35
    if pitcher_stat.get("batters_faced", 0) >= 70:
        base += 10
    elif pitcher_stat.get("batters_faced", 0) >= 30:
        base += 5
    if opponent_stat.get("pa", 0) >= 200:
        base += 8
    elif opponent_stat.get("pa", 0) >= 80:
        base += 4
    if opponent_stat.get("fallback") == "team_all_hands":
        base -= 5
    elif opponent_stat.get("fallback") == "league":
        base -= 12
    return _clamp_score(base)


def _date_from_filename(name: str) -> date | None:
    parts = name.split("_")
    for part in parts:
        if len(part) == 8 and part.isdigit():
            try:
                return datetime.strptime(part, "%Y%m%d").date()
            except ValueError:
                return None
    return None


def _mode(series: pd.Series) -> str | None:
    values = series.dropna()
    if values.empty:
        return None
    return str(values.mode().iloc[0])


def _safe_mean(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _clean_hand(value: Any) -> str | None:
    raw = str(value or "").strip().upper()[:1]
    return raw if raw in {"R", "L"} else None


def _blend_sample(value: Any, fallback: float, sample: Any, full_sample: float) -> float:
    try:
        val = float(value)
        if math.isnan(val):
            raise ValueError
    except (TypeError, ValueError):
        val = float(fallback)
    try:
        n = max(0.0, float(sample or 0))
    except (TypeError, ValueError):
        n = 0.0
    weight = min(1.0, n / max(float(full_sample), 1.0))
    return val * weight + float(fallback) * (1.0 - weight)


def _clamp_number(value: Any, low: float, high: float) -> float:
    try:
        num = float(value)
        if math.isnan(num):
            return (low + high) / 2.0
    except (TypeError, ValueError):
        return (low + high) / 2.0
    return max(low, min(high, num))


def _rate(num: Any, denom: Any) -> float | None:
    try:
        denom_float = float(denom)
        if denom_float <= 0:
            return None
        return float(num) / denom_float
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _scale_pct(value: Any, low: float, high: float) -> float:
    return _scale_number(value, low, high)


def _inverse_scale_pct(value: Any, low: float, high: float) -> float:
    return 100 - _scale_pct(value, low, high)


def _scale_number(value: Any, low: float, high: float) -> float:
    if value is None:
        return 50
    try:
        num = float(value)
        if math.isnan(num):
            return 50
    except (TypeError, ValueError):
        return 50
    if high == low:
        return 50
    return _clamp_score((num - low) / (high - low) * 100)


def _inverse_scale_number(value: Any, low: float, high: float) -> float:
    return 100 - _scale_number(value, low, high)


def _clamp_score(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 50
    if math.isnan(num):
        return 50
    return max(0, min(100, num))


def _to_int(value: Any) -> int | None:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
