"""
Leaderboard endpoints — read from player_season_boxscore parquets.

Resolution order per season (batting / pitching):
1) Parquet at warehouse root or under ``{season}/`` matching ``player_season_boxscore_*``.
   Rows are filtered by ``season`` when that column exists (multi-year files).
   Multiple rows per ``player_id`` (e.g. trades) are summed into one row; rate stats recomputed.
2) Drive-style CSV: ``{season}/player_season_batting_by_team.csv`` (export script).
3) Aggregate from ``{season}/{stage}/raw/game_*_feed_live*`` (same logic as daily ingest).
   Pitching rows include ``games`` / ``games_started`` from each game’s ``gamesPlayed`` / ``gamesStarted`` so SP/RP filters work without parquet.

Parquet reads are cached in memory keyed by (path, mtime).
Live aggregates are cached keyed by (season, stage, raw max-mtime, file count, total bytes)
so new CI games invalidate the cache automatically.
"""
from __future__ import annotations

import gzip
import json
import math
import os
import sys
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from api.paths import get_repo_root, get_warehouse_dir, safe_is_dir, safe_non_empty_file

router = APIRouter(prefix="/leaderboards", tags=["leaderboards"])

_parquet_cache: dict[tuple[str, float], Any] = {}
_live_cache: dict[tuple[Any, ...], tuple[tuple[float, int, int], Any]] = {}
_cache_lock = Lock()
_MAX_CACHE_ENTRIES = int(os.environ.get("MLB_LEADERBOARD_CACHE_MAX", "16"))
# Bump when raw→pitching aggregation gains columns (invalidates in-proc live cache).
_PITCHING_LIVE_CACHE_VER = 2
_BATTING_QUALIFIER_PA_PER_TEAM_GAME = 3.1
_PITCHING_QUALIFIER_IP_PER_TEAM_GAME = 1.0
_PREFER_RAW_LEADERBOARDS = os.environ.get("MLB_LEADERBOARDS_PREFER_RAW", "1") != "0"


def _warehouse() -> Path:
    return get_warehouse_dir()


def _ensure_repo_on_path() -> Path:
    root = get_repo_root()
    rs = str(root)
    if rs not in sys.path:
        sys.path.insert(0, rs)
    return root


def _find_parquet(kind: str, season: int) -> Optional[Path]:
    root = _warehouse()
    try:
        matches = sorted(root.glob(f"player_season_boxscore_{kind}*{season}*.parquet"))
        if matches:
            return matches[-1]
        matches = sorted(root.glob(f"{season}/player_season_boxscore_{kind}*.parquet"))
        return matches[-1] if matches else None
    except (OSError, TimeoutError):
        return None


def _drive_csv(kind: str, season: int) -> Path:
    name = (
        "player_season_batting_by_team.csv"
        if kind == "batting"
        else "player_season_pitching_by_team.csv"
    )
    return _warehouse() / str(season) / name


def _raw_fingerprint(warehouse: Path, season: int, stage: str) -> Optional[tuple[float, int, int]]:
    """(max mtime, file count, total bytes) — changes when CI adds/updates games so cache refreshes."""
    raw = warehouse / str(season) / stage / "raw"
    if not safe_is_dir(raw):
        return None
    try:
        files = [
            p
            for p in raw.iterdir()
            if p.is_file()
            and "feed_live" in p.name
            and (p.suffix == ".json" or p.name.endswith(".json.gz"))
        ]
    except (OSError, TimeoutError):
        return None
    if not files:
        return None
    try:
        mt = max(p.stat().st_mtime for p in files)
        nbytes = sum(p.stat().st_size for p in files)
    except (OSError, TimeoutError):
        return None
    return (mt, len(files), nbytes)


def _pick_stage_for_raw(warehouse: Path, season: int) -> Optional[str]:
    if _raw_fingerprint(warehouse, season, "regular_season"):
        return "regular_season"
    if _raw_fingerprint(warehouse, season, "spring_training"):
        return "spring_training"
    return None


def _open_raw_feed(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _team_games_from_raw(warehouse: Path, season: int, stage: str) -> dict[int, int]:
    raw = warehouse / str(season) / stage / "raw"
    out: dict[int, int] = {}
    if not safe_is_dir(raw):
        return out
    try:
        paths = sorted(
            p
            for p in raw.iterdir()
            if p.is_file()
            and "feed_live" in p.name
            and (p.suffix == ".json" or p.name.endswith(".json.gz"))
        )
    except (OSError, TimeoutError):
        return out
    for path in paths:
        try:
            with _open_raw_feed(path) as f:
                feed = json.load(f)
        except Exception:
            continue
        status = ((feed.get("gameData") or {}).get("status") or {})
        if str(status.get("abstractGameState") or "").strip().lower() != "final":
            continue
        plays = (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
        if not plays:
            continue
        teams = ((feed.get("gameData") or {}).get("teams") or {})
        for side in ("away", "home"):
            tid = ((teams.get(side) or {}).get("id"))
            try:
                tid_int = int(tid)
            except (TypeError, ValueError):
                continue
            out[tid_int] = out.get(tid_int, 0) + 1
    return out


def _qualification_thresholds(season: int) -> dict[str, Any]:
    wh = _warehouse()
    stage = _pick_stage_for_raw(wh, season) or "regular_season"
    team_games = _team_games_from_raw(wh, season, stage)
    games = max(team_games.values()) if team_games else 0
    return {
        "season": season,
        "stage": stage,
        "team_games": games,
        "min_pa": int(math.ceil(games * _BATTING_QUALIFIER_PA_PER_TEAM_GAME)) if games else 0,
        "min_ip": float(games * _PITCHING_QUALIFIER_IP_PER_TEAM_GAME) if games else 0.0,
        "batting_rule": "3.1 PA per team game",
        "pitching_rule": "1.0 IP per team game",
    }


def _read_parquet_dataframe(parquet: Path):
    import pandas as pd

    path = parquet.resolve()
    try:
        mtime = path.stat().st_mtime
    except (OSError, TimeoutError):
        return pd.read_parquet(parquet)

    key = (str(path), mtime)
    with _cache_lock:
        hit = _parquet_cache.get(key)
        if hit is not None:
            return hit
        df = pd.read_parquet(parquet)
        if len(_parquet_cache) >= _MAX_CACHE_ENTRIES:
            _parquet_cache.clear()
        _parquet_cache[key] = df
    return df


def _rollup_batting_by_player(df):
    import numpy as np
    import pandas as pd

    if df.empty:
        return df
    df = _ensure_player_name_column(df)
    team_cols = [c for c in ("team_abbrev", "team_id") if c in df.columns]
    team_pick = None
    if team_cols:
        sort_col = "plate_appearances" if "plate_appearances" in df.columns else "at_bats"
        if sort_col in df.columns:
            team_pick = (
                df[["player_id", sort_col, *team_cols]]
                .sort_values(["player_id", sort_col], ascending=[True, False])
                .drop_duplicates("player_id")[["player_id", *team_cols]]
            )
    sum_cols = [
        c
        for c in (
            "at_bats",
            "hits",
            "doubles",
            "triples",
            "home_runs",
            "rbi",
            "runs",
            "walks",
            "strikeouts",
            "hbp",
            "sac_flies",
            "stolen_bases",
            "plate_appearances",
            "total_bases",
        )
        if c in df.columns
    ]
    agg_map: dict[str, str] = {c: "sum" for c in sum_cols}
    agg_map["player_name"] = "first"
    g = df.groupby("player_id", as_index=False).agg(agg_map)
    ab = g["at_bats"].fillna(0).astype(float)
    h = g["hits"].fillna(0).astype(float)
    tb = g["total_bases"].fillna(0).astype(float)
    bb = g["walks"].fillna(0).astype(float)
    hbp = g["hbp"].fillna(0).astype(float)
    sf = g["sac_flies"].fillna(0).astype(float)
    g["avg"] = np.where(ab > 0, (h / ab).round(4), 0.0)
    obp_denom = ab + bb + hbp + sf
    g["obp"] = np.where(obp_denom > 0, ((h + bb + hbp) / obp_denom).round(4), 0.0)
    g["slg"] = np.where(ab > 0, (tb / ab).round(4), 0.0)
    g["ops"] = (g["obp"] + g["slg"]).round(4)
    g["pa"] = g["plate_appearances"].fillna(0).astype(int)
    g["hr"] = g["home_runs"].fillna(0).astype(int)
    if team_pick is not None:
        g = g.merge(team_pick, on="player_id", how="left")
    return g


def _rollup_pitching_by_player(df):
    import numpy as np
    import pandas as pd

    if df.empty:
        return df
    df = _ensure_player_name_column(df)
    team_cols = [c for c in ("team_abbrev", "team_id") if c in df.columns]
    team_pick = None
    if team_cols:
        sort_col = "batters_faced" if "batters_faced" in df.columns else "ip"
        if sort_col in df.columns:
            team_pick = (
                df[["player_id", sort_col, *team_cols]]
                .sort_values(["player_id", sort_col], ascending=[True, False])
                .drop_duplicates("player_id")[["player_id", *team_cols]]
            )
    sum_cols = [
        c
        for c in (
            "ip",
            "earned_runs",
            "runs",
            "hits",
            "walks",
            "strikeouts",
            "batters_faced",
            "home_runs",
            "hbp",
            "games",
            "games_started",
        )
        if c in df.columns
    ]
    agg_map: dict[str, str] = {c: "sum" for c in sum_cols}
    agg_map["player_name"] = "first"
    g = df.groupby("player_id", as_index=False).agg(agg_map)
    ip = g["ip"].replace(0, np.nan)
    er = g["earned_runs"].fillna(0).astype(float)
    k = g["strikeouts"].fillna(0).astype(float)
    h = g["hits"].fillna(0).astype(float)
    bb = g["walks"].fillna(0).astype(float)
    g["era"] = np.where(ip.notna() & (ip > 0), (er * 9.0 / ip).round(3), 0.0)
    g["whip"] = np.where(ip.notna() & (ip > 0), ((h + bb) / ip).round(3), 0.0)
    g["k_per_9"] = np.where(ip.notna() & (ip > 0), (k * 9.0 / ip).round(3), 0.0)
    g["ip"] = g["ip"].fillna(0.0).round(3)
    if team_pick is not None:
        g = g.merge(team_pick, on="player_id", how="left")
    return g


def _batting_needs_player_rollup(df) -> bool:
    if "player_id" not in df.columns or df.empty:
        return False
    return bool((df.groupby("player_id").size() > 1).any())


def _batting_df_for_rollup(df):
    """Map legacy parquet / mixed column names to fields _rollup_batting_by_player sums."""
    d = df.copy()
    pairs = [
        ("ab", "at_bats"),
        ("h", "hits"),
        ("r", "runs"),
        ("bb", "walks"),
        ("so", "strikeouts"),
        ("sf", "sac_flies"),
        ("tb", "total_bases"),
        ("hr", "home_runs"),
    ]
    for old, new in pairs:
        if old in d.columns and new not in d.columns:
            d[new] = d[old]
    if "pa" in d.columns and "plate_appearances" not in d.columns:
        d["plate_appearances"] = d["pa"]
    if "sb" in d.columns and "stolen_bases" not in d.columns:
        d["stolen_bases"] = d["sb"]
    return d


def _dedupe_batting_to_one_row_per_player(df):
    if not _batting_needs_player_rollup(df):
        _normalize_batting_columns(df)
        return df
    prep = _batting_df_for_rollup(df)
    return _rollup_batting_by_player(prep)


def _pitching_needs_player_rollup(df) -> bool:
    if "player_id" not in df.columns or df.empty:
        return False
    return bool((df.groupby("player_id").size() > 1).any())


def _pitching_df_for_rollup(df):
    d = df.copy()
    if "games" not in d.columns and "gamesPlayed" in d.columns:
        d["games"] = d["gamesPlayed"]
    if "games_started" not in d.columns and "gamesStarted" in d.columns:
        d["games_started"] = d["gamesStarted"]
    if "bb" in d.columns and "walks" not in d.columns:
        d["walks"] = d["bb"]
    if "so" in d.columns and "strikeouts" not in d.columns:
        d["strikeouts"] = d["so"]
    if "hr" in d.columns and "home_runs" not in d.columns:
        d["home_runs"] = d["hr"]
    # Legacy table uses outs; convert to ip for rollup if ip missing
    if "ip" not in d.columns and "outs" in d.columns:
        d["ip"] = d["outs"].fillna(0).astype(float) / 3.0
    return d


def _dedupe_pitching_to_one_row_per_player(df):
    if not _pitching_needs_player_rollup(df):
        return df
    prep = _pitching_df_for_rollup(df)
    return _rollup_pitching_by_player(prep)


def _load_batting_frame(season: int):
    import pandas as pd

    wh = _warehouse()
    if _PREFER_RAW_LEADERBOARDS:
        df, src = _load_batting_frame_from_raw(wh, season)
        if df is not None and not df.empty:
            return df, src

    pq = _find_parquet("batting", season)
    if pq is not None:
        df = _read_parquet_dataframe(pq)
        if "season" in df.columns:
            df = df.loc[df["season"] == season].copy()
        if not df.empty:
            df = _dedupe_batting_to_one_row_per_player(df)
            return df, "parquet"

    csv_path = _drive_csv("batting", season)
    if safe_non_empty_file(csv_path):
        df = pd.read_csv(csv_path)
        if "player_id" in df.columns and not df.empty:
            df = _rollup_batting_by_player(df)
            _normalize_batting_columns(df)
            return df, "csv"

    return _load_batting_frame_from_raw(wh, season)


def _load_batting_frame_from_raw(wh: Path, season: int):
    import pandas as pd

    stage = _pick_stage_for_raw(wh, season)
    if stage is None:
        return None, ""

    fp = _raw_fingerprint(wh, season, stage)
    if fp is None:
        return None, ""

    cache_key = ("batting", season, stage)
    with _cache_lock:
        hit = _live_cache.get(cache_key)
        if hit is not None and hit[0] == fp:
            return hit[1].copy(), f"raw:{stage}"

    _ensure_repo_on_path()
    from src.ingestion.season_exports import player_team_boxscore_rows

    bat_rows, _ = player_team_boxscore_rows(wh, season, stage, disable_progress=True)
    if not bat_rows:
        return None, ""
    df = pd.DataFrame(bat_rows)
    df = _rollup_batting_by_player(df)
    _normalize_batting_columns(df)
    with _cache_lock:
        if len(_live_cache) >= _MAX_CACHE_ENTRIES:
            _live_cache.clear()
        _live_cache[cache_key] = (fp, df.copy())
    return df.copy(), f"raw:{stage}"


def _normalize_batting_columns(df) -> None:
    """Align CSV/live column names with legacy parquet expectations."""
    if "pa" not in df.columns and "plate_appearances" in df.columns:
        df["pa"] = df["plate_appearances"]
    if "hr" not in df.columns and "home_runs" in df.columns:
        df["hr"] = df["home_runs"]


def _load_pitching_frame(season: int):
    import pandas as pd

    wh = _warehouse()
    if _PREFER_RAW_LEADERBOARDS:
        df, src = _load_pitching_frame_from_raw(wh, season)
        if df is not None and not df.empty:
            return df, src

    pq = _find_parquet("pitching", season)
    if pq is not None:
        df = _read_parquet_dataframe(pq)
        if "season" in df.columns:
            df = df.loc[df["season"] == season].copy()
        if not df.empty:
            df = _dedupe_pitching_to_one_row_per_player(df)
            return df, "parquet"

    csv_path = _drive_csv("pitching", season)
    csv_stale_no_gs: Any = None
    if safe_non_empty_file(csv_path):
        cdf = pd.read_csv(csv_path)
        if "player_id" in cdf.columns and not cdf.empty:
            cdf = _rollup_pitching_by_player(cdf)
            if "games_started" in cdf.columns:
                return cdf, "csv"
            csv_stale_no_gs = cdf

    df, src = _load_pitching_frame_from_raw(wh, season)
    if df is not None and not df.empty:
        return df, src

    if csv_stale_no_gs is not None and not csv_stale_no_gs.empty:
        return csv_stale_no_gs.copy(), "csv"
    return None, ""


def _load_pitching_frame_from_raw(wh: Path, season: int):
    import pandas as pd

    stage = _pick_stage_for_raw(wh, season)
    if stage is None:
        return None, ""

    fp = _raw_fingerprint(wh, season, stage)
    if fp is None:
        return None, ""

    cache_key = ("pitching", season, stage, _PITCHING_LIVE_CACHE_VER)
    with _cache_lock:
        hit = _live_cache.get(cache_key)
        if hit is not None and hit[0] == fp:
            return hit[1].copy(), f"raw:{stage}"

    _ensure_repo_on_path()
    from src.ingestion.season_exports import player_team_boxscore_rows

    _, pit_rows = player_team_boxscore_rows(wh, season, stage, disable_progress=True)
    if pit_rows:
        df = pd.DataFrame(pit_rows)
        df = _rollup_pitching_by_player(df)
        with _cache_lock:
            if len(_live_cache) >= _MAX_CACHE_ENTRIES:
                _live_cache.clear()
            _live_cache[cache_key] = (fp, df.copy())
        return df.copy(), f"raw:{stage}"

    return None, ""


def _warehouse_env_hint() -> str:
    wh = _warehouse()
    wh_s = str(wh).replace("\\", "/").lower()
    if not safe_is_dir(wh):
        msg = (
            f" — warehouse path is missing or not a directory: {wh}. "
            f"Unset MLB_WAREHOUSE_DIR in mlbops/.env (default: {{repo_root}}/data/warehouse/mlb) "
            f"or set it to your real mirror, then restart the API."
        )
        if "cloudstorage" in wh_s or "my drive" in wh_s:
            msg += (
                " Google Drive for Desktop can time out until the folder is hydrated; "
                "open it in Finder or run ./scripts/pull_mlbops_from_drive.sh and point at data/warehouse/mlb."
            )
        return msg
    if "path/to" in wh_s:
        return (
            " — MLB_WAREHOUSE_DIR looks like a documentation placeholder; remove it or "
            "point to your actual data/warehouse/mlb folder, then restart the API."
        )
    return ""


def _no_batting_detail(season: int) -> str:
    wh = _warehouse()
    return (
        f"Batting leaders source not found for {season}. "
        f"Expected one of: "
        f"parquet matching player_season_boxscore_batting*{season}* under {wh}, "
        f"or {wh / str(season) / 'player_season_batting_by_team.csv'}, "
        f"or feed_live files under {wh / str(season) / 'regular_season' / 'raw'} "
        f"(or spring_training/raw). "
        f"Sync warehouse from Drive or run: "
        f"python scripts/export_season_drive_artifacts.py --season {season} --only player-box"
        f"{_warehouse_env_hint()}"
    )


def _ensure_player_name_column(df):
    """Some sources use ``name``; hub expects ``player_name``."""
    if "player_name" not in df.columns and "name" in df.columns:
        return df.assign(player_name=df["name"])
    return df


def _reorder_leader_columns(df):
    """Put identity columns first so hub tables show name next to rank without horizontal scroll."""
    priority = [
        "player_id",
        "player_name",
        "team_abbrev",
        "team_id",
        "season",
    ]
    cols = list(df.columns)
    front = [c for c in priority if c in cols]
    rest = [c for c in cols if c not in front]
    return df[front + rest]


def _leaders_records(df):
    """Ordered columns + JSON-safe scalars (numpy / NaN)."""
    import math

    df = _ensure_player_name_column(df)
    df = _reorder_leader_columns(df)
    out: list[dict] = []
    for row in df.to_dict(orient="records"):
        rec: dict = {}
        for k, v in row.items():
            if v is None:
                rec[k] = None
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                rec[k] = None
            elif hasattr(v, "item"):
                try:
                    rec[k] = v.item()
                except (ValueError, AttributeError):
                    rec[k] = v
            else:
                rec[k] = v
        out.append(rec)
    return out


def _no_pitching_detail(season: int) -> str:
    wh = _warehouse()
    return (
        f"Pitching leaders source not found for {season}. "
        f"Expected parquet, {wh / str(season) / 'player_season_pitching_by_team.csv'}, "
        f"or raw games under {wh / str(season) / 'regular_season' / 'raw'}. "
        f"See batting error detail for export command."
        f"{_warehouse_env_hint()}"
    )


def _batting_leaders_sync(
    season: int,
    sort_by: str,
    min_pa: int,
    limit: int,
    qualified: bool = False,
) -> dict[str, Any]:
    try:
        import pandas as pd  # noqa: F401
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas not available in this environment.")

    df, _src = _load_batting_frame(season)
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail=_no_batting_detail(season))

    qualification = _qualification_thresholds(season) if qualified else None
    effective_min_pa = max(min_pa, int((qualification or {}).get("min_pa") or 0))
    if "pa" in df.columns:
        df = df[df["pa"] >= effective_min_pa]
    elif "plateAppearances" in df.columns:
        df = df[df["plateAppearances"] >= effective_min_pa]
    elif "plate_appearances" in df.columns:
        df = df[df["plate_appearances"] >= effective_min_pa]

    if sort_by not in df.columns:
        available = list(df.columns)
        raise HTTPException(
            status_code=400,
            detail=f"Column '{sort_by}' not found. Available: {available}",
        )

    df = df.sort_values(sort_by, ascending=False).head(limit)
    return {
        "season": season,
        "sort_by": sort_by,
        "min_pa": effective_min_pa,
        "qualified": qualified,
        "qualification": qualification,
        "source": _src,
        "leaders": _leaders_records(df),
    }


@router.get("/batting")
async def batting_leaders(
    season: int = Query(2025),
    sort_by: str = Query("ops", description="Column to sort by"),
    min_pa: int = Query(
        1,
        ge=0,
        description="Minimum PA (use ~50 for qualified-leader style; low values for early season)",
    ),
    qualified: bool = Query(False, description="Use current qualified batting threshold: 3.1 PA per team game."),
    limit: int = Query(25, ge=1, le=100),
):
    return await run_in_threadpool(_batting_leaders_sync, season, sort_by, min_pa, limit, qualified)


def _apply_pitcher_role_filter(df, role: str):
    """Starters vs relievers using GS vs relief appearances (G − GS).

    Starter: games_started > (games - games_started). Reliever: games_started < (...).
    Ties (50/50) appear only when role is ``all``.
    Returns (filtered_df, applied: bool). If source lacks columns, returns (df, False).
    """
    if df is None or df.empty or role == "all":
        return df, True
    if "games" not in df.columns or "games_started" not in df.columns:
        return df, False
    g = df["games"].fillna(0).astype(float)
    gs = df["games_started"].fillna(0).astype(float)
    relief = (g - gs).clip(lower=0)
    if role == "starter":
        mask = gs > relief
    elif role == "reliever":
        mask = gs < relief
    else:
        return df, True
    return df.loc[mask].copy(), True


def pitching_pitcher_ids_for_role(season: int, role: str) -> tuple[Optional[set[int]], bool]:
    """Pitcher MLBAM IDs to include in Statcast views.

    Returns ``(ids, supported)``:
    - ``ids`` is ``None`` → do not filter (all pitchers).
    - ``ids`` is ``set()`` → filter applied but no pitchers qualify (empty Statcast).
    - ``supported`` is ``False`` when ``role`` is not ``all`` but boxscore source lacks
      ``games`` / ``games_started`` (caller should show all pitches + warning from leaders).
    """
    pr = (role or "all").strip().lower()
    if pr not in ("all", "starter", "reliever"):
        pr = "all"
    if pr == "all":
        return None, True

    df, _src = _load_pitching_frame(season)
    if df is None or df.empty:
        return None, False

    if ("games" not in df.columns and "gamesPlayed" in df.columns) or (
        "games_started" not in df.columns and "gamesStarted" in df.columns
    ):
        df = df.copy()
        if "games" not in df.columns and "gamesPlayed" in df.columns:
            df["games"] = df["gamesPlayed"]
        if "games_started" not in df.columns and "gamesStarted" in df.columns:
            df["games_started"] = df["gamesStarted"]

    if "games" not in df.columns or "games_started" not in df.columns:
        return None, False

    df2, applied = _apply_pitcher_role_filter(df, pr)
    if not applied:
        return None, False
    if df2.empty:
        return set(), True
    out: set[int] = set()
    for x in df2["player_id"].dropna().unique():
        try:
            out.add(int(float(x)))
        except (TypeError, ValueError):
            continue
    return out, True


def _pitching_leaders_sync(
    season: int,
    sort_by: str,
    min_ip: float,
    limit: int,
    ascending: bool,
    pitcher_role: str = "all",
    qualified: bool = False,
) -> dict[str, Any]:
    try:
        import pandas as pd  # noqa: F401
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas not available in this environment.")

    df, _src = _load_pitching_frame(season)
    if df is None or df.empty:
        raise HTTPException(status_code=404, detail=_no_pitching_detail(season))

    if ("games" not in df.columns and "gamesPlayed" in df.columns) or (
        "games_started" not in df.columns and "gamesStarted" in df.columns
    ):
        df = df.copy()
        if "games" not in df.columns and "gamesPlayed" in df.columns:
            df["games"] = df["gamesPlayed"]
        if "games_started" not in df.columns and "gamesStarted" in df.columns:
            df["games_started"] = df["gamesStarted"]

    pitcher_role_filter_supported = (
        "games" in df.columns and "games_started" in df.columns
    )

    qualification = _qualification_thresholds(season) if qualified else None
    effective_min_ip = max(float(min_ip), float((qualification or {}).get("min_ip") or 0.0))
    if "ip" in df.columns:
        df = df[df["ip"] >= effective_min_ip]
    elif "inningsPitched" in df.columns:
        df = df[df["inningsPitched"] >= effective_min_ip]

    role_note: str | None = None
    if pitcher_role and pitcher_role != "all":
        before = len(df)
        df, applied = _apply_pitcher_role_filter(df, pitcher_role)
        if not applied:
            role_note = (
                "games / games_started not available in this pitching source; "
                "showing all pitchers. Use player_season_boxscore_pitching parquet for SP/RP."
            )
        elif before > 0 and len(df) == 0:
            role_note = f"No pitchers match “{pitcher_role}” with the current IP minimum."

    if sort_by not in df.columns:
        available = list(df.columns)
        raise HTTPException(
            status_code=400,
            detail=f"Column '{sort_by}' not found. Available: {available}",
        )

    df = df.sort_values(sort_by, ascending=ascending).head(limit)
    out: dict[str, Any] = {
        "season": season,
        "sort_by": sort_by,
        "min_ip": effective_min_ip,
        "qualified": qualified,
        "qualification": qualification,
        "source": _src,
        "pitcher_role": pitcher_role or "all",
        "pitcher_role_filter_supported": pitcher_role_filter_supported,
        "leaders": _leaders_records(df),
    }
    if role_note:
        out["pitcher_role_note"] = role_note
    return out


@router.get("/pitching")
async def pitching_leaders(
    season: int = Query(2025),
    sort_by: str = Query("era", description="Column to sort by (lower = better for ERA)"),
    min_ip: float = Query(
        0.0,
        ge=0.0,
        description="Minimum IP (use ~20 for qualified style; 0 for early season)",
    ),
    limit: int = Query(25, ge=1, le=100),
    ascending: bool = Query(True, description="Sort ascending (True for ERA, False for K)"),
    pitcher_role: str = Query(
        "all",
        description="Pitcher bucket: all | starter | reliever (uses games_started vs relief apps)",
    ),
    qualified: bool = Query(False, description="Use current qualified pitching threshold: 1.0 IP per team game."),
):
    pr = (pitcher_role or "all").strip().lower()
    if pr not in ("all", "starter", "reliever"):
        raise HTTPException(
            status_code=400,
            detail="pitcher_role must be one of: all, starter, reliever",
        )
    return await run_in_threadpool(
        _pitching_leaders_sync,
        season,
        sort_by,
        min_ip,
        limit,
        ascending,
        pr,
        qualified,
    )
