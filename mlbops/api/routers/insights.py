"""
Statcast-level insight bundles from pitches_enriched parquets.

GET /insights/statcast?season=&pitcher_role= — returns multiple insight snapshots computed
from pitch-level data:
  - fastball_whiff    FF/SI whiff% leaders (pitchers, min 30 pitches)
  - hardest_throwers  avg FF/SI release speed leaders (pitchers, min 30 pitches)
  - pitcher_luck      xwOBA allowed vs wOBA allowed — who's lucky / unlucky
  - exit_velocity     avg exit velocity leaders (batters, min 20 BIP)
  - barrel_leaders    barrel% leaders (batters, min 20 BIP)
  - spin_rate         highest avg spin rate — breaking balls (pitchers, min 20 pitches)
  - chase_kings       out-of-zone swing% leaders (pitchers, min 30 pitches)
  - bs75_leaders        BS75+% on tracked swings (pitchers, min 40 swings)
  - pitch_rv100_best    best (pitcher, pitch_type) by RV/100 (min 150 pitches)
  - pitch_rv100_worst   worst rows by RV/100
  - batter_xwoba        xwOBA on BIP leaders (batters, min 25 BBE)
  - batter_luck         wOBA vs xwOBA on BIP (lucky / unlucky batters)

Pitcher-only bundles honor ``pitcher_role`` (starter/reliever) using the same
``games`` / ``games_started`` boxscore rollup as ``/leaderboards/pitching``.
Batter bundles are unchanged.

NOTE on player_name column in pitches_enriched:
  The enriched parquets store the BATTER name in player_name (Statcast convention for
  the active hitter). Pitcher names are resolved via players_registry.json for the season.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from api.paths import get_warehouse_dir
from api.routers.leaderboards import pitching_pitcher_ids_for_role

router = APIRouter(prefix="/insights", tags=["insights"])

# ── column sets ──────────────────────────────────────────────────────────────

_WANT_COLS = {
    "pitcher",
    "batter",
    "player_name",   # batter name in this enrichment
    "pitch_type",
    "pitch_name",
    "description",
    "release_speed",
    "release_spin_rate",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
    "launch_speed",
    "launch_speed_angle",
    "zone",
    "bb_type",
    "bat_speed",
    "delta_run_exp",
}

_WHIFF_DESCS = frozenset({"swinging_strike", "swinging_strike_blocked", "foul_tip"})
_SWING_DESCS = frozenset({
    "swinging_strike", "swinging_strike_blocked", "foul_tip",
    "foul", "foul_bunt", "missed_bunt",
    "hit_into_play", "hit_into_play_no_out", "hit_into_play_score",
})
_FASTBALL_TYPES = frozenset({"FF", "SI"})
_BREAKING_TYPES = frozenset({"SL", "CU", "KC", "ST", "SV", "CS"})
_CHASE_ZONES = frozenset(range(11, 15))  # 11-14 = out of strike zone

# ── in-process cache ─────────────────────────────────────────────────────────

_cache: dict[tuple[int, str], tuple[tuple[float, int], Any]] = {}
_cache_lock = Lock()


def _fingerprint(paths: list[Path]) -> tuple[float, int]:
    if not paths:
        return (0.0, 0)
    try:
        return (max(p.stat().st_mtime for p in paths), len(paths))
    except OSError:
        return (0.0, len(paths))


def _safe(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 3)
    except (TypeError, ValueError):
        return None


# ── player name resolution ────────────────────────────────────────────────────

def _load_player_registry(season: int) -> dict[int, str]:
    """
    Build a {player_id: fullName} map from all available registry files.
    MLB MLBAM IDs are stable across seasons — a player absent from the
    requested season's file (or if that file is missing) will still be
    resolved from any other season's registry.
    """
    wh = get_warehouse_dir()
    combined: dict[int, str] = {}
    # Collect all registry paths; prioritise requested season last so it wins
    reg_paths = sorted(wh.glob("*/players_registry.json"))
    # Ensure the season-specific file (if present) is appended last
    season_path = wh / str(season) / "players_registry.json"
    if season_path in reg_paths:
        reg_paths.remove(season_path)
        reg_paths.append(season_path)
    for reg_path in reg_paths:
        try:
            raw = json.loads(reg_path.read_text())
            for k, v in raw.items():
                combined[int(k)] = v.get("fullName", f"ID {k}")
        except Exception:
            continue
    return combined


def _batter_name_map(df) -> dict[int, str]:
    """player_name col = batter name in this enrichment. Build batter_id → name dict."""
    if "player_name" not in df.columns or "batter" not in df.columns:
        return {}
    return (
        df[["batter", "player_name"]]
        .dropna()
        .drop_duplicates("batter")
        .set_index("batter")["player_name"]
        .to_dict()
    )


# ── data loading ─────────────────────────────────────────────────────────────

def _load_statcast_df(season: int, stage: str):
    """Load + cache all pitches_enriched parquets for the given season/stage."""
    try:
        import pandas as pd
    except ImportError:
        raise HTTPException(status_code=500, detail="pandas not installed.")

    wh = get_warehouse_dir()
    paths = sorted(wh.glob(f"{season}/{stage}/pitches_enriched/*.parquet"))
    if not paths:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No pitches_enriched parquets found for {season}/{stage}. "
                "Run daily ingest to populate Statcast data."
            ),
        )

    fp = _fingerprint(paths)
    key = (season, stage)

    with _cache_lock:
        hit = _cache.get(key)
        if hit is not None and hit[0] == fp:
            return hit[1].copy()

    dfs = []
    for p in paths:
        try:
            df = pd.read_parquet(p)
            keep = [c for c in _WANT_COLS if c in df.columns]
            dfs.append(df[keep])
        except Exception:
            continue

    if not dfs:
        raise HTTPException(
            status_code=404, detail="Could not read any pitches_enriched parquets."
        )

    combined = pd.concat(dfs, ignore_index=True)

    with _cache_lock:
        if len(_cache) > 8:
            _cache.clear()
        _cache[key] = (fp, combined)

    return combined.copy()


# ── insight computations ──────────────────────────────────────────────────────
# NOTE: pitcher groupbys use only "pitcher" (int ID); names resolved via registry.
#       batter groupbys use "batter" (int ID); names from player_name col in df.

def _fastball_whiff(df, pitcher_names: dict, min_pitches: int) -> list[dict]:
    fb = df[df["pitch_type"].isin(_FASTBALL_TYPES)].copy()
    if fb.empty:
        return []
    fb["is_whiff"] = fb["description"].isin(_WHIFF_DESCS)
    g = (
        fb.groupby("pitcher")
        .agg(n=("pitch_type", "count"), whiffs=("is_whiff", "sum"))
        .reset_index()
    )
    g = g[g["n"] >= min_pitches].copy()
    g["whiff_pct"] = (g["whiffs"] / g["n"] * 100).round(1)
    g = g.sort_values("whiff_pct", ascending=False).head(10)
    return [
        {
            "player_name": pitcher_names.get(int(r.pitcher), f"ID {int(r.pitcher)}"),
            "player_id": int(r.pitcher),
            "whiff_pct": _safe(r.whiff_pct),
            "n_pitches": int(r.n),
        }
        for r in g.itertuples()
    ]


def _hardest_throwers(df, pitcher_names: dict, min_pitches: int) -> list[dict]:
    fb = df[df["pitch_type"].isin(_FASTBALL_TYPES)].dropna(subset=["release_speed"])
    if fb.empty:
        return []
    g = (
        fb.groupby("pitcher")["release_speed"]
        .agg(["mean", "max", "count"])
        .reset_index()
    )
    g.columns = ["pitcher", "avg_velo", "max_velo", "n"]
    g = g[g["n"] >= min_pitches].copy()
    g = g.sort_values("avg_velo", ascending=False).head(10)
    return [
        {
            "player_name": pitcher_names.get(int(r.pitcher), f"ID {int(r.pitcher)}"),
            "player_id": int(r.pitcher),
            "avg_velo": _safe(r.avg_velo),
            "max_velo": _safe(r.max_velo),
            "n_pitches": int(r.n),
        }
        for r in g.itertuples()
    ]


def _pitcher_luck(df, pitcher_names: dict, min_bip: int) -> list[dict]:
    """
    luck_delta = xwOBA_allowed - wOBA_allowed
    Positive → pitcher getting away with bad contact quality (LUCKY)
    Negative → pitcher giving up more than contact quality predicts (UNLUCKY)
    """
    if "estimated_woba_using_speedangle" not in df.columns or "woba_value" not in df.columns:
        return []
    bip = df[df["woba_denom"].fillna(0) > 0].dropna(
        subset=["estimated_woba_using_speedangle", "woba_value"]
    )
    if bip.empty:
        return []
    g = (
        bip.groupby("pitcher")
        .agg(
            n_bip=("woba_denom", "count"),
            xwoba=("estimated_woba_using_speedangle", "mean"),
            woba=("woba_value", "mean"),
        )
        .reset_index()
    )
    g = g[g["n_bip"] >= min_bip].copy()
    g["luck_delta"] = (g["xwoba"] - g["woba"]).round(3)
    # Return top 10 luckiest + top 10 unluckiest so UI always has both halves
    top_lucky = g.sort_values("luck_delta", ascending=False).head(10)
    top_unlucky = g.sort_values("luck_delta", ascending=True).head(10)
    import pandas as pd
    combined = pd.concat([top_lucky, top_unlucky]).drop_duplicates("pitcher")
    return [
        {
            "player_name": pitcher_names.get(int(r.pitcher), f"ID {int(r.pitcher)}"),
            "player_id": int(r.pitcher),
            "xwoba_allowed": _safe(r.xwoba),
            "woba_allowed": _safe(r.woba),
            "luck_delta": _safe(r.luck_delta),
            "n_bip": int(r.n_bip),
        }
        for r in combined.itertuples()
    ]


def _exit_velocity(df, batter_names: dict, min_bip: int) -> list[dict]:
    if "launch_speed" not in df.columns:
        return []
    bip = df.dropna(subset=["launch_speed"])
    bip = bip[bip["launch_speed"] > 0]
    if bip.empty:
        return []
    g = (
        bip.groupby("batter")["launch_speed"]
        .agg(["mean", "max", "count"])
        .reset_index()
    )
    g.columns = ["batter", "avg_ev", "max_ev", "n"]
    g = g[g["n"] >= min_bip].copy()
    g = g.sort_values("avg_ev", ascending=False).head(10)
    return [
        {
            "player_name": batter_names.get(int(r.batter), f"ID {int(r.batter)}"),
            "player_id": int(r.batter),
            "avg_ev": _safe(r.avg_ev),
            "max_ev": _safe(r.max_ev),
            "n_bip": int(r.n),
        }
        for r in g.itertuples()
    ]


def _barrel_leaders(df, batter_names: dict, min_bip: int) -> list[dict]:
    if "launch_speed_angle" not in df.columns:
        return []
    bip = df[df["woba_denom"].fillna(0) > 0].copy()
    if bip.empty:
        return []
    bip["is_barrel"] = bip["launch_speed_angle"] == 6
    g = (
        bip.groupby("batter")
        .agg(n=("woba_denom", "count"), barrels=("is_barrel", "sum"))
        .reset_index()
    )
    g = g[g["n"] >= min_bip].copy()
    g["barrel_pct"] = (g["barrels"] / g["n"] * 100).round(1)
    g = g.sort_values("barrel_pct", ascending=False).head(10)
    return [
        {
            "player_name": batter_names.get(int(r.batter), f"ID {int(r.batter)}"),
            "player_id": int(r.batter),
            "barrel_pct": _safe(r.barrel_pct),
            "barrels": int(r.barrels),
            "n_bip": int(r.n),
        }
        for r in g.itertuples()
    ]


def _spin_rate(df, pitcher_names: dict, min_pitches: int) -> list[dict]:
    if "release_spin_rate" not in df.columns:
        return []
    breaking = df[df["pitch_type"].isin(_BREAKING_TYPES)].dropna(subset=["release_spin_rate"])
    if breaking.empty:
        return []
    g = (
        breaking.groupby(["pitcher", "pitch_type"])["release_spin_rate"]
        .agg(["mean", "count"])
        .reset_index()
    )
    g.columns = ["pitcher", "pitch_type", "avg_spin", "n"]
    g = g[g["n"] >= min_pitches].copy()
    g = g.sort_values("avg_spin", ascending=False).head(10)
    return [
        {
            "player_name": pitcher_names.get(int(r.pitcher), f"ID {int(r.pitcher)}"),
            "player_id": int(r.pitcher),
            "pitch_type": r.pitch_type,
            "avg_spin": _safe(r.avg_spin),
            "n_pitches": int(r.n),
        }
        for r in g.itertuples()
    ]


def _bs75_leaders(df, pitcher_names: dict, min_tracked_swings: int = 40) -> list[dict]:
    """
    Tracked swings: any swing outcome with non-null bat_speed.
    BS75+% = share of those swings where bat_speed >= 75 mph.
    """
    if "bat_speed" not in df.columns or "description" not in df.columns:
        return []
    sw = df[df["description"].isin(_SWING_DESCS)].dropna(subset=["bat_speed"])
    if sw.empty:
        return []
    sw = sw.copy()
    sw["fast_swing"] = sw["bat_speed"] >= 75.0
    g = (
        sw.groupby("pitcher")
        .agg(n=("bat_speed", "count"), fast=("fast_swing", "sum"))
        .reset_index()
    )
    g = g[g["n"] >= min_tracked_swings].copy()
    g["bs75_pct"] = (g["fast"] / g["n"] * 100).round(1)
    g = g.sort_values("bs75_pct", ascending=False).head(10)
    return [
        {
            "player_name": pitcher_names.get(int(r.pitcher), f"ID {int(r.pitcher)}"),
            "player_id": int(r.pitcher),
            "bs75_pct": _safe(r.bs75_pct),
            "n_tracked_swings": int(r.n),
            "n_fast_swings": int(r.fast),
        }
        for r in g.itertuples()
    ]


def _pitch_rv100_edges(df, pitcher_names: dict, min_pitches: int = 150) -> dict[str, list[dict]]:
    """
    League-wide (pitcher, pitch_type) RV/100 from Statcast delta_run_exp (batter-side runs).
    rv100 = -sum(delta_run_exp) / n * 100  (higher = better for the pitcher).
    """
    if "delta_run_exp" not in df.columns or "pitch_type" not in df.columns:
        return {"best": [], "worst": []}
    import pandas as pd

    pit = df.dropna(subset=["delta_run_exp", "pitch_type"]).copy()
    pit = pit[pit["pitch_type"].astype(str).str.len() > 0]
    if pit.empty:
        return {"best": [], "worst": []}
    g = (
        pit.groupby(["pitcher", "pitch_type"])
        .agg(
            n=("delta_run_exp", "count"),
            rv_sum=("delta_run_exp", "sum"),
            avg_velo=("release_speed", "mean"),
        )
        .reset_index()
    )
    g = g[g["n"] >= min_pitches].copy()
    g["rv100"] = (-g["rv_sum"] / g["n"] * 100).round(2)

    if "pitch_name" in pit.columns:

        def _mode_pitch_name(s) -> Optional[str]:
            s2 = s.dropna().astype(str)
            if s2.empty:
                return None
            return s2.value_counts().index[0]

        pn = (
            pit.groupby(["pitcher", "pitch_type"])["pitch_name"]
            .agg(_mode_pitch_name)
            .reset_index(name="pitch_name")
        )
        g = g.merge(pn, on=["pitcher", "pitch_type"], how="left")
    else:
        g["pitch_name"] = None

    def row_dict(row) -> dict:
        import pandas as pd

        pid = int(row["pitcher"])
        ptype = str(row["pitch_type"])
        pn = row["pitch_name"] if "pitch_name" in row.index else None
        pn_out = None if pn is None or pd.isna(pn) else str(pn)
        return {
            "player_name": pitcher_names.get(pid, f"ID {pid}"),
            "player_id": pid,
            "pitch_type": ptype,
            "pitch_name": pn_out,
            "rv100": _safe(row["rv100"]),
            "n_pitches": int(row["n"]),
            "avg_velo": _safe(row["avg_velo"]),
        }

    best = g.sort_values("rv100", ascending=False).head(10)
    worst = g.sort_values("rv100", ascending=True).head(10)
    return {
        "best": [row_dict(row) for _, row in best.iterrows()],
        "worst": [row_dict(row) for _, row in worst.iterrows()],
    }


def _batter_xwoba(df, batter_names: dict, min_bip: int = 25) -> list[dict]:
    if "estimated_woba_using_speedangle" not in df.columns:
        return []
    bip = df[df["woba_denom"].fillna(0) > 0].dropna(subset=["estimated_woba_using_speedangle"])
    if bip.empty:
        return []
    g = (
        bip.groupby("batter")["estimated_woba_using_speedangle"]
        .agg(["mean", "count"])
        .reset_index()
    )
    g.columns = ["batter", "xwoba", "n"]
    g = g[g["n"] >= min_bip].copy()
    g = g.sort_values("xwoba", ascending=False).head(10)
    return [
        {
            "player_name": batter_names.get(int(r.batter), f"ID {int(r.batter)}"),
            "player_id": int(r.batter),
            "xwoba": _safe(r.xwoba),
            "n_bip": int(r.n),
        }
        for r in g.itertuples()
    ]


def _batter_luck(df, batter_names: dict, min_bip: int = 25) -> list[dict]:
    """
    luck_delta = mean wOBA on BIP - mean xwOBA on BIP.
    Positive → outcomes better than contact quality (lucky at the plate).
    """
    if "estimated_woba_using_speedangle" not in df.columns or "woba_value" not in df.columns:
        return []
    bip = df[df["woba_denom"].fillna(0) > 0].dropna(
        subset=["estimated_woba_using_speedangle", "woba_value"]
    )
    if bip.empty:
        return []
    g = (
        bip.groupby("batter")
        .agg(
            n_bip=("woba_denom", "count"),
            xwoba=("estimated_woba_using_speedangle", "mean"),
            woba=("woba_value", "mean"),
        )
        .reset_index()
    )
    g = g[g["n_bip"] >= min_bip].copy()
    g["luck_delta"] = (g["woba"] - g["xwoba"]).round(3)
    top_lucky = g.sort_values("luck_delta", ascending=False).head(10)
    top_unlucky = g.sort_values("luck_delta", ascending=True).head(10)
    import pandas as pd
    combined = pd.concat([top_lucky, top_unlucky]).drop_duplicates("batter")
    return [
        {
            "player_name": batter_names.get(int(r.batter), f"ID {int(r.batter)}"),
            "player_id": int(r.batter),
            "xwoba_bip": _safe(r.xwoba),
            "woba_bip": _safe(r.woba),
            "luck_delta": _safe(r.luck_delta),
            "n_bip": int(r.n_bip),
        }
        for r in combined.itertuples()
    ]


def _chase_kings(df, pitcher_names: dict, min_pitches: int) -> list[dict]:
    if "zone" not in df.columns:
        return []
    ooz = df[df["zone"].isin(_CHASE_ZONES)].copy()
    if ooz.empty:
        return []
    ooz["is_swing"] = ooz["description"].isin(_SWING_DESCS)
    g = (
        ooz.groupby("pitcher")
        .agg(n=("description", "count"), chases=("is_swing", "sum"))
        .reset_index()
    )
    g = g[g["n"] >= min_pitches].copy()
    g["chase_pct"] = (g["chases"] / g["n"] * 100).round(1)
    g = g.sort_values("chase_pct", ascending=False).head(10)
    return [
        {
            "player_name": pitcher_names.get(int(r.pitcher), f"ID {int(r.pitcher)}"),
            "player_id": int(r.pitcher),
            "chase_pct": _safe(r.chase_pct),
            "n_pitches": int(r.n),
        }
        for r in g.itertuples()
    ]


# ── endpoint ──────────────────────────────────────────────────────────────────


def _statcast_insights_sync(
    season: int,
    stage: str,
    min_pitches: int,
    min_bip: int,
    pitcher_role: str = "all",
) -> dict[str, Any]:
    """Heavy pandas work — run via run_in_threadpool so it does not block the event loop."""
    import pandas as pd

    pr = (pitcher_role or "all").strip().lower()
    if pr not in ("all", "starter", "reliever"):
        pr = "all"

    df = _load_statcast_df(season, stage)
    pit_ids, role_supported = pitching_pitcher_ids_for_role(season, pr)
    if pit_ids is not None:
        if len(pit_ids) == 0:
            df = df.iloc[0:0].copy()
        else:
            pid = pd.to_numeric(df["pitcher"], errors="coerce")
            df = df[pid.isin(list(pit_ids))].copy()

    pitcher_names = _load_player_registry(season)
    batter_names = _batter_name_map(df)

    rv_edges = _pitch_rv100_edges(df, pitcher_names, min_pitches=150)

    return {
        "season": season,
        "stage": stage,
        "pitcher_role": pr,
        "pitcher_role_filter_supported": role_supported,
        "n_pitches_total": len(df),
        "bundles": {
            "fastball_whiff": _fastball_whiff(df, pitcher_names, min_pitches),
            "hardest_throwers": _hardest_throwers(df, pitcher_names, min_pitches),
            "pitcher_luck": _pitcher_luck(df, pitcher_names, min_bip),
            "exit_velocity": _exit_velocity(df, batter_names, min_bip),
            "barrel_leaders": _barrel_leaders(df, batter_names, min_bip),
            "spin_rate": _spin_rate(df, pitcher_names, 10),
            "chase_kings": _chase_kings(df, pitcher_names, min_pitches),
            "bs75_leaders": _bs75_leaders(df, pitcher_names, min_tracked_swings=40),
            "pitch_rv100_best": rv_edges["best"],
            "pitch_rv100_worst": rv_edges["worst"],
            "batter_xwoba": _batter_xwoba(df, batter_names, min_bip=max(25, min_bip)),
            "batter_luck": _batter_luck(df, batter_names, min_bip=max(25, min_bip)),
        },
    }


@router.get("/statcast")
async def statcast_insights(
    season: int = Query(default=2026, description="Season year"),
    stage: str = Query(default="regular_season", description="Game stage"),
    min_pitches: int = Query(default=20, ge=3, le=200),
    min_bip: int = Query(default=8, ge=3, le=100),
    pitcher_role: str = Query(
        "all",
        description="Filter pitcher-only bundles: all | starter | reliever (same boxscore GS logic as /leaderboards/pitching)",
    ),
):
    """
    Returns all Statcast insight bundles for the season in one request.
    Cached per (season, stage, file fingerprint) — auto-invalidates when new games arrive.
    """
    pr = (pitcher_role or "all").strip().lower()
    if pr not in ("all", "starter", "reliever"):
        raise HTTPException(
            status_code=400,
            detail="pitcher_role must be one of: all, starter, reliever",
        )
    return await run_in_threadpool(
        _statcast_insights_sync,
        season,
        stage,
        min_pitches,
        min_bip,
        pr,
    )
