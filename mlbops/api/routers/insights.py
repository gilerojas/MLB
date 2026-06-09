"""
Statcast-level insight bundles from pitches_enriched parquets.

GET /insights/statcast?season=&pitcher_role= — returns multiple insight snapshots computed
from pitch-level data:
  - fastball_whiff    FF/SI whiff% leaders (pitchers, min 30 pitches)
  - hardest_throwers  avg FF/SI release speed leaders (pitchers, min 30 pitches)
  - pitcher_luck      xwOBA allowed vs wOBA allowed — who's lucky / unlucky
  - exit_velocity     avg exit velocity leaders (batters, min 20 BIP)
  - barrel_leaders    barrel% leaders (batters, min 20 BIP)
  - farthest_home_runs longest HR by Statcast distance
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
import os
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from api.paths import get_warehouse_dir
from api.routers.leaderboards import _qualification_thresholds, pitching_pitcher_ids_for_role

router = APIRouter(prefix="/insights", tags=["insights"])

# ── column sets ──────────────────────────────────────────────────────────────

_WANT_COLS = {
    "pitcher",
    "batter",
    "player_name",   # batter name in this enrichment
    "game_pk",
    "game_date",
    "pitch_type",
    "pitch_name",
    "description",
    "events",
    "des",
    "release_speed",
    "release_spin_rate",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
    "launch_speed",
    "launch_angle",
    "launch_speed_angle",
    "hit_distance_sc",
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
_response_cache: dict[tuple[int, str, int, int, str, tuple[float, int]], dict[str, Any]] = {}
_cache_lock = Lock()
_STATCAST_CACHE_MAX_ENTRIES = max(1, int(os.environ.get("MLB_INSIGHTS_STATCAST_CACHE_MAX", "2")))
_RESPONSE_CACHE_MAX_ENTRIES = max(1, int(os.environ.get("MLB_INSIGHTS_RESPONSE_CACHE_MAX", "24")))


def _fingerprint(paths: list[Path]) -> tuple[float, int]:
    if not paths:
        return (0.0, 0)
    try:
        return (max(p.stat().st_mtime for p in paths), len(paths))
    except OSError:
        return (0.0, len(paths))


def _trim_cache(cache: dict, max_entries: int) -> None:
    """Bound insertion-ordered dict caches without dropping the current hot entry."""
    while len(cache) > max_entries:
        cache.pop(next(iter(cache)))


def _safe(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 3)
    except (TypeError, ValueError):
        return None


def _contact_rows(df, required: list[str] | tuple[str, ...] = ()) -> Any:
    """Rows that represent true batted-ball contact, not all PA outcomes."""
    if "description" in df.columns:
        mask = df["description"].astype(str).isin(
            {"hit_into_play", "hit_into_play_no_out", "hit_into_play_score"}
        )
    elif "launch_speed" in df.columns:
        mask = df["launch_speed"].notna()
    else:
        mask = df["woba_denom"].fillna(0) > 0

    out = df[mask].copy()
    if required:
        out = out.dropna(subset=list(required))
    if "launch_speed" in out.columns:
        out = out[(out["launch_speed"].isna()) | (out["launch_speed"] > 0)]
    return out


def _attach_sample_thresholds(rows: list[dict[str, Any]], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    return [dict(row, _sample_thresholds=thresholds) for row in rows]


def _sample_thresholds(season: int, min_pitches: int, min_bip: int) -> dict[str, Any]:
    """Season-progress sample floors for Statcast rate tiles."""
    q = _qualification_thresholds(season)
    games = int(q.get("team_games") or 0)
    bip_floor = max(int(min_bip), math.ceil(games * 1.5)) if games else int(min_bip)
    pitch_floor = max(int(min_pitches), math.ceil(games * 2.0)) if games else int(min_pitches)
    spin_floor = max(10, math.ceil(games * 1.25)) if games else 10
    tracked_floor = max(40, math.ceil(games * 1.5)) if games else 40
    pitch_type_floor = max(150, math.ceil(games * 3.0)) if games else 150
    return {
        "team_games": games,
        "bip": bip_floor,
        "bbe": bip_floor,
        "pitches": pitch_floor,
        "breaking_pitches": spin_floor,
        "tracked_swings": tracked_floor,
        "pitch_type_pitches": pitch_type_floor,
        "rules": {
            "bip": "max(requested min, 1.5 BIP/BBE per team game)",
            "pitches": "max(requested min, 2.0 pitches per team game)",
            "breaking_pitches": "max(10, 1.25 breaking pitches per team game)",
            "tracked_swings": "max(40, 1.5 tracked swings per team game)",
            "pitch_type_pitches": "max(150, 3.0 pitches per team game by pitch type)",
        },
    }


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
            return hit[1]

    dfs = []
    wanted = sorted(_WANT_COLS)
    for p in paths:
        try:
            df = pd.read_parquet(p, columns=wanted)
            dfs.append(df)
        except Exception:
            try:
                import pyarrow.parquet as pq

                available = set(pq.read_schema(p).names)
                keep = [c for c in wanted if c in available]
                if keep:
                    dfs.append(pd.read_parquet(p, columns=keep))
            except Exception:
                continue

    if not dfs:
        raise HTTPException(
            status_code=404, detail="Could not read any pitches_enriched parquets."
        )

    combined = pd.concat(dfs, ignore_index=True)

    with _cache_lock:
        _cache[key] = (fp, combined)
        _trim_cache(_cache, _STATCAST_CACHE_MAX_ENTRIES)

    return combined


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
    bip = _contact_rows(df)
    if bip.empty:
        return []
    g = (
        bip.groupby("pitcher")
        .agg(
            n_bip=("pitcher", "size"),
            xwoba=("estimated_woba_using_speedangle", "mean"),
            woba=("woba_value", "mean"),
            n_xwoba=("estimated_woba_using_speedangle", "count"),
            n_woba=("woba_value", "count"),
        )
        .reset_index()
    )
    g = g[(g["n_xwoba"] > 0) & (g["n_woba"] > 0)].copy()
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
    bip = _contact_rows(df)
    if bip.empty:
        return []
    g = (
        bip.groupby("batter")
        .agg(
            avg_ev=("launch_speed", "mean"),
            max_ev=("launch_speed", "max"),
            n=("batter", "size"),
            n_ev=("launch_speed", "count"),
        )
        .reset_index()
    )
    g = g[g["n_ev"] > 0].copy()
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
    bip = _contact_rows(df)
    if bip.empty:
        return []
    bip["is_barrel"] = bip["launch_speed_angle"] == 6
    g = (
        bip.groupby("batter")
        .agg(n=("batter", "size"), barrels=("is_barrel", "sum"))
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


def _farthest_home_runs(df, batter_names: dict, pitcher_names: dict) -> list[dict]:
    if "events" not in df.columns or "hit_distance_sc" not in df.columns:
        return []
    hrs = df[df["events"].astype(str).str.lower() == "home_run"].copy()
    if hrs.empty:
        return []
    import pandas as pd

    hrs["hit_distance_sc"] = pd.to_numeric(hrs["hit_distance_sc"], errors="coerce")
    hrs = hrs.dropna(subset=["hit_distance_sc"])
    hrs = hrs[hrs["hit_distance_sc"] > 0]
    if hrs.empty:
        return []
    hrs = hrs.sort_values("hit_distance_sc", ascending=False).head(10)

    out: list[dict] = []
    for r in hrs.itertuples():
        batter_id = int(r.batter) if not pd.isna(r.batter) else None
        pitcher_id = int(r.pitcher) if not pd.isna(r.pitcher) else None
        out.append(
            {
                "player_name": batter_names.get(batter_id, f"ID {batter_id}") if batter_id else "Unknown",
                "player_id": batter_id,
                "pitcher_name": pitcher_names.get(pitcher_id, f"ID {pitcher_id}") if pitcher_id else None,
                "pitcher_id": pitcher_id,
                "hit_distance": int(round(float(r.hit_distance_sc))),
                "launch_speed": _safe(getattr(r, "launch_speed", None)),
                "launch_angle": _safe(getattr(r, "launch_angle", None)),
                "game_date": str(getattr(r, "game_date", "")) or None,
                "game_pk": int(r.game_pk) if hasattr(r, "game_pk") and not pd.isna(r.game_pk) else None,
                "description": getattr(r, "des", None) if hasattr(r, "des") else None,
            }
        )
    return out


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
    bip = _contact_rows(df)
    if bip.empty:
        return []
    g = (
        bip.groupby("batter")
        .agg(
            xwoba=("estimated_woba_using_speedangle", "mean"),
            n=("batter", "size"),
            n_xwoba=("estimated_woba_using_speedangle", "count"),
        )
        .reset_index()
    )
    g = g[g["n_xwoba"] > 0].copy()
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
    bip = _contact_rows(df)
    if bip.empty:
        return []
    g = (
        bip.groupby("batter")
        .agg(
            n_bip=("batter", "size"),
            xwoba=("estimated_woba_using_speedangle", "mean"),
            woba=("woba_value", "mean"),
            n_xwoba=("estimated_woba_using_speedangle", "count"),
            n_woba=("woba_value", "count"),
        )
        .reset_index()
    )
    g = g[(g["n_xwoba"] > 0) & (g["n_woba"] > 0)].copy()
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

    wh = get_warehouse_dir()
    paths = sorted(wh.glob(f"{season}/{stage}/pitches_enriched/*.parquet"))
    fp = _fingerprint(paths)
    response_key = (season, stage, min_pitches, min_bip, pr, fp)
    with _cache_lock:
        cached = _response_cache.get(response_key)
        if cached is not None:
            return cached

    df_all = _load_statcast_df(season, stage)
    df = df_all
    pit_ids, role_supported = pitching_pitcher_ids_for_role(season, pr)
    if pit_ids is not None:
        if len(pit_ids) == 0:
            df = df.iloc[0:0].copy()
        else:
            pid = pd.to_numeric(df["pitcher"], errors="coerce")
            df = df[pid.isin(list(pit_ids))].copy()

    pitcher_names = _load_player_registry(season)
    batter_names = _batter_name_map(df_all)
    thresholds = _sample_thresholds(season, min_pitches, min_bip)

    rv_edges = _pitch_rv100_edges(df, pitcher_names, min_pitches=thresholds["pitch_type_pitches"])

    bundles = {
        "fastball_whiff": _fastball_whiff(df, pitcher_names, thresholds["pitches"]),
        "hardest_throwers": _hardest_throwers(df, pitcher_names, thresholds["pitches"]),
        "pitcher_luck": _pitcher_luck(df, pitcher_names, thresholds["bip"]),
        "exit_velocity": _exit_velocity(df_all, batter_names, thresholds["bip"]),
        "barrel_leaders": _barrel_leaders(df_all, batter_names, thresholds["bip"]),
        "farthest_home_runs": _farthest_home_runs(df_all, batter_names, pitcher_names),
        "spin_rate": _spin_rate(df, pitcher_names, thresholds["breaking_pitches"]),
        "chase_kings": _chase_kings(df, pitcher_names, thresholds["pitches"]),
        "bs75_leaders": _bs75_leaders(df, pitcher_names, min_tracked_swings=thresholds["tracked_swings"]),
        "pitch_rv100_best": rv_edges["best"],
        "pitch_rv100_worst": rv_edges["worst"],
        "batter_xwoba": _batter_xwoba(df_all, batter_names, min_bip=thresholds["bbe"]),
        "batter_luck": _batter_luck(df_all, batter_names, min_bip=thresholds["bbe"]),
    }
    bundles = {key: _attach_sample_thresholds(rows, thresholds) for key, rows in bundles.items()}

    out = {
        "season": season,
        "stage": stage,
        "pitcher_role": pr,
        "pitcher_role_filter_supported": role_supported,
        "n_pitches_total": len(df_all),
        "sample_thresholds": thresholds,
        "bundles": bundles,
    }
    with _cache_lock:
        _response_cache[response_key] = out
        _trim_cache(_response_cache, _RESPONSE_CACHE_MAX_ENTRIES)
    return out


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
