"""
Batter recent BBE / PA windows from Statcast pitch frames.

Used by ``morning_intel`` for watchlist pulse and anomaly detection.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _bid_match(s: pd.Series, bid: int) -> pd.Series:
    return pd.to_numeric(s, errors="coerce") == float(bid)


def ordered_bip_rows(df: pd.DataFrame, batter_id: int, anchor) -> pd.DataFrame:
    """BIP rows for batter at or before anchor, most recent BIP first."""
    sub = df[_bid_match(df["batter"], batter_id) & (df["gd"] <= anchor) & df["bip"]].copy()
    if sub.empty:
        return sub
    sub = sub.sort_values(["gd", "game_pk"], ascending=[False, False])
    return sub.reset_index(drop=True)


def bip_windows(sub: pd.DataFrame, n_recent: int = 10, n_base: int = 40) -> tuple[pd.DataFrame, pd.DataFrame]:
    if len(sub) < n_recent + n_base:
        return sub.iloc[0:0], sub.iloc[0:0]
    recent = sub.iloc[:n_recent]
    baseline = sub.iloc[n_recent : n_recent + n_base]
    return recent, baseline


def pa_windows_from_pitches(df: pd.DataFrame, batter_id: int, anchor, n_recent: int = 25, n_base: int = 75):
    """
    Approximate PA windows using distinct (game_pk, at_bat_number), chronological,
    then take the last ``n_recent`` vs the prior ``n_base`` PAs (all pitch rows for those PAs).
    """
    bat = df[_bid_match(df["batter"], batter_id) & (df["gd"] <= anchor)].copy()
    if bat.empty or "at_bat_number" not in bat.columns:
        return bat.iloc[0:0], bat.iloc[0:0]
    bat = bat.sort_values(["gd", "game_pk", "at_bat_number"])
    keys = bat.drop_duplicates(["game_pk", "at_bat_number"], keep="last")
    if len(keys) < n_recent + n_base:
        return bat.iloc[0:0], bat.iloc[0:0]
    recent_keys = keys.iloc[-n_recent:]
    base_keys = keys.iloc[-(n_recent + n_base) : -n_recent]
    rk = set(zip(recent_keys["game_pk"].astype(int), recent_keys["at_bat_number"].astype(int)))
    bk = set(zip(base_keys["game_pk"].astype(int), base_keys["at_bat_number"].astype(int)))

    def _in_set(row, keyset):
        try:
            return (int(row["game_pk"]), int(row["at_bat_number"])) in keyset
        except (TypeError, ValueError):
            return False

    rmask = bat.apply(lambda r: _in_set(r, rk), axis=1)
    bmask = bat.apply(lambda r: _in_set(r, bk), axis=1)
    return bat[rmask].copy(), bat[bmask].copy()


def summarize_bip_pool(bip_df: pd.DataFrame) -> dict:
    if bip_df.empty or "launch_speed" not in bip_df.columns:
        return {"n": 0}
    ev = bip_df["launch_speed"].astype(float)
    lsa = bip_df["launch_speed_angle"] if "launch_speed_angle" in bip_df.columns else None
    barrels = int((lsa == 6).sum()) if lsa is not None else 0
    xw = bip_df["estimated_woba_using_speedangle"]
    xw_m = float(xw.mean()) if xw.notna().any() else float("nan")
    return {
        "n": len(bip_df),
        "barrels": barrels,
        "barrel_pct": round(barrels / len(bip_df) * 100, 1) if len(bip_df) else 0.0,
        "avg_ev": round(float(ev.mean()), 2) if ev.notna().any() else None,
        "xwoba": round(xw_m, 3) if np.isfinite(xw_m) else None,
    }


def summarize_pa_pool(pa_df: pd.DataFrame) -> dict:
    """Light summary on all pitch rows in PA pool (contact rate proxy)."""
    if pa_df.empty:
        return {"n_pa_pitch_rows": 0}
    swings = pa_df["swing"].sum() if "swing" in pa_df.columns else 0
    n = len(pa_df)
    wh = pa_df["whiff"].sum() if "whiff" in pa_df.columns else 0
    return {
        "n_pa_pitch_rows": n,
        "swing_pct": round(swings / n * 100, 1) if n else None,
        "whiff_per_pitch": round(wh / n * 100, 1) if n else None,
    }
