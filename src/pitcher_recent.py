"""
Pitcher recent-start helpers from Statcast pitch frames (no matplotlib / no argparse).

Used by ``morning_intel`` for start-window pulse and anomaly detection.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def pitcher_game_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (pitcher, game_pk) with aggregate rates for that outing.

    Expects columns from ``morning_intel.enrich_pitch_features``:
    gd, game_pk, pitcher, release_speed, whiff, chase, bip,
    estimated_woba_using_speedangle, pitch_type.
    """
    if df.empty or "pitcher" not in df.columns or "game_pk" not in df.columns:
        return pd.DataFrame()
    x = df[df["pitcher"].notna() & df["game_pk"].notna()].copy()
    if x.empty:
        return pd.DataFrame()
    x["pid"] = pd.to_numeric(x["pitcher"], errors="coerce")
    x = x[x["pid"].notna()]
    x["gpk"] = pd.to_numeric(x["game_pk"], errors="coerce")
    x = x[x["gpk"].notna()]
    rows: list[dict] = []
    for (pid, gpk), gg in x.groupby(["pid", "gpk"], sort=False):
        n = int(len(gg))
        if n < 12:
            continue
        gdi = gg["gd"].max()
        velo = float(gg["release_speed"].mean()) if gg["release_speed"].notna().any() else float("nan")
        wh = float(gg["whiff"].mean() * 100) if "whiff" in gg.columns else float("nan")
        ch = float(gg["chase"].mean() * 100) if "chase" in gg.columns else float("nan")
        bip = gg[gg["bip"]] if "bip" in gg.columns else gg.iloc[0:0]
        xw = float(bip["estimated_woba_using_speedangle"].mean()) if len(bip) else float("nan")
        vc = gg["pitch_type"].astype(str).value_counts(normalize=True)
        top = str(vc.index[0]) if len(vc) else "UN"
        top_sh = float(vc.iloc[0]) * 100 if len(vc) else 0.0
        rows.append(
            {
                "pitcher": int(pid),
                "game_pk": int(gpk),
                "gd": gdi,
                "n_pitches": n,
                "avg_velo": velo,
                "whiff_pct": wh,
                "chase_pct": ch,
                "xwoba_bip": xw,
                "n_bip": int(len(bip)),
                "top_pitch_type": top,
                "top_pitch_share": top_sh,
            }
        )
    return pd.DataFrame(rows)


def pool_for_games(df: pd.DataFrame, pitcher_id: int, game_pks: set[int]) -> pd.DataFrame:
    m = pd.to_numeric(df["pitcher"], errors="coerce") == float(pitcher_id)
    gpk = pd.to_numeric(df["game_pk"], errors="coerce")
    return df[m & gpk.isin(game_pks)].copy()
