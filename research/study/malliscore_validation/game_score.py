"""Game Score benchmarks for the MalliScore validation study.

Two independent reference metrics for a single starting-pitcher outing. They are
benchmarks, never optimization targets: MalliScore that agrees with them perfectly
adds nothing, and MalliScore that disagrees wildly does not describe recognizable
outing quality. The useful result is strong agreement with interpretable exceptions.

Game Score v1 (Bill James, 1988) is lifted from the existing duplicated
implementations in `mlbops/api/intel_standouts.py:113` and
`scripts/pitcher_game_bat_speed_outcomes.py:92` so the study has a single source.

Game Score v2 (Tom Tango, 2016) does not exist anywhere in the repo and is
implemented here. It drops the arbitrary 50-point baseline, values outs uniformly
rather than rewarding innings past the fourth, and prices home runs explicitly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Tango's v2 coefficients, stated once so tests and docs can reference them.
GSV2_BASE = 40.0
GSV2_PER_OUT = 2.0
GSV2_PER_K = 1.0
GSV2_PER_BB = -2.0
GSV2_PER_HIT = -2.0
GSV2_PER_RUN = -3.0
GSV2_PER_HR = -6.0


def game_score_v1(
    outs: float,
    strikeouts: float,
    hits: float,
    earned_runs: float,
    unearned_runs: float,
    walks: float,
) -> float:
    """Bill James Game Score. 50 baseline, bonus for innings completed past the 4th."""
    if outs is None or not np.isfinite(outs) or outs <= 0:
        return np.nan
    full_innings = int(outs // 3)
    return float(
        50
        + outs
        + 2 * max(0, full_innings - 4)
        + strikeouts
        - 2 * hits
        - 4 * earned_runs
        - 2 * unearned_runs
        - walks
    )


def game_score_v2(
    outs: float,
    strikeouts: float,
    walks: float,
    hits: float,
    runs: float,
    home_runs: float,
) -> float:
    """Tom Tango Game Score v2. `runs` is total runs allowed, not earned runs.

    A home run is charged three times over -- as a hit, as a run, and as a home run --
    which is deliberate: it is the single most damaging outcome a pitcher allows.
    """
    if outs is None or not np.isfinite(outs) or outs <= 0:
        return np.nan
    return float(
        GSV2_BASE
        + GSV2_PER_OUT * outs
        + GSV2_PER_K * strikeouts
        + GSV2_PER_BB * walks
        + GSV2_PER_HIT * hits
        + GSV2_PER_RUN * runs
        + GSV2_PER_HR * home_runs
    )


def add_game_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized Game Score v1 and v2 over an aggregated starter-outing frame.

    Expects `outs`, `strikeouts`, `walks`, `hits`, `home_runs`, `earned_runs` and
    `runs`. Where `runs` is absent, earned runs stand in and `gs_v2_runs_imputed`
    marks the row so downstream analysis can exclude or caveat it.
    """
    out = df.copy()
    num = lambda c, d=0.0: pd.to_numeric(out.get(c), errors="coerce").fillna(d)  # noqa: E731

    outs = pd.to_numeric(out["outs"], errors="coerce")
    k, bb, h = num("strikeouts"), num("walks"), num("hits")
    hr, er = num("home_runs"), pd.to_numeric(out["earned_runs"], errors="coerce")

    if "runs" in out.columns:
        runs = pd.to_numeric(out["runs"], errors="coerce")
        out["gs_v2_runs_imputed"] = runs.isna() & er.notna()
        runs = runs.where(runs.notna(), er)
    else:
        runs = er
        out["gs_v2_runs_imputed"] = er.notna()

    unearned = (runs - er).clip(lower=0)
    full_innings = np.floor(outs / 3)

    valid = outs.gt(0)
    out["game_score_v1"] = (
        50 + outs + 2 * np.maximum(0, full_innings - 4) + k - 2 * h - 4 * er - 2 * unearned - bb
    ).where(valid)
    out["game_score_v2"] = (
        GSV2_BASE
        + GSV2_PER_OUT * outs
        + GSV2_PER_K * k
        + GSV2_PER_BB * bb
        + GSV2_PER_HIT * h
        + GSV2_PER_RUN * runs
        + GSV2_PER_HR * hr
    ).where(valid)
    out["runs_allowed"] = runs
    out["unearned_runs"] = unearned
    return out
