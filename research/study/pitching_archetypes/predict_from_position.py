"""Phase 5 (L4) -- does position on the continuum predict anything?

The question the study exists for. L3 refused the archetype *label* but kept the
map: pitchers have stable coordinates on a small number of interpretable axes.
Coordinates are only worth carrying if they condition something.

Every target is measured for split-half reliability before any model is fit.
This is the step that makes a null result interpretable. A target that is mostly
noise cannot be predicted by anything, and failing to predict it says nothing
about the coordinates -- it only restates that the target is noise. Reliability
is the ceiling any model could reach, so a model is judged against *that*, not
against 1.0.

Targets fall in two groups, and the distinction is the point:

  conditioning targets  platoon_split, tto_penalty. Differences of two noisy
                        means. These are the *interesting* ones -- they ask
                        whether arsenal shape governs how a pitcher's edge
                        changes by matchup or by time through the order.
  descriptive targets   swstr_rate, gb_rate, k_rate, bb_rate. Stable single
                        rates. These mostly confirm the coordinates carry real
                        information; note that shape predicting ground balls is
                        partly mechanical (sinkers induce grounders), so a hit
                        here is a sanity check, not a discovery.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/predict_from_position.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import RepeatedKFold, cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.pitching_archetypes.cluster_pitchers import (  # noqa: E402
    OUT, SEASONS, load_matrix,
)
from research.study.pitching_archetypes.features import SEED  # noqa: E402

CACHE = ROOT / "research/study/.cache/archetype_pitches_2024-2025-2026.parquet"
N_PCA = 10
MIN_PA_PER_SPLIT = 80

WHIFFS = {"swinging_strike", "swinging_strike_blocked", "foul_tip"}
CONDITIONING = ["platoon_split", "tto_penalty"]
DESCRIPTIVE = ["swstr_rate", "gb_rate", "k_rate", "bb_rate"]
TARGETS = CONDITIONING + DESCRIPTIVE


def load_pitches() -> pd.DataFrame:
    cols = ["pitcher", "game_pk", "game_year", "stand", "p_throws", "is_starter",
            "n_thruorder_pitcher", "woba_denom", "woba_value", "description",
            "events", "bb_type", "estimated_woba_using_speedangle"]
    df = pd.read_parquet(CACHE, columns=cols)
    df = df[df["game_year"].isin(SEASONS) & df["is_starter"]].copy()
    df["is_pa"] = df["woba_denom"].fillna(0) > 0
    # xwOBA where the batted ball supports it, actual wOBA otherwise (walks and
    # strikeouts have no batted-ball estimate). A mixed estimator, and recorded
    # as such -- the same compromise starter_outings.py makes.
    df["xwoba"] = df["estimated_woba_using_speedangle"].fillna(df["woba_value"])
    df["opposite_hand"] = df["stand"] != df["p_throws"]
    df["whiff"] = df["description"].isin(WHIFFS)
    return df


def stat_table(df: pd.DataFrame, min_n: int = 25) -> pd.DataFrame:
    """Every target for every pitcher in the given slice of pitches."""
    pa = df[df["is_pa"]]
    g = pa.groupby("pitcher", observed=True)
    out = pd.DataFrame({"xwoba_overall": g["xwoba"].mean(), "n_pa": g.size()})

    opp = pa[pa["opposite_hand"]].groupby("pitcher", observed=True)["xwoba"]
    same = pa[~pa["opposite_hand"]].groupby("pitcher", observed=True)["xwoba"]
    out["platoon_split"] = opp.mean() - same.mean()
    out["n_opp"], out["n_same"] = opp.size(), same.size()

    t3 = pa[pa["n_thruorder_pitcher"] >= 3].groupby("pitcher", observed=True)["xwoba"]
    t1 = pa[pa["n_thruorder_pitcher"] == 1].groupby("pitcher", observed=True)["xwoba"]
    out["tto_penalty"] = t3.mean() - t1.mean()
    out["n_t3"], out["n_t1"] = t3.size(), t1.size()

    out["swstr_rate"] = df.groupby("pitcher", observed=True)["whiff"].mean()
    bip = pa[pa["bb_type"].notna()]
    out["gb_rate"] = bip.assign(gb=bip["bb_type"].eq("ground_ball")).groupby(
        "pitcher", observed=True)["gb"].mean()
    ev = pa["events"].fillna("")
    out["k_rate"] = pa.assign(k=ev.eq("strikeout")).groupby(
        "pitcher", observed=True)["k"].mean()
    out["bb_rate"] = pa.assign(b=ev.isin(["walk", "hit_by_pitch"])).groupby(
        "pitcher", observed=True)["b"].mean()

    for c, n in (("platoon_split", ["n_opp", "n_same"]), ("tto_penalty", ["n_t3", "n_t1"])):
        out.loc[out[n].min(axis=1) < min_n, c] = np.nan
    return out


def reliability(df: pd.DataFrame, keep: set, rng) -> pd.DataFrame:
    """Spearman-Brown corrected odd/even-*game* split-half reliability.

    Splitting by game rather than by plate appearance keeps within-game
    correlation from leaking across the halves and inflating the estimate.
    """
    d = df[df["pitcher"].isin(keep)]
    games = d["game_pk"].unique()
    half = set(rng.choice(games, size=len(games) // 2, replace=False))
    a = stat_table(d[d["game_pk"].isin(half)])
    b = stat_table(d[~d["game_pk"].isin(half)])

    rows = []
    for t in TARGETS:
        common = a[t].dropna().index.intersection(b[t].dropna().index)
        r = float(np.corrcoef(a.loc[common, t], b.loc[common, t])[0, 1])
        sb = 2 * r / (1 + r) if r > -1 else np.nan
        rows.append({"target": t,
                     "kind": "conditioning" if t in CONDITIONING else "descriptive",
                     "n_pitchers": len(common), "half_r": round(r, 4),
                     "reliability_sb": round(sb, 4),
                     "ceiling_r2": round(max(sb, 0.0), 4)})
    return pd.DataFrame(rows)


def evaluate(y: np.ndarray, base: np.ndarray, coords: np.ndarray, label: str) -> list[dict]:
    """Out-of-sample R^2, baseline vs baseline+coordinates.

    The baseline already knows the pitcher's overall quality and workload, so the
    coordinates must earn their place *on top of* that rather than merely
    correlate with being good.
    """
    cv = RepeatedKFold(n_splits=5, n_repeats=6, random_state=SEED)
    rows = []
    for name, X in (("baseline (quality+workload)", base),
                    ("baseline + coords", np.hstack([base, coords])),
                    ("coords only", coords)):
        pipe = make_pipeline(StandardScaler(), RidgeCV(alphas=np.logspace(-2, 4, 25)))
        s = cross_val_score(pipe, X, y, cv=cv, scoring="r2")
        rows.append({"target": label, "model": name, "cv_r2": round(float(s.mean()), 4),
                     "cv_r2_sd": round(float(s.std()), 4)})
    rows.append({"target": label, "model": "increment from coords",
                 "cv_r2": round(rows[1]["cv_r2"] - rows[0]["cv_r2"], 4), "cv_r2_sd": None})
    return rows


def main() -> None:
    rng = np.random.default_rng(SEED)
    agg, X = load_matrix()
    coords = PCA(n_components=N_PCA, random_state=SEED).fit_transform(X)

    df = load_pitches()
    print(f"{df['is_pa'].sum():,} plate appearances / {len(df):,} pitches, "
          f"{df['pitcher'].nunique()} starters ({SEASONS[0]}-{SEASONS[-1]})")

    tgt = stat_table(df).reset_index()
    d = agg[["pitcher", "pitcher_name"]].assign(idx=np.arange(len(agg)))
    d = d.merge(tgt, on="pitcher", how="inner")
    d = d[(d[["n_opp", "n_same", "n_t1", "n_t3"]].min(axis=1) >= MIN_PA_PER_SPLIT)]
    d = d.dropna(subset=TARGETS)
    print(f"{len(d)} starters clear the {MIN_PA_PER_SPLIT}-PA floor in every split\n")

    print("L4-F1 -- split-half reliability: the ceiling any model can reach")
    rel = reliability(df, set(d["pitcher"]), rng)
    print(rel.to_string(index=False))
    rel.to_csv(OUT / "l4_target_reliability.csv", index=False)

    C = coords[d["idx"].to_numpy()]
    base = d[["xwoba_overall", "n_pa"]].to_numpy(dtype="float64")
    ceil = rel.set_index("target")["ceiling_r2"]

    print("\nL4-F2 -- out-of-sample prediction (repeated 5-fold CV, n="
          f"{len(d)})")
    rows = []
    for t in TARGETS:
        y = d[t].to_numpy(dtype="float64")
        r = evaluate(y, base, C, t)
        rows += r
        got = r[1]["cv_r2"]
        pct = f"{got / ceil[t]:.0%}" if ceil[t] > 0.01 else "n/a"
        print(f"\n  {t}  (sd {y.std():.4f}, reliability ceiling R2 {ceil[t]:.3f})")
        for x in r:
            sd = f" +/- {x['cv_r2_sd']:.3f}" if x["cv_r2_sd"] is not None else ""
            print(f"    {x['model']:30s} R2 = {x['cv_r2']:+.4f}{sd}")
        print(f"    -> best model reaches {pct} of the attainable ceiling")

    pd.DataFrame(rows).to_csv(OUT / "l4_prediction.csv", index=False)
    print(f"\noutputs -> {OUT}")


if __name__ == "__main__":
    main()
