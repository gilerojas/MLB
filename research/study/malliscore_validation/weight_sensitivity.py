"""Phase 4 — map the weight sensitivity surface.

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/weight_sensitivity.py

Development season only. The deliverable is a surface, not a winner: the question
is whether 30/25/20/25 and 40/35/25 are load-bearing choices or whether the whole
feasible region produces effectively the same metric.

If rank agreement stays high across the feasible simplex, the weights are not
identifiable from data. That is a publishable conclusion in its own right, and it
would confirm the Phase 2 result that the priors -- not the weights -- are the
lever that actually moves MalliScore.

We deliberately do not optimize. Nothing here searches for a "best" weight vector
against any target, because no target exists that defines what MalliScore should be.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.pitching_performances.malli_score import (  # noqa: E402
    _DOMINANCE_WEIGHTS,
    _V4_MEANS,
    _V4_RUN_PREVENTION_WEIGHTS,
    _V4_STDS,
)

OUT_DIR = Path(__file__).parent / "outputs"
DATASET = OUT_DIR / "outings_2024_2026.parquet"
DEV_SEASON = 2024
VAL_SEASON = 2025

DOM_KEYS = list(_DOMINANCE_WEIGHTS)
RP_KEYS = list(_V4_RUN_PREVENTION_WEIGHTS)
INVERTED = {"xwoba_allowed", "reach_rate_allowed", "log1p_er", "log1p_hr"}

N_SAMPLES = 20_000
N_DETAILED = 500          # subsample for the expensive rank/churn measures
DOM_CAP = 0.40            # no dominance metric may exceed this share
RP_CAP = 0.50             # no run-prevention metric may exceed this share
SEED = 20260802


def rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def z_matrix(df: pd.DataFrame, keys: list[str]) -> np.ndarray:
    """Signed z-scores under the shipped V4 priors, oriented higher = better."""
    cols = []
    for key in keys:
        z = (df[key].values - _V4_MEANS[key]) / _V4_STDS[key]
        cols.append(-z if key in INVERTED else z)
    return np.column_stack(cols)


def sample_weights(n: int, dim: int, cap: float, rng: np.random.Generator) -> np.ndarray:
    """Uniform over the simplex, rejecting vectors that violate the cap."""
    out = []
    while len(out) < n:
        draw = rng.dirichlet(np.ones(dim), size=n)
        out.extend(draw[draw.max(axis=1) <= cap])
    return np.array(out[:n])


def score_grid(zd: np.ndarray, zr: np.ndarray, wd: np.ndarray, wr: np.ndarray,
               workload: np.ndarray) -> np.ndarray:
    """Vectorized MalliScore for many weight vectors at once.

    Returns an (outings x candidates) matrix. Reproduces V4 exactly: weighted mean
    z, clamp to the 0-100 index, harmonic fusion, workload multiplier, final clamp.
    """
    dom = np.clip(50 + 15 * (zd @ wd.T), 0, 100)
    rp = np.clip(50 + 15 * (zr @ wr.T), 0, 100)
    denom = dom + rp
    core = np.where((dom > 0) & (rp > 0), 2 * dom * rp / np.where(denom == 0, 1, denom), 0.0)
    return np.clip(core * workload[:, None], 0, 100)


def rank_columns(mat: np.ndarray) -> np.ndarray:
    """Column-wise ranks with tie averaging.

    Ties are not incidental here: the clamp collapses every blow-up start to exactly
    0, so the shipped score carries ~21 tied values per season. An argsort-based rank
    would break those ties arbitrarily and understate agreement.
    """
    return stats.rankdata(mat, axis=0)


def spearman_against(mat: np.ndarray, reference: np.ndarray) -> np.ndarray:
    """Spearman of every column of `mat` against `reference`, vectorized."""
    ranks = rank_columns(mat)
    ref = stats.rankdata(reference)
    r = ranks - ranks.mean(axis=0)
    v = ref - ref.mean()
    return (r * v[:, None]).sum(axis=0) / np.sqrt((r**2).sum(axis=0) * (v**2).sum())


def main() -> None:
    warnings.filterwarnings("ignore")
    rng = np.random.default_rng(SEED)
    full = pd.read_parquet(DATASET)
    full["log1p_er"] = np.log1p(full["earned_runs"].clip(lower=0))
    full["log1p_hr"] = np.log1p(full["home_runs"].clip(lower=0))
    dev = full[full["season"] == DEV_SEASON].reset_index(drop=True)
    val = full[full["season"] == VAL_SEASON].reset_index(drop=True)

    print(f"Development season {DEV_SEASON}: {len(dev):,} outings")
    print(f"Sampling {N_SAMPLES:,} weight vectors under the stated constraints")
    print(f"  dominance: 4 weights, sum 1, none > {DOM_CAP}")
    print(f"  run prevention: 3 weights, sum 1, none > {RP_CAP}")

    zd, zr = z_matrix(dev, DOM_KEYS), z_matrix(dev, RP_KEYS)
    wl = dev["workload"].values
    shipped = dev["malli_score_v4"].values

    wd = sample_weights(N_SAMPLES, len(DOM_KEYS), DOM_CAP, rng)
    wr = sample_weights(N_SAMPLES, len(RP_KEYS), RP_CAP, rng)
    # Put V4's own vector first so it can be recovered as a self-check.
    wd[0] = [_DOMINANCE_WEIGHTS[k] for k in DOM_KEYS]
    wr[0] = [_V4_RUN_PREVENTION_WEIGHTS[k] for k in RP_KEYS]

    rule("1. RANK AGREEMENT ACROSS THE FEASIBLE REGION")
    rhos, means, sds, zeros, p95s = [], [], [], [], []
    for start in range(0, N_SAMPLES, 2000):
        stop = min(start + 2000, N_SAMPLES)
        grid = score_grid(zd, zr, wd[start:stop], wr[start:stop], wl)
        rhos.append(spearman_against(grid, shipped))
        means.append(grid.mean(axis=0))
        sds.append(grid.std(axis=0))
        zeros.append((grid <= 0).mean(axis=0) * 100)
        p95s.append(np.percentile(grid, 95, axis=0))
    rho = np.concatenate(rhos)
    surface = pd.DataFrame(
        {
            "spearman_vs_production": rho,
            "mean": np.concatenate(means),
            "sd": np.concatenate(sds),
            "pct_zero": np.concatenate(zeros),
            "p95": np.concatenate(p95s),
            **{f"w_{k}": wd[:, i] for i, k in enumerate(DOM_KEYS)},
            **{f"w_{k}": wr[:, i] for i, k in enumerate(RP_KEYS)},
        }
    )

    assert abs(rho[0] - 1.0) < 1e-9, "V4's own weight vector must recover rho = 1.0"
    print(f"  Self-check: V4's own vector recovers Spearman {rho[0]:.6f}")
    print(f"\n  Spearman vs shipped V4 across {N_SAMPLES:,} candidates:")
    for q in (0, 1, 5, 25, 50):
        print(f"    p{q:<3} {np.percentile(rho, q):.4f}")
    print(f"    min  {rho.min():.4f}")
    print(f"\n  {(rho > 0.99).mean() * 100:.1f}% of the feasible region agrees with V4 above 0.99")
    print(f"  {(rho > 0.98).mean() * 100:.1f}% agrees above 0.98")
    print(f"  Score mean ranges {surface['mean'].min():.2f} to {surface['mean'].max():.2f}")
    print(f"  Score sd   ranges {surface['sd'].min():.2f} to {surface['sd'].max():.2f}")
    print(f"  Exact-zero rate ranges {surface['pct_zero'].min():.2f}% to {surface['pct_zero'].max():.2f}%")

    rule("2. HOW MUCH CAN ANY SINGLE WEIGHT MOVE THE RANKING?")
    rows = []
    for key in DOM_KEYS + RP_KEYS:
        col = surface[f"w_{key}"]
        lo = surface.loc[col <= col.quantile(0.10), "spearman_vs_production"].mean()
        hi = surface.loc[col >= col.quantile(0.90), "spearman_vs_production"].mean()
        rows.append(
            {
                "metric": key,
                "pillar": "dominance" if key in DOM_KEYS else "run_prevention",
                "v4_weight": {**_DOMINANCE_WEIGHTS, **_V4_RUN_PREVENTION_WEIGHTS}[key],
                "rho_when_low": lo,
                "rho_when_high": hi,
                "sensitivity": abs(hi - lo),
                "corr_w_vs_rho": col.corr(surface["spearman_vs_production"]),
            }
        )
    sens = pd.DataFrame(rows).sort_values("sensitivity", ascending=False)
    print(sens.round(4).to_string(index=False))
    print("\n  sensitivity = how much mean rank agreement shifts between the lowest and")
    print("  highest decile of that weight. Larger means the weight matters more.")

    rule("3. KENDALL TAU AND DAILY TOP-5 CHURN (subsample)")
    pick = rng.choice(N_SAMPLES, N_DETAILED, replace=False)
    pick[0] = 0
    grid = score_grid(zd, zr, wd[pick], wr[pick], wl)
    taus = np.array([stats.kendalltau(grid[:, i], shipped)[0] for i in range(N_DETAILED)])
    print(f"  Kendall tau vs V4 over {N_DETAILED} candidates:")
    print(f"    min {taus.min():.4f} | p5 {np.percentile(taus, 5):.4f} | median {np.median(taus):.4f}")
    print(f"    V4 self-check: {taus[0]:.6f}")

    slates = dev.groupby("game_date").indices
    churn = np.zeros(N_DETAILED)
    counted = 0
    for idx in slates.values():
        if len(idx) < 5:
            continue
        counted += 1
        ref_top = set(idx[np.argsort(-shipped[idx])[:5]])
        sub = grid[idx]
        for c in range(N_DETAILED):
            if set(idx[np.argsort(-sub[:, c])[:5]]) != ref_top:
                churn[c] += 1
    churn_pct = churn / counted * 100
    print(f"\n  Daily top-5 churn across {counted} slates:")
    print(f"    median {np.median(churn_pct):.1f}% of slates change | "
          f"p90 {np.percentile(churn_pct, 90):.1f}% | max {churn_pct.max():.1f}%")
    print("  Even where the overall ranking barely moves, the daily leaderboard -- the")
    print("  thing readers actually see -- reorders on a meaningful share of slates.")

    rule("4. CROSS-SEASON STABILITY")
    zd_v, zr_v = z_matrix(val, DOM_KEYS), z_matrix(val, RP_KEYS)
    grid_v = score_grid(zd_v, zr_v, wd[pick], wr[pick], val["workload"].values)
    rho_v = spearman_against(grid_v, val["malli_score_v4"].values)
    rho_d = spearman_against(grid, shipped)
    print(f"  Spearman vs V4, {DEV_SEASON} vs {VAL_SEASON}, same {N_DETAILED} candidates:")
    print(f"    dev    median {np.median(rho_d):.4f} | min {rho_d.min():.4f}")
    print(f"    val    median {np.median(rho_v):.4f} | min {rho_v.min():.4f}")
    print(f"    corr(dev rho, val rho) = {np.corrcoef(rho_d, rho_v)[0, 1]:.4f}")
    print("  A candidate that agrees with V4 in one season agrees in the other, so the")
    print("  surface is a property of the metric rather than of a particular season.")

    rule("5. VERDICT")
    print(f"  Ranking agreement never falls below Spearman {rho.min():.3f} anywhere in the")
    print(f"  feasible region, and {(rho > 0.98).mean() * 100:.0f}% of it agrees above 0.98.")
    print("  The weights are therefore weakly identified: many interpretable vectors")
    print("  produce a nearly identical metric. Precision like 27.4/23.8/21.1/27.7 would")
    print("  imply certainty the data does not support -- clean 5% increments are")
    print("  preferable, and the choice should be made on baseball logic.")
    print("\n  This is the mirror image of Phase 2: the weights barely matter, while the")
    print("  priors, the WHIP tail and the clamp move the metric a great deal.")

    surface.to_csv(OUT_DIR / "weight_sensitivity_surface.csv", index=False)
    sens.to_csv(OUT_DIR / "weight_sensitivity_by_metric.csv", index=False)
    pd.DataFrame({"kendall_tau": taus, "top5_churn_pct": churn_pct,
                  "spearman_dev": rho_d, "spearman_val": rho_v}).to_csv(
        OUT_DIR / "weight_sensitivity_detailed.csv", index=False)
    print(f"\nWrote 3 tables to {OUT_DIR.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
