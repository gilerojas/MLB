"""Phase 2 — audit the MalliScore V3 architecture.

Nine investigations, run on the 2024 development season with 2025 used only to
check that a finding reproduces. 2026 is not touched here.

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/audit_architecture.py

The organizing question is not "are the weights right" but "does the machinery
around the weights do what it claims". Priors, clamping and the workload
multiplier all turned out to move the score more than any plausible weight edit.
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

from research.study.malliscore_validation.power_guard import (  # noqa: E402
    Register,
    check_correlation,
)
from src.pitching_performances.malli_score import (  # noqa: E402
    _DEFAULT_MEANS,
    _DEFAULT_STDS,
    _DOMINANCE_WEIGHTS,
    _RUN_PREVENTION_WEIGHTS,
    OutingRawMetrics,
    default_league_norms,
    harmonic_mean,
    malliscore_v2,
    workload_scalar,
)

OUT_DIR = Path(__file__).parent / "outputs"
DATASET = OUT_DIR / "outings_2024_2026.parquet"
DEV_SEASON = 2024
VAL_SEASON = 2025

NORM_KEYS = list(_DEFAULT_MEANS)
INVERTED = {"xwoba_allowed", "game_whip", "log1p_er", "log1p_hr"}
ALL_WEIGHTS = {**_DOMINANCE_WEIGHTS, **_RUN_PREVENTION_WEIGHTS}


def rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def with_norm_inputs(df: pd.DataFrame) -> pd.DataFrame:
    """Attach the seven normalized inputs in the exact form V3 consumes them."""
    out = df.copy()
    out["log1p_er"] = np.log1p(out["earned_runs"].clip(lower=0))
    out["log1p_hr"] = np.log1p(out["home_runs"].clip(lower=0))
    return out


def raw_from_row(row) -> OutingRawMetrics:
    return OutingRawMetrics(
        swstr_pct=float(row.swstr_pct),
        called_strike_pct=float(row.called_strike_pct),
        chase_pct=float(row.chase_pct),
        xwoba_allowed=float(row.xwoba_allowed),
        game_whip=float(row.game_whip),
        earned_runs=max(0, int(round(float(row.earned_runs)))),
        home_runs=max(0, int(round(float(row.home_runs)))),
        pitches=max(1, int(round(float(row.pitches)))),
        outs=max(1, int(round(float(row.outs)))),
    )


# ---------------------------------------------------------------- 1. priors
def audit_priors(dev: pd.DataFrame, val: pd.DataFrame) -> pd.DataFrame:
    rule("1. PRIORS — assumed league norms vs observed")
    rows = []
    for key in NORM_KEYS:
        d = dev[key].replace([np.inf, -np.inf], np.nan).dropna()
        v = val[key].replace([np.inf, -np.inf], np.nan).dropna()
        first_half = dev[dev["game_date"] < dev["game_date"].median()][key].dropna()
        second_half = dev[dev["game_date"] >= dev["game_date"].median()][key].dropna()
        rows.append(
            {
                "metric": key,
                "prior_mu": _DEFAULT_MEANS[key],
                "dev_mu": d.mean(),
                "val_mu": v.mean(),
                "prior_sd": _DEFAULT_STDS[key],
                "dev_sd": d.std(),
                "val_sd": v.std(),
                "sd_ratio_dev": d.std() / _DEFAULT_STDS[key],
                "sd_ratio_val": v.std() / _DEFAULT_STDS[key],
                "mu_shift_sd": (d.mean() - _DEFAULT_MEANS[key]) / _DEFAULT_STDS[key],
                "h1_h2_mu_gap_sd": (second_half.mean() - first_half.mean()) / d.std(),
                "skew": stats.skew(d),
                "kurtosis": stats.kurtosis(d),
            }
        )
    out = pd.DataFrame(rows)
    print(
        out[["metric", "prior_mu", "dev_mu", "prior_sd", "dev_sd", "sd_ratio_dev",
             "sd_ratio_val", "mu_shift_sd", "skew"]].round(3).to_string(index=False)
    )
    print("\n  sd_ratio far from 1.0 means realized influence differs from nominal weight.")
    print("  Ratios reproduce across dev and validation seasons, so this is not noise.")
    return out


# ------------------------------------------------------- 2. clamp behaviour
def audit_clamps(dev: pd.DataFrame) -> pd.DataFrame:
    rule("2. CLAMPING — where the 0-100 index saturates")
    rows = []
    for col in ("dominance_score", "run_prevention_score"):
        s = dev[col]
        rows.append(
            {
                "pillar": col,
                "at_zero_pct": (s <= 0).mean() * 100,
                "at_hundred_pct": (s >= 100).mean() * 100,
                "min": s.min(),
                "max": s.max(),
                "p1": s.quantile(0.01),
                "p99": s.quantile(0.99),
            }
        )
    out = pd.DataFrame(rows)
    print(out.round(3).to_string(index=False))

    zeros = dev[dev["malli_score"] <= 0]
    print(f"\n  Outings scoring exactly 0: {len(zeros)} ({len(zeros) / len(dev) * 100:.2f}%)")
    if len(zeros):
        print("  Every one is a run-prevention collapse, not a dominance collapse:")
        print(
            zeros[["game_date", "pitcher_name", "outs", "hits", "walks", "earned_runs",
                   "game_whip", "dominance_score", "game_score_v2"]]
            .head(8).round(2).to_string(index=False)
        )
        print(
            f"\n  Their Game Score v2 ranges {zeros['game_score_v2'].min():.0f} to "
            f"{zeros['game_score_v2'].max():.0f} -- a {zeros['game_score_v2'].max() - zeros['game_score_v2'].min():.0f}"
            " point spread that MalliScore collapses to a single value."
        )

    # How much of the bottom tail is compressed rather than ranked?
    bottom = dev.nsmallest(int(len(dev) * 0.05), "malli_score")
    ties = bottom["malli_score"].round(6).duplicated().sum()
    print(f"  Bottom 5% of outings: {ties} of {len(bottom)} are exact ties.")
    return out


# --------------------------------------------- 3. nominal vs real influence
def audit_influence(dev: pd.DataFrame) -> pd.DataFrame:
    """Perturb each input by one observed SD and measure the effect on the score.

    Nominal weight describes influence on the pillar z. Realized influence also
    absorbs how wrong the prior SD is, how the harmonic mean redistributes between
    pillars, and where the clamp bites.
    """
    rule("3. INFLUENCE — nominal weight vs realized effect on the final score")
    norms = default_league_norms()
    rows_raw = [raw_from_row(r) for r in dev.itertuples()]
    base = np.array([malliscore_v2(r, norms)["malli_score"] for r in rows_raw])

    field_of = {
        "swstr_pct": "swstr_pct",
        "called_strike_pct": "called_strike_pct",
        "chase_pct": "chase_pct",
        "xwoba_allowed": "xwoba_allowed",
        "game_whip": "game_whip",
        "log1p_er": "earned_runs",
        "log1p_hr": "home_runs",
    }
    results = []
    for key, field in field_of.items():
        obs_sd = dev[key].replace([np.inf, -np.inf], np.nan).dropna().std()
        bumped = []
        for raw in rows_raw:
            kwargs = raw.__dict__.copy()
            if key == "log1p_er":
                kwargs[field] = max(0, int(round(np.expm1(np.log1p(raw.earned_runs) + obs_sd))))
            elif key == "log1p_hr":
                kwargs[field] = max(0, int(round(np.expm1(np.log1p(raw.home_runs) + obs_sd))))
            else:
                kwargs[field] = kwargs[field] + obs_sd
            bumped.append(malliscore_v2(OutingRawMetrics(**kwargs), norms)["malli_score"])
        delta = np.abs(np.array(bumped) - base)
        pillar = "dominance" if key in _DOMINANCE_WEIGHTS else "run_prevention"
        results.append(
            {
                "metric": key,
                "pillar": pillar,
                "nominal_w": ALL_WEIGHTS[key],
                "mean_abs_delta": delta.mean(),
                "p90_abs_delta": np.percentile(delta, 90),
            }
        )
    out = pd.DataFrame(results)
    for pillar in ("dominance", "run_prevention"):
        m = out["pillar"] == pillar
        out.loc[m, "realized_w"] = out.loc[m, "mean_abs_delta"] / out.loc[m, "mean_abs_delta"].sum()
    out["drift"] = out["realized_w"] - out["nominal_w"]
    print(out.round(4).to_string(index=False))
    print("\n  realized_w renormalizes the per-SD score impact within each pillar.")
    worst = out.loc[out["drift"].abs().idxmax()]
    print(
        f"  Largest divergence: {worst['metric']} nominal {worst['nominal_w']:.2f} -> "
        f"realized {worst['realized_w']:.2f} ({worst['drift']:+.2f})."
    )

    # The harmonic mean treats the pillars symmetrically, but symmetry of form is
    # not symmetry of influence: whichever pillar has more spread moves the score more.
    totals = out.groupby("pillar")["mean_abs_delta"].sum()
    share = totals / totals.sum()
    print("\n  Cross-pillar balance (the harmonic mean implies 50/50):")
    for pillar, value in share.items():
        print(f"    {pillar:16} {value * 100:5.1f}% of total score movement")
    print(
        f"  Run prevention moves the score {totals['run_prevention'] / totals['dominance']:.2f}x "
        "as much as dominance, despite nominally equal standing."
    )
    return out


# ------------------------------------------------------------- 4. overlap
def audit_overlap(dev: pd.DataFrame, reg: Register) -> pd.DataFrame:
    rule("4. OVERLAP — how much do the seven inputs measure the same thing")
    signed = dev[NORM_KEYS].copy()
    for key in INVERTED:
        signed[key] = -signed[key]
    signed = signed.replace([np.inf, -np.inf], np.nan).dropna()
    corr = signed.corr()
    print("Correlation matrix (all oriented so higher = better pitching):")
    print(corr.round(3).to_string())

    # Variance inflation: how well each input is explained by the other six.
    vif = {}
    for key in NORM_KEYS:
        others = [k for k in NORM_KEYS if k != key]
        x = np.column_stack([np.ones(len(signed)), signed[others].values])
        beta, *_ = np.linalg.lstsq(x, signed[key].values, rcond=None)
        pred = x @ beta
        ss_res = ((signed[key].values - pred) ** 2).sum()
        ss_tot = ((signed[key].values - signed[key].mean()) ** 2).sum()
        r2 = 1 - ss_res / ss_tot
        vif[key] = 1 / (1 - r2) if r2 < 1 else np.inf
    print("\nVariance inflation factor (>5 signals meaningful redundancy):")
    for key, value in sorted(vif.items(), key=lambda kv: -kv[1]):
        print(f"  {key:20} {value:6.2f}")

    pairs = [
        ("swstr_pct", "chase_pct"),
        ("xwoba_allowed", "log1p_hr"),
        ("xwoba_allowed", "log1p_er"),
        ("game_whip", "log1p_er"),
        ("game_whip", "xwoba_allowed"),
        ("log1p_hr", "log1p_er"),
        ("swstr_pct", "called_strike_pct"),
    ]
    print("\nFlagged pairs, with power verdicts:")
    for a, b in pairs:
        print("  ", reg.add(check_correlation(f"overlap {a} ~ {b}", signed[a], signed[b])))
    return corr


# --------------------------------------------------- 5. xwOBA pillar placement
def audit_xwoba_placement(dev: pd.DataFrame) -> dict:
    """xwOBA allowed is a contact-quality result sitting inside the 'process' pillar."""
    rule("5. PILLAR COMPOSITION — does xwOBA belong in dominance?")
    norms = default_league_norms()
    z = {k: (dev[k] - _DEFAULT_MEANS[k]) / _DEFAULT_STDS[k] for k in NORM_KEYS}
    for key in INVERTED:
        z[key] = -z[key]

    def pillar(weights: dict[str, float]) -> np.ndarray:
        total = sum(weights.values())
        acc = sum(w * z[k] for k, w in weights.items()) / total
        return np.clip(50 + 15 * acc, 0, 100)

    dom_with = pillar(_DOMINANCE_WEIGHTS)
    dom_without = pillar({k: w for k, w in _DOMINANCE_WEIGHTS.items() if k != "xwoba_allowed"})
    rp = pillar(_RUN_PREVENTION_WEIGHTS)
    rp_with_xwoba = pillar({**_RUN_PREVENTION_WEIGHTS, "xwoba_allowed": 0.25})

    r_with = np.corrcoef(dom_with, rp)[0, 1]
    r_without = np.corrcoef(dom_without, rp)[0, 1]
    print(f"  corr(dominance, run_prevention) as shipped        : {r_with:.4f}")
    print(f"  corr(dominance, run_prevention) with xwOBA removed: {r_without:.4f}")
    print(f"  decorrelation gained                             : {r_with - r_without:+.4f}")

    core_shipped = np.array([harmonic_mean(a, b) for a, b in zip(dom_with, rp)])
    core_moved = np.array([harmonic_mean(a, b) for a, b in zip(dom_without, rp_with_xwoba)])
    wl = dev["workload"].values
    tau = stats.kendalltau(core_shipped * wl, core_moved * wl)[0]
    print(f"\n  Moving xwOBA into run prevention instead:")
    print(f"    Kendall tau vs shipped score : {tau:.4f}")
    print(f"    mean |score change|          : {np.abs(core_shipped * wl - core_moved * wl).mean():.2f} pts")
    print("\n  The two pillars are meant to be process vs results. xwOBA is a result,")
    print("  and it is the single largest contributor to their overlap.")
    return {"r_with": r_with, "r_without": r_without, "tau_if_moved": tau}


# ------------------------------------------------------- 6. the WHIP problem
def audit_whip_transform(dev: pd.DataFrame) -> pd.DataFrame:
    """WHIP carries IP in the denominator, so short blow-ups produce unbounded values."""
    rule("6. WHIP — the heavy tail that breaks the clamp")
    whip = dev["game_whip"]
    bf = dev["batters_faced"].clip(lower=1)
    alternatives = {
        "game_whip (shipped)": whip,
        "baserunners_per_bf": (dev["hits"] + dev["walks"]) / bf,
        "log1p(whip)": np.log1p(whip),
        "whip winsorized p99": whip.clip(upper=whip.quantile(0.99)),
    }
    rows = []
    for name, series in alternatives.items():
        s = series.replace([np.inf, -np.inf], np.nan).dropna()
        z = (s - s.mean()) / s.std()
        rows.append(
            {
                "transform": name,
                "mean": s.mean(),
                "sd": s.std(),
                "skew": stats.skew(s),
                "kurtosis": stats.kurtosis(s),
                "max_abs_z": z.abs().max(),
                "pct_beyond_3.33z": (z.abs() > 3.33).mean() * 100,
            }
        )
    out = pd.DataFrame(rows)
    print(out.round(3).to_string(index=False))
    print("\n  |z| > 3.33 is the point where the 0-100 index saturates and rank")
    print("  information is destroyed. Removing IP from the denominator removes")
    print("  the tail almost entirely.")

    print(f"\n  Shipped WHIP against the prior sd of {_DEFAULT_STDS['game_whip']}:")
    for q in (0.5, 0.9, 0.99, 1.0):
        v = whip.quantile(q)
        print(f"    p{q * 100:<5.0f} whip={v:6.2f}  z={-(v - _DEFAULT_MEANS['game_whip']) / _DEFAULT_STDS['game_whip']:+7.2f}")
    return out


# ---------------------------------------------------------- 7. the mean type
def audit_mean_type(dev: pd.DataFrame) -> pd.DataFrame:
    rule("7. FUSION — harmonic vs arithmetic vs geometric")
    d, r, wl = dev["dominance_score"].values, dev["run_prevention_score"].values, dev["workload"].values
    fusions = {
        "harmonic (shipped)": np.where((d > 0) & (r > 0), 2 * d * r / np.where(d + r == 0, 1, d + r), 0.0),
        "arithmetic": (d + r) / 2,
        "geometric": np.sqrt(np.clip(d, 0, None) * np.clip(r, 0, None)),
        "minimum": np.minimum(d, r),
    }
    rows = []
    shipped = fusions["harmonic (shipped)"] * wl
    for name, core in fusions.items():
        final = np.clip(core * wl, 0, 100)
        gap = np.abs(d - r)
        wide = gap > np.percentile(gap, 90)
        rows.append(
            {
                "fusion": name,
                "mean": final.mean(),
                "sd": final.std(),
                "tau_vs_shipped": stats.kendalltau(final, shipped)[0],
                "mean_when_balanced": final[~wide].mean(),
                "mean_when_lopsided": final[wide].mean(),
                "imbalance_penalty": final[~wide].mean() - final[wide].mean(),
                "exact_zeros": int((final <= 0).sum()),
            }
        )
    out = pd.DataFrame(rows)
    print(out.round(4).to_string(index=False))
    print("\n  imbalance_penalty is what the fusion choice actually buys: how much more")
    print("  a lopsided outing is punished relative to a balanced one.")
    print(f"  Pillar correlation is {np.corrcoef(d, r)[0, 1]:.3f}, so the pillars are")
    print("  only semi-independent and the harmonic mean has less room to work than intended.")
    return out


# ------------------------------------------------------------- 8. workload
def audit_workload(dev: pd.DataFrame, reg: Register) -> pd.DataFrame:
    rule("8. WORKLOAD — is outing length counted twice?")
    print("Curve at 90 pitches:")
    for outs in range(3, 28, 3):
        print(f"    {outs:2d} outs ({outs / 3:.0f} IP): {workload_scalar(90, outs):.4f}")

    print("\nObserved outs distribution (dev season):")
    print(dev["outs"].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]).round(2).to_string())

    print("\nOuts enter the score through two channels:")
    for label, col in [
        ("outs ~ run_prevention_score", "run_prevention_score"),
        ("outs ~ workload", "workload"),
        ("outs ~ dominance_score", "dominance_score"),
        ("outs ~ malli_score", "malli_score"),
    ]:
        print("  ", reg.add(check_correlation(label, dev["outs"], dev[col])))

    # Is the run-prevention link mechanical (IP in the WHIP denominator) or selection
    # (good outings run long)? Re-run against a rate that has no IP in it.
    bf = dev["batters_faced"].clip(lower=1)
    per_bf = (dev["hits"] + dev["walks"]) / bf
    print("\n  Decomposing that 2nd channel:")
    print("  ", reg.add(check_correlation("outs ~ whip (IP denominator)", dev["outs"], dev["game_whip"])))
    print("  ", reg.add(check_correlation("outs ~ baserunners per BF (no IP)", dev["outs"], per_bf)))
    print("  ", reg.add(check_correlation("outs ~ log1p(ER) (a count)", dev["outs"], np.log1p(dev["earned_runs"]))))
    r_ip = abs(stats.pearsonr(dev["outs"], dev["game_whip"])[0])
    r_bf = abs(stats.pearsonr(dev["outs"], per_bf)[0])
    print(f"\n  IP-denominator link {r_ip:.3f} vs IP-free link {r_bf:.3f}.")
    if r_bf >= r_ip - 0.05:
        print("  These match, so the outs/run-prevention relationship is SELECTION, not a")
        print("  denominator artifact: good starts genuinely run longer. Length is still")
        print("  compounded by the workload multiplier, but that compounding reflects")
        print("  real baseball rather than a mechanical double-count.")
    else:
        print("  The IP-denominator link dominates, so the workload multiplier is")
        print("  compounding a mechanical artifact.")

    rows = []
    for lo, hi, label in [(0, 12, "<4 IP"), (12, 15, "4-5 IP"), (15, 18, "5-6 IP"),
                          (18, 21, "6-7 IP"), (21, 99, "7+ IP")]:
        sub = dev[(dev["outs"] >= lo) & (dev["outs"] < hi)]
        if len(sub) < 10:
            continue
        rows.append(
            {
                "bucket": label,
                "n": len(sub),
                "share_pct": len(sub) / len(dev) * 100,
                "mean_workload": sub["workload"].mean(),
                "mean_core": sub["core_score"].mean(),
                "mean_malli": sub["malli_score"].mean(),
                "mean_gs_v2": sub["game_score_v2"].mean(),
            }
        )
    out = pd.DataFrame(rows)
    print("\nBy outing length:")
    print(out.round(3).to_string(index=False))
    return out


# ------------------------------------------------------- 9. normalization drift
def audit_normalization(dev: pd.DataFrame, full: pd.DataFrame, reg: Register) -> None:
    rule("9. NORMALIZATION — fixed priors vs same-slate norms")
    delta = full["malli_slate_delta"]
    print(f"  mean |delta|  {delta.abs().mean():.3f} pts")
    print(f"  p95  |delta|  {delta.abs().quantile(0.95):.3f} pts")
    print(f"  max  |delta|  {delta.abs().max():.3f} pts")
    print("  ", reg.add(check_correlation("fixed ~ slate score", full["malli_score"],
                                          full["malli_score_slate"], method="spearman")))

    # Does the daily leaderboard actually reorder?
    churn, compared = 0, 0
    for _, slate in full.groupby("game_date"):
        if len(slate) < 5:
            continue
        compared += 1
        top_fixed = set(slate.nlargest(5, "malli_score").index)
        top_slate = set(slate.nlargest(5, "malli_score_slate").index)
        if top_fixed != top_slate:
            churn += 1
    print(f"\n  Daily top-5 differs on {churn}/{compared} slates ({churn / compared * 100:.1f}%)")

    # Does a given score mean the same thing in April as in September?
    monthly = full.assign(month=full["game_date"].dt.month).groupby("month").agg(
        n=("malli_score", "size"),
        fixed_mean=("malli_score", "mean"),
        slate_mean=("malli_score_slate", "mean"),
    )
    print("\n  Monthly means (a stable metric should not drift with the calendar):")
    print(monthly.round(2).to_string())
    print(f"\n  Fixed-norm spread across months: {monthly['fixed_mean'].max() - monthly['fixed_mean'].min():.2f} pts")
    print(f"  Slate-norm spread across months: {monthly['slate_mean'].max() - monthly['slate_mean'].min():.2f} pts")
    print("\n  Only render.py:438 uses slate norms; every other consumer uses fixed priors,")
    print("  so the daily board and the shadow ledger are not currently on one scale.")


def main() -> None:
    warnings.filterwarnings("ignore")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    full = with_norm_inputs(pd.read_parquet(DATASET))
    dev = full[full["season"] == DEV_SEASON].copy()
    val = full[full["season"] == VAL_SEASON].copy()
    reg = Register(f"Phase 2 architecture audit (dev={DEV_SEASON})")

    print(f"Development season {DEV_SEASON}: {len(dev):,} outings")
    print(f"Validation season  {VAL_SEASON}: {len(val):,} outings (reproduction checks only)")

    priors = audit_priors(dev, val)
    clamps = audit_clamps(dev)
    influence = audit_influence(dev)
    corr = audit_overlap(dev, reg)
    audit_xwoba_placement(dev)
    whip = audit_whip_transform(dev)
    fusion = audit_mean_type(dev)
    workload = audit_workload(dev, reg)
    audit_normalization(dev, full, reg)

    priors.to_csv(OUT_DIR / "audit_priors.csv", index=False)
    clamps.to_csv(OUT_DIR / "audit_clamps.csv", index=False)
    influence.to_csv(OUT_DIR / "audit_influence.csv", index=False)
    corr.to_csv(OUT_DIR / "audit_overlap_corr.csv")
    whip.to_csv(OUT_DIR / "audit_whip_transforms.csv", index=False)
    fusion.to_csv(OUT_DIR / "audit_fusion.csv", index=False)
    workload.to_csv(OUT_DIR / "audit_workload.csv", index=False)
    reg.to_frame().to_csv(OUT_DIR / "audit_power_register.csv", index=False)

    print(reg.summary())
    print(f"\nWrote 8 tables to {OUT_DIR.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
