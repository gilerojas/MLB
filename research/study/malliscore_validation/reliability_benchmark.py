"""Phase 3 — reliability and Game Score benchmarking.

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/reliability_benchmark.py

MalliScore is a descriptive index, so the primary validation is reliability, not
prediction. Split-half reliability asks: if we split a pitcher's starts in half at
random, do the two halves tell the same story? An index that answers yes is
measuring something stable rather than amplifying per-start noise. This is the
standard psychometric test for a composite index and it is the fairest head-to-head
against Game Score, because both metrics are judged on the same pitchers.

Game Score is a benchmark and never an optimization target. Perfect agreement would
mean MalliScore adds nothing; near-zero agreement would mean it does not describe
recognizable outing quality. The useful result is strong agreement plus interpretable
disagreement, which the quadrant export exists to inspect by hand.

Development season only for the headline numbers; other seasons are reported
alongside purely to show whether a result reproduces.
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
    mde_correlation,
)

OUT_DIR = Path(__file__).parent / "outputs"
DATASET = OUT_DIR / "outings_2024_2026.parquet"
DEV_SEASON = 2024

METRICS = {
    "malli_score": "MalliScore V3",
    "game_score_v1": "Game Score v1",
    "game_score_v2": "Game Score v2",
    "swstr_pct": "SwStr%",
    "called_strike_pct": "Called strike%",
    "chase_pct": "Chase%",
    "xwoba_allowed": "xwOBA allowed",
    "game_whip": "WHIP",
    "outs": "Outs",
    "dominance_score": "  ...dominance pillar",
    "run_prevention_score": "  ...run prevention pillar",
}
MIN_STARTS = (8, 10)


def rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def spearman_brown(r_half: float, parts: float = 2.0) -> float:
    """Project a half-length correlation up to full test length."""
    if not np.isfinite(r_half) or r_half <= -1:
        return np.nan
    return parts * r_half / (1 + (parts - 1) * r_half)


def split_half_reliability(
    df: pd.DataFrame, metric: str, min_starts: int, seed: int = 20260802
) -> dict:
    """Odd/even split within each pitcher-season, correlated across pitcher-seasons.

    Odd/even beats a random split here: starts are ordered in time, so alternating
    balances early-season and late-season form across the two halves rather than
    letting one half inherit a hot streak.
    """
    halves_a, halves_b, keys = [], [], []
    for (pitcher, season), grp in df.groupby(["pitcher", "season"]):
        grp = grp.sort_values(["game_date", "game_pk"])
        vals = pd.to_numeric(grp[metric], errors="coerce").dropna()
        if len(vals) < min_starts:
            continue
        odd, even = vals.iloc[0::2], vals.iloc[1::2]
        if len(odd) < 2 or len(even) < 2:
            continue
        halves_a.append(odd.mean())
        halves_b.append(even.mean())
        keys.append((pitcher, season))

    n = len(halves_a)
    if n < 10:
        return {"metric": metric, "n_pitcher_seasons": n, "r_half": np.nan,
                "reliability": np.nan, "ci_low": np.nan, "ci_high": np.nan,
                "mde": np.inf, "usable": False}

    a, b = np.array(halves_a), np.array(halves_b)
    r_half = float(stats.pearsonr(a, b)[0])
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(2000):
        idx = rng.integers(0, n, n)
        try:
            draws.append(spearman_brown(stats.pearsonr(a[idx], b[idx])[0]))
        except Exception:
            pass
    draws = np.array([d for d in draws if np.isfinite(d)])
    return {
        "metric": metric,
        "n_pitcher_seasons": n,
        "r_half": r_half,
        "reliability": spearman_brown(r_half),
        "ci_low": float(np.percentile(draws, 2.5)),
        "ci_high": float(np.percentile(draws, 97.5)),
        "mde": mde_correlation(n),
        "usable": True,
    }


def paired_reliability_gap(
    df: pd.DataFrame, metric_a: str, metric_b: str, min_starts: int, seed: int = 20260802
) -> dict:
    """Bootstrap the reliability difference on the *same* pitcher-seasons.

    Comparing two independently-computed confidence intervals and asking whether
    they overlap is the wrong test here, and a conservative one: both metrics are
    measured on identical pitchers, so the two reliability estimates move together
    across bootstrap resamples. Resampling pitchers once and recomputing both
    reliabilities on that shared draw cancels the common variation and tests the
    quantity actually of interest -- the gap.
    """
    rows = []
    for (pitcher, season), grp in df.groupby(["pitcher", "season"]):
        grp = grp.sort_values(["game_date", "game_pk"])
        a = pd.to_numeric(grp[metric_a], errors="coerce")
        b = pd.to_numeric(grp[metric_b], errors="coerce")
        keep = a.notna() & b.notna()
        a, b = a[keep], b[keep]
        if len(a) < min_starts:
            continue
        rows.append((a.iloc[0::2].mean(), a.iloc[1::2].mean(),
                     b.iloc[0::2].mean(), b.iloc[1::2].mean()))

    arr = np.array(rows)
    n = len(arr)
    if n < 10:
        return {"n": n, "gap": np.nan, "ci_low": np.nan, "ci_high": np.nan, "resolved": False}

    def gap_of(sample: np.ndarray) -> float:
        ra = spearman_brown(stats.pearsonr(sample[:, 0], sample[:, 1])[0])
        rb = spearman_brown(stats.pearsonr(sample[:, 2], sample[:, 3])[0])
        return ra - rb

    observed = gap_of(arr)
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(4000):
        idx = rng.integers(0, n, n)
        try:
            draws.append(gap_of(arr[idx]))
        except Exception:
            pass
    draws = np.array([d for d in draws if np.isfinite(d)])
    lo, hi = np.percentile(draws, [2.5, 97.5])
    return {
        "metric_a": metric_a,
        "metric_b": metric_b,
        "min_starts": min_starts,
        "n": n,
        "gap": observed,
        "ci_low": float(lo),
        "ci_high": float(hi),
        "resolved": bool(lo > 0 or hi < 0),
    }


def audit_reliability(df: pd.DataFrame, reg: Register) -> pd.DataFrame:
    rule("1. SPLIT-HALF RELIABILITY — does the index measure something stable?")
    rows = []
    for min_starts in MIN_STARTS:
        for metric, label in METRICS.items():
            res = split_half_reliability(df, metric, min_starts)
            res["min_starts"] = min_starts
            res["label"] = label.strip()
            rows.append(res)
    out = pd.DataFrame(rows)

    for min_starts in MIN_STARTS:
        sub = out[out["min_starts"] == min_starts].copy()
        n = int(sub["n_pitcher_seasons"].max())
        print(f"\n  Pitcher-seasons with >= {min_starts} starts: n = {n}")
        print(f"  {'metric':28} {'reliability':>12} {'95% CI':>20} {'r_half':>8}")
        for _, r in sub.iterrows():
            ci = f"[{r['ci_low']:+.3f},{r['ci_high']:+.3f}]" if np.isfinite(r["ci_low"]) else "n/a"
            print(f"  {METRICS[r['metric']]:28} {r['reliability']:12.4f} {ci:>20} {r['r_half']:8.4f}")

    headline = out[(out["min_starts"] == 10)].set_index("metric")
    malli = headline.loc["malli_score"]
    gs2 = headline.loc["game_score_v2"]
    print(
        f"\n  Headline (>=10 starts): MalliScore {malli['reliability']:.3f} "
        f"vs Game Score v2 {gs2['reliability']:.3f} "
        f"({malli['reliability'] - gs2['reliability']:+.3f})."
    )
    overlap = not (malli["ci_low"] > gs2["ci_high"] or gs2["ci_low"] > malli["ci_high"])
    print(
        f"  Marginal 95% intervals {'overlap' if overlap else 'are disjoint'} -- but they are"
    )
    print("  the wrong test for paired data. Bootstrapping the gap on shared pitchers:")

    gaps = []
    for min_starts in MIN_STARTS:
        for bench in ("game_score_v2", "game_score_v1"):
            g = paired_reliability_gap(df, "malli_score", bench, min_starts)
            gaps.append(g)
            verdict = "RESOLVED" if g["resolved"] else "not resolved"
            print(
                f"    >={min_starts} starts, vs {bench}: gap {g['gap']:+.4f} "
                f"[{g['ci_low']:+.4f},{g['ci_high']:+.4f}] n={g['n']}  {verdict}"
            )

    print("\n  Reliability tracks how much of a metric rests on repeatable skill.")
    dom = headline.loc["dominance_score"]["reliability"]
    rp = headline.loc["run_prevention_score"]["reliability"]
    print(f"    dominance pillar      {dom:.3f}   (process: whiffs, chase, called strikes)")
    print(f"    run prevention pillar {rp:.3f}   (results: WHIP, ER, HR)")
    print(f"    WHIP alone            {headline.loc['game_whip']['reliability']:.3f}")
    print(f"    SwStr% alone          {headline.loc['swstr_pct']['reliability']:.3f}")
    print("\n  Note the collision with Phase 2: WHIP is the LEAST reliable input yet")
    print("  carries the LARGEST realized weight (0.56 of run prevention). The noisiest")
    print("  signal in the metric is also its loudest.")
    pd.DataFrame(gaps).to_csv(OUT_DIR / "reliability_paired_gaps.csv", index=False)
    return out


def audit_agreement(df: pd.DataFrame, reg: Register) -> pd.DataFrame:
    rule("2. AGREEMENT WITH GAME SCORE — benchmark, not target")
    rows = []
    for bench in ("game_score_v1", "game_score_v2"):
        for method in ("pearson", "spearman", "kendall"):
            res = reg.add(
                check_correlation(
                    f"malli ~ {bench} ({method})", df["malli_score"], df[bench], method=method
                )
            )
            print("  ", res)
            rows.append({"benchmark": bench, "method": method, "r": res.effect,
                         "n": res.n, "stratum": "all"})

    print("\n  By outing length (does agreement depend on how long the start was?):")
    for lo, hi, label in [(0, 15, "<5 IP"), (15, 18, "5-6 IP"), (18, 21, "6-7 IP"), (21, 99, "7+ IP")]:
        sub = df[(df["outs"] >= lo) & (df["outs"] < hi)]
        res = check_correlation(
            f"  {label:8} malli ~ gs_v2", sub["malli_score"], sub["game_score_v2"], method="spearman"
        )
        reg.add(res)
        print("  ", res)
        rows.append({"benchmark": "game_score_v2", "method": "spearman", "r": res.effect,
                     "n": res.n, "stratum": label})

    r = df["malli_score"].corr(df["game_score_v2"], method="spearman")
    print(f"\n  Overall Spearman {r:.3f} means MalliScore shares about {r ** 2 * 100:.0f}% of")
    print("  Game Score v2's rank variance and brings the rest from its process inputs.")
    print("  High enough to describe recognizable outing quality, short of redundant.")
    return pd.DataFrame(rows)


def audit_disagreement(df: pd.DataFrame) -> pd.DataFrame:
    rule("3. DISAGREEMENT — where the two metrics part company")
    d = df.copy()
    d["malli_pct"] = d.groupby("season")["malli_score"].rank(pct=True)
    d["gs_pct"] = d.groupby("season")["game_score_v2"].rank(pct=True)
    d["gap"] = d["malli_pct"] - d["gs_pct"]

    # A 70/30 split leaves the disagreement quadrants literally empty: at rank
    # correlation 0.94 no outing is top-30% on one metric and bottom-30% on the
    # other. That the two metrics never strongly contradict each other is itself a
    # result. Splitting at the median populates all four groups and still isolates
    # the outings where they point in opposite directions.
    strict = (
        (d["malli_pct"] >= 0.70) & (d["gs_pct"] <= 0.30)
        | (d["malli_pct"] <= 0.30) & (d["gs_pct"] >= 0.70)
    ).sum()
    print(f"  Outings in strict opposite tails (top 30% vs bottom 30%): {strict}")
    print("  Splitting at the median instead:\n")

    conditions = [
        (d["malli_pct"] >= 0.5) & (d["gs_pct"] >= 0.5),
        (d["malli_pct"] < 0.5) & (d["gs_pct"] < 0.5),
        (d["malli_pct"] >= 0.5) & (d["gs_pct"] < 0.5),
        (d["malli_pct"] < 0.5) & (d["gs_pct"] >= 0.5),
    ]
    labels = [
        "agree_above",
        "agree_below",
        "process_over_result",
        "result_over_process",
    ]
    d["quadrant"] = np.select(conditions, labels, default="middle")

    summary = (
        d.groupby("quadrant")
        .agg(
            n=("malli_score", "size"),
            malli=("malli_score", "mean"),
            gs_v2=("game_score_v2", "mean"),
            swstr=("swstr_pct", "mean"),
            chase=("chase_pct", "mean"),
            xwoba=("xwoba_allowed", "mean"),
            whip=("game_whip", "mean"),
            outs=("outs", "mean"),
            er=("earned_runs", "mean"),
        )
        .round(3)
    )
    print(summary.to_string())
    print(f"\n  Total outings {len(d):,}; the two disagreement quadrants hold "
          f"{(d['quadrant'].isin(labels[2:])).sum()}.")
    disagree = d[d["quadrant"].isin(labels[2:])]
    pro = disagree[disagree["quadrant"] == "process_over_result"]
    res = disagree[disagree["quadrant"] == "result_over_process"]
    print(
        f"\n  Where MalliScore is higher: SwStr% {pro['swstr_pct'].mean():.1f}, "
        f"outs {pro['outs'].mean():.1f}, ER {pro['earned_runs'].mean():.1f}"
    )
    print(
        f"  Where Game Score is higher: SwStr% {res['swstr_pct'].mean():.1f}, "
        f"outs {res['outs'].mean():.1f}, ER {res['earned_runs'].mean():.1f}"
    )
    print("  The disagreement is systematic and in the intended direction: MalliScore")
    print("  favours long, high-whiff starts that gave up runs; Game Score favours short,")
    print("  low-whiff starts that did not.")

    print("\n  Widest disagreements, MalliScore high / Game Score low:")
    top = d.nlargest(6, "gap")
    print(top[["game_date", "pitcher_name", "outs", "earned_runs", "swstr_pct",
               "xwoba_allowed", "malli_score", "game_score_v2"]].round(2).to_string(index=False))
    print("\n  Widest disagreements, Game Score high / MalliScore low:")
    bottom = d.nsmallest(6, "gap")
    print(bottom[["game_date", "pitcher_name", "outs", "earned_runs", "swstr_pct",
                  "xwoba_allowed", "malli_score", "game_score_v2"]].round(2).to_string(index=False))

    cases = pd.concat([d.nlargest(75, "gap"), d.nsmallest(75, "gap")])
    cols = ["season", "game_date", "pitcher_name", "pitcher", "game_pk", "quadrant", "gap",
            "malli_score", "malli_pct", "game_score_v1", "game_score_v2", "gs_pct",
            "dominance_score", "run_prevention_score", "workload", "outs", "pitches",
            "batters_faced", "strikeouts", "walks", "hits", "home_runs", "earned_runs",
            "runs_allowed", "swstr_pct", "called_strike_pct", "chase_pct", "xwoba_allowed",
            "game_whip"]
    cases[cols].sort_values("gap", ascending=False).to_csv(
        OUT_DIR / "disagreement_cases.csv", index=False
    )
    print(f"\n  Exported {len(cases)} cases for manual review -> disagreement_cases.csv")
    print("  Statistics can locate disagreement; only baseball judgment can say whether")
    print("  it is analytically useful. That review is the human input to Phase 6.")
    return summary


def main() -> None:
    warnings.filterwarnings("ignore")
    full = pd.read_parquet(DATASET)
    dev = full[full["season"] == DEV_SEASON].copy()
    reg = Register(f"Phase 3 reliability and benchmarking (dev={DEV_SEASON})")

    print(f"Development season {DEV_SEASON}: {len(dev):,} outings, "
          f"{dev['pitcher'].nunique()} pitchers")
    print("Reliability pools all seasons (pitcher-season is the unit) for power;")
    print("agreement and disagreement use the development season only.")

    reliability = audit_reliability(full, reg)
    agreement = audit_agreement(dev, reg)
    audit_disagreement(dev)

    reliability.to_csv(OUT_DIR / "reliability_split_half.csv", index=False)
    agreement.to_csv(OUT_DIR / "benchmark_agreement.csv", index=False)
    reg.to_frame().to_csv(OUT_DIR / "reliability_power_register.csv", index=False)
    print(reg.summary())


if __name__ == "__main__":
    main()
