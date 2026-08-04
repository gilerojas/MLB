"""Phase 6 — build V4 candidates, decide on 2024, then validate once each on 2025 and 2026.

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/decide_v4.py

Order of operations is the point. Validation is spendable exactly once:

    1. Every candidate is compared on 2024 alone.
    2. The winner is frozen, then run against 2025. Confirm; do not re-tune.
    3. The frozen winner is run once against 2026.
    4. A contradiction at step 2 or 3 is reported, not repaired.

Candidates are deliberately few and each embodies one argument, so the decision can
be explained in prose. A large search would produce a better number and a worse
explanation, and would be optimizing against a target that does not exist.
"""

from __future__ import annotations

import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.malliscore_validation.power_guard import Register, check_correlation  # noqa: E402
from research.study.malliscore_validation.reliability_benchmark import (  # noqa: E402
    paired_reliability_gap,
    split_half_reliability,
)
from src.pitching_performances.malli_score import (  # noqa: E402
    _DEFAULT_MEANS,
    _DEFAULT_STDS,
    _DOMINANCE_WEIGHTS,
    _RUN_PREVENTION_WEIGHTS,
)

OUT_DIR = Path(__file__).parent / "outputs"
DATASET = OUT_DIR / "outings_2024_2026.parquet"
DEV, VAL, CONFIRM = 2024, 2025, 2026
INVERTED = {"xwoba_allowed", "baserunners_per_bf", "game_whip", "log1p_er", "log1p_hr"}

# Empirical means and standard deviations measured on the 2024 development season.
# These replace the a-priori guesses, which were wrong by up to 2x on spread.
EMPIRICAL = {
    "swstr_pct": (11.78, 4.61),
    "called_strike_pct": (16.17, 3.96),
    "chase_pct": (28.48, 7.85),
    "xwoba_allowed": (0.321, 0.089),
    "baserunners_per_bf": (0.303, 0.110),
    "game_whip": (1.42, 1.01),
    "log1p_er": (1.053, 0.623),
    "log1p_hr": (0.429, 0.452),
}


@dataclass
class Candidate:
    name: str
    argument: str
    dominance: dict[str, float]
    run_prevention: dict[str, float]
    norms: dict[str, tuple[float, float]] = field(default_factory=lambda: dict(EMPIRICAL))

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        def pillar(weights: dict[str, float]) -> np.ndarray:
            total = sum(weights.values())
            acc = np.zeros(len(df))
            for key, w in weights.items():
                mu, sd = self.norms[key]
                z = (df[key].values - mu) / sd
                acc += w * (-z if key in INVERTED else z)
            return np.clip(50 + 15 * acc / total, 0, 100)

        dom, rp = pillar(self.dominance), pillar(self.run_prevention)
        denom = dom + rp
        core = np.where((dom > 0) & (rp > 0), 2 * dom * rp / np.where(denom == 0, 1, denom), 0.0)
        return pd.DataFrame(
            {
                "dominance": dom,
                "run_prevention": rp,
                "core": core,
                "score": np.clip(core * df["workload"].values, 0, 100),
            },
            index=df.index,
        )


def build_candidates() -> list[Candidate]:
    v3_norms = {k: (_DEFAULT_MEANS[k], _DEFAULT_STDS[k]) for k in _DEFAULT_MEANS}
    return [
        Candidate(
            "A. V3 as shipped",
            "Control. A-priori priors, WHIP, xwOBA inside dominance.",
            dict(_DOMINANCE_WEIGHTS), dict(_RUN_PREVENTION_WEIGHTS), v3_norms,
        ),
        Candidate(
            "B. V3 + empirical priors",
            "Only fixes the priors. Isolates how much the miscalibration alone cost.",
            dict(_DOMINANCE_WEIGHTS), dict(_RUN_PREVENTION_WEIGHTS),
        ),
        Candidate(
            "C. B + baserunners per BF",
            "Also removes IP from the run-prevention denominator, killing the tail.",
            dict(_DOMINANCE_WEIGHTS),
            {"baserunners_per_bf": 0.40, "log1p_er": 0.35, "log1p_hr": 0.25},
        ),
        Candidate(
            "D. C + xwOBA moved to run prevention",
            "Makes the pillars mean what they are named: process vs results.",
            {"swstr_pct": 0.40, "called_strike_pct": 0.30, "chase_pct": 0.30},
            {"baserunners_per_bf": 0.30, "xwoba_allowed": 0.30,
             "log1p_er": 0.25, "log1p_hr": 0.15},
        ),
        Candidate(
            "E. C + reliability-weighted run prevention",
            "Shifts weight off the least reliable input toward the most reliable.",
            {"swstr_pct": 0.35, "called_strike_pct": 0.25, "chase_pct": 0.20,
             "xwoba_allowed": 0.20},
            {"baserunners_per_bf": 0.30, "log1p_er": 0.40, "log1p_hr": 0.30},
        ),
    ]


def evaluate(cand: Candidate, df: pd.DataFrame, label: str) -> dict:
    """Score one candidate on the measurable decision criteria."""
    scored = cand.score(df)
    d = df.copy()
    d["cand_score"] = scored["score"].values
    d["cand_dom"] = scored["dominance"].values
    d["cand_rp"] = scored["run_prevention"].values

    rel = split_half_reliability(d, "cand_score", 10)
    return {
        "candidate": cand.name,
        "season": label,
        "n": len(d),
        "mean": d["cand_score"].mean(),
        "sd": d["cand_score"].std(),
        "exact_zeros": int((d["cand_score"] <= 0).sum()),
        "zero_pct": (d["cand_score"] <= 0).mean() * 100,
        "pillar_corr": float(np.corrcoef(scored["dominance"], scored["run_prevention"])[0, 1]),
        "reliability": rel["reliability"],
        "rel_ci_low": rel["ci_low"],
        "rel_ci_high": rel["ci_high"],
        "spearman_gs_v2": float(stats.spearmanr(d["cand_score"], d["game_score_v2"])[0]),
        "spearman_vs_v3": float(stats.spearmanr(d["cand_score"], d["malli_score"])[0]),
        "corr_outs": float(stats.spearmanr(d["cand_score"], d["outs"])[0]),
    }


# The single judgment input in the whole decision, stated openly so it can be argued
# with. Everything else below is measured. Scored 0-1 on "how cleanly can this be
# explained to a reader who has never seen MalliScore".
INTERPRETABILITY = {
    "A. V3 as shipped": 0.70,
    "B. V3 + empirical priors": 0.80,
    "C. B + baserunners per BF": 0.90,
    "D. C + xwOBA moved to run prevention": 0.85,
    "E. C + reliability-weighted run prevention": 0.75,
}
INTERPRETABILITY_WHY = {
    "A. V3 as shipped": "carries an unexplainable defect: blow-ups all score exactly 0",
    "B. V3 + empirical priors": "same story as V3, with constants that match reality",
    "C. B + baserunners per BF": "one input swap; 'baserunners per batter faced' is plain",
    "D. C + xwOBA moved to run prevention": "pillars finally mean what they are named",
    "E. C + reliability-weighted run prevention": "reweighting the weakly-identified weights",
}

CRITERION_WEIGHTS = {
    "interpretability": 0.30,
    "stability": 0.25,
    "non_redundancy": 0.20,
    "disagreement": 0.15,
    "signal": 0.10,
}


def score_candidates(table: pd.DataFrame, dev: pd.DataFrame, candidates: list[Candidate],
                     interpretability: dict[str, float] | None = None) -> pd.DataFrame:
    """Apply the pre-agreed multi-objective rubric. Higher is better on every axis."""
    interp = interpretability or INTERPRETABILITY
    rows = []
    for _, r in table.iterrows():
        name = r["candidate"]
        # Stability: reliability is the direct measure of a descriptive index's
        # stability, rescaled so the observed spread uses the full 0-1 range.
        stability = r["reliability"]
        # Non-redundancy: lower pillar correlation means the two halves say
        # different things. Also credit removing the exact-zero collapse, which
        # destroys rank information outright.
        non_redundancy = (1 - r["pillar_corr"]) * 0.7 + (r["exact_zeros"] == 0) * 0.3
        # Credible disagreement: agreement with Game Score should sit in a healthy
        # band. Too high is redundant, too low is unrecognizable. Peak at 0.92.
        disagreement = 1 - abs(r["spearman_gs_v2"] - 0.92) / 0.92
        # Phase 5 confirmed a null for every variant, so this axis cannot separate
        # candidates. It is carried at its agreed weight and scored flat, rather than
        # quietly dropped.
        signal = 0.5
        parts = {
            "interpretability": interp[name],
            "stability": stability,
            "non_redundancy": non_redundancy,
            "disagreement": disagreement,
            "signal": signal,
        }
        rows.append({
            "candidate": name,
            **{k: v for k, v in parts.items()},
            "total": sum(CRITERION_WEIGHTS[k] * v for k, v in parts.items()),
        })
    return pd.DataFrame(rows).sort_values("total", ascending=False).reset_index(drop=True)


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["log1p_er"] = np.log1p(out["earned_runs"].clip(lower=0))
    out["log1p_hr"] = np.log1p(out["home_runs"].clip(lower=0))
    out["baserunners_per_bf"] = (out["hits"] + out["walks"]) / out["batters_faced"].clip(lower=1)
    return out


def rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def main() -> None:
    warnings.filterwarnings("ignore")
    full = prepare(pd.read_parquet(DATASET))
    dev = full[full["season"] == DEV].copy()
    candidates = build_candidates()
    reg = Register("Phase 6 V4 decision")

    rule(f"STEP 1 — COMPARE ALL CANDIDATES ON {DEV} ONLY")
    for c in candidates:
        print(f"  {c.name}\n      {c.argument}")
    rows = [evaluate(c, dev, str(DEV)) for c in candidates]
    table = pd.DataFrame(rows)
    cols = ["candidate", "mean", "sd", "exact_zeros", "pillar_corr", "reliability",
            "spearman_gs_v2", "spearman_vs_v3", "corr_outs"]
    print("\n" + table[cols].round(4).to_string(index=False))

    print("\n  Reading the columns against the decision criteria:")
    print("    exact_zeros   the V3 defect: blow-ups collapsing to a single value")
    print("    pillar_corr   lower means process and results are genuinely distinct")
    print("    reliability   split-half, >=10 starts; higher is a better descriptive index")
    print("    spearman_gs_v2  agreement with the benchmark; ~0.90-0.95 is the healthy band")
    print("    corr_outs     how much the score is simply a measure of length")

    # Reliability is the primary criterion, so test it paired against the control.
    print("\n  Paired reliability gap vs V3 (same pitchers, bootstrap on the difference):")
    for c in candidates[1:]:
        d = dev.copy()
        d["cand_score"] = c.score(d)["score"].values
        gap = paired_reliability_gap(d, "cand_score", "malli_score", 10)
        verdict = "RESOLVED" if gap["resolved"] else "not resolved"
        print(f"    {c.name:44} {gap['gap']:+.4f} "
              f"[{gap['ci_low']:+.4f},{gap['ci_high']:+.4f}]  {verdict}")

    rule("STEP 2 — SCORE AGAINST THE AGREED CRITERIA")
    scored = score_candidates(table, dev, candidates)
    print(scored.round(4).to_string(index=False))
    print("\n  Criterion weights are the ones agreed before the study: interpretability 30,")
    print("  stability 25, non-redundancy 20, credible disagreement 15, next-start signal 10.")
    print("  They govern model selection only and are not part of MalliScore.")
    print("\n  Only interpretability is assigned by judgment; it is documented per candidate")
    print("  below and its influence is stress-tested immediately after.")
    for c in candidates:
        print(f"    {c.name:44} {INTERPRETABILITY[c.name]:.2f}  {INTERPRETABILITY_WHY[c.name]}")

    winner = scored.iloc[0]["candidate"]
    runner_up = scored.iloc[1]["candidate"]
    print(f"\n  Winner: {winner}")
    print(f"  Runner-up: {runner_up} "
          f"(margin {scored.iloc[0]['total'] - scored.iloc[1]['total']:+.4f})")

    # The one judgment input should not be the thing deciding the outcome.
    print("\n  Stress test — does the winner survive any interpretability scoring?")
    flips = 0
    rng = np.random.default_rng(20260802)
    for _ in range(2000):
        jittered = dict(zip(INTERPRETABILITY, rng.uniform(0.5, 1.0, len(INTERPRETABILITY))))
        alt = score_candidates(table, dev, candidates, interpretability=jittered)
        if alt.iloc[0]["candidate"] != winner:
            flips += 1
    print(f"    With interpretability scores drawn at random 2000 times, the winner")
    print(f"    changed {flips / 20:.1f}% of the time.")
    if flips / 2000 > 0.5:
        print("    The judgment input is driving the result, so the measured criteria")
        print("    alone do not separate these candidates. Say so rather than hiding it.")
    else:
        print("    The winner is driven by the measured criteria, not by the judgment call.")

    chosen = next(c for c in candidates if c.name == winner)
    print(f"\n  Proceeding with: {chosen.name}\n      {chosen.argument}")

    rule(f"STEP 3 — VALIDATE THE FROZEN CANDIDATE ON {VAL}")
    print("  The candidate is now fixed. Nothing below may change it.\n")
    val = full[full["season"] == VAL].copy()
    v3_val = evaluate(candidates[0], val, str(VAL))
    c_val = evaluate(chosen, val, str(VAL))
    print(pd.DataFrame([v3_val, c_val])[cols].round(4).to_string(index=False))

    d = val.copy()
    d["cand_score"] = chosen.score(d)["score"].values
    gap = paired_reliability_gap(d, "cand_score", "malli_score", 10)
    print(f"\n  Reliability gap vs V3 on {VAL}: {gap['gap']:+.4f} "
          f"[{gap['ci_low']:+.4f},{gap['ci_high']:+.4f}] n={gap['n']}  "
          f"{'RESOLVED' if gap['resolved'] else 'not resolved'}")
    dev_gap = paired_reliability_gap(dev.assign(cand_score=chosen.score(dev)["score"].values),
                                     "cand_score", "malli_score", 10)
    print(f"  Same measurement on {DEV} was {dev_gap['gap']:+.4f} — "
          f"{'consistent' if np.sign(gap['gap']) == np.sign(dev_gap['gap']) else 'CONTRADICTED'}")

    rule(f"STEP 4 — CONFIRM ONCE ON {CONFIRM}")
    conf = full[full["season"] == CONFIRM].copy()
    c_conf = evaluate(chosen, conf, str(CONFIRM))
    v3_conf = evaluate(candidates[0], conf, str(CONFIRM))
    print(pd.DataFrame([v3_conf, c_conf])[cols].round(4).to_string(index=False))
    d = conf.copy()
    d["cand_score"] = chosen.score(d)["score"].values
    gap_c = paired_reliability_gap(d, "cand_score", "malli_score", 10)
    print(f"\n  Reliability gap vs V3 on {CONFIRM}: {gap_c['gap']:+.4f} "
          f"[{gap_c['ci_low']:+.4f},{gap_c['ci_high']:+.4f}] n={gap_c['n']}  "
          f"{'RESOLVED' if gap_c['resolved'] else 'not resolved'}")

    rule("STEP 5 — THE DEFECT V4 FIXES")
    for season, frame in [(DEV, dev), (VAL, val), (CONFIRM, conf)]:
        s = chosen.score(frame)["score"]
        v3_zeros = int((frame["malli_score"] <= 0).sum())
        print(f"  {season}: V3 collapsed {v3_zeros} outings to exactly 0; "
              f"V4 collapses {int((s <= 0).sum())}.")
    collapsed = dev[dev["malli_score"] <= 0].copy()
    collapsed["v4"] = chosen.score(collapsed)["score"].values
    print(f"\n  Those {len(collapsed)} outings under V4 now span "
          f"{collapsed['v4'].min():.1f} to {collapsed['v4'].max():.1f}, "
          f"tracking Game Score v2 from {collapsed['game_score_v2'].min():.0f} "
          f"to {collapsed['game_score_v2'].max():.0f}.")

    all_rows = pd.DataFrame(rows + [v3_val, c_val, v3_conf, c_conf])
    all_rows.to_csv(OUT_DIR / "v4_candidate_comparison.csv", index=False)
    reg.to_frame().to_csv(OUT_DIR / "v4_decision_power_register.csv", index=False)
    print(f"\nWrote v4_candidate_comparison.csv to {OUT_DIR.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
