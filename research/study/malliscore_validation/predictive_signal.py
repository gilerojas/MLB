"""Phase 5 — does MalliScore carry information Game Score does not?

    ./mlb_env.nosync/bin/python research/study/malliscore_validation/predictive_signal.py

This is a validation test, not an optimization target. MalliScore is descriptive:
a small predictive gain is an acceptable, even expected, result. Nothing here feeds
back into the weights.

The question is strictly incremental, so the models are nested:

    1. pitcher rolling baseline alone          (established ability)
    2. baseline + Game Score v2                (ability + a conventional result metric)
    3. baseline + Game Score v2 + MalliScore   (does the process signal add anything?)

Targets are next-start *components*, never next-start MalliScore, which would be
circular. Fit on 2024, evaluated once out-of-sample on 2025.

`statsmodels` is absent from every environment here, so this uses scipy + sklearn
rather than adding a dependency.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import r2_score, roc_auc_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.malliscore_validation.power_guard import Register, Result, verdict  # noqa: E402

OUT_DIR = Path(__file__).parent / "outputs"
DATASET = OUT_DIR / "outings_2024_2026.parquet"
TRAIN_SEASON = 2024
TEST_SEASON = 2025
SEED = 20260802

# A gain in out-of-sample R^2 smaller than this would not justify any change to
# MalliScore, however tight its confidence interval.
MEANINGFUL_R2_GAIN = 0.005
MEANINGFUL_AUC_GAIN = 0.010

BASELINE = [
    "malli_score_roll5",
    "swstr_pct_roll5",
    "called_strike_pct_roll5",
    "chase_pct_roll5",
    "xwoba_allowed_roll5",
    "game_whip_roll5",
    "outs_roll5",
    "earned_runs_roll5",
]
TARGETS = {
    "next_swstr_pct": "next-start SwStr%",
    "next_xwoba_allowed": "next-start xwOBA allowed",
    "next_k_minus_bb_pct": "next-start K-BB%",
    "next_game_whip": "next-start WHIP",
}
MODELS = {
    "1. baseline": BASELINE,
    "2. + Game Score v2": BASELINE + ["game_score_v2"],
    "3. + GSv2 + MalliScore": BASELINE + ["game_score_v2", "malli_score"],
}


def rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def prepare(df: pd.DataFrame, features: list[str], target: str) -> pd.DataFrame:
    cols = features + [target, "pitcher", "season"]
    out = df[cols].replace([np.inf, -np.inf], np.nan).dropna()
    return out


def fit_eval(train: pd.DataFrame, test: pd.DataFrame, features: list[str],
             target: str) -> tuple[float, np.ndarray, np.ndarray]:
    """Ridge on standardized features; returns out-of-sample R^2 and predictions."""
    scaler = StandardScaler().fit(train[features])
    model = Ridge(alpha=1.0, random_state=SEED)
    model.fit(scaler.transform(train[features]), train[target])
    pred = model.predict(scaler.transform(test[features]))
    return r2_score(test[target], pred), pred, test[target].values


def bootstrap_gain(actual: np.ndarray, pred_a: np.ndarray, pred_b: np.ndarray,
                   metric: str = "r2", n_boot: int = 2000) -> tuple[float, float, float]:
    """Paired bootstrap of the improvement from model a to model b.

    Both models predict the same held-out rows, so resampling rows once and scoring
    both on that draw removes the shared variation -- the same paired logic used for
    the reliability gap in Phase 3.
    """
    rng = np.random.default_rng(SEED)
    n = len(actual)
    score = r2_score if metric == "r2" else roc_auc_score
    observed = score(actual, pred_b) - score(actual, pred_a)
    draws = []
    for _ in range(n_boot):
        idx = rng.integers(0, n, n)
        if metric == "auc" and len(np.unique(actual[idx])) < 2:
            continue
        try:
            draws.append(score(actual[idx], pred_b[idx]) - score(actual[idx], pred_a[idx]))
        except Exception:
            continue
    lo, hi = np.percentile(draws, [2.5, 97.5])
    return observed, float(lo), float(hi)


def run_regression_targets(train_all: pd.DataFrame, test_all: pd.DataFrame,
                           reg: Register) -> pd.DataFrame:
    rule("1. NEXT-START COMPONENTS — nested out-of-sample comparison")
    rows = []
    for target, label in TARGETS.items():
        widest = MODELS["3. + GSv2 + MalliScore"]
        train = prepare(train_all, widest, target)
        test = prepare(test_all, widest, target)
        print(f"\n  {label}   (train n={len(train):,}, test n={len(test):,})")

        preds, scores = {}, {}
        for name, features in MODELS.items():
            r2, pred, actual = fit_eval(train, test, features, target)
            preds[name], scores[name] = pred, r2
            print(f"    {name:26} out-of-sample R2 = {r2:+.4f}")

        actual = test[target].values
        for a, b, tag in [
            ("1. baseline", "2. + Game Score v2", "GSv2 over baseline"),
            ("2. + Game Score v2", "3. + GSv2 + MalliScore", "MalliScore over GSv2"),
        ]:
            gain, lo, hi = bootstrap_gain(actual, preds[a], preds[b])
            v = verdict(gain, (lo, hi), MEANINGFUL_R2_GAIN, 0.0, MEANINGFUL_R2_GAIN)
            res = Result(f"{target}: {tag}", gain, len(actual), lo, hi,
                         MEANINGFUL_R2_GAIN, v, MEANINGFUL_R2_GAIN)
            reg.add(res)
            print(f"      {tag:22} {gain:+.4f} [{lo:+.4f},{hi:+.4f}]  {v}")
            rows.append({"target": target, "comparison": tag, "gain": gain,
                         "ci_low": lo, "ci_high": hi, "verdict": v,
                         **{f"r2_{k}": v2 for k, v2 in scores.items()}})
    return pd.DataFrame(rows)


def run_elite_classifier(train_all: pd.DataFrame, test_all: pd.DataFrame,
                         reg: Register) -> pd.DataFrame:
    rule("2. PROBABILITY OF AN ELITE NEXT START")
    threshold = train_all["malli_score"].quantile(0.90)
    print(f"  'Elite' = next-start MalliScore above the {TRAIN_SEASON} p90 of {threshold:.2f}")
    print("  The label uses MalliScore, but the features never do beyond the shared")
    print("  baseline, so the comparison between models remains fair.\n")

    widest = MODELS["3. + GSv2 + MalliScore"]
    train = prepare(train_all, widest, "next_malli_score")
    test = prepare(test_all, widest, "next_malli_score")
    y_train = (train["next_malli_score"] >= threshold).astype(int)
    y_test = (test["next_malli_score"] >= threshold).astype(int)
    print(f"  train n={len(train):,} ({y_train.mean() * 100:.1f}% elite) | "
          f"test n={len(test):,} ({y_test.mean() * 100:.1f}% elite)")

    preds, rows = {}, []
    for name, features in MODELS.items():
        scaler = StandardScaler().fit(train[features])
        clf = LogisticRegression(max_iter=2000, random_state=SEED)
        clf.fit(scaler.transform(train[features]), y_train)
        p = clf.predict_proba(scaler.transform(test[features]))[:, 1]
        preds[name] = p
        auc = roc_auc_score(y_test, p)
        print(f"    {name:26} out-of-sample AUC = {auc:.4f}")

    for a, b, tag in [
        ("1. baseline", "2. + Game Score v2", "GSv2 over baseline"),
        ("2. + Game Score v2", "3. + GSv2 + MalliScore", "MalliScore over GSv2"),
    ]:
        gain, lo, hi = bootstrap_gain(y_test.values, preds[a], preds[b], metric="auc")
        v = verdict(gain, (lo, hi), MEANINGFUL_AUC_GAIN, 0.0, MEANINGFUL_AUC_GAIN)
        reg.add(Result(f"elite next start: {tag}", gain, len(y_test), lo, hi,
                       MEANINGFUL_AUC_GAIN, v, MEANINGFUL_AUC_GAIN))
        print(f"      {tag:22} {gain:+.4f} [{lo:+.4f},{hi:+.4f}]  {v}")
        rows.append({"target": "elite_next_start", "comparison": tag, "gain": gain,
                     "ci_low": lo, "ci_high": hi, "verdict": v})
    return pd.DataFrame(rows)


def run_within_pitcher(train_all: pd.DataFrame, test_all: pd.DataFrame,
                       reg: Register) -> None:
    """Does an outing above a pitcher's own baseline predict his next start?

    Between-pitcher analysis can be carried entirely by "good pitchers are good".
    Differencing against each pitcher's rolling baseline removes that, leaving only
    the within-pitcher question: does a start that beat expectations mean anything?
    """
    rule("3. WITHIN-PITCHER — controlling for who the pitcher is")
    frames = []
    for df in (train_all, test_all):
        d = df.copy()
        d["malli_vs_own"] = d["malli_score"] - d["malli_score_roll5"]
        d["gs_vs_own"] = d["game_score_v2"] - d["game_score_v2_roll5"]
        d["next_swstr_vs_own"] = d["next_swstr_pct"] - d["swstr_pct_roll5"]
        frames.append(d)
    train, test = frames

    cols = ["malli_vs_own", "gs_vs_own", "next_swstr_vs_own"]
    train = train[cols].replace([np.inf, -np.inf], np.nan).dropna()
    test = test[cols].replace([np.inf, -np.inf], np.nan).dropna()
    print(f"  train n={len(train):,} | test n={len(test):,}")

    preds = {}
    for name, features in [("gs deviation", ["gs_vs_own"]),
                           ("gs + malli deviation", ["gs_vs_own", "malli_vs_own"])]:
        model = Ridge(alpha=1.0, random_state=SEED).fit(train[features], train["next_swstr_vs_own"])
        preds[name] = model.predict(test[features])
        print(f"    {name:24} R2 = {r2_score(test['next_swstr_vs_own'], preds[name]):+.4f}")

    gain, lo, hi = bootstrap_gain(test["next_swstr_vs_own"].values,
                                  preds["gs deviation"], preds["gs + malli deviation"])
    v = verdict(gain, (lo, hi), MEANINGFUL_R2_GAIN, 0.0, MEANINGFUL_R2_GAIN)
    reg.add(Result("within-pitcher: MalliScore over GSv2", gain, len(test), lo, hi,
                   MEANINGFUL_R2_GAIN, v, MEANINGFUL_R2_GAIN))
    print(f"      MalliScore over GSv2   {gain:+.4f} [{lo:+.4f},{hi:+.4f}]  {v}")


def main() -> None:
    warnings.filterwarnings("ignore")
    full = pd.read_parquet(DATASET)
    train_all = full[full["season"] == TRAIN_SEASON].copy()
    test_all = full[full["season"] == TEST_SEASON].copy()
    reg = Register(f"Phase 5 predictive signal (train={TRAIN_SEASON}, test={TEST_SEASON})")

    print(f"Train {TRAIN_SEASON}: {len(train_all):,} outings")
    print(f"Test  {TEST_SEASON}: {len(test_all):,} outings — the single designated read")
    print("\nMalliScore is descriptive. A small gain here is an acceptable result and")
    print("does not argue against the metric; a large gain would be a bonus, not the point.")

    regression = run_regression_targets(train_all, test_all, reg)
    elite = run_elite_classifier(train_all, test_all, reg)
    run_within_pitcher(train_all, test_all, reg)

    rule("VERDICT")
    usable = [r for r in reg.results if r.verdict != "UNDERPOWERED"]
    positive = [r for r in usable if r.verdict == "RESOLVED" and r.effect > 0]
    malli_tests = [r for r in reg.results if "MalliScore over GSv2" in r.name]
    print(f"  {len(positive)} of {len(reg.results)} comparisons showed a resolved positive gain.")
    print("\n  MalliScore's incremental contribution over Game Score v2:")
    for r in malli_tests:
        print(f"    {r.name:52} {r.effect:+.4f}  {r.verdict}")
    print("\n  Read this as construct validation, not as a scoreboard. Phase 3's")
    print("  reliability result is the stronger evidence for MalliScore; this phase only")
    print("  asks whether the process inputs contain signal Game Score misses.")

    pd.concat([regression, elite]).to_csv(OUT_DIR / "predictive_signal.csv", index=False)
    reg.to_frame().to_csv(OUT_DIR / "predictive_power_register.csv", index=False)
    print(reg.summary())


if __name__ == "__main__":
    main()
