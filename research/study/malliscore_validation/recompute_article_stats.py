"""Recompute the article-facing V4 statistics on the complete-season dataset."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.malliscore_validation.reliability_benchmark import (  # noqa: E402
    paired_reliability_gap,
    split_half_reliability,
)

OUT_DIR = Path(__file__).parent / "outputs"
DATASET = OUT_DIR / "outings_2024_2026.parquet"
REPORT = OUT_DIR / "article_complete_season_stats.json"

INPUTS = {
    "swstr_pct": "SwStr%",
    "chase_pct": "Chase%",
    "called_strike_pct": "Called strike%",
    "xwoba_allowed": "xwOBA allowed",
    "outs": "Outs",
    "reach_rate_allowed": "RRA",
    "earned_runs": "Earned runs",
    "home_runs": "Home runs",
    "dominance_score_v4": "Dominance pillar",
    "run_prevention_score_v4": "Run Prevention pillar",
    "malli_score_v4": "MalliScore V4",
    "game_score_v2": "Game Score v2",
}


def pct_rank(values: np.ndarray, score: float) -> float:
    return float((values < score).mean() * 100.0)


def main() -> None:
    df = pd.read_parquet(DATASET)
    df["game_date"] = pd.to_datetime(df["game_date"])
    v4 = df["malli_score_v4"].to_numpy()
    print(f"n={len(df):,}  {df['game_date'].min().date()} to {df['game_date'].max().date()}")
    print(df.groupby("season").size().to_string())

    dist = {
        "n": int(len(df)),
        "by_season": {int(k): int(v) for k, v in df.groupby("season").size().items()},
        "date_min": str(df["game_date"].min().date()),
        "date_max": str(df["game_date"].max().date()),
        "median": float(np.median(v4)),
        "p90": float(np.quantile(v4, 0.90)),
        "p99": float(np.quantile(v4, 0.99)),
        "min": float(np.min(v4)),
        "max": float(np.max(v4)),
        "zeros": int((v4 == 0).sum()),
        "below_12": int((v4 < 12).sum()),
        "share_ge_50": float((v4 >= 50).mean()),
        "share_ge_60": float((v4 >= 60).mean()),
        "share_ge_70": float((v4 >= 70).mean()),
        "top35_cutoff": float(np.quantile(v4, 1 - 0.35)),
        "top12_cutoff": float(np.quantile(v4, 1 - 0.12)),
        "top2_cutoff": float(np.quantile(v4, 1 - 0.02)),
    }
    print("\nV4 distribution")
    for k in ("median", "p90", "p99", "min", "max", "zeros"):
        print(f"  {k}: {dist[k]}")
    print(
        f"  share >=50 {dist['share_ge_50']:.3f}  >=60 {dist['share_ge_60']:.3f}  "
        f">=70 {dist['share_ge_70']:.3f}"
    )

    rel_rows = []
    for metric, label in INPUTS.items():
        res = split_half_reliability(df, metric, min_starts=8)
        res["label"] = label
        res["min_starts"] = 8
        rel_rows.append(res)
        print(
            f"  rel8 {label:24} {res['reliability']:.3f}  n={res['n_pitcher_seasons']}"
        )
    rel10_v4 = split_half_reliability(df, "malli_score_v4", 10)
    rel10_gs = split_half_reliability(df, "game_score_v2", 10)
    gap = paired_reliability_gap(df, "malli_score_v4", "game_score_v2", 10)
    print(
        f"\nHeadline >=10: V4 {rel10_v4['reliability']:.3f}  GSv2 {rel10_gs['reliability']:.3f}  "
        f"gap {gap['gap']:+.3f} [{gap['ci_low']:+.3f},{gap['ci_high']:+.3f}] n={gap['n']}"
    )

    agreement = {}
    for season, sub in df.groupby("season"):
        sub = sub.dropna(subset=["malli_score_v4", "game_score_v2"])
        r = float(stats.spearmanr(sub["malli_score_v4"], sub["game_score_v2"]).statistic)
        agreement[int(season)] = r
        print(f"  Spearman {season}: {r:.3f}")

    # Scores live on different numeric scales. Compare within-season ranks rather
    # than subtracting raw points, then keep the two median disagreement groups.
    ranked = df.dropna(subset=["malli_score_v4", "game_score_v2"]).copy()
    ranked["malli_pct"] = ranked.groupby("season")["malli_score_v4"].rank(pct=True)
    ranked["gs_pct"] = ranked.groupby("season")["game_score_v2"].rank(pct=True)
    fav_malli = ranked[(ranked["malli_pct"] >= 0.5) & (ranked["gs_pct"] < 0.5)]
    fav_gs = ranked[(ranked["malli_pct"] < 0.5) & (ranked["gs_pct"] >= 0.5)]
    disagree = {
        "malli_n": int(len(fav_malli)),
        "malli_swstr": float(fav_malli["swstr_pct"].mean()),
        "malli_ip": float(fav_malli["outs"].mean() / 3.0),
        "malli_er": float(fav_malli["earned_runs"].mean()),
        "gs_n": int(len(fav_gs)),
        "gs_swstr": float(fav_gs["swstr_pct"].mean()),
        "gs_ip": float(fav_gs["outs"].mean() / 3.0),
        "gs_er": float(fav_gs["earned_runs"].mean()),
    }
    print("\nDisagreement tails")
    print(
        f"  Malli-fav n={disagree['malli_n']} SwStr {disagree['malli_swstr']:.1f}% "
        f"{disagree['malli_ip']:.1f} IP {disagree['malli_er']:.1f} ER"
    )
    print(
        f"  GS-fav    n={disagree['gs_n']} SwStr {disagree['gs_swstr']:.1f}% "
        f"{disagree['gs_ip']:.1f} IP {disagree['gs_er']:.1f} ER"
    )

    examples = {}
    for label, game_pk, pitcher in (
        ("imanaga", 746165, 684007),  # may need lookup
        ("wacha", None, 608379),
        ("liberatore", None, 669461),
        ("misiorowski", None, 694973),
    ):
        sub = df.copy()
        if pitcher:
            sub = sub[sub["pitcher"] == pitcher]
        if game_pk:
            sub = sub[sub["game_pk"] == game_pk]
        if label == "imanaga":
            sub = df[(df["pitcher"] == 668881) | (df["pitcher_name"].str.contains("Imanaga", na=False))]
            sub = sub[sub["game_date"] == "2024-05-18"]
        elif label == "wacha":
            sub = df[df["pitcher_name"].str.contains("Wacha", na=False)]
            sub = sub[sub["game_date"] == "2024-07-19"]
        elif label == "liberatore":
            sub = df[df["pitcher_name"].str.contains("Liberatore", na=False)]
            sub = sub[sub["game_date"] == "2026-08-02"]
        elif label == "misiorowski":
            sub = df[df["pitcher_name"].str.contains("Misiorowski", na=False)]
            if not sub.empty:
                sub = sub.loc[[sub["malli_score_v4"].idxmax()]]
        if sub.empty:
            examples[label] = None
            print(f"  {label}: NOT FOUND")
            continue
        row = sub.iloc[0]
        examples[label] = {
            "name": row.get("pitcher_name"),
            "date": str(pd.Timestamp(row["game_date"]).date()),
            "game_pk": int(row["game_pk"]),
            "line": f"{row['outs']/3:.1f} IP, {int(row['hits'])} H, {int(row['walks'])} BB, {int(row['earned_runs'])} ER, {int(row['strikeouts'])} K",
            "swstr": float(row["swstr_pct"]),
            "chase": float(row["chase_pct"]),
            "called": float(row["called_strike_pct"]),
            "xwoba": float(row["xwoba_allowed"]),
            "gs_v2": float(row["game_score_v2"]),
            "dom": float(row["dominance_score_v4"]),
            "rp": float(row["run_prevention_score_v4"]),
            "workload": float(row["workload_v4"]),
            "v4": float(row["malli_score_v4"]),
            "percentile": pct_rank(v4, float(row["malli_score_v4"])),
        }
        print(f"  {label}: {examples[label]}")

    top = df.loc[df["malli_score_v4"].idxmax()]
    print(
        f"\nMax V4 {top['malli_score_v4']:.1f} {top.get('pitcher_name')} {pd.Timestamp(top['game_date']).date()}"
    )

    payload = {
        "distribution": dist,
        "reliability_min8": [
            {
                "metric": r["metric"],
                "label": r["label"],
                "reliability": r["reliability"],
                "n": r["n_pitcher_seasons"],
            }
            for r in rel_rows
        ],
        "reliability_min10": {
            "malli_v4": rel10_v4,
            "game_score_v2": rel10_gs,
            "paired_gap": gap,
        },
        "spearman_by_season": agreement,
        "disagreement": disagree,
        "examples": examples,
        "max_start": {
            "name": top.get("pitcher_name"),
            "date": str(pd.Timestamp(top["game_date"]).date()),
            "score": float(top["malli_score_v4"]),
        },
    }
    # numpy/bool cleanup
    def convert(obj):
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, dict):
            return {str(k): convert(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert(v) for v in obj]
        return obj

    REPORT.write_text(json.dumps(convert(payload), indent=2))
    print(f"\nWrote {REPORT}")


if __name__ == "__main__":
    main()
