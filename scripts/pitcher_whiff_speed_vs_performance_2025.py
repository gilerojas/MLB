#!/usr/bin/env python3
"""
Merge 2025 starter-game bat tracking (whiff bat speed) with season boxscore pitching
to compare elite pitchers' opponent whiff swing speeds vs K%, K/BB, etc.

Reads:
  - outputs/bat_speed_starter_outcomes/pitcher_game_table.csv (from pitcher_game_bat_speed_outcomes.py)
  - data/warehouse/mlb/player_season_boxscore_pitching_2024_2025.parquet

Writes:
  - outputs/bat_speed_starter_outcomes/pitcher_2025_whiff_speed_vs_performance.csv
  - outputs/bat_speed_starter_outcomes/pitcher_2025_whiff_speed_vs_performance.md
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--table",
        type=Path,
        default=_REPO / "outputs" / "bat_speed_starter_outcomes" / "pitcher_game_table.csv",
    )
    ap.add_argument(
        "--season-pitching",
        type=Path,
        default=_REPO
        / "data"
        / "warehouse"
        / "mlb"
        / "player_season_boxscore_pitching_2024_2025.parquet",
    )
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--min-starts", type=int, default=10, help="Min starter rows in table (proxy for starts)")
    ap.add_argument("--min-whiff-tracked", type=int, default=80, help="Min total whiffs with bat_speed across games")
    ap.add_argument("--top-n", type=int, default=40)
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=_REPO / "outputs" / "bat_speed_starter_outcomes",
    )
    args = ap.parse_args()

    if not args.table.is_file():
        print(f"Missing {args.table}; run scripts/pitcher_game_bat_speed_outcomes.py first.", file=sys.stderr)
        sys.exit(1)
    if not args.season_pitching.is_file():
        print(f"Missing {args.season_pitching}", file=sys.stderr)
        sys.exit(1)

    g = pd.read_csv(args.table)
    g = g[g["season"] == args.season].copy()
    if g.empty:
        print(f"No rows for season {args.season} in {args.table}", file=sys.stderr)
        sys.exit(1)

    # Aggregate per pitcher (starter outings only)
    def _wmean(series_vals: pd.Series, weights: pd.Series) -> float:
        w = weights.fillna(0).values
        v = pd.to_numeric(series_vals, errors="coerce").fillna(0).values
        s = w.sum()
        if s <= 0:
            return float("nan")
        return float((v * w).sum() / s)

    rows = []
    for pid, sub in g.groupby("pitcher_id"):
        nw = sub["n_whiff_tracked"].fillna(0).astype(float)
        rows.append(
            {
                "pitcher_id": int(pid),
                "n_starts_in_sample": len(sub),
                "total_whiff_tracked": float(nw.sum()),
                "whiff_bs_weighted_mean": _wmean(sub["mean_whiff_bat_speed"], nw),
                "mean_bat_speed_weighted": _wmean(sub["mean_bat_speed"], sub["n_tracked_swings"].fillna(0).astype(float)),
                "bs75_pct_weighted": _wmean(sub["bs75_pct"], sub["n_tracked_swings"].fillna(0).astype(float)),
                "mean_game_score": float(sub["game_score"].mean()),
                "rv_per_start": float(sub["rv_game"].mean()),
            }
        )
    agg = pd.DataFrame(rows)

    sea = pd.read_parquet(args.season_pitching)
    sea = sea[sea["season"] == args.season].copy()
    sea = sea.rename(
        columns={
            "player_id": "pitcher_id",
            "so": "season_k",
            "bb": "season_bb",
            "ip": "season_ip",
        }
    )
    keep = [
        "pitcher_id",
        "player_name",
        "team_id",
        "games",
        "games_started",
        "season_ip",
        "batters_faced",
        "era",
        "season_k",
        "season_bb",
        "k_pct",
        "bb_pct",
        "k_minus_bb_pct",
        "k9",
        "bb9",
    ]
    sea = sea[[c for c in keep if c in sea.columns]]

    m = agg.merge(sea, on="pitcher_id", how="inner")
    m["k_bb"] = np.where(m["season_bb"] > 0, m["season_k"] / m["season_bb"], np.nan)

    m = m[m["n_starts_in_sample"] >= args.min_starts]
    m = m[m["total_whiff_tracked"] >= args.min_whiff_tracked]

    # Full table sorted by K-BB% (season) — "quality" axis
    m = m.sort_values("k_minus_bb_pct", ascending=False, na_position="last")

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "pitcher_2025_whiff_speed_vs_performance.csv"
    m.to_csv(full_path, index=False)

    top = m.head(args.top_n)
    top_path = out_dir / f"pitcher_2025_top{args.top_n}_by_k_minus_bb_whiff_speed.csv"
    top.to_csv(top_path, index=False)

    # Spearman: whiff speed vs k_minus_bb_pct (do good pitchers see faster/slower whiff swings?)
    corr = m["whiff_bs_weighted_mean"].corr(m["k_minus_bb_pct"], method="spearman")
    corr_kpct = m["whiff_bs_weighted_mean"].corr(m["k_pct"], method="spearman")

    md_lines = [
        f"# 2025 pitchers: whiff bat speed vs performance (n≥{args.min_starts} starts in sample, ≥{args.min_whiff_tracked} tracked whiffs)",
        "",
        f"- **Merged rows:** {len(m)} pitchers (starter-game sample × season boxscore).",
        f"- **Spearman ρ (weighted mean whiff BS vs season K−BB%):** {corr:.3f}",
        f"- **Spearman ρ (weighted mean whiff BS vs season K%):** {corr_kpct:.3f}",
        "",
        "Negative ρ → elite strikeout pitchers tend to see **slower** mean whiff swing speeds in this merge (or vice versa).",
        "",
        f"## Top {args.top_n} by season K−BB% (with whiff speed)",
        "",
        "| Rank | Pitcher | K−BB% | K% | BB% | K/BB | Whiff BS (wt) | Starts (sample) | ERA |",
        "|------|---------|-------|-----|-----|------|----------------|-----------------|-----|",
    ]
    for i, (_, r) in enumerate(top.iterrows(), start=1):
        name = str(r.get("player_name", r["pitcher_id"]))[:40]
        kmb = float(r["k_minus_bb_pct"]) if pd.notna(r["k_minus_bb_pct"]) else float("nan")
        kp = float(r["k_pct"]) if pd.notna(r["k_pct"]) else float("nan")
        bp = float(r["bb_pct"]) if pd.notna(r["bb_pct"]) else float("nan")
        kbb = float(r["k_bb"]) if pd.notna(r["k_bb"]) else float("nan")
        wbs = float(r["whiff_bs_weighted_mean"]) if pd.notna(r["whiff_bs_weighted_mean"]) else float("nan")
        ns = int(r["n_starts_in_sample"])
        era = float(r["era"]) if pd.notna(r.get("era")) else float("nan")
        md_lines.append(
            f"| {i} | {name} | {kmb:.3f} | {kp:.3f} | {bp:.3f} | {kbb:.2f} | {wbs:.1f} | {ns} | {era:.2f} |"
        )

    md_lines.extend(
        [
            "",
            f"Files: `{full_path.relative_to(_REPO)}`, `{top_path.relative_to(_REPO)}`",
        ]
    )
    (out_dir / "pitcher_2025_whiff_speed_vs_performance.md").write_text("\n".join(md_lines), encoding="utf-8")

    print(f"Wrote {len(m)} pitchers -> {full_path}")
    print(f"Top {args.top_n} -> {top_path}")
    print(f"Spearman whiff_BS vs K-BB%: {corr:.3f}  |  vs K%: {corr_kpct:.3f}")


if __name__ == "__main__":
    main()
