"""
Build league-wide benchmarks for pitcher-card highlighting.

This script scans Statcast `pitches_enriched` parquet files from the MLB
warehouse and computes global percentile cutpoints for the metrics that are
color‑coded in `mallitalytics_daily_card.py`:

- Velo        (release_speed, mph)          — higher is better
- Whiff%      (whiff / pitches)            — higher is better
- Chase%      (chase / pitches)            — higher is better
- Str%        (strikes / pitches)          — higher is better
- xwOBA*      (estimated_woba_using_speedangle) — lower is better (quality of contact allowed)

Output:
- Writes a JSON file with percentile thresholds to:
    `config/pitch_metric_benchmarks_SEASON.json`

Usage (from repo root):

  python scripts/build_pitch_metric_benchmarks.py --season 2024 --game-type regular_season

You can re‑run this script when you add new seasons or want updated benchmarks.
`mallitalytics_daily_card.py` can then be wired to read this JSON and use
fixed thresholds for its gradient color mapping.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build league-wide pitcher-card metric benchmarks")
    p.add_argument("--season", type=int, required=True, help="Season year to scan (e.g. 2024)")
    p.add_argument(
        "--game-type",
        type=str,
        default="regular_season",
        help="Game type folder under season (e.g. regular_season, spring_training)",
    )
    p.add_argument(
        "--root",
        type=str,
        default="data/warehouse/mlb",
        help="Root directory of MLB warehouse (default: data/warehouse/mlb)",
    )
    p.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Optional cap on number of parquet files to sample (0 = use all)",
    )
    return p.parse_args()


def find_parquets(root: Path, season: int, game_type: str, max_files: int = 0) -> List[Path]:
    base = root / str(season) / game_type
    if not base.exists():
        raise SystemExit(f"Warehouse path not found: {base}")

    pattern = "game_*_pitches_enriched.parquet"
    files = sorted(base.rglob(pattern))
    if not files:
        raise SystemExit(f"No parquet files matching {pattern} found under {base}")
    if max_files and max_files > 0:
        files = files[: max_files]
    return files


def process_file(path: Path) -> pd.DataFrame:
    """
    Read a pitches_enriched parquet and compute per-pitch flags.
    Requires pitcher and pitch_type for aggregation.
    """
    required = ["pitcher", "pitch_type", "release_speed", "description", "zone", "type"]
    optional = ["estimated_woba_using_speedangle"]
    try:
        df = pd.read_parquet(path, columns=required + optional)
    except Exception:
        df = pd.read_parquet(path, columns=required)
        df["estimated_woba_using_speedangle"] = np.nan
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns {missing} in {path.name}")

    swing_codes = [
        "foul_bunt", "foul", "hit_into_play", "swinging_strike", "foul_tip",
        "swinging_strike_blocked", "missed_bunt", "bunt_foul_tip",
    ]
    whiff_codes = ["swinging_strike", "foul_tip", "swinging_strike_blocked"]

    df = df.copy()
    df["pitch_type"] = df["pitch_type"].fillna("UN").astype(str)
    df["release_speed"] = pd.to_numeric(df["release_speed"], errors="coerce")
    df["swing"] = df["description"].isin(swing_codes)
    df["whiff"] = df["description"].isin(whiff_codes)
    df["in_zone"] = pd.to_numeric(df["zone"], errors="coerce").lt(10)
    df["chase"] = (~df["in_zone"]) & df["swing"]
    df["is_strike"] = df["type"] == "S"
    if "estimated_woba_using_speedangle" in df.columns:
        df["xwoba"] = pd.to_numeric(df["estimated_woba_using_speedangle"], errors="coerce")
    else:
        df["xwoba"] = np.nan
    return df


def collect_metrics(files: List[Path]) -> Dict[str, np.ndarray]:
    """
    For each game file, aggregate by (pitcher, pitch_type) to get one rate per
    pitcher-pitch_type per game. Then collect those rates league-wide so
    percentiles reflect the distribution of Whiff%, Chase%, Str% (and mean
    velo, mean xwOBA) as shown on the card.
    """
    velo_rates: List[float] = []
    whiff_rates: List[float] = []
    chase_rates: List[float] = []
    strike_rates: List[float] = []
    xwoba_means: List[float] = []

    for i, path in enumerate(files, start=1):
        try:
            df = process_file(path)
        except Exception as exc:
            print(f"[WARN] Skipping {path.name}: {exc}")
            continue

        if df.empty or "pitcher" not in df.columns or "pitch_type" not in df.columns:
            continue

        g = df.groupby(["pitcher", "pitch_type"]).agg(
            count=("pitch_type", "count"),
            velo=("release_speed", "mean"),
            whiff=("whiff", "sum"),
            chase=("chase", "sum"),
            strike=("is_strike", "sum"),
            xwoba=("xwoba", "mean"),
        ).reset_index()

        g = g[g["count"] >= 1]
        velo_rates.extend(g["velo"].dropna().tolist())
        whiff_rates.extend((g["whiff"] / g["count"]).tolist())
        chase_rates.extend((g["chase"] / g["count"]).tolist())
        strike_rates.extend((g["strike"] / g["count"]).tolist())
        xwoba_means.extend(g["xwoba"].dropna().tolist())

        if i % 50 == 0 or i == len(files):
            print(f"  Processed {i}/{len(files)} files")

    if not velo_rates:
        raise SystemExit("No velocity data collected; aborting.")

    return {
        "velo": np.array(velo_rates, dtype=float),
        "whiff": np.array(whiff_rates, dtype=float),
        "chase": np.array(chase_rates, dtype=float),
        "strike": np.array(strike_rates, dtype=float),
        "xwoba": np.array(xwoba_means, dtype=float) if xwoba_means else np.array([], dtype=float),
    }


def percentiles(arr: np.ndarray, qs: List[float]) -> Dict[str, float]:
    if arr.size == 0:
        return {}
    pct_vals = np.percentile(arr, qs).tolist()
    return {f"p{int(q)}": float(v) for q, v in zip(qs, pct_vals)}


def main() -> None:
    args = parse_args()
    root = Path(args.root)
    print(f"Scanning warehouse at {root} for season={args.season}, game_type={args.game_type} ...")
    files = find_parquets(root, args.season, args.game_type, max_files=args.max_files)
    print(f"Found {len(files)} parquet files to process.")

    metrics = collect_metrics(files)

    qs = [5, 20, 40, 60, 80, 95]
    out: Dict[str, object] = {
        "meta": {
            "season": args.season,
            "game_type": args.game_type,
            "files_used": len(files),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "percentiles": qs,
        },
        # For velo / whiff / chase / strike, higher is better
        "velocity_mph": percentiles(metrics["velo"], qs),
        "whiff_per_pitch": percentiles(metrics["whiff"], qs),
        "chase_per_pitch": percentiles(metrics["chase"], qs),
        "strike_per_pitch": percentiles(metrics["strike"], qs),
        # For xwOBA allowed, lower is better; percentiles still reported high-to-low
        "xwoba_allowed": percentiles(metrics["xwoba"], qs),
    }

    cfg_dir = Path("config")
    cfg_dir.mkdir(parents=True, exist_ok=True)
    out_path = cfg_dir / f"pitch_metric_benchmarks_{args.season}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"\nSaved benchmarks to: {out_path}\n")


if __name__ == "__main__":
    main()

