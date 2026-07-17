"""
Build league-wide benchmarks for pitcher-card highlighting.

This script scans Statcast `pitches_enriched` parquet files from the MLB
warehouse and computes global percentile cutpoints for the metrics that are
color‑coded in `mallitalytics_daily_card.py`:

- Velo        (release_speed, mph)          — higher is better
- Whiff%      (whiff / swings)             — higher is better
- Chase%      (chase / out-of-zone pitches) — higher is better
- BS75+%      (fast / tracked swings)       — lower is better for the pitcher
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
import os
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict
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
        default=os.environ.get("MLB_WAREHOUSE_DIR", "").strip() or "data/warehouse/mlb",
        help="Root of MLB warehouse (default: MLB_WAREHOUSE_DIR env, else data/warehouse/mlb)",
    )
    p.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Optional cap on number of parquet files to sample (0 = use all)",
    )
    return p.parse_args()


def find_parquets(root: Path, season: int, game_type: str, max_files: int = 0) -> List[Path]:
    """
    Canonical layout: {root}/{season}/{game_type}/pitches_enriched/game_*_pitches_enriched.parquet
    Falls back to any *.parquet in pitches_enriched/, then rglob under game_type.
    """
    base = (root / str(season) / game_type).resolve()
    if not base.exists():
        raise SystemExit(
            f"Warehouse path not found: {base}\n"
            "Set --root or MLB_WAREHOUSE_DIR to your mirror (e.g. Google Drive sync path)."
        )

    enriched = base / "pitches_enriched"
    files: List[Path] = []
    if enriched.is_dir():
        files = sorted(enriched.glob("game_*_pitches_enriched.parquet"))
        if not files:
            files = sorted(enriched.glob("*.parquet"))
    if not files:
        files = sorted(base.rglob("game_*_pitches_enriched.parquet"))
    if not files:
        raise SystemExit(
            f"No pitches_enriched parquet files under:\n  {base}\n"
            f"Expected files like: {enriched / 'game_*_pitches_enriched.parquet'}\n"
            "Sync data/warehouse/mlb from Drive (Hub Settings → Sync or rclone), or point "
            "MLB_WAREHOUSE_DIR / --root at a folder that contains "
            f"{season}/{game_type}/pitches_enriched/*.parquet"
        )
    if max_files and max_files > 0:
        files = files[: max_files]
    return files


def process_file(path: Path) -> pd.DataFrame:
    """
    Read a pitches_enriched parquet and compute per-pitch flags.
    Requires pitcher and pitch_type for aggregation.
    """
    required = ["pitcher", "pitch_type", "release_speed", "description", "zone", "type"]
    optional = ["estimated_woba_using_speedangle", "bat_speed"]
    try:
        df = pd.read_parquet(path, columns=required + optional)
    except Exception:
        df = pd.read_parquet(path, columns=required)
        df["estimated_woba_using_speedangle"] = np.nan
        df["bat_speed"] = np.nan
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
    z = pd.to_numeric(df["zone"], errors="coerce")
    df["in_zone"] = (z < 10) & (z > 0)
    df["out_zone"] = (z > 10) | (z == 0)
    df["chase"] = df["out_zone"] & df["swing"]
    df["is_strike"] = df["type"] == "S"
    if "bat_speed" in df.columns:
        df["bat_speed"] = pd.to_numeric(df["bat_speed"], errors="coerce")
        df["tracked_swing"] = df["swing"] & df["bat_speed"].notna()
        df["fast_ge_75"] = df["tracked_swing"] & (df["bat_speed"] >= 75.0)
    else:
        df["tracked_swing"] = False
        df["fast_ge_75"] = False
    if "estimated_woba_using_speedangle" in df.columns:
        df["xwoba"] = pd.to_numeric(df["estimated_woba_using_speedangle"], errors="coerce")
    else:
        df["xwoba"] = np.nan
    return df


def collect_metrics(files: List[Path]) -> Dict[str, object]:
    """Collect stable game-level pitch metrics globally and by pitch type."""
    metric_names = ("velo", "whiff", "chase", "fast_swing", "strike", "zone", "xwoba")
    global_values: dict[str, list[float]] = {key: [] for key in metric_names}
    pitch_values: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {key: [] for key in metric_names}
    )

    def add(metric: str, frame: pd.DataFrame, values: pd.Series) -> None:
        clean = pd.to_numeric(values, errors="coerce")
        for idx, value in clean.dropna().items():
            numeric = float(value)
            global_values[metric].append(numeric)
            pitch_values[str(frame.loc[idx, "pitch_type"])][metric].append(numeric)

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
            swing=("swing", "sum"),
            out_zone=("out_zone", "sum"),
            chase=("chase", "sum"),
            tracked_swing=("tracked_swing", "sum"),
            fast_ge_75=("fast_ge_75", "sum"),
            strike=("is_strike", "sum"),
            in_zone=("in_zone", "sum"),
            xwoba=("xwoba", "mean"),
            xwoba_n=("xwoba", "count"),
        ).reset_index()

        add("velo", g[g["count"] >= 5], g.loc[g["count"] >= 5, "velo"])
        add("whiff", g[g["swing"] >= 5], g.loc[g["swing"] >= 5, "whiff"] / g.loc[g["swing"] >= 5, "swing"])
        add("chase", g[g["out_zone"] >= 5], g.loc[g["out_zone"] >= 5, "chase"] / g.loc[g["out_zone"] >= 5, "out_zone"])
        add("fast_swing", g[g["tracked_swing"] >= 5], g.loc[g["tracked_swing"] >= 5, "fast_ge_75"] / g.loc[g["tracked_swing"] >= 5, "tracked_swing"])
        add("strike", g[g["count"] >= 8], g.loc[g["count"] >= 8, "strike"] / g.loc[g["count"] >= 8, "count"])
        add("zone", g[g["count"] >= 8], g.loc[g["count"] >= 8, "in_zone"] / g.loc[g["count"] >= 8, "count"])
        add("xwoba", g[g["xwoba_n"] >= 3], g.loc[g["xwoba_n"] >= 3, "xwoba"])

        if i % 50 == 0 or i == len(files):
            print(f"  Processed {i}/{len(files)} files")

    if not global_values["velo"]:
        raise SystemExit("No velocity data collected; aborting.")
    return {
        "global": {key: np.array(values, dtype=float) for key, values in global_values.items()},
        "by_pitch_type": {
            pitch_type: {key: np.array(values, dtype=float) for key, values in metrics.items()}
            for pitch_type, metrics in pitch_values.items()
        },
    }


def percentiles(arr: np.ndarray, qs: List[float]) -> Dict[str, float]:
    if arr.size == 0:
        return {}
    pct_vals = np.percentile(arr, qs).tolist()
    return {f"p{int(q)}": float(v) for q, v in zip(qs, pct_vals)}


def summarize(arr: np.ndarray, qs: List[float]) -> Dict[str, float | int]:
    if arr.size == 0:
        return {}
    return {
        **percentiles(arr, qs),
        "mean": float(np.mean(arr)),
        "n": int(arr.size),
    }


def main() -> None:
    args = parse_args()
    root = Path(args.root).expanduser().resolve()
    print(f"Scanning warehouse at {root} for season={args.season}, game_type={args.game_type} ...")
    files = find_parquets(root, args.season, args.game_type, max_files=args.max_files)
    print(f"Found {len(files)} parquet files to process.")

    collected = collect_metrics(files)
    metrics = collected["global"]

    qs = [5, 20, 40, 60, 80, 95]
    out: Dict[str, object] = {
        "meta": {
            "season": args.season,
            "game_type": args.game_type,
            "files_used": len(files),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "percentiles": qs,
        },
        # Percentiles store raw magnitude. Renderer applies metric direction.
        "velocity_mph": summarize(metrics["velo"], qs),
        "whiff_per_swing": summarize(metrics["whiff"], qs),
        "chase_per_out_zone": summarize(metrics["chase"], qs),
        "fast_swing_per_tracked_swing": summarize(metrics["fast_swing"], qs),
        "strike_per_pitch": summarize(metrics["strike"], qs),
        "zone_per_pitch": summarize(metrics["zone"], qs),
        # For xwOBA allowed, lower is better; percentiles still reported high-to-low
        "xwoba_allowed": summarize(metrics["xwoba"], qs),
    }
    metric_key_map = {
        "velo": "velocity_mph",
        "whiff": "whiff_per_swing",
        "chase": "chase_per_out_zone",
        "fast_swing": "fast_swing_per_tracked_swing",
        "strike": "strike_per_pitch",
        "zone": "zone_per_pitch",
        "xwoba": "xwoba_allowed",
    }
    out["by_pitch_type"] = {
        pitch_type: {
            metric_key_map[key]: summarize(values, qs)
            for key, values in pitch_metrics.items()
            if values.size >= 30
        }
        for pitch_type, pitch_metrics in collected["by_pitch_type"].items()
    }
    out["by_pitch_type"] = {
        pitch_type: metrics_for_pitch
        for pitch_type, metrics_for_pitch in out["by_pitch_type"].items()
        if metrics_for_pitch
    }

    cfg_dir = Path("config")
    cfg_dir.mkdir(parents=True, exist_ok=True)
    out_path = cfg_dir / f"pitch_metric_benchmarks_{args.season}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"\nSaved benchmarks to: {out_path}\n")


if __name__ == "__main__":
    main()
