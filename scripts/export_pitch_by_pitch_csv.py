#!/usr/bin/env python3
"""
Export pitch-by-pitch data for a pitcher from a pitches_enriched parquet to CSV.
Uses the same normalization and quality-of-contact logic as the pitching card so you can
verify strikes (type, is_strike) and hard contact (launch_speed, hard_hit, is_damage) vs Savant.

Usage:
  cd MLB && python scripts/export_pitch_by_pitch_csv.py --parquet /path/to/game_788106_20260308_pitches_enriched.parquet --pitcher 622663 --output severino_788106.csv
  cd MLB && python scripts/export_pitch_by_pitch_csv.py --parquet ../WBC/data/warehouse/2026/pitches_enriched/game_788106_20260308_pitches_enriched.parquet --pitcher 622663
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import numpy as np

_SCRIPT_DIR = Path(__file__).resolve().parent
_MLB_ROOT = _SCRIPT_DIR.parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

# Reuse exact same normalization and process_pitches from the card
from mallitalytics_daily_card import load_game, process_pitches

# Columns to export (order matters for readability)
# exit_velocity_mph = hitter exit velo off the pitch (from launch_speed); only set for balls in play
EXPORT_COLUMNS = [
    "inning",
    "at_bat_number",
    "pitch_number",
    "pitch_type",
    "description",
    "type",
    "is_strike",
    "balls",
    "strikes",
    "release_speed",
    "plate_x",
    "plate_z",
    "zone",
    "in_zone",
    "exit_velocity_mph",
    "launch_angle",
    "estimated_woba_using_speedangle",
    "is_bip",
    "hard_hit",
    "is_damage",
    "bb_type",
    "events",
    "swing",
    "whiff",
    "chase",
]

OPTIONAL_COLUMNS = [
    "game_pk",
    "game_date",
    "inning_topbot",
    "release_spin_rate",
    "pfx_x",
    "pfx_z",
    "hc_x",
    "hc_y",
    "hit_distance_sc",
]


def main():
    ap = argparse.ArgumentParser(description="Export pitch-by-pitch CSV (strikes + quality of contact)")
    ap.add_argument("--parquet", required=True, help="Path to game_*_pitches_enriched.parquet")
    ap.add_argument("--pitcher", type=int, required=True, help="Pitcher MLB person ID (e.g. 622663)")
    ap.add_argument("--output", "-o", default=None, help="Output CSV path (default: pitch_by_pitch_<game>_<pitcher>.csv)")
    args = ap.parse_args()

    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        print(f"ERROR: Parquet not found: {parquet_path}", file=sys.stderr)
        sys.exit(1)

    df = load_game(str(parquet_path), args.pitcher)
    df = process_pitches(df)

    # Hitter exit velocity (mph) off the pitch — Statcast launch_speed; only on balls in play
    if "launch_speed" in df.columns:
        df["exit_velocity_mph"] = pd.to_numeric(df["launch_speed"], errors="coerce")
    else:
        df["exit_velocity_mph"] = np.nan

    # Build export column list: only include columns that exist
    out_cols = [c for c in EXPORT_COLUMNS if c in df.columns]
    for c in OPTIONAL_COLUMNS:
        if c in df.columns and c not in out_cols:
            out_cols.append(c)

    out = df[out_cols].copy()
    # Make booleans readable in CSV
    for col in ("is_strike", "is_bip", "hard_hit", "is_damage", "swing", "whiff", "chase", "in_zone"):
        if col in out.columns:
            out[col] = out[col].astype(bool).map({True: "Y", False: "N"})

    if args.output:
        out_path = Path(args.output)
    else:
        name = parquet_path.stem.replace("_pitches_enriched", "").replace("game_", "")
        out_path = _MLB_ROOT / "outputs" / "pitching_cards" / f"pitch_by_pitch_{name}_{args.pitcher}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Exported {len(out)} pitches to {out_path}")
    print(f"  is_strike=Y count: {(out['is_strike'] == 'Y').sum()}")
    if "hard_hit" in out.columns:
        print(f"  hard_hit=Y count: {(out['hard_hit'] == 'Y').sum()}")
    if "is_damage" in out.columns:
        print(f"  is_damage=Y count: {(out['is_damage'] == 'Y').sum()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
