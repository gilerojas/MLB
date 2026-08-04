#!/usr/bin/env python3
"""Render the Mallitalytics daily pitching performances table."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pitching_performances import build_pitching_performance_rows, render_pitching_performance_table


def _warehouse_root() -> Path:
    raw = os.environ.get("MLB_WAREHOUSE_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return ROOT / "data" / "warehouse" / "mlb"


def _write_csv(rows: list[dict], path: Path) -> Path:
    fields = [
        "rank",
        "pitcher",
        "team",
        "opponent",
        "pitches",
        "whiffs",
        "swstr_pct",
        "csw",
        "csw_pct",
        "kbb_pct",
        "hit_by_pitch",
        "batters_faced",
        "reach_rate_allowed",
        "xwoba_allowed",
        "xwoba_pa",
        "chases",
        "out_zone",
        "chase_pct",
        "damage_pa",
        "damage_pct",
        "malli_score",
        "malli_score_version",
        "malli_score_v3",
        "summary",
        "player_id",
        "game_pk",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fields})
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Mallitalytics pitching performances table")
    parser.add_argument("--date", default=None, help="Game date YYYY-MM-DD. Defaults to latest local raw+enriched date.")
    parser.add_argument("--season", type=int, default=None, help="Season year. Defaults to current year.")
    parser.add_argument("--top-n", type=int, default=30, help="Rows to render.")
    parser.add_argument("--min-pitches", type=int, default=30, help="Minimum pitches for inclusion.")
    parser.add_argument("--warehouse", type=Path, default=None, help="Warehouse root. Defaults to MLB_WAREHOUSE_DIR or data/warehouse/mlb.")
    parser.add_argument("--out", type=Path, default=None, help="Output PNG path.")
    parser.add_argument("--csv", type=Path, default=None, help="Optional CSV output path.")
    args = parser.parse_args()

    warehouse = (args.warehouse or _warehouse_root()).expanduser().resolve()
    season = args.season or (int(args.date[:4]) if args.date else datetime.now().year)
    date_str, rows = build_pitching_performance_rows(
        warehouse,
        date_str=args.date,
        season=season,
        min_pitches=args.min_pitches,
        top_n=args.top_n,
    )
    ymd = date_str.replace("-", "")
    out_path = args.out or (ROOT / "outputs" / "pitching_performances" / f"pitching_performances_{ymd}.png")
    csv_path = args.csv or out_path.with_suffix(".csv")

    render_pitching_performance_table(rows, date_str, out_path)
    _write_csv(rows, csv_path)
    print(f"Date: {date_str}")
    print(f"Rows: {len(rows)}")
    print(f"Saved PNG: {out_path}")
    print(f"Saved CSV: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
