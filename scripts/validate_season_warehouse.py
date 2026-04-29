#!/usr/bin/env python3
"""
Light validation for season folder before Drive sync.

Checks:
  - team_standings_regular_season.csv: 30 unique team_id (MLB), required columns
  - players_registry.json: keys match nested id when present
  - raw vs pitches_enriched pairing (optional stage)

Usage:
  python scripts/validate_season_warehouse.py --season 2025
  python scripts/validate_season_warehouse.py --season 2025 --strict
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingestion.boxscore_aggregate import find_stage_raw_paths


def validate_standings_csv(path: Path, strict: bool) -> bool:
    if not path.is_file():
        print(f"ERROR: missing {path}", file=sys.stderr)
        return False
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    ids = {r.get("team_id") for r in rows if r.get("team_id")}
    if len(ids) != 30:
        msg = f"expected 30 team_id in standings, got {len(ids)}"
        print(f"{'ERROR' if strict else 'WARN'}: {msg}", file=sys.stderr)
        if strict:
            return False
    for col in ("wins", "losses", "team_abbrev"):
        if rows and col not in rows[0]:
            print(f"ERROR: standings missing column {col}", file=sys.stderr)
            return False
    print(f"OK standings: {len(rows)} rows, {len(ids)} teams")
    return True


def validate_registry(path: Path, strict: bool) -> bool:
    if not path.is_file():
        print(f"SKIP registry (no file): {path}")
        return True
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        print("ERROR: players_registry.json root must be object", file=sys.stderr)
        return False
    bad = 0
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        pid = v.get("id")
        if pid is not None and str(pid) != str(k):
            bad += 1
    if bad:
        print(f"ERROR: registry has {bad} entries where key != id", file=sys.stderr)
        return False
    print(f"OK registry: {len(data)} players, key/id consistent")
    return True


def validate_raw_parquet_pairing(
    warehouse: Path, season: int, stage: str, strict: bool
) -> bool:
    raw_dir = warehouse / str(season) / stage / "raw"
    pe_dir = warehouse / str(season) / stage / "pitches_enriched"
    if not raw_dir.is_dir():
        print(f"SKIP raw/parquet (no raw dir): {raw_dir}")
        return True
    raw_paths = find_stage_raw_paths(warehouse, season, stage)
    missing_parquet = 0
    for rp in raw_paths:
        m = re.search(r"game_(\d+)_(\d+)_feed_live", rp.name)
        if not m:
            continue
        stem = f"game_{m.group(1)}_{m.group(2)}_pitches_enriched.parquet"
        if not (pe_dir / stem).is_file():
            missing_parquet += 1
    if missing_parquet:
        print(
            f"{'ERROR' if strict else 'WARN'}: {missing_parquet} raw feed(s) "
            f"without matching pitches_enriched parquet",
            file=sys.stderr,
        )
        if strict:
            return False
    else:
        print(f"OK raw/pitches_enriched: {len(raw_paths)} raw files checked")
    return True


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    p.add_argument(
        "--warehouse",
        type=Path,
        default=_REPO_ROOT / "data" / "warehouse" / "mlb",
    )
    p.add_argument("--stage", default="regular_season")
    p.add_argument("--strict", action="store_true")
    args = p.parse_args()
    y = args.warehouse / str(args.season)
    failed = False
    st = y / "team_standings_regular_season.csv"
    if st.is_file():
        if not validate_standings_csv(st, args.strict):
            failed = True
    else:
        print(f"SKIP standings CSV (not found): {st}")

    if not validate_registry(y / "players_registry.json", args.strict):
        failed = True

    if not validate_raw_parquet_pairing(
        args.warehouse, args.season, args.stage, args.strict
    ):
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
