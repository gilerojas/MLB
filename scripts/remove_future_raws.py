"""
Remove raw feed_live files that have no real play data (Scheduled / Pre-Game with 0–1 plays),
then run the load script to re-fetch those games and populate with final data.

Run from repo root:
  python scripts/remove_future_raws.py [--warehouse data/warehouse/mlb] [--dry-run]
  python scripts/remove_future_raws.py --no-reload   # only remove, do not run loader
"""
import argparse
import gzip
import json
import re
import subprocess
import sys
from pathlib import Path

# stage folder name -> gameType for schedule API (inverse of GAME_TYPE_TO_STAGE)
STAGE_TO_GAME_TYPE = {
    "spring_training": "S",
    "regular_season": "R",
    "all_star": "A",
    "playoffs": "P",
    "playoffs/wild_card": "F",
    "playoffs/division": "D",
    "playoffs/championship": "L",
    "playoffs/world_series": "W",
}

# Statuses that mean the game was not played (or feed is pre-game)
NON_FINAL_STATES = {"Scheduled", "Pre-Game", "Preview", "Warmup"}

# Minimum plays to consider "has real data" (otherwise we treat as empty even if status is Final)
MIN_PLAYS = 2


def _raw_stem(path: Path) -> str:
    name = path.name
    if name.endswith(".json.gz"):
        return name[:-7]
    if name.endswith(".json"):
        return name[:-5]
    return path.stem


def _open_raw(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def inspect_raw(path: Path) -> tuple[int, str]:
    """Return (play_count, detailedState) for a raw feed file. On error, return (-1, '')."""
    try:
        with _open_raw(path) as f:
            feed = json.load(f)
    except Exception:
        return -1, ""
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    status = (feed.get("gameData", {}).get("status") or {}).get("detailedState", "")
    return len(plays), status


def find_rawname_game_pk_date(path: Path) -> tuple[str, str] | None:
    """From path like .../raw/game_831483_20260223_feed_live.json return (game_pk, date_ymd)."""
    name = _raw_stem(path)
    m = re.match(r"game_(\d+)_(\d{8})_feed_live", name)
    if not m:
        return None
    return m.group(1), m.group(2)


def main():
    p = argparse.ArgumentParser(
        description="Remove raw files with no real play data, then re-fetch via load script"
    )
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse/mlb"))
    p.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        help="Limit to these years (default: all years under warehouse)",
    )
    p.add_argument("--dry-run", action="store_true", help="Only print, do not delete or reload")
    p.add_argument(
        "--no-reload",
        action="store_true",
        help="Only remove bad raws; do not run load_mlb_warehouse",
    )
    args = p.parse_args()

    warehouse = args.warehouse.resolve()
    if not warehouse.is_dir():
        p.error(f"Warehouse not found: {warehouse}")

    # Find all raw files (optionally filtered by year)
    raw_glob = list(warehouse.rglob("raw/*_feed_live.json*"))
    if args.years is not None:
        years_set = set(args.years)
        filtered = []
        for r in raw_glob:
            try:
                top = r.relative_to(warehouse).parts[0]
                if top.isdigit() and int(top) in years_set:
                    filtered.append(r)
            except (ValueError, IndexError):
                pass
        raw_glob = filtered

    pattern = re.compile(r"game_(\d+)_(\d{8})_feed_live\.(?:json\.gz|json)$")
    to_remove: list[Path] = []

    for raw_path in raw_glob:
        if not raw_path.is_file() or not pattern.search(raw_path.name):
            continue
        n_plays, status = inspect_raw(raw_path)
        if n_plays < 0:
            to_remove.append(raw_path)
            continue
        if status in NON_FINAL_STATES and n_plays <= 1:
            to_remove.append(raw_path)
            continue
        if n_plays < MIN_PLAYS:
            to_remove.append(raw_path)

    if not to_remove:
        print("No raw files with missing/insufficient play data found.")
        return

    print(f"Found {len(to_remove)} raw file(s) with no or insufficient play data.")
    for path in to_remove[:20]:
        rel = path.relative_to(warehouse)
        n_plays, status = inspect_raw(path)
        print(f"  {rel}  plays={n_plays}  status={status or '?'}")
    if len(to_remove) > 20:
        print(f"  ... and {len(to_remove) - 20} more")

    if args.dry_run:
        print("Run without --dry-run to delete (and optionally reload).")
        return

    # Remove raw files and collect (year, stage, date_ymd) for reload
    removed_dirs: set[tuple[int, str]] = set()
    removed_dates_by_stage: dict[tuple[int, str], set[str]] = {}

    for path in to_remove:
        # Remove corresponding pitches_enriched so it gets regenerated with new raw
        try:
            rel = path.relative_to(warehouse)
            parts = rel.parts
            if len(parts) >= 4:
                year, stage = parts[0], parts[1]
                key = (int(year), stage)
                removed_dirs.add(key)
                info = find_rawname_game_pk_date(path)
                if info:
                    game_pk, date_ymd = info
                    date_str = f"{date_ymd[:4]}-{date_ymd[4:6]}-{date_ymd[6:8]}"
                    removed_dates_by_stage.setdefault(key, set()).add(date_str)
                    enriched_path = warehouse / year / stage / "pitches_enriched" / f"game_{game_pk}_{date_ymd}_pitches_enriched.parquet"
                    if enriched_path.exists():
                        enriched_path.unlink()
                        print(f"  Removed enriched: {enriched_path.relative_to(warehouse)}")
        except (ValueError, IndexError, OSError):
            pass
        try:
            path.unlink()
        except Exception as e:
            print(f"  Error removing {path}: {e}")
            continue

    print(f"Removed {len(to_remove)} file(s).")

    if args.no_reload or not removed_dates_by_stage:
        return

    # Run load script per (year, stage) with --dates so we re-fetch only those dates
    repo_root = Path(__file__).resolve().parent.parent
    game_type_map = STAGE_TO_GAME_TYPE

    for (year, stage) in sorted(removed_dirs):
        game_type = game_type_map.get(stage, "R")
        dates = sorted(removed_dates_by_stage.get((year, stage), []))
        if not dates:
            continue
        cmd = [
            sys.executable,
            "-m",
            "src.ingestion.load_mlb_warehouse",
            "--warehouse",
            str(warehouse),
            "--season",
            str(year),
            "--game-type",
            game_type,
            "--dates",
            *dates,
        ]
        print(f"\nRunning: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=repo_root)
        if result.returncode != 0:
            print(f"  Load script exited with {result.returncode}")


if __name__ == "__main__":
    main()
