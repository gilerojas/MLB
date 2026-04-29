#!/usr/bin/env python3
"""
Export season-level CSVs next to the warehouse year folder (for Drive sync).

Writes under {warehouse}/{season}/:
  - team_standings_regular_season.csv  (Stats API)
  - team_season_stats.csv              (per-team hitting + pitching season stats)
  - player_season_batting_by_team.csv  (boxscore sums, one row per player_id+team_id)
  - player_season_pitching_by_team.csv

Optional:
  - players_registry.csv from existing players_registry.json

Usage (2025, progress bars, optional registry CSV):

  cd MLB && python scripts/export_season_drive_artifacts.py --season 2025 --players-registry-csv

Interrupt (Ctrl+C) during team API calls saves progress to
``team_season_stats.partial.csv``; run the same command again to resume.
Use ``--fresh-team-stats`` to ignore/delete partial and refetch all team stats.

Partial rerun:

  python scripts/export_season_drive_artifacts.py --season 2025 --only team-stats
  python scripts/export_season_drive_artifacts.py --season 2025 --only player-box

Other flags: ``--no-progress``, ``--warehouse``, ``--no-team-stats``, etc.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingestion.season_exports import (
    fetch_standings_regular_season,
    fetch_team_season_stats_rows,
    player_team_boxscore_rows,
    write_csv,
)


def _flatten_registry_csv(registry_path: Path, out_csv: Path) -> None:
    if not registry_path.is_file():
        print(f"  Skip players_registry.csv (missing {registry_path})", file=sys.stderr)
        return
    with open(registry_path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        print("  Skip players_registry.csv (JSON root is not an object)", file=sys.stderr)
        return
    rows: list[dict] = []
    for key, pdata in data.items():
        if not isinstance(pdata, dict):
            continue
        row = {
            "player_id": pdata.get("id", key),
            "full_name": pdata.get("fullName"),
            "first_name": pdata.get("firstName"),
            "last_name": pdata.get("lastName"),
            "primary_number": pdata.get("primaryNumber"),
            "birth_date": pdata.get("birthDate"),
            "current_age": pdata.get("currentAge"),
            "height": pdata.get("height"),
            "weight": pdata.get("weight"),
            "active": pdata.get("active"),
            "primary_position": (pdata.get("primaryPosition") or {}).get("abbreviation"),
            "bat_side": (pdata.get("batSide") or {}).get("code"),
            "pitch_hand": (pdata.get("pitchHand") or {}).get("code"),
        }
        rows.append(row)
    rows.sort(
        key=lambda r: int(r["player_id"])
        if str(r.get("player_id") or "").isdigit()
        else 0
    )
    write_csv(out_csv, rows)
    print(f"  Wrote {out_csv} ({len(rows)} rows)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export season CSVs for Drive / analytics.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=_REPO_ROOT / "data" / "warehouse" / "mlb",
        help="Warehouse root (contains <season>/...)",
    )
    parser.add_argument(
        "--stage",
        default="regular_season",
        help="Warehouse stage for player boxscore aggregates (default regular_season)",
    )
    parser.add_argument("--no-standings", action="store_true")
    parser.add_argument("--no-team-stats", action="store_true")
    parser.add_argument("--no-player-box", action="store_true")
    parser.add_argument(
        "--players-registry-csv",
        action="store_true",
        help="Write players_registry.csv from players_registry.json if present",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm bars (API + raw feed scans)",
    )
    parser.add_argument(
        "--only",
        choices=("all", "standings", "team-stats", "player-box", "registry"),
        default="all",
        help="Run only this step (team-stats needs team_standings_regular_season.csv on disk)",
    )
    parser.add_argument(
        "--fresh-team-stats",
        action="store_true",
        help="Delete team_season_stats.partial.csv and do not resume; refetch all team stats",
    )
    args = parser.parse_args()
    season = args.season
    year_dir = args.warehouse / str(season)
    year_dir.mkdir(parents=True, exist_ok=True)
    disable_p = args.no_progress

    only = args.only
    run_standings = only in ("all", "standings") and not args.no_standings
    run_team = only in ("all", "team-stats") and not args.no_team_stats
    run_player = only in ("all", "player-box") and not args.no_player_box
    run_registry = only in ("all", "registry")

    print(f"Season {season} -> {year_dir}")

    try:
        if run_standings:
            rows = fetch_standings_regular_season(season)
            path = year_dir / "team_standings_regular_season.csv"
            write_csv(path, rows)
            print(f"  Wrote {path} ({len(rows)} rows)")

        team_ids: list[int] = []
        if run_team:
            standings_path = year_dir / "team_standings_regular_season.csv"
            if standings_path.is_file():
                with open(standings_path, newline="", encoding="utf-8") as f:
                    team_ids = sorted(
                        {int(r["team_id"]) for r in csv.DictReader(f) if r.get("team_id")}
                    )
            else:
                rows_st = fetch_standings_regular_season(season)
                team_ids = sorted(
                    {int(r["team_id"]) for r in rows_st if r.get("team_id")}
                )

        partial_path = year_dir / "team_season_stats.partial.csv"
        if run_team and team_ids:
            if args.fresh_team_stats:
                partial_path.unlink(missing_ok=True)
            existing: list[dict] = []
            if partial_path.is_file() and not args.fresh_team_stats:
                with open(partial_path, newline="", encoding="utf-8") as f:
                    existing = list(csv.DictReader(f))
                if existing:
                    print(f"  Resuming team stats from {partial_path} ({len(existing)} rows)")

            stats_rows, completed = fetch_team_season_stats_rows(
                season,
                team_ids,
                disable_progress=disable_p,
                existing_rows=existing,
            )
            path = year_dir / "team_season_stats.csv"
            if not completed:
                write_csv(partial_path, stats_rows)
                print(
                    f"\n  Interrupted: saved {len(stats_rows)} rows to {partial_path}",
                    file=sys.stderr,
                )
                print(
                    "  Re-run the same command to resume, or use --fresh-team-stats to start over.",
                    file=sys.stderr,
                )
                return 130
            write_csv(path, stats_rows)
            partial_path.unlink(missing_ok=True)
            print(f"  Wrote {path} ({len(stats_rows)} rows)")

        if run_player:
            bat, pit = player_team_boxscore_rows(
                args.warehouse, season, args.stage, disable_progress=disable_p
            )
            p1 = year_dir / "player_season_batting_by_team.csv"
            p2 = year_dir / "player_season_pitching_by_team.csv"
            write_csv(p1, bat)
            write_csv(p2, pit)
            print(f"  Wrote {p1} ({len(bat)} rows)")
            print(f"  Wrote {p2} ({len(pit)} rows)")

        if run_registry and args.players_registry_csv:
            reg = year_dir / "players_registry.json"
            _flatten_registry_csv(reg, year_dir / "players_registry.csv")
        elif run_registry and not args.players_registry_csv:
            print("  --only registry requires --players-registry-csv", file=sys.stderr)
            return 2

    except KeyboardInterrupt:
        print(
            "\n  Interrupted. Any steps that already finished left CSVs on disk.",
            file=sys.stderr,
        )
        print(
            "  Re-run with e.g. --only team-stats or --only player-box to redo a phase.",
            file=sys.stderr,
        )
        return 130

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
