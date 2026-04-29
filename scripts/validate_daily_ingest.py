#!/usr/bin/env python3
"""Validate that final games for ingest dates have raw and pitches_enriched files.

This is intentionally date-scoped for CI. The broader season validator can report
old historical gaps; this one answers the daily operational question:
"Can cards/intel safely use yesterday's warehouse?"
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingestion.mlb_warehouse_schema import BASE_URL, SPORT_ID_MLB, GAME_TYPE_TO_STAGE


def _stage(game_type: str) -> str:
    return GAME_TYPE_TO_STAGE.get((game_type or "R").strip().upper(), "regular_season")


def _dates_from_args(args: argparse.Namespace) -> list[str]:
    if args.dates:
        return args.dates
    today = date.today()
    return [
        (today - timedelta(days=k)).strftime("%Y-%m-%d")
        for k in range(1, int(args.last_days) + 1)
    ]


def _fetch_final_games(season: int, game_type: str, dates: list[str]) -> list[dict]:
    games_by_pk: dict[int, dict] = {}
    gt = (game_type or "R").strip().upper()
    for d in dates:
        resp = requests.get(
            f"{BASE_URL}/schedule",
            params={"sportId": SPORT_ID_MLB, "date": d},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                if (game.get("gameType") or "").strip().upper() != gt:
                    continue
                try:
                    if int(game.get("season")) != int(season):
                        continue
                except (TypeError, ValueError):
                    continue
                if (game.get("status") or {}).get("abstractGameState") != "Final":
                    continue
                games_by_pk[int(game["gamePk"])] = game
    return list(games_by_pk.values())


def _exists_raw(raw_dir: Path, game_pk: int, ymd: str) -> bool:
    stem = f"game_{game_pk}_{ymd}_feed_live"
    return (raw_dir / f"{stem}.json.gz").is_file() or (raw_dir / f"{stem}.json").is_file()


def _team_names(game: dict) -> str:
    teams = game.get("teams") or {}
    away = ((teams.get("away") or {}).get("team") or {}).get("name") or "Away"
    home = ((teams.get("home") or {}).get("team") or {}).get("name") or "Home"
    return f"{away} @ {home}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--game-type", default="R")
    parser.add_argument("--dates", nargs="+", metavar="YYYY-MM-DD")
    parser.add_argument("--last-days", type=int, default=1)
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=_REPO_ROOT / "data" / "warehouse" / "mlb",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=_REPO_ROOT / "data" / "daily_ingest_validation_report.md",
    )
    args = parser.parse_args()

    dates = _dates_from_args(args)
    stage = _stage(args.game_type)
    games = _fetch_final_games(args.season, args.game_type, dates)
    raw_dir = args.warehouse / str(args.season) / stage / "raw"
    pq_dir = args.warehouse / str(args.season) / stage / "pitches_enriched"

    missing_raw: list[dict] = []
    missing_parquet: list[dict] = []
    ok = 0
    for game in sorted(games, key=lambda g: (g.get("officialDate", ""), int(g.get("gamePk") or 0))):
        game_pk = int(game["gamePk"])
        ymd = str(game.get("officialDate") or "").replace("-", "")
        raw_ok = _exists_raw(raw_dir, game_pk, ymd)
        pq_path = pq_dir / f"game_{game_pk}_{ymd}_pitches_enriched.parquet"
        pq_ok = pq_path.is_file()
        row = {
            "game_pk": game_pk,
            "date": game.get("officialDate"),
            "matchup": _team_names(game),
        }
        if not raw_ok:
            missing_raw.append(row)
        if not pq_ok:
            missing_parquet.append(row)
        if raw_ok and pq_ok:
            ok += 1

    failed = bool(missing_raw or missing_parquet)
    lines = [
        "# Daily Ingest Validation",
        "",
        f"- Season: `{args.season}`",
        f"- Stage: `{stage}`",
        f"- Dates: `{', '.join(dates)}`",
        f"- Final games expected: `{len(games)}`",
        f"- Complete raw + parquet: `{ok}`",
        f"- Missing raw: `{len(missing_raw)}`",
        f"- Missing pitches_enriched: `{len(missing_parquet)}`",
        "",
    ]
    if missing_raw:
        lines.append("## Missing Raw Feeds")
        for row in missing_raw:
            lines.append(f"- `{row['date']}` game `{row['game_pk']}`: {row['matchup']}")
        lines.append("")
    if missing_parquet:
        lines.append("## Missing pitches_enriched Parquets")
        for row in missing_parquet:
            lines.append(f"- `{row['date']}` game `{row['game_pk']}`: {row['matchup']}")
        lines.append("")
    if failed:
        lines.extend([
            "## Operational Impact",
            "Pitcher cards and Statcast-dependent intel may fail or silently omit games until this is rerun after Savant has data.",
            "",
        ])
    else:
        lines.append("All final games for the target date window have raw and pitches_enriched files.")

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))
    print(f"VALIDATION_REPORT={args.report}")
    if failed:
        for row in missing_parquet:
            print(
                f"::error::Missing pitches_enriched for {row['date']} game {row['game_pk']} ({row['matchup']})",
                file=sys.stderr,
            )
        for row in missing_raw:
            print(
                f"::error::Missing raw feed for {row['date']} game {row['game_pk']} ({row['matchup']})",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
