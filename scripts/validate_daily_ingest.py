#!/usr/bin/env python3
"""Validate that final games for ingest dates have raw and pitches_enriched files.

This is intentionally date-scoped for CI. The broader season validator can report
old historical gaps; this one answers the daily operational question:
"Can cards/intel safely use yesterday's warehouse?"
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ingestion.mlb_warehouse_schema import BASE_URL, SPORT_ID_MLB, GAME_TYPE_TO_STAGE

_NON_PLAYED_FINAL_STATES = {
    "postponed",
    "suspended",
    "cancelled",
    "canceled",
}
_PLAYED_FINAL_STATES = {
    "final",
    "game over",
    "completed early",
    "final: tied",
}


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


def _is_played_final_game(game: dict) -> bool:
    status = game.get("status") or {}
    abstract = str(status.get("abstractGameState") or "").strip().lower()
    detailed = str(status.get("detailedState") or "").strip().lower()
    status_code = str(status.get("statusCode") or "").strip().upper()
    coded = str(status.get("codedGameState") or "").strip().upper()

    if detailed in _NON_PLAYED_FINAL_STATES:
        return False
    if status_code in {"DR", "DI", "S", "C"} or coded in {"D", "S", "C"}:
        return False
    if detailed in _PLAYED_FINAL_STATES:
        return True
    return abstract == "final"


def _fetch_games(season: int, game_type: str, dates: list[str]) -> tuple[list[dict], list[dict]]:
    games_by_pk: dict[int, dict] = {}
    skipped_by_pk: dict[int, dict] = {}
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
                if _is_played_final_game(game):
                    games_by_pk[int(game["gamePk"])] = game
                    continue
                status = game.get("status") or {}
                if str(status.get("abstractGameState") or "").strip().lower() == "final":
                    skipped_by_pk[int(game["gamePk"])] = game
                    continue
    return list(games_by_pk.values()), list(skipped_by_pk.values())


def _exists_raw(raw_dir: Path, game_pk: int, ymd: str) -> bool:
    stem = f"game_{game_pk}_{ymd}_feed_live"
    return (raw_dir / f"{stem}.json.gz").is_file() or (raw_dir / f"{stem}.json").is_file()


def _raw_path(raw_dir: Path, game_pk: int, ymd: str) -> Path | None:
    stem = f"game_{game_pk}_{ymd}_feed_live"
    gz = raw_dir / f"{stem}.json.gz"
    js = raw_dir / f"{stem}.json"
    if gz.is_file():
        return gz
    if js.is_file():
        return js
    return None


def _open_raw(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _raw_feed_ready(path: Path) -> tuple[bool, str]:
    """Validate that a raw feed is not a pregame/stale shell."""
    try:
        with _open_raw(path) as f:
            feed = json.load(f)
    except Exception as exc:
        return False, f"could not read raw feed: {exc}"

    status = ((feed.get("gameData") or {}).get("status") or {})
    if not _is_played_final_game({"status": status}):
        state = status.get("detailedState") or status.get("abstractGameState") or "unknown"
        return False, f"raw feed is not final ({state})"

    plays = (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
    if not plays:
        return False, "raw feed has no plays"

    teams = (((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {})
    team_ab = 0
    player_stat_rows = 0
    for side in ("away", "home"):
        side_box = teams.get(side) or {}
        batting = ((side_box.get("teamStats") or {}).get("batting") or {})
        try:
            team_ab += int(batting.get("atBats") or 0)
        except (TypeError, ValueError):
            pass
        for player in (side_box.get("players") or {}).values():
            stats = player.get("stats") or {}
            if (stats.get("batting") or {}) or (stats.get("pitching") or {}):
                player_stat_rows += 1

    if team_ab <= 0:
        return False, "raw feed has zero team at-bats"
    if player_stat_rows <= 0:
        return False, "raw feed has no populated player stat rows"
    return True, ""


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
    parser.add_argument(
        "--allow-latest-statcast-lag-hours",
        type=int,
        default=0,
        help=(
            "Do not fail for missing pitches_enriched on the newest requested date. "
            "Use this for scheduled runs where Baseball Savant can lag after final games."
        ),
    )
    args = parser.parse_args()

    dates = _dates_from_args(args)
    latest_date = max(dates) if dates else ""
    allow_latest_statcast_lag = args.allow_latest_statcast_lag_hours > 0
    stage = _stage(args.game_type)
    games, skipped_games = _fetch_games(args.season, args.game_type, dates)
    raw_dir = args.warehouse / str(args.season) / stage / "raw"
    pq_dir = args.warehouse / str(args.season) / stage / "pitches_enriched"

    missing_raw: list[dict] = []
    stale_raw: list[dict] = []
    missing_parquet: list[dict] = []
    pending_parquet: list[dict] = []
    ok = 0
    for game in sorted(games, key=lambda g: (g.get("officialDate", ""), int(g.get("gamePk") or 0))):
        game_pk = int(game["gamePk"])
        ymd = str(game.get("officialDate") or "").replace("-", "")
        path = _raw_path(raw_dir, game_pk, ymd)
        raw_ok = path is not None
        raw_reason = ""
        if path is not None:
            raw_ok, raw_reason = _raw_feed_ready(path)
        pq_path = pq_dir / f"game_{game_pk}_{ymd}_pitches_enriched.parquet"
        pq_ok = pq_path.is_file()
        row = {
            "game_pk": game_pk,
            "date": game.get("officialDate"),
            "matchup": _team_names(game),
        }
        if path is None:
            missing_raw.append(row)
        elif not raw_ok:
            stale_raw.append(row | {"reason": raw_reason, "file": path.name})
        if not pq_ok and allow_latest_statcast_lag and row["date"] == latest_date:
            pending_parquet.append(row)
        elif not pq_ok:
            missing_parquet.append(row)
        if raw_ok and pq_ok:
            ok += 1

    failed = bool(missing_raw or stale_raw or missing_parquet)
    lines = [
        "# Daily Ingest Validation",
        "",
        f"- Season: `{args.season}`",
        f"- Stage: `{stage}`",
        f"- Dates: `{', '.join(dates)}`",
        f"- Final games expected: `{len(games)}`",
        f"- Skipped postponed/suspended/cancelled finals: `{len(skipped_games)}`",
        f"- Complete raw + parquet: `{ok}`",
        f"- Missing raw: `{len(missing_raw)}`",
        f"- Stale/incomplete raw: `{len(stale_raw)}`",
        f"- Missing pitches_enriched: `{len(missing_parquet)}`",
        f"- Statcast pending for newest date: `{len(pending_parquet)}`",
        "",
    ]
    if skipped_games:
        lines.append("## Skipped Non-Played Final States")
        for game in sorted(skipped_games, key=lambda g: (g.get("officialDate", ""), int(g.get("gamePk") or 0))):
            status = game.get("status") or {}
            reason = status.get("reason")
            suffix = f" ({reason})" if reason else ""
            lines.append(
                f"- `{game.get('officialDate')}` game `{game.get('gamePk')}`: "
                f"{_team_names(game)} — {status.get('detailedState') or 'non-played final'}{suffix}"
            )
        lines.append("")
    if missing_raw:
        lines.append("## Missing Raw Feeds")
        for row in missing_raw:
            lines.append(f"- `{row['date']}` game `{row['game_pk']}`: {row['matchup']}")
        lines.append("")
    if stale_raw:
        lines.append("## Stale/Incomplete Raw Feeds")
        for row in stale_raw:
            lines.append(
                f"- `{row['date']}` game `{row['game_pk']}`: {row['matchup']} "
                f"({row['file']}: {row['reason']})"
            )
        lines.append("")
    if missing_parquet:
        lines.append("## Missing pitches_enriched Parquets")
        for row in missing_parquet:
            lines.append(f"- `{row['date']}` game `{row['game_pk']}`: {row['matchup']}")
        lines.append("")
    if pending_parquet:
        lines.append("## Statcast Pending for Newest Date")
        lines.append(
            "These games have raw final feeds, but Baseball Savant has not produced "
            "pitches_enriched yet. This is allowed for the newest date in scheduled runs."
        )
        for row in pending_parquet:
            lines.append(f"- `{row['date']}` game `{row['game_pk']}`: {row['matchup']}")
        lines.append("")
    if failed:
        lines.extend([
            "## Operational Impact",
            "Pitcher cards and Statcast-dependent intel may fail or silently omit games until this is rerun after Savant has data.",
            "",
        ])
    elif pending_parquet:
        lines.extend([
            "## Operational Impact",
            "The workflow can continue, but Statcast-dependent intel should use the latest complete enriched date until Savant catches up.",
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
        for row in stale_raw:
            print(
                f"::error::Stale/incomplete raw feed for {row['date']} game {row['game_pk']} "
                f"({row['matchup']}): {row['reason']}",
                file=sys.stderr,
            )
        return 1
    for row in pending_parquet:
        print(
            f"::warning::Statcast pending for newest date {row['date']} game {row['game_pk']} ({row['matchup']})",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
