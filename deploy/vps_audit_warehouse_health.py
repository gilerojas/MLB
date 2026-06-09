#!/usr/bin/env python3
"""
Audit VPS MLB warehouse health against MLB Stats API.

Run inside the API container on the VPS:
  docker compose --env-file /srv/mlbops/env/mlbops.env run --rm api \
    python /app/deploy/vps_audit_warehouse_health.py \
      --warehouse /data/warehouse/mlb --season 2026 --date-to 2026-06-02
"""
from __future__ import annotations

import argparse
import gzip
import json
import random
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
SPORT_ID_MLB = 1
RAW_RE = re.compile(r"^game_(\d+)_(\d{8})_feed_live\.json(?:\.gz)?$")
PITCH_RE = re.compile(r"^game_(\d+)_(\d{8})_pitches_enriched\.parquet$")


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _today_minus_one() -> date:
    return date.today() - timedelta(days=1)


def _is_final(game: dict[str, Any]) -> bool:
    status = game.get("status") or {}
    abstract = str(status.get("abstractGameState") or "").lower()
    detailed = str(status.get("detailedState") or "").lower()
    status_code = str(status.get("statusCode") or "").upper()
    coded = str(status.get("codedGameState") or "").upper()
    if detailed in {"postponed", "suspended", "cancelled", "canceled"}:
        return False
    if status_code in {"DR", "DI", "S", "C"} or coded in {"D", "S", "C"}:
        return False
    return detailed in {"final", "game over", "completed early", "final: tied"} or abstract == "final"


def _game_preference_key(game: dict[str, Any]) -> tuple[int, str, str]:
    status = game.get("status") or {}
    detailed = str(status.get("detailedState") or "").lower()
    status_code = str(status.get("statusCode") or "").upper()
    coded = str(status.get("codedGameState") or "").upper()
    non_played = (
        detailed in {"postponed", "suspended", "cancelled", "canceled"}
        or status_code in {"DR", "DI", "S", "C"}
        or coded in {"D", "S", "C"}
    )
    state_score = 0 if non_played else 2 if _is_final(game) else 1
    return (state_score, str(game.get("officialDate") or ""), str(game.get("gameDate") or ""))


def fetch_schedule(season: int, game_type: str) -> list[dict[str, Any]]:
    res = requests.get(
        SCHEDULE_URL,
        params={"sportId": SPORT_ID_MLB, "season": season, "gameType": game_type},
        timeout=45,
    )
    res.raise_for_status()
    by_pk: dict[int, dict[str, Any]] = {}
    for day in res.json().get("dates", []):
        for game in day.get("games", []):
            try:
                pk = int(game.get("gamePk"))
            except (TypeError, ValueError):
                continue
            current = by_pk.get(pk)
            if current is None or _game_preference_key(game) > _game_preference_key(current):
                by_pk[pk] = game
    return list(by_pk.values())


def index_files(root: Path, season: int, stage: str) -> tuple[dict[int, Path], dict[int, Path], Counter[str], Counter[str]]:
    base = root / str(season) / stage
    raw_by_pk: dict[int, Path] = {}
    pitch_by_pk: dict[int, Path] = {}
    raw_by_date: Counter[str] = Counter()
    pitch_by_date: Counter[str] = Counter()

    for path in sorted((base / "raw").glob("game_*_feed_live.json*")):
        match = RAW_RE.match(path.name)
        if not match:
            continue
        pk = int(match.group(1))
        ymd = match.group(2)
        raw_by_pk[pk] = path
        raw_by_date[ymd] += 1

    for path in sorted((base / "pitches_enriched").glob("game_*_pitches_enriched.parquet")):
        match = PITCH_RE.match(path.name)
        if not match:
            continue
        pk = int(match.group(1))
        ymd = match.group(2)
        pitch_by_pk[pk] = path
        pitch_by_date[ymd] += 1

    return raw_by_pk, pitch_by_pk, raw_by_date, pitch_by_date


def _open_raw(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def sample_raw_parse(paths: list[Path], sample_size: int) -> list[str]:
    errors: list[str] = []
    if sample_size <= 0 or not paths:
        return errors
    sample = random.sample(paths, min(sample_size, len(paths)))
    for path in sample:
        try:
            with _open_raw(path) as handle:
                data = json.load(handle)
            if not data.get("gamePk") and not (data.get("gameData") or {}).get("game", {}).get("pk"):
                errors.append(f"{path.name}: missing gamePk")
        except Exception as exc:
            errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
    return errors


def sample_parquet_read(paths: list[Path], sample_size: int) -> list[str]:
    errors: list[str] = []
    if sample_size <= 0 or not paths:
        return errors
    try:
        import pandas as pd
    except Exception as exc:
        return [f"pandas import failed: {type(exc).__name__}: {exc}"]

    sample = random.sample(paths, min(sample_size, len(paths)))
    for path in sample:
        try:
            df = pd.read_parquet(path)
            if df.empty:
                errors.append(f"{path.name}: empty parquet")
            missing_cols = {"game_pk", "game_date"} - set(df.columns)
            if missing_cols:
                errors.append(f"{path.name}: missing columns {sorted(missing_cols)}")
        except Exception as exc:
            errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit MLB warehouse against MLB Stats API schedule.")
    parser.add_argument("--warehouse", type=Path, default=Path("/srv/mlbops/warehouse/mlb"))
    parser.add_argument("--season", type=int, default=date.today().year)
    parser.add_argument("--stage", default="regular_season")
    parser.add_argument("--game-type", default="R")
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--sample-raw", type=int, default=25)
    parser.add_argument("--sample-parquets", type=int, default=25)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    date_from = _parse_date(args.date_from)
    date_to = _parse_date(args.date_to) or _today_minus_one()
    warehouse = args.warehouse

    games = fetch_schedule(args.season, args.game_type)
    expected: dict[int, dict[str, Any]] = {}
    expected_by_date: Counter[str] = Counter()
    non_final_by_date: Counter[str] = Counter()
    for game in games:
        official = _parse_date(game.get("officialDate"))
        if official is None:
            continue
        if date_from and official < date_from:
            continue
        if official > date_to:
            continue
        ymd = official.strftime("%Y%m%d")
        if _is_final(game):
            pk = int(game["gamePk"])
            expected[pk] = game
            expected_by_date[ymd] += 1
        else:
            non_final_by_date[ymd] += 1

    raw_by_pk, pitch_by_pk, raw_by_date, pitch_by_date = index_files(warehouse, args.season, args.stage)
    expected_pks = set(expected)
    raw_pks = set(raw_by_pk)
    pitch_pks = set(pitch_by_pk)

    missing_raw = sorted(expected_pks - raw_pks)
    missing_pitch = sorted(expected_pks - pitch_pks)
    raw_without_pitch = sorted(raw_pks - pitch_pks)
    pitch_without_raw = sorted(pitch_pks - raw_pks)
    extra_raw = sorted(raw_pks - expected_pks)
    extra_pitch = sorted(pitch_pks - expected_pks)

    dates = sorted(set(expected_by_date) | set(raw_by_date) | set(pitch_by_date))
    daily_rows = []
    for ymd in dates:
        exp_n = expected_by_date[ymd]
        raw_n = raw_by_date[ymd]
        pitch_n = pitch_by_date[ymd]
        if exp_n != raw_n or exp_n != pitch_n:
            daily_rows.append(
                {
                    "date": f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}",
                    "expected_final": exp_n,
                    "raw": raw_n,
                    "pitches_enriched": pitch_n,
                    "non_final": non_final_by_date[ymd],
                }
            )

    raw_errors = sample_raw_parse(list(raw_by_pk.values()), args.sample_raw)
    parquet_errors = sample_parquet_read(list(pitch_by_pk.values()), args.sample_parquets)

    report = {
        "season": args.season,
        "stage": args.stage,
        "game_type": args.game_type,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat(),
        "warehouse": str(warehouse),
        "expected_final_games": len(expected_pks),
        "raw_files": len(raw_pks),
        "pitches_enriched_files": len(pitch_pks),
        "missing_raw_count": len(missing_raw),
        "missing_pitch_count": len(missing_pitch),
        "raw_without_pitch_count": len(raw_without_pitch),
        "pitch_without_raw_count": len(pitch_without_raw),
        "extra_raw_count": len(extra_raw),
        "extra_pitch_count": len(extra_pitch),
        "sample_raw_errors": raw_errors,
        "sample_parquet_errors": parquet_errors,
        "daily_mismatches": daily_rows[:80],
        "missing_raw_examples": missing_raw[:25],
        "missing_pitch_examples": missing_pitch[:25],
        "raw_without_pitch_examples": raw_without_pitch[:25],
        "pitch_without_raw_examples": pitch_without_raw[:25],
        "extra_raw_examples": extra_raw[:25],
        "extra_pitch_examples": extra_pitch[:25],
    }

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("=== MLB warehouse health audit ===")
    print(f"warehouse: {warehouse}")
    print(f"season/stage: {args.season}/{args.stage}  through: {date_to.isoformat()}")
    print(f"expected final games from MLB API: {len(expected_pks)}")
    print(f"raw feed_live files:              {len(raw_pks)}")
    print(f"pitches_enriched parquets:        {len(pitch_pks)}")
    print("")
    print(f"missing raw:                      {len(missing_raw)}")
    print(f"missing pitches_enriched:         {len(missing_pitch)}")
    print(f"raw without pitch parquet:        {len(raw_without_pitch)}")
    print(f"pitch parquet without raw:        {len(pitch_without_raw)}")
    print(f"extra raw not expected-final:     {len(extra_raw)}")
    print(f"extra pitch not expected-final:   {len(extra_pitch)}")
    print("")
    if daily_rows:
        print("daily mismatches:")
        for row in daily_rows[:30]:
            print(
                f"  {row['date']}: expected={row['expected_final']} "
                f"raw={row['raw']} pitches={row['pitches_enriched']} non_final={row['non_final']}"
            )
        if len(daily_rows) > 30:
            print(f"  ... {len(daily_rows) - 30} more")
    else:
        print("daily mismatches: none")

    if raw_errors:
        print("\nraw sample errors:")
        for err in raw_errors[:10]:
            print(f"  {err}")
    if parquet_errors:
        print("\nparquet sample errors:")
        for err in parquet_errors[:10]:
            print(f"  {err}")

    hard_fail = bool(missing_raw or missing_pitch or raw_errors or parquet_errors)
    warn_only = bool(raw_without_pitch or pitch_without_raw or extra_raw or extra_pitch or daily_rows)
    if hard_fail:
        print("\nRESULT: FAIL")
        return 2
    if warn_only:
        print("\nRESULT: WARN")
        return 1
    print("\nRESULT: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
