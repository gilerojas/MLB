#!/usr/bin/env python3
"""Fast local health check for MLB Ops.

This intentionally avoids network calls and full-season scans. It checks only
the dependencies that have repeatedly caused morning friction: ports, local DB,
warehouse files for today/yesterday, generated outputs, and fast leaderboard
artifacts.
"""

from __future__ import annotations

import argparse
import os
import socket
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WAREHOUSE = ROOT / "data" / "warehouse" / "mlb"
DB_PATH = ROOT / "data" / "hub.db"
OUTPUTS_DIR = ROOT / "outputs"
ET = ZoneInfo("America/New_York")


def _load_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _warehouse(env: dict[str, str]) -> Path:
    raw = os.environ.get("MLB_WAREHOUSE_DIR") or env.get("MLB_WAREHOUSE_DIR") or ""
    return Path(raw).expanduser().resolve() if raw else DEFAULT_WAREHOUSE


def _port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) == 0


def _count(pattern: str, base: Path) -> int:
    try:
        return len(list(base.glob(pattern)))
    except OSError:
        return 0


def _queue_counts() -> tuple[int, str]:
    if not DB_PATH.is_file():
        return 0, "missing"
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            total = conn.execute("select count(*) from content_queue").fetchone()[0]
            drafts = conn.execute(
                "select count(*) from content_queue where status = 'draft'"
            ).fetchone()[0]
            failed = conn.execute(
                "select count(*) from content_queue where status = 'failed'"
            ).fetchone()[0]
        return int(total), f"{drafts} draft, {failed} failed"
    except sqlite3.Error as exc:
        return 0, f"error: {exc}"


def _print_check(ok: bool, label: str, detail: str) -> None:
    mark = "OK " if ok else "WARN"
    print(f"{mark}  {label:<28} {detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fast MLB Ops local readiness check")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on warnings")
    args = parser.parse_args()

    env = _load_env(ROOT / "mlbops" / ".env")
    warehouse = _warehouse(env)
    now_et = datetime.now(ET)
    today = now_et.date()
    yesterday = today - timedelta(days=1)
    season = today.year

    warnings = 0

    print("MLB Ops doctor")
    print(f"repo:      {ROOT}")
    print(f"warehouse: {warehouse}")
    print(f"date ET:   {today.isoformat()}  yesterday: {yesterday.isoformat()}")
    print()

    api_ok = _port_open("127.0.0.1", 8000)
    hub_port = int(env.get("MLBOPS_HUB_PORT") or os.environ.get("MLBOPS_HUB_PORT") or "3000")
    hub_ok = _port_open("127.0.0.1", hub_port)
    _print_check(api_ok, "API port 8000", "listening" if api_ok else "not listening")
    _print_check(hub_ok, f"Hub port {hub_port}", "listening" if hub_ok else "not listening")

    wh_ok = warehouse.is_dir()
    warnings += 0 if wh_ok else 1
    _print_check(wh_ok, "Warehouse directory", "readable" if wh_ok else "missing")

    for label, day in (("Yesterday data", yesterday), ("Today data", today)):
        ymd = day.strftime("%Y%m%d")
        base = warehouse / str(day.year) / "regular_season"
        raw_n = _count(f"game_*_{ymd}_feed_live.json*", base / "raw")
        pitch_n = _count(f"game_*_{ymd}_pitches_enriched.parquet", base / "pitches_enriched")
        ok = raw_n > 0 and pitch_n > 0
        if label == "Yesterday data":
            warnings += 0 if ok else 1
        _print_check(ok, label, f"raw={raw_n}, pitches_enriched={pitch_n}")

    fast_batting = list(warehouse.glob(f"player_season_boxscore_batting*{season}*.parquet"))
    fast_pitching = list(warehouse.glob(f"player_season_boxscore_pitching*{season}*.parquet"))
    csv_batting = warehouse / str(season) / "player_season_batting_by_team.csv"
    csv_pitching = warehouse / str(season) / "player_season_pitching_by_team.csv"
    leaders_fast = (fast_batting and fast_pitching) or (csv_batting.is_file() and csv_pitching.is_file())
    warnings += 0 if leaders_fast else 1
    _print_check(
        bool(leaders_fast),
        "Leaderboard fast files",
        "present" if leaders_fast else "missing; Insights may rebuild from raw and feel slow",
    )

    queue_total, queue_detail = _queue_counts()
    _print_check(queue_total > 0, "hub.db queue", f"{queue_total} rows ({queue_detail})")

    image_count = _count("*.png", OUTPUTS_DIR) + _count("*/*.png", OUTPUTS_DIR)
    _print_check(image_count > 0, "Generated images", f"{image_count} png file(s)")

    sync_sentinel = ROOT / "data" / ".last_drive_sync"
    _print_check(sync_sentinel.is_file(), "Drive sync marker", sync_sentinel.read_text().strip()[:19] if sync_sentinel.is_file() else "missing")

    print()
    if warnings:
        print(f"Result: {warnings} warning(s). Fix these before relying on heavy tabs/cards.")
    else:
        print("Result: no blocking local warnings.")
    return 1 if args.strict and warnings else 0


if __name__ == "__main__":
    raise SystemExit(main())
