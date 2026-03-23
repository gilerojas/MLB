#!/usr/bin/env python3
"""
Aggregate one pitcher's Spring Training boxscore stats from warehouse raw feeds.

Uses the same boxscore parsing as scripts/st_leaders_tweet.py (liveData.boxscore).

Usage (repo root):
  python scripts/st_player_pitching_summary.py --player-id 691725
  python scripts/st_player_pitching_summary.py --player-id 691725 --tweet
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent


def _open_raw(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _innings_to_float(ip_str: str) -> float:
    if not ip_str or ip_str in (".--", "-.--"):
        return 0.0
    s = str(ip_str).strip()
    if "." not in s:
        try:
            return float(s)
        except ValueError:
            return 0.0
    a, b = s.split(".", 1)
    try:
        whole = int(a)
        partial = int(b)
    except ValueError:
        return 0.0
    return whole + partial / 3.0


def _ip_display(ip: float) -> str:
    """Decimal innings → MLB-style string (e.g. 11.2 = 11⅔)."""
    if ip <= 0:
        return "0"
    whole = int(ip)
    frac = round((ip - whole) * 3)
    if frac == 0:
        return str(whole)
    return f"{whole}.{frac}"


def find_st_raw_paths(warehouse: Path, season: int) -> list[Path]:
    base = warehouse / str(season) / "spring_training" / "raw"
    if not base.exists():
        return []
    by_key: dict[str, Path] = {}
    for raw_path in base.glob("game_*_feed_live.json*"):
        if not (
            raw_path.name.endswith(".json")
            or raw_path.name.endswith(".json.gz")
        ):
            continue
        stem = raw_path.name[:-7] if raw_path.name.endswith(".json.gz") else raw_path.name[:-5]
        m = re.match(r"game_(\d+)_(\d+)_feed_live", stem)
        if not m:
            continue
        key = f"{m.group(1)}_{m.group(2)}"
        if key not in by_key or (
            raw_path.name.endswith(".json")
            and not raw_path.name.endswith(".json.gz")
        ):
            by_key[key] = raw_path
    return sorted(by_key.values())


def aggregate_pitcher(
    warehouse: Path,
    season: int,
    player_id: int,
) -> dict:
    pid_key = f"ID{player_id}"
    tot = {
        "ip": 0.0,
        "strikeOuts": 0,
        "earnedRuns": 0,
        "runs": 0,
        "hits": 0,
        "baseOnBalls": 0,
        "battersFaced": 0,
        "games": 0,
    }
    for raw_path in find_st_raw_paths(warehouse, season):
        try:
            with _open_raw(raw_path) as f:
                feed = json.load(f)
        except Exception:
            continue
        box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
        for side in ("away", "home"):
            players = (box.get(side) or {}).get("players") or {}
            if pid_key not in players:
                continue
            p = players[pid_key]
            pit = (p.get("stats") or {}).get("pitching") or {}
            if not pit:
                continue
            ip_val = pit.get("inningsPitched")
            if isinstance(ip_val, str):
                ipf = _innings_to_float(ip_val)
            else:
                ipf = float(ip_val or 0)
            if ipf <= 0 and pit.get("outs"):
                ipf = int(pit["outs"]) / 3.0
            if ipf <= 0:
                continue
            tot["ip"] += ipf
            for k in (
                "strikeOuts",
                "earnedRuns",
                "runs",
                "hits",
                "baseOnBalls",
                "battersFaced",
            ):
                v = pit.get(k)
                if isinstance(v, (int, float)):
                    tot[k] = tot.get(k, 0) + int(v)
            tot["games"] += 1
    return tot


def build_tweet_line(name: str, team_tag: str, tot: dict) -> str:
    ip = tot["ip"]
    if ip <= 0:
        return f"No ST pitching boxscore yet for {name} in warehouse."
    era = (tot["earnedRuns"] * 9.0) / ip
    whip = (tot["hits"] + tot["baseOnBalls"]) / ip
    k9 = (tot["strikeOuts"] * 9.0) / ip
    ip_s = _ip_display(ip)
    return (
        f"Congrats to {name} on making the Opening Day roster{team_tag}.\n\n"
        f"Spring Training (boxscore, Mallitalytics warehouse):\n"
        f"{ip_s} IP · {era:.2f} ERA · {whip:.2f} WHIP · {tot['strikeOuts']} K · "
        f"{tot['baseOnBalls']} BB · {tot['hits']} H · {tot['games']} app.\n\n"
        f"#RingTheBell #MLB"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="ST pitching totals for one player from warehouse raw")
    ap.add_argument("--warehouse", type=Path, default=None)
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--player-id", type=int, required=True)
    ap.add_argument("--name", type=str, default="Andrew Painter")
    ap.add_argument("--team-tag", type=str, default=" with the Phillies")
    ap.add_argument("--tweet", action="store_true", help="Print tweet-ready block")
    args = ap.parse_args()

    wh = args.warehouse or (_REPO_ROOT / "data" / "warehouse" / "mlb")
    if not wh.is_absolute():
        wh = _REPO_ROOT / wh

    tot = aggregate_pitcher(wh, args.season, args.player_id)
    if args.tweet:
        text = build_tweet_line(args.name, args.team_tag, tot)
        print(text)
        print(f"\n({len(text)} chars)")
    else:
        print(json.dumps(tot, indent=2))
        if tot["ip"] > 0:
            ip = tot["ip"]
            era = (tot["earnedRuns"] * 9.0) / ip
            whip = (tot["hits"] + tot["baseOnBalls"]) / ip
            print(
                f"IP display: {_ip_display(ip)} | ERA {era:.2f} | WHIP {whip:.2f} | "
                f"K/9 {(tot['strikeOuts'] * 9) / ip:.1f}"
            )


if __name__ == "__main__":
    main()
    sys.exit(0)
