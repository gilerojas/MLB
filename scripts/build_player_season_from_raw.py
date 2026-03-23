#!/usr/bin/env python3
"""
Build season-level batting & pitching stats from MLB warehouse raw feed_live JSONs.

Inputs (read-only):
  MLB/data/warehouse/mlb/2024/regular_season/raw/game_*_feed_live.json
  MLB/data/warehouse/mlb/2025/regular_season/raw/game_*_feed_live.json

Outputs:
  MLB/data/warehouse/mlb/player_season_boxscore_batting_2024_2025.parquet
  MLB/data/warehouse/mlb/player_season_boxscore_pitching_2024_2025.parquet

These tables aggregate per-season, per-player boxscore stats (no Statcast),
which we can then join to WBC rosters in notebooks like DR_Roster_MLB_stats.ipynb.

Usage (from repo root or MLB/):
  cd MLB && python scripts/build_player_season_from_raw.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def parse_ip_to_outs(ip_str: str | None) -> int:
    """Convert inningsPitched string like '2.1' to outs (e.g. 7)."""
    if not ip_str:
        return 0
    try:
        parts = str(ip_str).split(".")
        whole = int(parts[0])
        rem = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        rem = max(0, min(2, rem))
        return whole * 3 + rem
    except Exception:
        return 0


def safe_int(v) -> int:
    try:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return 0
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return 0


def extract_game_rows(feed_path: Path) -> Tuple[List[dict], List[dict]]:
    """Read one feed_live JSON and return (batting_rows, pitching_rows)."""
    with feed_path.open() as f:
        data = json.load(f)

    gd = data.get("gameData", {}) or {}
    game_info = gd.get("game", {}) or {}
    season = int(game_info.get("season", 0) or 0)
    game_type = (game_info.get("type") or "").strip().upper()
    if game_type != "R":
        return [], []  # only regular season

    game_pk = int(data.get("gamePk", game_info.get("pk", 0)) or 0)
    box = data.get("liveData", {}).get("boxscore", {}) or {}
    teams = box.get("teams", {}) or {}

    batting_rows: List[dict] = []
    pitching_rows: List[dict] = []

    for side in ("away", "home"):
        tnode = teams.get(side) or {}
        team = tnode.get("team") or {}
        team_id = safe_int(team.get("id"))
        players = tnode.get("players") or {}

        for pid_key, pnode in players.items():
            person = pnode or {}
            player_id = safe_int(person.get("person", {}).get("id", person.get("id")))
            player_name = person.get("person", {}).get("fullName") or person.get(
                "fullName", ""
            )
            stats = person.get("stats", {}) or {}

            bat = stats.get("batting") or {}
            if bat:
                # Plate appearances if present, else derive crudely
                ab = safe_int(bat.get("atBats"))
                bb = safe_int(bat.get("baseOnBalls"))
                hbp = safe_int(bat.get("hitByPitch"))
                sf = safe_int(bat.get("sacFlies"))
                sh = safe_int(bat.get("sacBunts"))
                pa_stat = bat.get("plateAppearances")
                if pa_stat is not None:
                    pa = safe_int(pa_stat)
                else:
                    pa = ab + bb + hbp + sf + sh

                row_b = {
                    "season": season,
                    "game_pk": game_pk,
                    "team_id": team_id,
                    "side": side,
                    "player_id": player_id,
                    "player_name": player_name,
                    "games": 1,
                    "pa": pa,
                    "ab": ab,
                    "r": safe_int(bat.get("runs")),
                    "h": safe_int(bat.get("hits")),
                    "doubles": safe_int(bat.get("doubles")),
                    "triples": safe_int(bat.get("triples")),
                    "hr": safe_int(bat.get("homeRuns")),
                    "bb": bb,
                    "so": safe_int(bat.get("strikeOuts")),
                    "hbp": hbp,
                    "sf": sf,
                    "sh": sh,
                    "sb": safe_int(bat.get("stolenBases")),
                    "cs": safe_int(bat.get("caughtStealing")),
                    "rbi": safe_int(bat.get("rbi")),
                    "tb": safe_int(bat.get("totalBases")),
                }
                batting_rows.append(row_b)

            pit = stats.get("pitching") or {}
            if pit:
                outs = safe_int(pit.get("outs"))
                if outs == 0:
                    outs = parse_ip_to_outs(pit.get("inningsPitched"))

                row_p = {
                    "season": season,
                    "game_pk": game_pk,
                    "team_id": team_id,
                    "side": side,
                    "player_id": player_id,
                    "player_name": player_name,
                    "games": safe_int(pit.get("gamesPlayed") or 1),
                    "games_started": safe_int(pit.get("gamesStarted")),
                    "outs": outs,
                    "batters_faced": safe_int(pit.get("battersFaced")),
                    "hits": safe_int(pit.get("hits")),
                    "runs": safe_int(pit.get("runs")),
                    "earned_runs": safe_int(pit.get("earnedRuns")),
                    "hr": safe_int(pit.get("homeRuns")),
                    "bb": safe_int(pit.get("baseOnBalls")),
                    "so": safe_int(pit.get("strikeOuts")),
                    "hbp": safe_int(pit.get("hitByPitch")),
                    "pitches": safe_int(pit.get("numberOfPitches")),
                    "strikes": safe_int(pit.get("strikes")),
                    "wins": safe_int(pit.get("wins")),
                    "losses": safe_int(pit.get("losses")),
                    "saves": safe_int(pit.get("saves")),
                    "holds": safe_int(pit.get("holds")),
                    "blown_saves": safe_int(pit.get("blownSaves")),
                }
                pitching_rows.append(row_p)

    return batting_rows, pitching_rows


def build_season_tables(years: List[int], warehouse_root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate batting & pitching per season+player from raw JSONs."""
    batting_all: List[dict] = []
    pitching_all: List[dict] = []

    for year in years:
        raw_dir = warehouse_root / str(year) / "regular_season" / "raw"
        if not raw_dir.exists():
            print(f"[warn] Raw dir not found for {year}: {raw_dir}")
            continue
        print(f"[year {year}] Scanning {raw_dir} ...")
        for path in sorted(raw_dir.glob("game_*_feed_live.json")):
            b_rows, p_rows = extract_game_rows(path)
            if b_rows:
                batting_all.extend(b_rows)
            if p_rows:
                pitching_all.extend(p_rows)

    bat_df = pd.DataFrame(batting_all)
    pit_df = pd.DataFrame(pitching_all)

    def agg_batting(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        grp = (
            df.groupby(["season", "player_id", "player_name", "team_id"], as_index=False)
            .agg(
                games=("games", "sum"),
                pa=("pa", "sum"),
                ab=("ab", "sum"),
                r=("r", "sum"),
                h=("h", "sum"),
                doubles=("doubles", "sum"),
                triples=("triples", "sum"),
                hr=("hr", "sum"),
                bb=("bb", "sum"),
                so=("so", "sum"),
                hbp=("hbp", "sum"),
                sf=("sf", "sum"),
                sh=("sh", "sum"),
                sb=("sb", "sum"),
                cs=("cs", "sum"),
                rbi=("rbi", "sum"),
                tb=("tb", "sum"),
            )
        )
        # Derived rates
        grp["avg"] = np.where(grp["ab"] > 0, grp["h"] / grp["ab"], np.nan)
        denom_obp = grp["ab"] + grp["bb"] + grp["hbp"] + grp["sf"]
        grp["obp"] = np.where(denom_obp > 0, (grp["h"] + grp["bb"] + grp["hbp"]) / denom_obp, np.nan)
        grp["slg"] = np.where(grp["ab"] > 0, grp["tb"] / grp["ab"], np.nan)
        grp["ops"] = grp["obp"] + grp["slg"]
        return grp

    def agg_pitching(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        grp = (
            df.groupby(["season", "player_id", "player_name", "team_id"], as_index=False)
            .agg(
                games=("games", "sum"),
                games_started=("games_started", "sum"),
                outs=("outs", "sum"),
                batters_faced=("batters_faced", "sum"),
                hits=("hits", "sum"),
                runs=("runs", "sum"),
                earned_runs=("earned_runs", "sum"),
                hr=("hr", "sum"),
                bb=("bb", "sum"),
                so=("so", "sum"),
                hbp=("hbp", "sum"),
                pitches=("pitches", "sum"),
                strikes=("strikes", "sum"),
                wins=("wins", "sum"),
                losses=("losses", "sum"),
                saves=("saves", "sum"),
                holds=("holds", "sum"),
                blown_saves=("blown_saves", "sum"),
            )
        )
        grp["ip"] = grp["outs"] / 3.0
        grp["era"] = np.where(grp["ip"] > 0, grp["earned_runs"] * 9.0 / grp["ip"], np.nan)
        grp["k9"] = np.where(grp["ip"] > 0, grp["so"] * 9.0 / grp["ip"], np.nan)
        grp["bb9"] = np.where(grp["ip"] > 0, grp["bb"] * 9.0 / grp["ip"], np.nan)
        grp["hr9"] = np.where(grp["ip"] > 0, grp["hr"] * 9.0 / grp["ip"], np.nan)
        denom_bf = grp["batters_faced"].replace(0, np.nan)
        grp["k_pct"] = grp["so"] / denom_bf
        grp["bb_pct"] = grp["bb"] / denom_bf
        grp["k_minus_bb_pct"] = grp["k_pct"] - grp["bb_pct"]
        return grp

    bat_season = agg_batting(bat_df)
    pit_season = agg_pitching(pit_df)
    return bat_season, pit_season


def main() -> int:
    parser = argparse.ArgumentParser(description="Build player-season boxscore tables from raw feed_live JSONs")
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2024, 2025],
        help="Seasons to include (default: 2024 2025)",
    )
    parser.add_argument(
        "--warehouse-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "warehouse" / "mlb",
        help="Root of MLB warehouse (default: data/warehouse/mlb)",
    )
    args = parser.parse_args()

    print("Building player-season tables from raw feed_live JSONs...")
    print(f"  Warehouse root: {args.warehouse_root}")
    print(f"  Seasons:        {args.years}")

    bat_season, pit_season = build_season_tables(args.years, args.warehouse_root)

    out_root = args.warehouse_root
    bat_path = out_root / "player_season_boxscore_batting_2024_2025.parquet"
    pit_path = out_root / "player_season_boxscore_pitching_2024_2025.parquet"

    bat_season.to_parquet(bat_path, index=False)
    pit_season.to_parquet(pit_path, index=False)

    print("\nDone.")
    print(f"  Batting season table : {bat_path}  ({len(bat_season)} rows)")
    print(f"  Pitching season table: {pit_path}  ({len(pit_season)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

