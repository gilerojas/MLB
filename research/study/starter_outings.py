"""Warehouse -> starter outing aggregation, shared by the projection scripts.

Everything here is pandas/numpy only.  It is deliberately kept free of
scikit-learn and joblib so that lightweight jobs -- notably the nightly shadow
slate scorer running inside the mlbops api container -- can build actual
starter outings without the modelling stack installed.

`scripts/run_starter_projection_experiment.py` imports these definitions; this
module is the single source of truth for them.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.pitching_performances.malli_score import (
    OutingRawMetrics,
    default_league_norms,
    malliscore_v2,
)



WHIFF_DESCRIPTIONS = {"swinging_strike", "swinging_strike_blocked", "foul_tip", "missed_bunt"}
CALLED_STRIKE_DESCRIPTIONS = {"called_strike"}
SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "foul_bunt",
    "missed_bunt",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}
HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk", "hit_by_pitch"}
OUT_EVENTS = {
    "strikeout": 1,
    "field_out": 1,
    "force_out": 1,
    "fielders_choice_out": 1,
    "sac_fly": 1,
    "sac_bunt": 1,
    "double_play": 2,
    "grounded_into_double_play": 2,
    "strikeout_double_play": 2,
    "triple_play": 3,
}
PITCH_MIX_TYPES = ("FF", "SI", "FC", "SL", "ST", "CU", "KC", "CH", "FS", "SV")


def print_header(title: str) -> None:
    print("\n" + title)
    print("=" * len(title), flush=True)


def dataless(path: Path) -> bool:
    try:
        return bool(getattr(path.stat(), "st_flags", 0) & 0x40000000)
    except OSError:
        return False


def season_from_path(path: Path) -> int | None:
    for part in path.parts:
        if len(part) == 4 and part.isdigit():
            return int(part)
    return None


def sort_key(path: Path) -> tuple[str, str]:
    parts = path.stem.split("_")
    date_token = next((p for p in parts if len(p) == 8 and p.isdigit()), "00000000")
    return date_token, path.name


def _outs_from_ip(value: object) -> int | None:
    if value is None:
        return None
    text = str(value)
    if not text or text.lower() == "nan":
        return None
    if "." not in text:
        try:
            return int(float(text)) * 3
        except ValueError:
            return None
    whole, frac = text.split(".", 1)
    try:
        return int(whole) * 3 + int((frac[:1] or "0"))
    except ValueError:
        return None


def box_rows_from_feed(path: Path) -> list[dict[str, object]]:
    """Extract starter boxscore rows from one raw feed_live file."""
    import gzip

    rows: list[dict[str, object]] = []
    try:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as f:
            feed = json.load(f)
    except Exception:
        return rows
    game_pk = feed.get("gamePk")
    venue = ((feed.get("gameData") or {}).get("venue") or {})
    field_info = venue.get("fieldInfo") or {}
    box = ((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
    for team_box in box.values():
        team = ((team_box.get("team") or {}).get("abbreviation") or (team_box.get("team") or {}).get("teamCode"))
        for player in (team_box.get("players") or {}).values():
            person = player.get("person") or {}
            stats = (player.get("stats") or {}).get("pitching") or {}
            if not stats or int(stats.get("gamesStarted") or 0) < 1:
                continue
            try:
                pitcher_id = int(person.get("id"))
            except (TypeError, ValueError):
                continue
            outs = stats.get("outs")
            if outs is None:
                outs = _outs_from_ip(stats.get("inningsPitched"))
            rows.append(
                {
                    "game_pk": game_pk,
                    "pitcher": pitcher_id,
                    "box_pitcher_name": person.get("fullName"),
                    "box_team": team,
                    "venue_id": venue.get("id"),
                    "venue_name": venue.get("name"),
                    "roof_type": field_info.get("roofType"),
                    "turf_type": field_info.get("turfType"),
                    "box_outs": outs,
                    "box_pitches": stats.get("numberOfPitches") or stats.get("pitchesThrown"),
                    "box_strikeouts": stats.get("strikeOuts"),
                    "box_walks": stats.get("baseOnBalls"),
                    "box_hits": stats.get("hits"),
                    "box_home_runs": stats.get("homeRuns"),
                    "box_earned_runs": stats.get("earnedRuns"),
                    "box_batters_faced": stats.get("battersFaced"),
                }
            )
    return rows


def add_team_context(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    top = out["inning_topbot"].astype(str).str.lower().str.startswith("top")
    if "batter_team" not in out.columns:
        out["batter_team"] = np.where(top, out["away_team"], out["home_team"])
    if "pitcher_team" not in out.columns:
        out["pitcher_team"] = np.where(top, out["home_team"], out["away_team"])
    return out


def add_pitch_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = add_team_context(df)
    desc = out.get("description", pd.Series(index=out.index, dtype="object")).fillna("")
    events = out.get("events", pd.Series(index=out.index, dtype="object"))
    zone = pd.to_numeric(out.get("zone"), errors="coerce")
    out["is_pa_end"] = events.notna()
    out["is_swing"] = desc.isin(SWING_DESCRIPTIONS)
    out["is_whiff"] = desc.isin(WHIFF_DESCRIPTIONS)
    out["is_called_strike"] = desc.isin(CALLED_STRIKE_DESCRIPTIONS)
    out["out_zone"] = zone.ge(11)
    out["is_chase"] = out["out_zone"] & out["is_swing"]
    out["is_k"] = events.eq("strikeout")
    out["is_walk"] = events.isin(WALK_EVENTS)
    out["is_hit"] = events.isin(HIT_EVENTS)
    out["is_hr"] = events.eq("home_run")
    out["outs_on_play"] = events.map(OUT_EVENTS).fillna(0).astype(int)
    xwoba = pd.to_numeric(out.get("estimated_woba_using_speedangle"), errors="coerce")
    if "woba_value" in out.columns and "woba_denom" in out.columns:
        woba = pd.to_numeric(out["woba_value"], errors="coerce")
        denom = pd.to_numeric(out["woba_denom"], errors="coerce").fillna(0)
        out["pa_xwoba"] = xwoba.where(xwoba.notna(), woba.where(denom > 0))
    else:
        out["pa_xwoba"] = xwoba
    return out


def first_pitcher_rows(pitches: pd.DataFrame) -> pd.DataFrame:
    ordered = pitches.sort_values(["game_date", "game_pk", "pitcher_team", "inning", "at_bat_number", "pitch_number"])
    return ordered.groupby(["game_pk", "pitcher_team"], as_index=False).first()[
        ["game_pk", "pitcher_team", "pitcher", "game_date", "season"]
    ]


def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    bf = pd.to_numeric(out["batters_faced"], errors="coerce").clip(lower=1)
    outs = pd.to_numeric(out["outs"], errors="coerce").clip(lower=1)
    out["k_rate"] = out["strikeouts"] / bf
    out["bb_rate"] = out["walks"] / bf
    out["hit_rate"] = out["hits"] / bf
    out["hr_rate"] = out["home_runs"] / bf
    out["er_rate"] = out["earned_runs"] / outs
    return out


def aggregate_starter_outings(pitches: pd.DataFrame, box: pd.DataFrame) -> pd.DataFrame:
    print_header("3. Building Starter Outings")
    pitches = add_pitch_flags(pitches)
    starters = first_pitcher_rows(pitches).rename(columns={"pitcher": "starter"})
    data = pitches.merge(starters[["game_pk", "pitcher_team", "starter"]], on=["game_pk", "pitcher_team"], how="inner")
    data = data[data["pitcher"].eq(data["starter"])].copy()
    data["opponent"] = data["batter_team"]
    pa = data[data["is_pa_end"]].copy()

    group_cols = [
        "season",
        "game_date",
        "game_pk",
        "pitcher",
        "pitcher_team",
        "opponent",
        "p_throws",
        "home_team",
        "away_team",
    ]
    rows = (
        data.groupby(group_cols)
        .agg(
            pitches=("pitch_number", "size"),
            outs=("outs_on_play", "sum"),
            whiffs=("is_whiff", "sum"),
            called_strikes=("is_called_strike", "sum"),
            out_zone_pitches=("out_zone", "sum"),
            chases=("is_chase", "sum"),
            avg_release_speed=("release_speed", "mean"),
            max_release_speed=("release_speed", "max"),
        )
        .reset_index()
    )
    pa_rows = (
        pa.groupby(group_cols)
        .agg(
            batters_faced=("is_pa_end", "sum"),
            strikeouts=("is_k", "sum"),
            walks=("is_walk", "sum"),
            hits=("is_hit", "sum"),
            home_runs=("is_hr", "sum"),
            xwoba_allowed=("pa_xwoba", "mean"),
        )
        .reset_index()
    )
    rows = rows.merge(pa_rows, on=group_cols, how="left")

    mix = (
        data.assign(pitch_type=data["pitch_type"].fillna("__missing__"))
        .groupby(group_cols + ["pitch_type"])
        .size()
        .rename("n")
        .reset_index()
    )
    totals = mix.groupby(group_cols)["n"].transform("sum")
    mix["pct"] = mix["n"] / totals * 100.0
    wide_mix = mix.pivot_table(index=group_cols, columns="pitch_type", values="pct", aggfunc="sum").reset_index()
    for pitch_type in PITCH_MIX_TYPES:
        col = f"pitch_mix_{pitch_type}_pct"
        wide_mix[col] = wide_mix[pitch_type] if pitch_type in wide_mix.columns else 0.0
    wide_mix["primary_pitch_pct"] = wide_mix[[f"pitch_mix_{pt}_pct" for pt in PITCH_MIX_TYPES]].max(axis=1)
    fb = data[data["pitch_type"].isin(["FF", "SI", "FC"])]
    fb_speed = fb.groupby(group_cols)["release_speed"].mean().rename("fb_release_speed").reset_index()
    rows = rows.merge(wide_mix[group_cols + ["primary_pitch_pct", *[f"pitch_mix_{pt}_pct" for pt in PITCH_MIX_TYPES]]], on=group_cols, how="left")
    rows = rows.merge(fb_speed, on=group_cols, how="left")

    rows["swstr_pct"] = rows["whiffs"] / rows["pitches"].clip(lower=1) * 100.0
    rows["called_strike_pct"] = rows["called_strikes"] / rows["pitches"].clip(lower=1) * 100.0
    rows["chase_pct"] = rows["chases"] / rows["out_zone_pitches"].clip(lower=1) * 100.0
    rows["xwoba_allowed"] = rows["xwoba_allowed"].fillna(0.320)
    rows["earned_runs"] = np.nan

    if not box.empty:
        merge_cols = ["game_pk", "pitcher"]
        box_keep = [
            c
            for c in [
                "game_pk",
                "pitcher",
                "box_pitcher_name",
                "venue_name",
                "roof_type",
                "turf_type",
                "box_outs",
                "box_pitches",
                "box_strikeouts",
                "box_walks",
                "box_hits",
                "box_home_runs",
                "box_earned_runs",
                "box_batters_faced",
            ]
            if c in box.columns
        ]
        rows = rows.merge(box[box_keep], on=merge_cols, how="left")
        for actual, box_col in [
            ("outs", "box_outs"),
            ("pitches", "box_pitches"),
            ("strikeouts", "box_strikeouts"),
            ("walks", "box_walks"),
            ("hits", "box_hits"),
            ("home_runs", "box_home_runs"),
            ("earned_runs", "box_earned_runs"),
            ("batters_faced", "box_batters_faced"),
        ]:
            if box_col in rows.columns:
                rows[actual] = pd.to_numeric(rows[box_col], errors="coerce").where(
                    pd.to_numeric(rows[box_col], errors="coerce").notna(),
                    rows[actual],
                )
    rows["earned_runs"] = pd.to_numeric(rows["earned_runs"], errors="coerce")
    rows = rows[rows["pitches"].ge(30)].copy()
    rows = add_rates(rows)
    rows["pitcher_name"] = rows.get("box_pitcher_name", pd.Series(index=rows.index)).fillna(rows["pitcher"].astype(str))
    print(f"Starter outings: {len(rows):,}")
    print(f"Date range: {rows['game_date'].min()} to {rows['game_date'].max()}")
    return rows.sort_values(["game_date", "game_pk", "pitcher_team"]).reset_index(drop=True)


def malli_score_from_actual(row: pd.Series) -> float:
    outs = max(1, int(round(float(row["outs"]))))
    ip = outs / 3.0
    whip = (float(row["hits"]) + float(row["walks"])) / ip if ip > 0 else 1.3
    earned_runs = row["earned_runs"]
    if pd.isna(earned_runs):
        earned_runs = 2
    raw = OutingRawMetrics(
        swstr_pct=float(row["swstr_pct"]),
        called_strike_pct=float(row["called_strike_pct"]),
        chase_pct=float(row["chase_pct"]),
        xwoba_allowed=float(row["xwoba_allowed"]),
        game_whip=float(whip),
        earned_runs=max(0, int(round(float(earned_runs)))),
        home_runs=max(0, int(round(float(row["home_runs"])))),
        pitches=max(1, int(round(float(row["pitches"])))),
        outs=outs,
    )
    return malliscore_v2(raw, default_league_norms())["malli_score"]
