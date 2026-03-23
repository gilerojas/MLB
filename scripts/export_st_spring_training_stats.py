#!/usr/bin/env python3
"""
Export Spring Training (or any warehouse stage) batting & pitching stats to CSV
from **pitches_enriched** parquets (Statcast merged with feed play_id).

Counting stats use terminal `events` per plate appearance (same idea as
`mallitalytics_daily_card.compute_box_score` / `process_pitches`).

**Batting CSV**: PA, AB, H, 1B–HR, BB, SO, HBP, SF, SH, GIDP, AVG/OBP/SLG/OPS,
K%/BB%, wOBA (from woba_value/woba_denom), xwOBA on BIP, HH% (95+ mph BIP),
avg EV/LA on BIP, bat_speed / swing_length means when present.

**Pitching CSV**: Official **IP**, **ER**, **RA** (runs allowed), **ERA**, **WHIP**, **K9**,
**BB9** from `liveData.boxscore` in raw feed_live (summed per player) when raws exist;
**IP_est** = innings from Statcast PA-ending outs ÷3 (proxy only). Pitch analytics
(Zone%, xwOBA on BIP, …) still come from pitches_enriched.

**Batting CSV**: **R**, **RBI**, **SB** from the same boxscore aggregates. Other columns
from Statcast/pitch logic.

**Team / name search**: ST games often show the **affiliate** abbrev (e.g. SWB, not NYY).
Jasson Domínguez appears as **SWB** in many ST rows — filter by `mlbam_id` **691176** or
accent in **Domínguez**.

Adds **age** with priority: **feed** `gameData.players[..].currentAge` from
`raw/game_*_feed_live.json(.gz)` → parquet `age_bat`/`age_pit` → MLB Stats API.
See `docs/FEED_LIVE_STRUCTURE.md`. **Team** from API `currentTeam` or PA-side
majority (`team_from_games`).

Usage (repo root):
  python scripts/export_st_spring_training_stats.py
  python scripts/export_st_spring_training_stats.py --season 2026 --stage spring_training
  python scripts/export_st_spring_training_stats.py --out-dir outputs/st_exports
  python scripts/export_st_spring_training_stats.py --no-api
  python scripts/export_st_spring_training_stats.py --no-feed   # skip scanning raw feeds for age
"""

from __future__ import annotations

import argparse
import gzip
import importlib.util
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Reuse feed boxscore aggregation (same raw paths as --no-feed age scan)
_spec = importlib.util.spec_from_file_location("st_leaders_tweet", _SCRIPT_DIR / "st_leaders_tweet.py")
_st_leaders = importlib.util.module_from_spec(_spec)
if _spec.loader is None:
    raise RuntimeError("importlib could not load st_leaders_tweet.py")
_spec.loader.exec_module(_st_leaders)
aggregate_boxscore_from_raw = _st_leaders.aggregate_boxscore_from_raw

# Outs on PA-ending play (for IP_est)
_OUTS_ON_EVENT: dict[str, int] = {
    "strikeout": 1,
    "strikeout_double_play": 2,
    "field_out": 1,
    "field_error": 1,
    "fielders_choice": 1,
    "fielders_choice_out": 1,
    "force_out": 1,
    "double_play": 2,
    "grounded_into_double_play": 2,
    "sac_fly": 1,
    "sac_bunt": 1,
    "sac_fly_double_play": 2,
    "catcher_interf": 0,
    "single": 0,
    "double": 0,
    "triple": 0,
    "home_run": 0,
    "walk": 0,
    "intent_walk": 0,
    "hit_by_pitch": 0,
    "truncated_pa": 0,
}

CSW_CODES = frozenset(
    ["called_strike", "swinging_strike", "swinging_strike_blocked", "foul_tip"]
)
SWING_CODES = frozenset(
    [
        "foul_bunt",
        "foul",
        "hit_into_play",
        "swinging_strike",
        "foul_tip",
        "swinging_strike_blocked",
        "missed_bunt",
        "bunt_foul_tip",
    ]
)
WHIFF_CODES = frozenset(["swinging_strike", "foul_tip", "swinging_strike_blocked"])


def _open_feed_json(path: Path):
    """Open warehouse raw feed (.json or .json.gz), same idea as load_mlb_warehouse._open_raw."""
    lp = path.name.lower()
    if lp.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def collect_feed_current_ages(raw_dir: Path) -> dict[int, int]:
    """
    Merge ``currentAge`` from ``gameData.players`` across all feed_live raws
    in ``raw_dir`` (``game_{pk}_{date}_feed_live.json*``). Later files overwrite
    earlier keys (values are usually identical per season).
    """
    ages: dict[int, int] = {}
    if not raw_dir.is_dir():
        return ages
    for path in sorted(raw_dir.glob("game_*_feed_live.json*")):
        lp = path.name.lower()
        if not (lp.endswith(".json") or lp.endswith(".json.gz")):
            continue
        try:
            with _open_feed_json(path) as fh:
                data = json.load(fh)
        except Exception:
            continue
        players = (data.get("gameData") or {}).get("players") or {}
        for _key, pdata in players.items():
            if not isinstance(pdata, dict):
                continue
            pid = pdata.get("id")
            if pid is None:
                continue
            try:
                pid = int(pid)
            except (TypeError, ValueError):
                continue
            ca = pdata.get("currentAge")
            if ca is None:
                continue
            try:
                ages[pid] = int(ca)
            except (TypeError, ValueError):
                pass
    return ages


def _apply_feed_ages_to_age_parquet(df: pd.DataFrame, feed_ages: dict[int, int]) -> pd.DataFrame:
    """Prefer feed ``currentAge`` over existing ``age_parquet`` when present."""
    if df.empty or not feed_ages:
        return df
    out = df.copy()
    mapped = out["mlbam_id"].astype(int).map(feed_ages)
    out["age_parquet"] = mapped.where(mapped.notna(), out["age_parquet"])
    return out


def _boxscore_pid_key(k: Any) -> int | None:
    try:
        return int(str(k).replace("ID", ""))
    except (TypeError, ValueError):
        return None


def _merge_boxscore_totals(
    bdf: pd.DataFrame,
    pdf: pd.DataFrame,
    batting_totals: dict[Any, dict],
    pitching_totals: dict[Any, dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fill R/RBI/SB from boxscore batting; IP/ER/RA + official counting line and
    ERA/WHIP/K9/BB9 from boxscore pitching. See ``st_leaders_tweet.aggregate_boxscore_from_raw``.
    """
    box_bat = {pk: v for k, v in batting_totals.items() if (pk := _boxscore_pid_key(k)) is not None}
    box_pit = {pk: v for k, v in pitching_totals.items() if (pk := _boxscore_pid_key(k)) is not None}

    if not bdf.empty and box_bat:
        bdf = bdf.copy()
        rs, rbis, sbs = [], [], []
        for pid in bdf["mlbam_id"].astype(int):
            agg = box_bat.get(int(pid))
            if not agg:
                rs.append("")
                rbis.append("")
                sbs.append("")
            else:
                rs.append(int(agg.get("runs") or 0))
                rbis.append(int(agg.get("rbi") or 0))
                sbs.append(int(agg.get("stolenBases") or 0))
        bdf["R"], bdf["RBI"], bdf["SB"] = rs, rbis, sbs

    if not pdf.empty:
        pdf = pdf.copy()
        # Use float columns so Arrow/string dtypes (from "") do not reject numeric boxscore values.
        pdf["IP"] = np.nan
        pdf["ER"] = np.nan
        pdf["RA"] = np.nan
        pdf["ERA"] = np.nan
        if box_pit:
            for idx, row in pdf.iterrows():
                pid = int(row["mlbam_id"])
                agg = box_pit.get(pid)
                if not agg:
                    continue
                ip = float(agg.get("ip") or 0.0)
                er = int(agg.get("earnedRuns") or 0)
                ra = int(agg.get("runs") or 0)
                h = int(agg.get("hits") or 0)
                bb = int(agg.get("baseOnBalls") or 0)
                so = int(agg.get("strikeOuts") or 0)
                hr = int(agg.get("homeRuns") or 0)
                hbp = int(agg.get("hitByPitch") or 0)
                bf = int(agg.get("battersFaced") or 0)
                if ip <= 0:
                    continue
                pdf.at[idx, "IP"] = round(ip, 3)
                pdf.at[idx, "ER"] = er
                pdf.at[idx, "RA"] = ra
                pdf.at[idx, "H"] = h
                pdf.at[idx, "BB"] = bb
                pdf.at[idx, "SO"] = so
                pdf.at[idx, "HR"] = hr
                pdf.at[idx, "HBP"] = hbp
                pdf.at[idx, "BF"] = bf
                pdf.at[idx, "ERA"] = er / ip * 9.0
                pdf.at[idx, "WHIP"] = (h + bb) / ip
                pdf.at[idx, "K9"] = so / ip * 9.0
                pdf.at[idx, "BB9"] = bb / ip * 9.0

    return bdf, pdf


def _norm_events(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .replace("nan", np.nan)
    )


def _normalize_description_inplace(df: pd.DataFrame) -> None:
    if "description" not in df.columns:
        df["description"] = ""
        return
    d = (
        df["description"]
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(",", "")
    )
    bip_like = d.str.contains("in_play|hit_into_play", na=False, regex=True)
    df["description"] = d.where(~bip_like, "hit_into_play")


def _terminal_pa_df(df: pd.DataFrame) -> pd.DataFrame:
    if "events" not in df.columns or df["events"].isna().all():
        return pd.DataFrame()
    ev = df[df["events"].notna()].copy()
    ev["_e"] = _norm_events(ev["events"])
    ev = ev[ev["_e"].notna() & (ev["_e"] != "truncated_pa")]
    if ev.empty:
        return ev.drop(columns=["_e"], errors="ignore")
    sort_cols = [c for c in ["game_pk", "inning", "at_bat_number", "pitch_number"] if c in ev.columns]
    if len(sort_cols) < 2:
        return ev.drop(columns=["_e"], errors="ignore")
    ev = ev.sort_values(sort_cols)
    out = ev.groupby(["game_pk", "at_bat_number"], as_index=False, dropna=False).last()
    return out.drop(columns=["_e"], errors="ignore")


def _batter_team_row(r: pd.Series) -> str:
    top = str(r.get("inning_topbot", "") or "").upper().startswith("T")
    away = str(r.get("away_team", "") or "")
    home = str(r.get("home_team", "") or "")
    return away if top else home


def _pitcher_team_row(r: pd.Series) -> str:
    top = str(r.get("inning_topbot", "") or "").upper().startswith("T")
    away = str(r.get("away_team", "") or "")
    home = str(r.get("home_team", "") or "")
    return home if top else away


def _process_pitch_rows(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    _normalize_description_inplace(d)
    if "pitch_type" not in d.columns:
        d["pitch_type"] = "UN"
    if "stand" not in d.columns:
        d["stand"] = "R"
    d["swing"] = d["description"].isin(SWING_CODES)
    d["whiff"] = d["description"].isin(WHIFF_CODES)
    if "zone" not in d.columns or d["zone"].isna().all():
        d["zone"] = 14
    z = pd.to_numeric(d["zone"], errors="coerce").fillna(14)
    d["in_zone"] = (z < 10) & (z > 0)
    d["chase"] = (~d["in_zone"]) & d["swing"]
    if "type" in d.columns:
        t = d["type"].astype(str).str.strip().str.upper()
        d["is_strike"] = t.isin(("S", "X"))
    else:
        d["is_strike"] = False
    d["is_bip"] = d["description"] == "hit_into_play"
    if "bb_type" in d.columns:
        d["bb_type"] = (
            d["bb_type"].astype(str).str.strip().str.lower().str.replace(" ", "_", regex=False)
        )
        d["is_gb_bip"] = (d["bb_type"] == "ground_ball") & d["is_bip"]
    else:
        d["is_gb_bip"] = False
    if "launch_speed" in d.columns:
        ls = pd.to_numeric(d["launch_speed"], errors="coerce")
        d["hard_hit"] = (ls >= 95.0) & d["is_bip"]
    else:
        d["hard_hit"] = False
    d["csw"] = d["description"].isin(CSW_CODES)
    return d


def fetch_people_batch(ids: list[int], batch_size: int = 50) -> dict[int, dict[str, Any]]:
    """
    Resolve names, ages, and team abbrevs with one people request + one teams request
    per batch (no per-player team HTTP follow-up).
    """
    out: dict[int, dict[str, Any]] = {}
    for i in range(0, len(ids), batch_size):
        batch = ids[i : i + batch_size]
        url = (
            "https://statsapi.mlb.com/api/v1/people?personIds="
            + ",".join(str(x) for x in batch)
            + "&hydrate=currentTeam"
        )
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            people = r.json().get("people", [])
            team_ids: set[int] = set()
            for p in people:
                tid = (p.get("currentTeam") or {}).get("id")
                if tid is not None:
                    team_ids.add(int(tid))
            team_abb: dict[int, str] = {}
            if team_ids:
                try:
                    tr = requests.get(
                        "https://statsapi.mlb.com/api/v1/teams?teamIds="
                        + ",".join(str(x) for x in sorted(team_ids)),
                        timeout=30,
                    )
                    tr.raise_for_status()
                    for t in tr.json().get("teams", []):
                        team_abb[int(t["id"])] = (t.get("abbreviation") or "").strip()
                except Exception:
                    pass
            for p in people:
                pid = int(p["id"])
                tid = (p.get("currentTeam") or {}).get("id")
                team = ""
                if tid is not None:
                    team = team_abb.get(int(tid), "")
                out[pid] = {
                    "name": p.get("fullName", f"ID {pid}"),
                    "age": p.get("currentAge", ""),
                    "team_abb": team,
                }
        except Exception:
            for pid in batch:
                out.setdefault(pid, {"name": f"ID {pid}", "age": "", "team_abb": ""})
    return out


def run_export(
    warehouse: Path,
    season: int,
    stage: str,
    out_dir: Path,
    use_api: bool,
    use_feed_ages: bool = True,
    use_boxscore: bool = True,
) -> tuple[Path, Path]:
    enriched = warehouse / str(season) / stage / "pitches_enriched"
    if not enriched.exists():
        raise FileNotFoundError(f"No pitches_enriched dir: {enriched}")

    paths = sorted(enriched.glob("game_*_pitches_enriched.parquet"))
    if not paths:
        raise FileNotFoundError(f"No parquet files in {enriched}")

    bat: dict[int, dict[str, Any]] = defaultdict(
        lambda: {
            "pa": 0,
            "1b": 0,
            "2b": 0,
            "3b": 0,
            "hr": 0,
            "bb": 0,
            "so": 0,
            "hbp": 0,
            "sf": 0,
            "sh": 0,
            "gidp": 0,
            "woba_num": 0.0,
            "woba_den": 0.0,
            "xwoba_sum": 0.0,
            "xwoba_n": 0,
            "bip": 0,
            "hh": 0,
            "gb": 0,
            "ev_sum": 0.0,
            "ev_n": 0,
            "la_sum": 0.0,
            "la_n": 0,
            "bat_speed_sum": 0.0,
            "bat_speed_n": 0,
            "swing_len_sum": 0.0,
            "swing_len_n": 0,
            "age_sum": 0.0,
            "age_n": 0,
            "games": set(),
            "team_votes": Counter(),
            "name_hint": "",
        }
    )

    pit: dict[int, dict[str, Any]] = defaultdict(
        lambda: {
            "bf": 0,
            "outs": 0,
            "k": 0,
            "bb": 0,
            "hbp": 0,
            "hr": 0,
            "h": 0,
            "pitches": 0,
            "velo_sum": 0.0,
            "velo_n": 0,
            "spin_sum": 0.0,
            "spin_n": 0,
            "xwoba_sum": 0.0,
            "xwoba_n": 0,
            "bip": 0,
            "hh": 0,
            "gb": 0,
            "in_zone": 0,
            "swings": 0,
            "whiffs": 0,
            "chases": 0,
            "csw": 0,
            "age_sum": 0.0,
            "age_n": 0,
            "games": set(),
            "team_votes": Counter(),
        }
    )

    for path in paths:
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        if df.empty or "batter" not in df.columns or "pitcher" not in df.columns:
            continue

        gpk = int(df["game_pk"].iloc[0]) if "game_pk" in df.columns else None

        # --- Terminal PA rows (one per PA)
        pa_df = _terminal_pa_df(df)
        if not pa_df.empty:
            pa_df["_e"] = _norm_events(pa_df["events"])
            pa_df = pa_df[pa_df["_e"].notna() & (pa_df["_e"] != "truncated_pa")]
            _normalize_description_inplace(pa_df)
            pa_df["_desc"] = pa_df["description"]

        if not pa_df.empty:
            for _, row in pa_df.iterrows():
                bid = int(row["batter"])
                pid = int(row["pitcher"])
                e = row["_e"]
                b = bat[bid]
                p = pit[pid]

                b["pa"] += 1
                p["bf"] += 1
                if gpk is not None:
                    b["games"].add(gpk)
                    p["games"].add(gpk)
                b["team_votes"][_batter_team_row(row)] += 1
                p["team_votes"][_pitcher_team_row(row)] += 1

                if not b["name_hint"] and pd.notna(row.get("player_name")):
                    b["name_hint"] = str(row["player_name"])

                if e == "single":
                    b["1b"] += 1
                    p["h"] += 1
                elif e == "double":
                    b["2b"] += 1
                    p["h"] += 1
                elif e == "triple":
                    b["3b"] += 1
                    p["h"] += 1
                elif e == "home_run":
                    b["hr"] += 1
                    p["hr"] += 1
                    p["h"] += 1
                elif e in ("walk", "intent_walk"):
                    b["bb"] += 1
                    p["bb"] += 1
                elif e in ("strikeout", "strikeout_double_play"):
                    b["so"] += 1
                    p["k"] += 1
                elif e == "hit_by_pitch":
                    b["hbp"] += 1
                    p["hbp"] += 1
                elif e == "sac_fly":
                    b["sf"] += 1
                elif e == "sac_bunt":
                    b["sh"] += 1
                elif e == "grounded_into_double_play":
                    b["gidp"] += 1

                wv, wd = row.get("woba_value"), row.get("woba_denom")
                try:
                    wvf = float(wv) if pd.notna(wv) else None
                    wdf = float(wd) if pd.notna(wd) else None
                    if wvf is not None and wdf is not None and wdf != 0:
                        b["woba_num"] += wvf
                        b["woba_den"] += wdf
                except (TypeError, ValueError):
                    pass

                outs = _OUTS_ON_EVENT.get(str(e), 0)
                p["outs"] += outs

                # BIP quality on terminal pitch (BIP outcomes)
                if row.get("_desc") == "hit_into_play":
                    xw = row.get("estimated_woba_using_speedangle")
                    if pd.notna(xw):
                        try:
                            xf = float(xw)
                            b["xwoba_sum"] += xf
                            b["xwoba_n"] += 1
                            p["xwoba_sum"] += xf
                            p["xwoba_n"] += 1
                        except (TypeError, ValueError):
                            pass
                    b["bip"] += 1
                    p["bip"] += 1
                    ev = row.get("launch_speed")
                    if pd.notna(ev):
                        try:
                            evf = float(ev)
                            b["ev_sum"] += evf
                            b["ev_n"] += 1
                        except (TypeError, ValueError):
                            pass
                    la = row.get("launch_angle")
                    if pd.notna(la):
                        try:
                            b["la_sum"] += float(la)
                            b["la_n"] += 1
                        except (TypeError, ValueError):
                            pass
                    ev2 = row.get("launch_speed")
                    if pd.notna(ev2):
                        try:
                            if float(ev2) >= 95.0:
                                b["hh"] += 1
                                p["hh"] += 1
                        except (TypeError, ValueError):
                            pass
                    bt = row.get("bb_type")
                    if pd.notna(bt) and str(bt).strip():
                        bbt = str(bt).strip().lower().replace(" ", "_")
                        if bbt == "ground_ball":
                            b["gb"] += 1
                            p["gb"] += 1

        # Age from all rows
        if "age_bat" in df.columns:
            ab = pd.to_numeric(df["age_bat"], errors="coerce")
            for bid, g in df.groupby("batter"):
                sub = ab.loc[g.index]
                m = sub.notna()
                if m.any():
                    bb = bat[int(bid)]
                    bb["age_sum"] += float(sub[m].sum())
                    bb["age_n"] += int(m.sum())
        if "age_pit" in df.columns:
            ap = pd.to_numeric(df["age_pit"], errors="coerce")
            for pid, g in df.groupby("pitcher"):
                sub = ap.loc[g.index]
                m = sub.notna()
                if m.any():
                    pp = pit[int(pid)]
                    pp["age_sum"] += float(sub[m].sum())
                    pp["age_n"] += int(m.sum())

        # Pitch-level pitcher metrics (all pitches)
        proc = _process_pitch_rows(df)
        if "release_speed" in proc.columns:
            rs = pd.to_numeric(proc["release_speed"], errors="coerce")
        else:
            rs = pd.Series(np.nan, index=proc.index)
        if "release_spin_rate" in proc.columns:
            sp = pd.to_numeric(proc["release_spin_rate"], errors="coerce")
        else:
            sp = pd.Series(np.nan, index=proc.index)

        for pid, g in proc.groupby(proc["pitcher"]):
            pid = int(pid)
            p = pit[pid]
            p["pitches"] += len(g)
            p["in_zone"] += int(g["in_zone"].sum())
            p["swings"] += int(g["swing"].sum())
            p["whiffs"] += int(g["whiff"].sum())
            p["chases"] += int(g["chase"].sum())
            p["csw"] += int(g["csw"].sum())
            v = rs.loc[g.index]
            m = v.notna()
            if m.any():
                p["velo_sum"] += float(v[m].sum())
                p["velo_n"] += int(m.sum())
            s = sp.loc[g.index]
            m2 = s.notna()
            if m2.any():
                p["spin_sum"] += float(s[m2].sum())
                p["spin_n"] += int(m2.sum())

        # Bat speed / swing length on swings
        if "bat_speed" in proc.columns and "swing" in proc.columns:
            bs = pd.to_numeric(proc["bat_speed"], errors="coerce")
            for bid, g in proc.groupby(proc["batter"]):
                sw = g["swing"]
                sub = bs.loc[g.index]
                m = sw & sub.notna()
                if m.any():
                    bb = bat[int(bid)]
                    bb["bat_speed_sum"] += float(sub[m].sum())
                    bb["bat_speed_n"] += int(m.sum())
        if "swing_length" in proc.columns and "swing" in proc.columns:
            sl = pd.to_numeric(proc["swing_length"], errors="coerce")
            for bid, g in proc.groupby(proc["batter"]):
                sw = g["swing"]
                sub = sl.loc[g.index]
                m = sw & sub.notna()
                if m.any():
                    bb = bat[int(bid)]
                    bb["swing_len_sum"] += float(sub[m].sum())
                    bb["swing_len_n"] += int(m.sum())

    # --- Build DataFrames
    def _top_team(votes: Counter) -> str:
        if not votes:
            return ""
        return votes.most_common(1)[0][0]

    bat_rows = []
    for bid, b in bat.items():
        pa = b["pa"]
        if pa == 0:
            continue
        h = b["1b"] + b["2b"] + b["3b"] + b["hr"]
        ab = pa - b["bb"] - b["hbp"] - b["sf"] - b["sh"]
        tb = b["1b"] + 2 * b["2b"] + 3 * b["3b"] + 4 * b["hr"]
        avg = (h / ab) if ab > 0 else np.nan
        obp_d = ab + b["bb"] + b["hbp"] + b["sf"]
        obp = ((h + b["bb"] + b["hbp"]) / obp_d) if obp_d > 0 else np.nan
        slg = (tb / ab) if ab > 0 else np.nan
        ops = (obp + slg) if not (np.isnan(obp) or np.isnan(slg)) else np.nan
        woba = (b["woba_num"] / b["woba_den"]) if b["woba_den"] > 0 else np.nan
        xwoba = (b["xwoba_sum"] / b["xwoba_n"]) if b["xwoba_n"] > 0 else np.nan
        hh_pct = (b["hh"] / b["bip"]) if b["bip"] > 0 else np.nan
        gb_pct = (b["gb"] / b["bip"]) if b["bip"] > 0 else np.nan
        ev_avg = (b["ev_sum"] / b["ev_n"]) if b["ev_n"] > 0 else np.nan
        la_avg = (b["la_sum"] / b["la_n"]) if b["la_n"] > 0 else np.nan
        bat_rows.append(
            {
                "mlbam_id": bid,
                "player_name": b["name_hint"],
                "team_from_games": _top_team(b["team_votes"]),
                "age_parquet": (b["age_sum"] / b["age_n"]) if b["age_n"] else np.nan,
                "games": len(b["games"]),
                "PA": pa,
                "AB": ab,
                "H": h,
                "1B": b["1b"],
                "2B": b["2b"],
                "3B": b["3b"],
                "HR": b["hr"],
                "R": "",
                "RBI": "",
                "SB": "",
                "BB": b["bb"],
                "SO": b["so"],
                "HBP": b["hbp"],
                "SF": b["sf"],
                "SH": b["sh"],
                "GIDP": b["gidp"],
                "AVG": avg,
                "OBP": obp,
                "SLG": slg,
                "OPS": ops,
                "K_pct": b["so"] / pa,
                "BB_pct": b["bb"] / pa,
                "wOBA": woba,
                "xwOBA_on_BIP": xwoba,
                "HH_pct": hh_pct,
                "GB_pct_on_BIP": gb_pct,
                "EV_avg_on_BIP": ev_avg,
                "LA_avg_on_BIP": la_avg,
                "bat_speed_avg": (b["bat_speed_sum"] / b["bat_speed_n"]) if b["bat_speed_n"] else np.nan,
                "swing_length_avg": (b["swing_len_sum"] / b["swing_len_n"]) if b["swing_len_n"] else np.nan,
            }
        )

    pit_rows = []
    for pid, p in pit.items():
        bf = p["bf"]
        if bf == 0 and p["pitches"] == 0:
            continue
        ip = p["outs"] / 3.0 if p["outs"] else 0.0
        h, bb, k, hr = p["h"], p["bb"], p["k"], p["hr"]
        whip = ((h + bb) / ip) if ip > 0 else np.nan
        k9 = (k / ip * 9.0) if ip > 0 else np.nan
        bb9 = (bb / ip * 9.0) if ip > 0 else np.nan
        xwoba = (p["xwoba_sum"] / p["xwoba_n"]) if p["xwoba_n"] > 0 else np.nan
        hh_pct = (p["hh"] / p["bip"]) if p["bip"] > 0 else np.nan
        gb_pct = (p["gb"] / p["bip"]) if p["bip"] > 0 else np.nan
        zone_pct = (p["in_zone"] / p["pitches"]) if p["pitches"] else np.nan
        whiff_pct = (p["whiffs"] / p["swings"]) if p["swings"] else np.nan
        chase_pct = (p["chases"] / (p["pitches"] - p["in_zone"])) if (p["pitches"] - p["in_zone"]) > 0 else np.nan
        csw_pct = (p["csw"] / p["pitches"]) if p["pitches"] else np.nan
        avg_velo = (p["velo_sum"] / p["velo_n"]) if p["velo_n"] else np.nan
        avg_spin = (p["spin_sum"] / p["spin_n"]) if p["spin_n"] else np.nan

        pit_rows.append(
            {
                "mlbam_id": pid,
                "player_name": "",
                "team_from_games": _top_team(p["team_votes"]),
                "age_parquet": (p["age_sum"] / p["age_n"]) if p["age_n"] else np.nan,
                "games": len(p["games"]),
                "BF": bf,
                "IP_est": round(ip, 3),
                "H": h,
                "ER": "",
                "BB": bb,
                "SO": k,
                "HR": hr,
                "HBP": p["hbp"],
                "WHIP": whip,
                "K9": k9,
                "BB9": bb9,
                "pitches": p["pitches"],
                "Zone_pct": zone_pct,
                "Whiff_pct": whiff_pct,
                "Chase_pct": chase_pct,
                "CSW_pct": csw_pct,
                "avg_velo": avg_velo,
                "avg_spin_rpm": avg_spin,
                "xwOBA_on_BIP": xwoba,
                "HH_pct_against": hh_pct,
                "GB_pct_on_BIP": gb_pct,
            }
        )

    bdf = pd.DataFrame(bat_rows)
    pdf = pd.DataFrame(pit_rows)

    if use_boxscore:
        try:
            batting_totals, pitching_totals = aggregate_boxscore_from_raw(warehouse, season, stage)
        except Exception:
            batting_totals, pitching_totals = {}, {}
        bdf, pdf = _merge_boxscore_totals(bdf, pdf, batting_totals, pitching_totals)

    raw_dir = warehouse / str(season) / stage / "raw"
    feed_ages = collect_feed_current_ages(raw_dir) if use_feed_ages else {}
    if feed_ages:
        bdf = _apply_feed_ages_to_age_parquet(bdf, feed_ages)
        pdf = _apply_feed_ages_to_age_parquet(pdf, feed_ages)

    if use_api and (not bdf.empty or not pdf.empty):
        all_ids = sorted(set(bdf["mlbam_id"].tolist()) | set(pdf["mlbam_id"].tolist()))
        bio = fetch_people_batch(all_ids)

        def _enrich_with_api(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df
            out = df.copy()
            pids = out["mlbam_id"].astype(int)
            names = [bio.get(int(pid), {}).get("name", "") or "" for pid in pids]
            ages_api = [bio.get(int(pid), {}).get("age", "") for pid in pids]
            teams_api = [(bio.get(int(pid), {}).get("team_abb") or "").strip() for pid in pids]
            mask_nm = out["player_name"].fillna("").astype(str).str.strip().eq("")
            out.loc[mask_nm, "player_name"] = pd.Series(names, index=out.index)[mask_nm]
            out["age"] = out["age_parquet"]
            for i, pid in enumerate(pids):
                if pd.isna(out["age"].iloc[i]):
                    ag = ages_api[i]
                    if ag != "" and ag is not None:
                        try:
                            out.iat[i, out.columns.get_loc("age")] = int(ag)
                        except (TypeError, ValueError):
                            pass
            out["team"] = [
                teams_api[i] or str(out["team_from_games"].iloc[i] or "").strip()
                for i in range(len(out))
            ]
            return out

        bdf = _enrich_with_api(bdf) if not bdf.empty else bdf
        pdf = _enrich_with_api(pdf) if not pdf.empty else pdf
    else:
        if not bdf.empty:
            bdf["age"] = bdf["age_parquet"]
            bdf["team"] = bdf["team_from_games"]
        if not pdf.empty:
            pdf["age"] = pdf["age_parquet"]
            pdf["team"] = pdf["team_from_games"]

    # Column order / cleanup
    drop_cols = ["age_parquet", "team_from_games"]
    for c in drop_cols:
        if c in bdf.columns:
            bdf = bdf.drop(columns=[c])
        if c in pdf.columns:
            pdf = pdf.drop(columns=[c])

    def _reorder(df: pd.DataFrame, first: list[str]) -> pd.DataFrame:
        if df.empty:
            return df
        rest = [c for c in df.columns if c not in first]
        cols = [c for c in first if c in df.columns] + rest
        return df[cols]

    bdf = _reorder(
        bdf,
        [
            "mlbam_id",
            "player_name",
            "team",
            "age",
            "games",
            "PA",
            "AB",
            "H",
            "1B",
            "2B",
            "3B",
            "HR",
            "R",
            "RBI",
            "SB",
        ],
    )
    pdf = _reorder(
        pdf,
        [
            "mlbam_id",
            "player_name",
            "team",
            "age",
            "games",
            "BF",
            "IP",
            "IP_est",
            "H",
            "ER",
            "RA",
            "ERA",
            "BB",
            "SO",
            "HR",
            "HBP",
        ],
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f"{season}_{stage.replace('/', '_')}"
    bat_path = out_dir / f"st_{tag}_batting.csv"
    pit_path = out_dir / f"st_{tag}_pitching.csv"
    sort_b = "PA" if not bdf.empty and "PA" in bdf.columns else None
    sort_p = "BF" if not pdf.empty and "BF" in pdf.columns else None
    if sort_b:
        bdf = bdf.sort_values(sort_b, ascending=False)
    if sort_p:
        pdf = pdf.sort_values(sort_p, ascending=False)
    bdf.to_csv(bat_path, index=False, float_format="%.4f")
    pdf.to_csv(pit_path, index=False, float_format="%.4f")
    return bat_path, pit_path, len(feed_ages)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--warehouse", type=Path, default=_REPO_ROOT / "data" / "warehouse" / "mlb")
    p.add_argument("--season", type=int, default=2026)
    p.add_argument("--stage", type=str, default="spring_training")
    p.add_argument("--out-dir", type=Path, default=_REPO_ROOT / "outputs" / "st_exports")
    p.add_argument("--no-api", action="store_true", help="Skip MLB Stats API (names/age/team partial)")
    p.add_argument(
        "--no-feed",
        action="store_true",
        help="Do not scan raw feed_live JSON for gameData.players currentAge",
    )
    p.add_argument(
        "--no-boxscore",
        action="store_true",
        help="Do not merge R/RBI/SB/ER/RA/IP from liveData.boxscore in raw feeds",
    )
    args = p.parse_args()

    b, pit, n_feed = run_export(
        args.warehouse,
        args.season,
        args.stage,
        args.out_dir,
        use_api=not args.no_api,
        use_feed_ages=not args.no_feed,
        use_boxscore=not args.no_boxscore,
    )
    extra = f"  (feed player ages merged: {n_feed} IDs)\n" if n_feed else ""
    print(f"{extra}Wrote:\n  {b}\n  {pit}")


if __name__ == "__main__":
    main()
