"""
Morning intel — Mallitalytics MLB Content Command Center (v1).

All intel automation lives under ./morning_intel/ (this folder).
Replaces legacy morning_digest — do not run both.

Usage:
    python morning_intel/morning_intel.py
    python morning_intel/morning_intel.py --dry-run --skip-notify
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import smtplib
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

_INTEL_DIR = Path(__file__).resolve().parent
REPO_ROOT = _INTEL_DIR.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "mlbops"))
load_dotenv(_INTEL_DIR / ".env")
load_dotenv(REPO_ROOT / "jobs" / ".env")
load_dotenv(REPO_ROOT / "mlbops" / ".env")

from api.db.database import insert_queue_item, log_notification  # noqa: E402
from src import batter_recent as _br  # noqa: E402
from src import pitcher_recent as _pr  # noqa: E402

WAREHOUSE_ROOT = REPO_ROOT / "data" / "warehouse" / "mlb"
OUTPUTS_ROOT = REPO_ROOT / "outputs"
INTEL_OUT = _INTEL_DIR / "snapshots"
STATS_BASE = "https://statsapi.mlb.com/api/v1"
MLB_NEWS_RSS = "https://www.mlb.com/feeds/news/rss.xml"
SPORT_ID = 1
PARQUET_NAME_RE = re.compile(r"game_(\d+)_(\d{8})_pitches_enriched\.parquet$", re.I)
READ_COLS = [
    "game_pk", "game_date", "pitcher", "batter", "player_name", "pitch_type",
    "release_speed", "description", "zone", "type", "launch_speed",
    "launch_speed_angle", "estimated_woba_using_speedangle",
    "at_bat_number",
]
SWING_CODES = [
    "foul_bunt", "foul", "hit_into_play", "swinging_strike", "foul_tip",
    "swinging_strike_blocked", "missed_bunt", "bunt_foul_tip",
]
WHIFF_CODES = ["swinging_strike", "foul_tip", "swinging_strike_blocked"]
BASELINE_MAX_CAL_DAYS = 120
RECENT_STARTS = 3
BASELINE_STARTS = 10
RECENT_BBE = 10
BASELINE_BBE = 40
RECENT_PA_PITCH_ROWS = 25
BASELINE_PA_PITCH_ROWS = 75
MIN_PITCHES_WINDOW = 40
MIN_PITCHES_BASELINE = 80
MIN_BIP_WINDOW = 12
MIN_BIP_BASELINE = 25
# (stat_group, api_field, targets, short_label, unit_phrase)
# Hitting: HR, XBH components, batter Ks, career-ish hit totals.
# Pitching: pitcher Ks, saves, complete games (not HR/hits allowed — those are defensive noise).
# Season splits from Stats API — pitching K targets must be season-sized (1500+ never hits in April).
MILESTONE_RULES: list[tuple[str, str, list[int], str, str]] = [
    ("hitting", "homeRuns", [10, 20, 30, 40, 50, 100, 200, 300, 400, 500, 600, 700], "HR", "home runs"),
    ("hitting", "doubles", [5, 10, 15, 20, 25, 30, 40, 50], "2B", "doubles"),
    ("hitting", "triples", [2, 3, 5, 7, 10, 15], "3B", "triples"),
    ("hitting", "strikeOuts", [50, 75, 100, 125, 150, 175, 200], "bK", "batter strikeouts"),
    ("hitting", "hits", [1000, 1500, 2000, 2500, 3000, 3500], "H", "hits"),
    ("pitching", "strikeOuts", [25, 50, 75, 100, 125, 150, 175, 200, 250, 300], "K", "strikeouts"),
    ("pitching", "saves", [5, 10, 15, 20, 25, 30, 40, 50], "SV", "saves"),
    ("pitching", "completeGames", [1, 2, 3, 4, 5], "CG", "complete games"),
]

# Plain-language tail for milestone strings (avoid "10 from 10" reading as a ratio)
_MILESTONE_UNIT = {
    "HR": "home runs",
    "H": "hits",
    "K": "strikeouts",
    "2B": "doubles",
    "3B": "triples",
    "bK": "batter strikeouts",
    "SV": "saves",
    "CG": "complete games",
}


def _fmt_season_milestone_line(name: str, cur: int, lab: str, tgt: int) -> str:
    need = tgt - cur
    unit = _MILESTONE_UNIT.get(lab, lab)
    return f"{name}: {cur} {lab} · {need} away from {tgt} {unit}"


def _season_milestone_eligible(cur: int, lab: str, tgt: int) -> bool:
    """Within 10 of next mark; skip vacuous zeros for stats that spam early-season lists."""
    if not (cur < tgt <= cur + 10):
        return False
    if lab == "HR" and cur == 0:
        return False
    if lab == "CG" and cur == 0:
        return False
    if lab == "SV" and cur == 0:
        return False
    return True


def _float_stat(val, default: float = 0.0) -> float:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def should_skip_hitting_milestones(groups: dict, primary_pos: Optional[str]) -> bool:
    """Hide hitting chase rows for pitchers (PH/NL HR noise). Not a duplicate of leaderboards.

    1) MLB primary position P/SP/RP + PA under 80 → skip (covers early season before IP accrues).
    2) Any season pitching workload + low PA → skip (backup if position missing).
    3) ≥80 PA → never skip (two-way / real hitter sample).
    """
    hit = groups.get("hitting") or {}
    pit = groups.get("pitching") or {}
    ab = int(_float_stat(hit.get("atBats")))
    pa = int(_float_stat(hit.get("plateAppearances")))
    plate = max(ab, pa)
    if plate >= 80:
        return False
    pos = (primary_pos or "").strip().upper()
    if pos in ("P", "SP", "RP"):
        return True
    ip = _float_stat(pit.get("inningsPitched"))
    games_p = int(_float_stat(pit.get("gamesPlayed")))
    return (ip >= 0.1 or games_p >= 1)


def diversify_milestones_by_stat(
    mlines: list[str], mdetail: list[dict], cap: int = 45
) -> tuple[list[str], list[dict]]:
    """Round-robin by stat label so HR does not crowd out SV/K/2B/etc."""
    if not mlines or not mdetail or len(mlines) != len(mdetail):
        return mlines[:cap], mdetail[:cap]
    by_stat: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for line, det in zip(mlines, mdetail):
        by_stat[det["stat"]].append((line, det))
    for sk in by_stat:
        by_stat[sk].sort(key=lambda z: z[1]["need"])
    priority = ["SV", "CG", "K", "3B", "2B", "bK", "HR", "H"]
    stat_keys = [s for s in priority if s in by_stat]
    stat_keys.extend(s for s in sorted(by_stat.keys()) if s not in stat_keys)
    out: list[tuple[str, dict]] = []
    idx = 0
    while len(out) < cap:
        progressed = False
        for sk in stat_keys:
            bucket = by_stat[sk]
            if idx < len(bucket):
                out.append(bucket[idx])
                progressed = True
                if len(out) >= cap:
                    break
        if not progressed:
            break
        idx += 1
    if not out:
        return [], []
    a, b = zip(*out)
    return list(a), list(b)


@dataclass
class IntelReport:
    anchor_date: str
    season: int
    news_stories: list[dict] = field(default_factory=list)
    yesterday_results: list[str] = field(default_factory=list)
    transactions: list[str] = field(default_factory=list)
    transactions_detail: list[dict] = field(default_factory=list)
    probables_today: list[str] = field(default_factory=list)
    probables_tomorrow: list[str] = field(default_factory=list)
    milestones: list[str] = field(default_factory=list)
    milestones_detail: list[dict] = field(default_factory=list)
    anomalies_pitchers: list[dict] = field(default_factory=list)
    anomalies_batters: list[dict] = field(default_factory=list)
    tweet_drafts: list[str] = field(default_factory=list)
    queue_ids: list[int] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _parse_file_date(path: Path):
    m = PARQUET_NAME_RE.search(path.name)
    if not m:
        return None
    d = m.group(2)
    try:
        return date(int(d[:4]), int(d[4:6]), int(d[6:8]))
    except ValueError:
        return None


def _python_exe() -> Path:
    v = REPO_ROOT / "mlb_env" / "bin" / "python"
    return v if v.exists() else Path(sys.executable)


def _extract_saved_png(stdout: str):
    for line in stdout.splitlines():
        m = re.search(r"(?:→|->)\s*Saved:\s*(.+\.png)", line)
        if m:
            p = Path(m.group(1).strip())
            if p.is_absolute():
                return p
            c = (REPO_ROOT / p).resolve()
            return c if c.exists() else p.resolve()
    return None


def _image_public_url(abs_path: Path) -> str:
    base = os.getenv("PUBLIC_STATIC_BASE_URL", "http://localhost:8000/static").rstrip("/")
    try:
        rel = abs_path.resolve().relative_to(OUTPUTS_ROOT.resolve())
        return f"{base}/{rel.as_posix()}"
    except ValueError:
        return base


def _norm_game_date_series(s: pd.Series) -> pd.Series:
    out = []
    for v in s:
        if pd.isna(v):
            out.append(None)
        elif isinstance(v, date) and not isinstance(v, datetime):
            out.append(v)
        elif isinstance(v, datetime):
            out.append(v.date())
        else:
            try:
                out.append(pd.to_datetime(v).date())
            except Exception:
                out.append(None)
    return pd.Series(out, index=s.index)


def iter_enriched_parquets(season: int, stage: str = "regular_season"):
    base = WAREHOUSE_ROOT / str(season) / stage
    if not base.exists():
        return []
    out = []
    for path in sorted(base.rglob("game_*_pitches_enriched.parquet")):
        fd = _parse_file_date(path)
        if fd is not None:
            out.append((path, fd))
    return out


def load_parquet_paths(paths: list[Path]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_parquet(p, columns=READ_COLS)
        except Exception:
            try:
                df = pd.read_parquet(p)
            except Exception:
                continue
        for c in READ_COLS:
            if c not in df.columns:
                df[c] = np.nan
        df = df[[c for c in READ_COLS]]
        df["_source_file"] = str(p)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=READ_COLS + ["_source_file"])
    return pd.concat(frames, ignore_index=True)


def enrich_pitch_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["gd"] = _norm_game_date_series(df["game_date"])
    df["pitch_type"] = df["pitch_type"].fillna("UN").astype(str)
    df["release_speed"] = pd.to_numeric(df["release_speed"], errors="coerce")
    df["description"] = df["description"].fillna("").astype(str)
    df["zone"] = pd.to_numeric(df["zone"], errors="coerce")
    df["type"] = df["type"].fillna("").astype(str)
    df["launch_speed"] = pd.to_numeric(df["launch_speed"], errors="coerce")
    df["launch_speed_angle"] = pd.to_numeric(df["launch_speed_angle"], errors="coerce")
    df["estimated_woba_using_speedangle"] = pd.to_numeric(
        df["estimated_woba_using_speedangle"], errors="coerce"
    )
    if "at_bat_number" in df.columns:
        df["at_bat_number"] = pd.to_numeric(df["at_bat_number"], errors="coerce")
    df["swing"] = df["description"].isin(SWING_CODES)
    df["whiff"] = df["description"].isin(WHIFF_CODES)
    df["in_zone"] = df["zone"].lt(10)
    df["chase"] = (~df["in_zone"]) & df["swing"]
    df["bip"] = df["type"] == "X"
    return df


def paths_in_calendar_range(indexed, d_lo, d_hi):
    return [p for p, fd in indexed if d_lo <= fd <= d_hi]


def _pitcher_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["pitcher"].notna()].copy()
    if df.empty:
        return pd.DataFrame()
    pit = df["pitcher"].astype(int)
    g = df.assign(_p=pit).groupby("_p", as_index=True)
    velo = g["release_speed"].mean()
    n = g.size()
    whiff = g["whiff"].mean()
    chase = g["chase"].mean()
    bip = df[df["bip"]]
    if not bip.empty:
        bx = bip.assign(_p=bip["pitcher"].astype(int)).groupby("_p")
        xwoba_bip = bx["estimated_woba_using_speedangle"].mean()
        ev_bip = bx["launch_speed"].mean()
        n_bip = bx.size()
    else:
        xwoba_bip = pd.Series(dtype=float)
        ev_bip = pd.Series(dtype=float)
        n_bip = pd.Series(dtype=int)
    out = pd.DataFrame({"n": n, "avg_velo": velo, "whiff": whiff, "chase": chase})
    out["xwoba_bip"] = xwoba_bip
    out["ev_bip"] = ev_bip
    out["n_bip"] = n_bip
    out.index.name = "pitcher"
    return out.reset_index()


def _batter_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["batter"].notna() & df["bip"]].copy()
    if df.empty:
        return pd.DataFrame()
    b = df["batter"].astype(int)
    g = df.assign(_b=b).groupby("_b", as_index=True)
    n_bip = g.size()
    ev = g["launch_speed"].mean()
    xw = g["estimated_woba_using_speedangle"].mean()
    lsa = df["launch_speed_angle"]
    barrel = (lsa == 6).groupby(df["batter"].astype(int)).mean()
    out = pd.DataFrame({
        "n_bip": n_bip, "avg_ev": ev, "xwoba_bip": xw, "barrel_pct": barrel * 100,
    })
    out.index.name = "batter"
    return out.reset_index()


def _dominant_mix_shift(window_df, base_df, pid):
    w = window_df[_pid_match_series(window_df["pitcher"], pid)]
    b = base_df[_pid_match_series(base_df["pitcher"], pid)]
    if len(w) < MIN_PITCHES_WINDOW or len(b) < MIN_PITCHES_BASELINE:
        return None
    wc = w["pitch_type"].value_counts(normalize=True)
    bc = b["pitch_type"].value_counts(normalize=True)
    keys = set(wc.index) | set(bc.index)
    best = None
    for k in keys:
        pv = float(wc.get(k, 0.0))
        pb = float(bc.get(k, 0.0))
        if pb < 0.05 and pv < 0.05:
            continue
        delta = pv - pb
        if best is None or abs(delta) > abs(best[2]):
            best = (k, pb, pv)
    if best is None:
        return None
    k, pb, pv = best
    if abs(pv - pb) < 0.08:
        return None
    return (k, pb * 100, pv * 100)


def _anomaly_pitcher_label() -> str:
    return f"last {RECENT_STARTS} vs prior {BASELINE_STARTS} starts"


def detect_pitcher_anomalies(full_df, anchor):
    """Last ``RECENT_STARTS`` starts vs prior ``BASELINE_STARTS`` starts (pitch pools)."""
    pit_df = full_df[full_df["gd"] <= anchor].copy()
    games = _pr.pitcher_game_table(pit_df)
    if games.empty:
        return []
    wlab = _anomaly_pitcher_label()
    out: list[dict] = []
    for pid, grp in games.groupby("pitcher"):
        grp = grp.sort_values("gd", ascending=False)
        if len(grp) < RECENT_STARTS + BASELINE_STARTS:
            continue
        pid = int(pid)
        rg = {int(x) for x in grp.iloc[:RECENT_STARTS]["game_pk"]}
        bg = {int(x) for x in grp.iloc[RECENT_STARTS : RECENT_STARTS + BASELINE_STARTS]["game_pk"]}
        wpool = _pr.pool_for_games(pit_df, pid, rg)
        bpool = _pr.pool_for_games(pit_df, pid, bg)
        if len(wpool) < 50 or len(bpool) < 100:
            continue
        w_vel = float(wpool["release_speed"].mean())
        b_vel = float(bpool["release_speed"].mean())
        velo_d = w_vel - b_vel
        if abs(velo_d) >= 1.2:
            out.append({
                "player_id": pid,
                "role": "pitcher",
                "window_days": None,
                "window_kind": "starts",
                "window_starts": RECENT_STARTS,
                "baseline_starts": BASELINE_STARTS,
                "window_label": wlab,
                "metric": "avg_velo_mph",
                "window": round(w_vel, 2),
                "baseline": round(b_vel, 2),
                "delta": round(velo_d, 2),
                "n_window": len(wpool),
                "n_baseline": len(bpool),
                "counts": {"pitches_recent": len(wpool), "pitches_baseline": len(bpool)},
            })
        w_wh = float(wpool["whiff"].mean())
        b_wh = float(bpool["whiff"].mean())
        d_wh = (w_wh - b_wh) * 100.0
        if abs(d_wh) >= 5.0:
            out.append({
                "player_id": pid,
                "role": "pitcher",
                "window_days": None,
                "window_kind": "starts",
                "window_starts": RECENT_STARTS,
                "baseline_starts": BASELINE_STARTS,
                "window_label": wlab,
                "metric": "whiff_pct",
                "window": round(w_wh * 100.0, 1),
                "baseline": round(b_wh * 100.0, 1),
                "delta": round(d_wh, 1),
                "n_window": len(wpool),
                "n_baseline": len(bpool),
                "counts": {"pitches_recent": len(wpool), "pitches_baseline": len(bpool)},
            })
        w_ch = float(wpool["chase"].mean())
        b_ch = float(bpool["chase"].mean())
        d_ch = (w_ch - b_ch) * 100.0
        if abs(d_ch) >= 5.0:
            out.append({
                "player_id": pid,
                "role": "pitcher",
                "window_days": None,
                "window_kind": "starts",
                "window_starts": RECENT_STARTS,
                "baseline_starts": BASELINE_STARTS,
                "window_label": wlab,
                "metric": "chase_pct",
                "window": round(w_ch * 100.0, 1),
                "baseline": round(b_ch * 100.0, 1),
                "delta": round(d_ch, 1),
                "n_window": len(wpool),
                "n_baseline": len(bpool),
                "counts": {"pitches_recent": len(wpool), "pitches_baseline": len(bpool)},
            })
        wb = wpool[wpool["bip"]]
        bb = bpool[bpool["bip"]]
        if len(wb) >= 6 and len(bb) >= 15:
            xw_w = float(wb["estimated_woba_using_speedangle"].mean())
            xw_b = float(bb["estimated_woba_using_speedangle"].mean())
            d_xw = xw_w - xw_b
            if abs(d_xw) >= 0.04:
                out.append({
                    "player_id": pid,
                    "role": "pitcher",
                    "window_days": None,
                    "window_kind": "starts",
                    "window_starts": RECENT_STARTS,
                    "baseline_starts": BASELINE_STARTS,
                    "window_label": wlab,
                    "metric": "xwoba_on_BIP",
                    "window": round(xw_w, 3),
                    "baseline": round(xw_b, 3),
                    "delta": round(d_xw, 3),
                    "n_window": len(wb),
                    "n_baseline": len(bb),
                    "counts": {"bip_recent": len(wb), "bip_baseline": len(bb)},
                })
        mix = _dominant_mix_shift(wpool, bpool, pid)
        if mix:
            name, pb, pv = mix
            out.append({
                "player_id": pid,
                "role": "pitcher",
                "window_days": None,
                "window_kind": "starts",
                "window_starts": RECENT_STARTS,
                "baseline_starts": BASELINE_STARTS,
                "window_label": wlab,
                "metric": f"mix_{name}_pct",
                "window": round(pv, 1),
                "baseline": round(pb, 1),
                "delta": round(pv - pb, 1),
                "n_window": len(wpool),
                "n_baseline": len(bpool),
                "counts": {"pitches_recent": len(wpool), "pitches_baseline": len(bpool)},
            })
    return out


def detect_batter_anomalies(full_df, anchor):
    """Last ``RECENT_BBE`` BBE vs prior ``BASELINE_BBE``, else PA pitch-row windows."""
    pit_df = full_df[full_df["gd"] <= anchor].copy()
    out: list[dict] = []
    seen: set[int] = set()
    bip_df = pit_df[pit_df["bip"]]
    if not bip_df.empty:
        for bid, _ in bip_df.groupby("batter"):
            bid = int(bid)
            if bid in seen:
                continue
            ordered = _br.ordered_bip_rows(pit_df, bid, anchor)
            if len(ordered) < RECENT_BBE + BASELINE_BBE:
                continue
            seen.add(bid)
            wr, br = _br.bip_windows(ordered, RECENT_BBE, BASELINE_BBE)
            if wr.empty or br.empty:
                continue
            sw = _br.summarize_bip_pool(wr)
            sb = _br.summarize_bip_pool(br)
            wlab = f"last {RECENT_BBE} vs prior {BASELINE_BBE} BBE"
            ev_w = sw.get("avg_ev")
            ev_b = sb.get("avg_ev")
            if ev_w is not None and ev_b is not None:
                ev_d = float(ev_w) - float(ev_b)
                if abs(ev_d) >= 2.0:
                    out.append({
                        "player_id": bid,
                        "role": "batter",
                        "window_days": None,
                        "window_kind": "bbe",
                        "window_n": RECENT_BBE,
                        "baseline_n": BASELINE_BBE,
                        "window_label": wlab,
                        "metric": "avg_EV_mph",
                        "window": round(float(ev_w), 2),
                        "baseline": round(float(ev_b), 2),
                        "delta": round(ev_d, 2),
                        "n_window": sw["n"],
                        "n_baseline": sb["n"],
                        "counts": {
                            "barrels_recent": sw.get("barrels", 0),
                            "barrels_baseline": sb.get("barrels", 0),
                        },
                    })
            br_w = float(sw.get("barrel_pct") or 0.0)
            br_b = float(sb.get("barrel_pct") or 0.0)
            br_d = br_w - br_b
            if abs(br_d) >= 4.0:
                out.append({
                    "player_id": bid,
                    "role": "batter",
                    "window_days": None,
                    "window_kind": "bbe",
                    "window_n": RECENT_BBE,
                    "baseline_n": BASELINE_BBE,
                    "window_label": wlab,
                    "metric": "barrel_pct",
                    "window": round(br_w, 1),
                    "baseline": round(br_b, 1),
                    "delta": round(br_d, 1),
                    "n_window": sw["n"],
                    "n_baseline": sb["n"],
                    "counts": {
                        "barrels_recent": sw.get("barrels", 0),
                        "barrels_baseline": sb.get("barrels", 0),
                    },
                })
            xw_w = sw.get("xwoba")
            xw_b = sb.get("xwoba")
            if xw_w is not None and xw_b is not None:
                d_xw = float(xw_w) - float(xw_b)
                if abs(d_xw) >= 0.04:
                    out.append({
                        "player_id": bid,
                        "role": "batter",
                        "window_days": None,
                        "window_kind": "bbe",
                        "window_n": RECENT_BBE,
                        "baseline_n": BASELINE_BBE,
                        "window_label": wlab,
                        "metric": "xwoba_on_BIP",
                        "window": round(float(xw_w), 3),
                        "baseline": round(float(xw_b), 3),
                        "delta": round(d_xw, 3),
                        "n_window": sw["n"],
                        "n_baseline": sb["n"],
                        "counts": {
                            "barrels_recent": sw.get("barrels", 0),
                            "barrels_baseline": sb.get("barrels", 0),
                        },
                    })

    # PA-based fallback for batters without enough BBE
    for bid, _ in pit_df.groupby("batter"):
        bid = int(bid)
        if bid in seen:
            continue
        wr, br = _br.pa_windows_from_pitches(
            pit_df,
            bid,
            anchor,
            n_recent=RECENT_PA_PITCH_ROWS,
            n_base=BASELINE_PA_PITCH_ROWS,
        )
        if wr.empty or br.empty:
            continue
        sw = _br.summarize_pa_pool(wr)
        sb = _br.summarize_pa_pool(br)
        w_wh = sw.get("whiff_per_pitch")
        b_wh = sb.get("whiff_per_pitch")
        if w_wh is not None and b_wh is not None:
            d = float(w_wh) - float(b_wh)
            if abs(d) >= 5.0:
                out.append({
                    "player_id": bid,
                    "role": "batter",
                    "window_days": None,
                    "window_kind": "pa",
                    "window_n": RECENT_PA_PITCH_ROWS,
                    "baseline_n": BASELINE_PA_PITCH_ROWS,
                    "window_label": (
                        f"last {RECENT_PA_PITCH_ROWS} vs prior {BASELINE_PA_PITCH_ROWS} "
                        "PA (pitch rows)"
                    ),
                    "metric": "whiff_pct",
                    "window": round(float(w_wh), 1),
                    "baseline": round(float(b_wh), 1),
                    "delta": round(d, 1),
                    "n_window": sw["n_pa_pitch_rows"],
                    "n_baseline": sb["n_pa_pitch_rows"],
                    "counts": {},
                })
    return out


def rank_unique_anomalies(items, top=25):
    best = {}
    for it in items:
        key = (it["player_id"], it["role"], it["metric"])
        cur = best.get(key)
        if cur is None or abs(float(it["delta"])) > abs(float(cur["delta"])):
            best[key] = it
    ranked = sorted(best.values(), key=lambda x: abs(float(x["delta"])), reverse=True)
    return ranked[:top]


def _pid_match_series(s: pd.Series, pid: int) -> pd.Series:
    """Parquet may store batter/pitcher as int, float, or string; normalize for joins."""
    num = pd.to_numeric(s, errors="coerce")
    return num == float(pid)


def _parse_ip_to_outs(ip_raw) -> int:
    """Innings pitched string '12.1' -> outs (37)."""
    if ip_raw is None or ip_raw == "":
        return 0
    try:
        if isinstance(ip_raw, (int, float)) and not isinstance(ip_raw, bool):
            f = float(ip_raw)
            whole = int(f)
            frac = int(round((f - whole) * 10))
            frac = max(0, min(2, frac))
            return whole * 3 + frac
        parts = str(ip_raw).strip().split(".")
        whole = int(parts[0]) if parts[0] else 0
        rem = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        rem = max(0, min(2, rem))
        return whole * 3 + rem
    except Exception:
        return 0


def _fmt_ip_from_outs(outs: int) -> str:
    if outs <= 0:
        return "0 IP"
    w, r = divmod(outs, 3)
    if r == 0:
        return f"{w} IP"
    return f"{w}.{r} IP"


def _extract_ops(hit: dict | None) -> float | None:
    if not hit:
        return None
    for key in ("ops", "onBasePlusSlugging"):
        v = hit.get(key)
        if v is not None and v != "":
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    obp = _float_stat(hit.get("obp"))
    slg = _float_stat(hit.get("slg"))
    if obp or slg:
        return obp + slg
    return None


def _extract_era(pit: dict | None) -> float | None:
    if not pit:
        return None
    v = pit.get("era")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt_rate333(v: float | None) -> str | None:
    if v is None:
        return None
    s = f"{v:.3f}"
    return s[1:] if s.startswith("0.") else s


def _fmt_era_val(v: float | None) -> str | None:
    if v is None:
        return None
    return f"{v:.2f}"


def _season_compare_rows_for_player(
    pos: str, g_curr: dict, g_prev: dict,
) -> list[dict]:
    """OPS (batting) and ERA (pitching) vs prior season from Stats API splits."""
    compares: list[dict] = []
    hit_c = g_curr.get("hitting") or {}
    hit_p = g_prev.get("hitting") or {}
    pit_c = g_curr.get("pitching") or {}
    pit_p = g_prev.get("pitching") or {}

    if "batter" in pos or pos == "two-way":
        pa_c = int(_float_stat(hit_c.get("plateAppearances")))
        pa_p = int(_float_stat(hit_p.get("plateAppearances")))
        if pa_c > 0 or pa_p > 0:
            ops_c = _extract_ops(hit_c) if pa_c > 0 else None
            ops_p = _extract_ops(hit_p) if pa_p > 0 else None
            compares.append({
                "role": "batter",
                "stat": "OPS",
                "this_value": _fmt_rate333(ops_c),
                "last_value": _fmt_rate333(ops_p),
                "this_n": pa_c,
                "n_label": "PA",
                "this_volume": f"{pa_c} PA" if pa_c else None,
                "last_volume": f"{pa_p} PA" if pa_p else None,
            })

    if "pitcher" in pos or pos == "two-way":
        outs_c = _parse_ip_to_outs(pit_c.get("inningsPitched"))
        outs_p = _parse_ip_to_outs(pit_p.get("inningsPitched"))
        if outs_c > 0 or outs_p > 0:
            era_c = _extract_era(pit_c) if outs_c > 0 else None
            era_p = _extract_era(pit_p) if outs_p > 0 else None
            compares.append({
                "role": "pitcher",
                "stat": "ERA",
                "this_value": _fmt_era_val(era_c),
                "last_value": _fmt_era_val(era_p),
                "this_n": outs_c,
                "n_label": "IP",
                "this_volume": _fmt_ip_from_outs(outs_c),
                "last_volume": _fmt_ip_from_outs(outs_p),
            })
    return compares


def _append_season_compare_to_parts(parts: list[str], compares: list[dict], prev_season: int) -> None:
    if not compares:
        return
    bits = []
    for c in compares:
        stat = c["stat"]
        a = c.get("this_value") or "—"
        b = c.get("last_value") or "—"
        bits.append(f"{stat} {a} vs {b} ({prev_season})")
    parts.append(" · ".join(bits))


def build_watchlist_pulse(
    full_df, anchor, wl, st_curr: dict, st_prev: dict, season: int,
) -> tuple[list[str], list[dict]]:
    """Statcast start / BBE / PA pools + season vs last-season rate compares."""
    prev_season = season - 1
    lines: list[str] = []
    detail: list[dict] = []
    pit_df = pd.DataFrame()
    if full_df is not None and not full_df.empty:
        pit_df = full_df[full_df["gd"] <= anchor].copy()

    p_lab = _anomaly_pitcher_label()
    b_bbe_lab = f"last {RECENT_BBE} vs prior {BASELINE_BBE} BBE"
    pa_lab = (
        f"last {RECENT_PA_PITCH_ROWS} vs prior {BASELINE_PA_PITCH_ROWS} PA (pitch rows)"
    )

    def _pitcher_window(pid: int) -> tuple[Optional[dict], list[str], list[str]]:
        meta: Optional[dict] = None
        bits: list[str] = []
        rlines: list[str] = []
        if pit_df.empty:
            rlines.append("No Statcast warehouse rows through anchor.")
            return meta, bits, rlines
        games = _pr.pitcher_game_table(pit_df)
        gsub = games[games["pitcher"] == int(pid)].sort_values("gd", ascending=False)
        need_g = RECENT_STARTS + BASELINE_STARTS
        if len(gsub) < need_g:
            if len(gsub) == 0:
                rlines.append("No pitcher outings with 12+ pitches in warehouse.")
            else:
                rlines.append(
                    f"Only {len(gsub)} start-sized outing(s); need {need_g} for windowed pulse.",
                )
            return meta, bits, rlines
        rg = {int(x) for x in gsub.iloc[:RECENT_STARTS]["game_pk"]}
        bg = {int(x) for x in gsub.iloc[RECENT_STARTS:need_g]["game_pk"]}
        wpool = _pr.pool_for_games(pit_df, int(pid), rg)
        bpool = _pr.pool_for_games(pit_df, int(pid), bg)
        if len(wpool) < 50 or len(bpool) < 100:
            rlines.append(
                f"Thin pitch pools for starts ({len(wpool)} recent / {len(bpool)} baseline pitches).",
            )
            return meta, bits, rlines
        w_vel = float(wpool["release_speed"].mean())
        b_vel = float(bpool["release_speed"].mean())
        d_vel = w_vel - b_vel
        w_wh = float(wpool["whiff"].mean()) * 100.0
        b_wh = float(bpool["whiff"].mean()) * 100.0
        d_wh = w_wh - b_wh
        bits.append(
            f"P {p_lab}: velo {w_vel:.1f} vs {b_vel:.1f} mph (Δ{d_vel:+.1f}); "
            f"whiff {w_wh:.1f}% vs {b_wh:.1f}% (Δ{d_wh:+.1f})",
        )
        rlines.append(
            f"Pitching — {p_lab}: {len(wpool)} vs {len(bpool)} pitches; "
            f"velo Δ{d_vel:+.1f} mph; whiff Δ{d_wh:+.1f} pts",
        )
        wb = wpool[wpool["bip"]]
        bb = bpool[bpool["bip"]]
        if len(wb) >= 6 and len(bb) >= 15:
            xw_w = float(wb["estimated_woba_using_speedangle"].mean())
            xw_b = float(bb["estimated_woba_using_speedangle"].mean())
            d_xw = xw_w - xw_b
            bits.append(f"BIP xwOBA {xw_w:.3f} vs {xw_b:.3f} (Δ{d_xw:+.3f})")
        meta = {
            "role": "pitcher",
            "window_kind": "starts",
            "window_label": p_lab,
            "counts": {
                "pitches_recent": len(wpool),
                "pitches_baseline": len(bpool),
            },
        }
        return meta, bits, rlines

    def _batter_window(pid: int) -> tuple[Optional[dict], list[str], list[str]]:
        meta: Optional[dict] = None
        bits: list[str] = []
        rlines: list[str] = []
        if pit_df.empty:
            rlines.append("No Statcast warehouse rows through anchor.")
            return meta, bits, rlines
        ordered = _br.ordered_bip_rows(pit_df, int(pid), anchor)
        n_need = RECENT_BBE + BASELINE_BBE
        if len(ordered) >= n_need:
            wr, br = _br.bip_windows(ordered, RECENT_BBE, BASELINE_BBE)
            if wr.empty or br.empty:
                return meta, bits, rlines
            sw = _br.summarize_bip_pool(wr)
            sb = _br.summarize_bip_pool(br)
            ev_w = sw.get("avg_ev")
            ev_b = sb.get("avg_ev")
            xw_w = sw.get("xwoba")
            xw_b = sb.get("xwoba")
            frag = [f"H {b_bbe_lab}"]
            if ev_w is not None and ev_b is not None:
                frag.append(
                    f"EV {ev_w} vs {ev_b} (Δ{float(ev_w) - float(ev_b):+.1f})",
                )
            if xw_w is not None and xw_b is not None:
                frag.append(
                    f"xwOBA {xw_w} vs {xw_b} (Δ{float(xw_w) - float(xw_b):+.3f})",
                )
            bits.append(" · ".join(frag))
            rlines.append(
                f"Hitting — {b_bbe_lab}: {sw['n']} vs {sb['n']} BBE",
            )
            meta = {
                "role": "batter",
                "window_kind": "bbe",
                "window_label": b_bbe_lab,
                "counts": {
                    "bbe_recent": sw.get("n", 0),
                    "bbe_baseline": sb.get("n", 0),
                    "barrels_recent": sw.get("barrels", 0),
                    "barrels_baseline": sb.get("barrels", 0),
                },
            }
            return meta, bits, rlines
        wr, br = _br.pa_windows_from_pitches(
            pit_df,
            int(pid),
            anchor,
            n_recent=RECENT_PA_PITCH_ROWS,
            n_base=BASELINE_PA_PITCH_ROWS,
        )
        if wr.empty or br.empty:
            rlines.append(
                "Not enough BBE or tracked PA windows for Statcast pulse.",
            )
            return meta, bits, rlines
        sw = _br.summarize_pa_pool(wr)
        sb = _br.summarize_pa_pool(br)
        w_wh = sw.get("whiff_per_pitch")
        b_wh = sb.get("whiff_per_pitch")
        if w_wh is not None and b_wh is not None:
            d_wh = float(w_wh) - float(b_wh)
            bits.append(
                f"H {pa_lab}: whiff/pitch {w_wh:.1f}% vs {b_wh:.1f}% (Δ{d_wh:+.1f})",
            )
            rlines.append(
                f"Hitting — {pa_lab}: {sw['n_pa_pitch_rows']} vs "
                f"{sb['n_pa_pitch_rows']} pitch rows; whiff Δ{d_wh:+.1f} pts",
            )
        else:
            bits.append(f"H {pa_lab}: pitch-row pool only")
            rlines.append(
                f"Hitting — {pa_lab}: {sw.get('n_pa_pitch_rows')} vs "
                f"{sb.get('n_pa_pitch_rows')} pitch rows",
            )
        meta = {
            "role": "batter",
            "window_kind": "pa",
            "window_label": pa_lab,
            "counts": {
                "pa_pitch_rows_recent": sw.get("n_pa_pitch_rows", 0),
                "pa_pitch_rows_baseline": sb.get("n_pa_pitch_rows", 0),
            },
        }
        return meta, bits, rlines

    for p in sorted(wl, key=lambda x: x.get("priority", 99)):
        pid = int(p["player_id"])
        name = p.get("player_name", str(pid))
        pos = (p.get("position") or "").lower()
        recent_lines: list[str] = []
        statcast_bits: list[str] = []
        pitcher_meta: Optional[dict] = None
        batter_meta: Optional[dict] = None

        if "pitcher" in pos or pos == "two-way":
            pitcher_meta, pb, pl = _pitcher_window(pid)
            statcast_bits.extend(pb)
            recent_lines.extend(pl)
        if "batter" in pos or pos == "two-way":
            batter_meta, bb, bl = _batter_window(pid)
            statcast_bits.extend(bb)
            recent_lines.extend(bl)

        if not statcast_bits:
            recent_lines.append(
                "No Statcast pulse row — verify MLB player_id or warehouse coverage.",
            )
            statcast_bits.append("no Statcast window — verify player_id")

        parts = [name]
        parts.extend(statcast_bits)
        g_curr = st_curr.get(pid) or {}
        g_prev = st_prev.get(pid) or {}
        compares = _season_compare_rows_for_player(pos, g_curr, g_prev)
        _append_season_compare_to_parts(parts, compares, prev_season)

        if len(parts) == 1:
            parts.append("no season line from API (position / id)")
            recent_lines.append("No MLB season hitting or pitching line returned for this id.")

        pulse_summary = " — ".join([name] + statcast_bits)
        lines.append(" — ".join(parts))
        detail.append({
            "player_id": pid,
            "player_name": name,
            "pulse_summary": pulse_summary,
            "pitcher_window": pitcher_meta,
            "batter_window": batter_meta,
            "recent_lines": recent_lines,
            "compares": compares,
            "season": season,
            "prev_season": prev_season,
        })
    return lines, detail


def hydrate_anomaly_names(items, names):
    for it in items:
        it["player_name"] = names.get(it["player_id"], str(it["player_id"]))


def parse_mlb_news_rss(xml_text: str, limit: int = 6) -> list[dict]:
    """Parse MLB's public RSS feed into a small, safe newsletter payload."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    stories: list[dict] = []
    seen: set[str] = set()
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        if not title or not url.startswith("https://www.mlb.com/") or url in seen:
            continue
        seen.add(url)
        published_raw = (item.findtext("pubDate") or "").strip()
        published = ""
        if published_raw:
            try:
                published = parsedate_to_datetime(published_raw).isoformat()
            except (TypeError, ValueError, OverflowError):
                published = published_raw
        author = (item.findtext("{http://purl.org/dc/elements/1.1/}creator") or "MLB.com").strip()
        image_node = item.find("image")
        image_url = (image_node.get("href") or "").strip() if image_node is not None else ""
        if image_url and not image_url.startswith("https://"):
            image_url = ""
        stories.append({
            "title": title,
            "url": url,
            "author": author or "MLB.com",
            "published_at": published,
            "image_url": image_url,
        })
        if len(stories) >= limit:
            break
    return stories


def api_mlb_news(limit: int = 6) -> list[dict]:
    try:
        response = requests.get(MLB_NEWS_RSS, timeout=20)
        response.raise_for_status()
        return parse_mlb_news_rss(response.text, limit=limit)
    except Exception:
        return []


def api_game_results(day: date) -> list[str]:
    """Return concise final scores for the previous day's MLB slate."""
    try:
        response = requests.get(
            f"{STATS_BASE}/schedule",
            params={"sportId": SPORT_ID, "date": day.isoformat()},
            timeout=20,
        )
        response.raise_for_status()
        lines: list[str] = []
        for date_block in response.json().get("dates") or []:
            for game in date_block.get("games") or []:
                if (game.get("status") or {}).get("abstractGameState") != "Final":
                    continue
                teams = game.get("teams") or {}
                away = teams.get("away") or {}
                home = teams.get("home") or {}
                away_name = (away.get("team") or {}).get("name", "Away")
                home_name = (home.get("team") or {}).get("name", "Home")
                lines.append(
                    f"{away_name} {away.get('score', 0)} · {home_name} {home.get('score', 0)}"
                )
        return lines
    except Exception:
        return []


def api_transactions(day):
    ds = day.isoformat()
    try:
        r = requests.get(
            f"{STATS_BASE}/transactions",
            params={"sportId": SPORT_ID, "startDate": ds, "endDate": ds},
            timeout=20,
        )
        r.raise_for_status()
        return r.json().get("transactions", []) or []
    except Exception as e:
        return [{"error": str(e)}]


# MLB franchise ids (for picking a big-league logo when toTeam/fromTeam mix MLB + MiLB).
_MLB_TEAM_IDS_FOR_TX_LOGO = frozenset({
    108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121,
    133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 158,
})


def transaction_mlb_team_id(t: dict) -> int | None:
    """Pick an MLB team id from a Stats API transaction (toTeam / fromTeam)."""
    if "error" in t:
        return None

    def _gid(key: str) -> int | None:
        o = t.get(key)
        if not o or not isinstance(o, dict):
            return None
        v = o.get("id")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    to_id = _gid("toTeam")
    fr_id = _gid("fromTeam")
    if to_id in _MLB_TEAM_IDS_FOR_TX_LOGO:
        return to_id
    if fr_id in _MLB_TEAM_IDS_FOR_TX_LOGO:
        return fr_id
    return None


def fmt_transaction(t, fallback_day: "date | None" = None):
    if "error" in t:
        return f"(transactions API: {t['error']})"
    desc = t.get("description") or ""
    name = (
        (t.get("person") or {}).get("fullName")
        or t.get("playerName") or t.get("name", "")
    )
    typ = t.get("typeDesc") or t.get("type") or ""
    # Date prefix — try transaction's own date field, then fall back to query day
    raw_date = t.get("date") or t.get("effectiveDate") or ""
    date_label = ""
    if raw_date:
        try:
            d = date.fromisoformat(str(raw_date)[:10])
            date_label = f"[{d.strftime('%m/%d')}] "
        except (ValueError, TypeError):
            pass
    if not date_label and fallback_day:
        date_label = f"[{fallback_day.strftime('%m/%d')}] "
    return f"{date_label}{name}: {typ} — {desc[:120]}"


def api_probable_lines(day):
    try:
        r = requests.get(
            f"{STATS_BASE}/schedule",
            params={
                "sportId": SPORT_ID, "date": day.isoformat(),
                "hydrate": "probablePitcher(note),team",
            },
            timeout=20,
        )
        r.raise_for_status()
        dates = r.json().get("dates") or []
        lines = []
        for d in dates:
            for g in d.get("games", []):
                away = (g.get("teams") or {}).get("away", {}) or {}
                home = (g.get("teams") or {}).get("home", {}) or {}
                at = (away.get("team") or {}).get("name", "Away")
                ht = (home.get("team") or {}).get("name", "Home")
                ap = (away.get("probablePitcher") or {}).get("fullName", "TBA")
                hp = (home.get("probablePitcher") or {}).get("fullName", "TBA")
                lines.append(f"{at} ({ap}) @ {ht} ({hp})")
        return lines
    except Exception as e:
        return [f"(schedule API error: {e})"]


def api_people_names(ids):
    if not ids:
        return {}
    meta = api_people_meta(ids)
    return {k: v["name"] for k, v in meta.items()}


def api_people_meta(ids):
    """Names + currentTeam id for hub logos (one Stats API call with hydrate)."""
    if not ids:
        return {}
    out: dict[int, dict] = {}
    chunk = 50
    for i in range(0, len(ids), chunk):
        part = ids[i : i + chunk]
        try:
            r = requests.get(
                f"{STATS_BASE}/people",
                params={
                    "personIds": ",".join(str(x) for x in part),
                    "hydrate": "currentTeam",
                },
                timeout=20,
            )
            r.raise_for_status()
            for p in r.json().get("people", []) or []:
                pid = int(p["id"])
                tid = (p.get("currentTeam") or {}).get("id")
                pp = (p.get("primaryPosition") or {}).get("abbreviation") or ""
                out[pid] = {
                    "name": p.get("fullName", str(pid)),
                    "team_id": int(tid) if tid is not None else None,
                    "primary_position": pp.strip(),
                }
        except Exception:
            for pid in part:
                out.setdefault(
                    int(pid),
                    {"name": str(pid), "team_id": None, "primary_position": ""},
                )
    return out


def api_season_hitting_pitching(person_ids, season):
    result = defaultdict(dict)
    if not person_ids:
        return {}
    chunk = 25
    for i in range(0, len(person_ids), chunk):
        part = person_ids[i : i + chunk]
        try:
            r = requests.get(
                f"{STATS_BASE}/people",
                params={
                    "personIds": ",".join(str(x) for x in part),
                    "hydrate": f"stats(group=[hitting,pitching],type=season,season={season})",
                },
                timeout=25,
            )
            r.raise_for_status()
            for person in r.json().get("people", []) or []:
                pid = int(person["id"])
                for st in person.get("stats", []) or []:
                    gname = (st.get("group") or {}).get("displayName", "")
                    splits = st.get("splits") or []
                    if not splits:
                        continue
                    stat = splits[0].get("stat") or {}
                    gl = gname.lower()
                    if gl == "hitting":
                        result[pid]["hitting"] = stat
                    elif gl == "pitching":
                        result[pid]["pitching"] = stat
        except Exception:
            continue
    return dict(result)


def build_findings_blob(report: IntelReport) -> str:
    blob = {
        "anchor_date": report.anchor_date,
        "news_headlines": [story.get("title") for story in report.news_stories[:6]],
        "yesterday_results": report.yesterday_results,
        "transactions": report.transactions[:20],
        "probables_today": report.probables_today[:12],
        "milestones": report.milestones[:20],
        "pitcher_anomalies": report.anomalies_pitchers[:15],
        "batter_anomalies": report.anomalies_batters[:15],
        "notes": report.notes,
    }
    return json.dumps(blob, indent=2, default=str)


def anthropic_api_key():
    return (os.getenv("ANTHROPIC_API_KEY") or os.getenv("anthropic_api_key") or "").strip()


def generate_tweet_drafts_claude(findings_summary: str, n: int = 5):
    key = anthropic_api_key()
    if not key:
        return ["(Set ANTHROPIC_API_KEY or anthropic_api_key for AI tweet drafts.)"]
    try:
        import anthropic
    except ImportError:
        return ["(Install anthropic package: pip install anthropic)"]
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5")
    client = anthropic.Anthropic(api_key=key)
    prompt = f"""You write for the X account @Mallitalytics. Voice: analytical, data-first, no fluff, no engagement bait. MLB stats and process, not hot takes.

Given the following morning intel summary, write exactly {n} distinct tweet drafts. Rules:
- Each tweet MUST be under 280 characters (hard limit).
- Plain text only, no markdown.
- Optional hashtags: at most one line may include #Mallitalytics or #MLB — not every tweet needs hashtags.
- Lead with the most interesting number or comparison when possible.
- Treat news headlines as link context only. Never invent article details that are not present in the summary.
- Prefer measured Mallitalytics data signals for statistical claims.

Intel summary:
{findings_summary}

Respond with a JSON array of {n} strings only, no other prose. Example: ["tweet1", "tweet2"]"""
    try:
        msg = client.messages.create(
            model=model, max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        print(f"  Claude drafts unavailable ({type(exc).__name__}); newsletter will continue.")
        return ["(AI tweet drafts unavailable for this edition.)"]
    text = ""
    for block in msg.content:
        if hasattr(block, "text"):
            text += block.text
    text = text.strip()
    m = re.search(r"\[[\s\S]*\]", text)
    if m:
        try:
            arr = json.loads(m.group(0))
            if isinstance(arr, list):
                out = [str(x).strip() for x in arr if str(x).strip()]
                return [t[:280] for t in out[:n]]
        except json.JSONDecodeError:
            pass
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return [ln[:280] for ln in lines[:n]] if lines else [text[:280]]


def run_pitcher_card(player_id: int, game_date: str):
    env = {**os.environ, "MPLBACKEND": "Agg"}
    cmd = [
        str(_python_exe()), str(REPO_ROOT / "scripts" / "mallitalytics_daily_card.py"),
        "--pitchers", str(player_id), "--date", game_date,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT), env=env, timeout=300)
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        print(f"    pitcher card script exit {proc.returncode}: {out[-500:]}")
        return None
    return _extract_saved_png(out)


def run_batter_card(player_id: int, game_date: str):
    env = {**os.environ, "MPLBACKEND": "Agg"}
    cmd = [
        str(_python_exe()), str(REPO_ROOT / "scripts" / "batter_card_daily.py"),
        "--batter", str(player_id), "--date", game_date,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT), env=env, timeout=300)
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        print(f"    batter card script exit {proc.returncode}: {out[-500:]}")
        return None
    return _extract_saved_png(out)


def queue_text_draft(title, tweet, game_date, season, meta=None):
    try:
        return insert_queue_item(
            content_type="text_only", title=title[:200], tweet_text=tweet[:280],
            image_path="", image_url="", game_date=game_date, season=season,
            stage="regular_season", meta=meta,
        )
    except Exception as e:
        print(f"  queue text_only failed: {e}")
        return None


def queue_card(content_type, png_path, tweet, game_date, season, player_id, player_name, meta=None):
    try:
        return insert_queue_item(
            content_type=content_type, title=png_path.stem[:200], tweet_text=tweet[:280],
            image_path=str(png_path.resolve()), image_url=_image_public_url(png_path.resolve()),
            game_date=game_date, season=season, stage="regular_season",
            player_id=player_id, player_name=player_name, meta=meta,
        )
    except Exception as e:
        print(f"  queue {content_type} failed: {e}")
        return None


def _anomaly_window_phrase(a: dict) -> str:
    lab = a.get("window_label")
    if lab:
        return str(lab)
    wd = a.get("window_days")
    if wd is not None:
        return f"{wd}d vs prior stretch"
    return "vs baseline"


def render_digest_plain(report: IntelReport) -> str:
    lines = [
        f"MALLITALYTICS MORNING INTEL | {report.anchor_date}", "",
        "THE LEADOFF",
    ]
    lines += [f"- {s['title']} — {s['url']}" for s in report.news_stories[:6]] or ["No headlines available."]
    lines += ["", "LAST NIGHT"]
    lines += report.yesterday_results or ["No final games."]
    lines += ["", "DATA SIGNALS — PITCHERS"]
    for anomaly in report.anomalies_pitchers[:5]:
        lines.append(
            f"{anomaly.get('player_name')} · {anomaly['metric']}: "
            f"{anomaly['window']} vs {anomaly['baseline']} (Δ{anomaly['delta']})"
        )
    if not report.anomalies_pitchers:
        lines.append("No qualified pitcher signals.")
    lines += ["", "DATA SIGNALS — HITTERS"]
    for anomaly in report.anomalies_batters[:5]:
        lines.append(
            f"{anomaly.get('player_name')} · {anomaly['metric']}: "
            f"{anomaly['window']} vs {anomaly['baseline']} (Δ{anomaly['delta']})"
        )
    if not report.anomalies_batters:
        lines.append("No qualified hitter signals.")
    lines += ["", "TODAY'S BOARD"]
    lines += report.probables_today[:15] or ["(none)"]
    lines += ["", "TOMORROW"]
    lines += report.probables_tomorrow[:15] or ["(none)"]
    lines += ["", "ROSTER WIRE"]
    lines += report.transactions[:12] or ["(none)"]
    lines += ["", "MILESTONE RADAR"]
    lines += report.milestones[:20] or ["(none)"]
    lines += ["", "CONTENT NOTEBOOK (PRIVATE)"]
    lines += [f"{i+1}. {t}" for i, t in enumerate(report.tweet_drafts)]
    if report.notes:
        lines += ["", "PIPELINE NOTES"] + report.notes
    return "\n".join(lines)


def _email_date(value: str) -> str:
    try:
        return date.fromisoformat(value).strftime("%A, %B %-d, %Y")
    except ValueError:
        return value


def _email_list(items: list[str], empty: str = "Nothing to report.", limit: int = 8) -> str:
    rows = items[:limit] or [empty]
    return "".join(
        "<tr><td style='padding:9px 0;border-bottom:1px solid #e5e7eb;"
        "font-size:14px;line-height:1.45;color:#263548'>"
        f"{html.escape(str(row))}</td></tr>"
        for row in rows
    )


def _anomaly_rows(items: list[dict], empty: str) -> str:
    if not items:
        return (
            "<tr><td style='padding:12px 0;color:#6b7788;font-size:14px'>"
            f"{html.escape(empty)}</td></tr>"
        )
    rows = []
    for item in items[:5]:
        name = html.escape(str(item.get("player_name") or item.get("player_id") or "Unknown"))
        metric = html.escape(str(item.get("metric") or "Signal"))
        window = html.escape(str(item.get("window") or "—"))
        baseline = html.escape(str(item.get("baseline") or "—"))
        delta = html.escape(str(item.get("delta") or "—"))
        rows.append(
            "<tr><td style='padding:10px 0;border-bottom:1px solid #e5e7eb'>"
            f"<div style='font-size:14px;font-weight:700;color:#172235'>{name}</div>"
            f"<div style='font-size:12px;line-height:1.45;color:#667386;margin-top:3px'>{metric} · "
            f"{window} vs {baseline} · <span style='color:#007f78'>Δ{delta}</span></div>"
            "</td></tr>"
        )
    return "".join(rows)


def render_digest_html(report: IntelReport) -> str:
    """Render a responsive, email-client-safe private morning newsletter."""
    stories = report.news_stories[:6]
    lead = stories[0] if stories else None
    lead_html = ""
    if lead:
        image_url = html.escape(str(lead.get("image_url") or ""), quote=True)
        image_html = (
            f"<img src='{image_url}' alt='' width='620' style='display:block;width:100%;"
            "height:auto;max-height:310px;object-fit:cover;border:0'>"
            if image_url else ""
        )
        lead_html = (
            "<tr><td style='padding:0 0 18px'>"
            f"<a href='{html.escape(str(lead['url']), quote=True)}' style='text-decoration:none'>{image_html}"
            "<div style='padding:16px 18px;background:#122033'>"
            "<div style='font-size:11px;font-weight:700;color:#43c5bc;text-transform:uppercase'>Top story</div>"
            f"<div style='font-size:22px;line-height:1.25;font-weight:800;color:#ffffff;margin-top:6px'>{html.escape(str(lead['title']))}</div>"
            f"<div style='font-size:12px;color:#aeb9c8;margin-top:8px'>{html.escape(str(lead.get('author') or 'MLB.com'))} · MLB.com</div>"
            "</div></a></td></tr>"
        )
    secondary_news = "".join(
        "<tr><td style='padding:10px 0;border-bottom:1px solid #e5e7eb'>"
        f"<a href='{html.escape(str(story['url']), quote=True)}' style='font-size:14px;line-height:1.4;"
        f"font-weight:700;color:#172235;text-decoration:none'>{html.escape(str(story['title']))}</a>"
        f"<div style='font-size:11px;color:#7a8696;margin-top:3px'>{html.escape(str(story.get('author') or 'MLB.com'))}</div>"
        "</td></tr>"
        for story in stories[1:]
    ) or "<tr><td style='padding:12px 0;color:#6b7788'>No MLB headlines available.</td></tr>"

    tweet_rows = "".join(
        "<tr><td style='padding:10px 0;border-bottom:1px solid #dbe4ee;font-size:13px;"
        f"line-height:1.5;color:#263548'><strong style='color:#d85a1a'>{idx}.</strong> {html.escape(tweet)}</td></tr>"
        for idx, tweet in enumerate(report.tweet_drafts[:5], 1)
    ) or "<tr><td style='padding:10px 0;color:#6b7788'>No drafts generated.</td></tr>"

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>@media only screen and (max-width:680px){{.shell{{width:100%!important}}.pad{{padding-left:18px!important;padding-right:18px!important}}.col{{display:block!important;width:100%!important}}}}</style>
</head><body style="margin:0;padding:0;background:#eef1f4;font-family:Arial,Helvetica,sans-serif">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#eef1f4"><tr><td align="center" style="padding:24px 8px">
<table role="presentation" class="shell" width="680" cellspacing="0" cellpadding="0" border="0" style="width:680px;max-width:680px;background:#ffffff">
<tr><td class="pad" style="padding:26px 30px 22px;background:#0b1726;border-top:4px solid #e96724">
  <div style="font-size:12px;font-weight:700;color:#43c5bc;text-transform:uppercase">Mallitalytics</div>
  <div style="font-size:28px;line-height:1.15;font-weight:800;color:#ffffff;margin-top:5px">Morning Intel</div>
  <div style="font-size:13px;color:#aeb9c8;margin-top:8px">{html.escape(_email_date(report.anchor_date))} · Your daily baseball briefing</div>
</td></tr>
<tr><td class="pad" style="padding:24px 30px 8px">
  <div style="font-size:11px;font-weight:800;color:#e96724;text-transform:uppercase;margin-bottom:12px">The leadoff</div>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0">{lead_html}{secondary_news}</table>
</td></tr>
<tr><td class="pad" style="padding:20px 30px">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>
    <td class="col" width="49%" valign="top" style="padding-right:10px">
      <div style="font-size:11px;font-weight:800;color:#2b6cb0;text-transform:uppercase;margin-bottom:6px">Last night</div>
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0">{_email_list(report.yesterday_results, 'No final games.', 10)}</table>
    </td>
    <td class="col" width="49%" valign="top" style="padding-left:10px">
      <div style="font-size:11px;font-weight:800;color:#007f78;text-transform:uppercase;margin-bottom:6px">Today&apos;s board</div>
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0">{_email_list(report.probables_today, 'No games scheduled.', 10)}</table>
    </td>
  </tr></table>
</td></tr>
<tr><td class="pad" style="padding:20px 30px;background:#f7f9fb;border-top:1px solid #dfe5eb;border-bottom:1px solid #dfe5eb">
  <div style="font-size:11px;font-weight:800;color:#e96724;text-transform:uppercase;margin-bottom:10px">Mallitalytics data signals</div>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>
    <td class="col" width="49%" valign="top" style="padding-right:10px"><div style="font-size:13px;font-weight:800;color:#172235">Pitchers</div><table role="presentation" width="100%">{_anomaly_rows(report.anomalies_pitchers, 'No qualified pitcher signals.')}</table></td>
    <td class="col" width="49%" valign="top" style="padding-left:10px"><div style="font-size:13px;font-weight:800;color:#172235">Hitters</div><table role="presentation" width="100%">{_anomaly_rows(report.anomalies_batters, 'No qualified hitter signals.')}</table></td>
  </tr></table>
</td></tr>
<tr><td class="pad" style="padding:22px 30px">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>
    <td class="col" width="49%" valign="top" style="padding-right:10px"><div style="font-size:11px;font-weight:800;color:#7c3f98;text-transform:uppercase;margin-bottom:6px">Roster wire</div><table role="presentation" width="100%">{_email_list(report.transactions, 'No notable moves.', 8)}</table></td>
    <td class="col" width="49%" valign="top" style="padding-left:10px"><div style="font-size:11px;font-weight:800;color:#a06a00;text-transform:uppercase;margin-bottom:6px">Milestone radar</div><table role="presentation" width="100%">{_email_list(report.milestones, 'No milestones in range.', 8)}</table></td>
  </tr></table>
</td></tr>
<tr><td class="pad" style="padding:22px 30px;background:#edf3f8;border-top:1px solid #d7e1ea">
  <div style="font-size:11px;font-weight:800;color:#d85a1a;text-transform:uppercase">Private content notebook</div>
  <div style="font-size:12px;color:#6b7788;margin:5px 0 8px">Starting points for today&apos;s Mallitalytics posts. Review before publishing.</div>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0">{tweet_rows}</table>
</td></tr>
<tr><td class="pad" style="padding:18px 30px;background:#0b1726;color:#8fa0b4;font-size:11px;line-height:1.5">
  News links: MLB.com · Data: MLB Stats API and Statcast · Internal edition<br>Mallitalytics turns baseball data into useful context.
</td></tr>
</table></td></tr></table></body></html>"""


def send_resend_twilio(subject, html_body, plain_body, dry):
    if dry:
        print("\n[DRY RUN] Skip Resend/Twilio")
        return
    resend_key = os.getenv("RESEND_API_KEY", "")
    resend_from = os.getenv("RESEND_FROM_EMAIL") or "onboarding@resend.dev"
    recipient = (
        os.getenv("MORNING_INTEL_TO_EMAIL")
        or os.getenv("RESEND_TO_EMAIL")
        or ""
    )
    email_sent = False
    if resend_key and recipient:
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
                json={
                    "from": resend_from, "to": recipient, "subject": subject,
                    "html": html_body,
                    "text": plain_body,
                },
                timeout=20,
            )
            if resp.status_code in (200, 201):
                eid = resp.json().get("id")
                print(f"  Resend ok id={eid}")
                log_notification("morning_intel", "email", recipient, subject, plain_body[:200], "sent", eid)
                email_sent = True
            else:
                print(f"  Resend failed {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"  Resend error: {e}")
    gmail_user = os.getenv("GMAIL_SMTP_USER", "").strip()
    gmail_password = os.getenv("GMAIL_APP_PASSWORD", "").replace(" ", "").strip()
    if not email_sent and gmail_user and gmail_password and recipient:
        try:
            message = EmailMessage()
            message["Subject"] = subject
            message["From"] = f"Mallitalytics <{gmail_user}>"
            message["To"] = recipient
            message.set_content(plain_body)
            message.add_alternative(html_body, subtype="html")
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=25) as smtp:
                smtp.login(gmail_user, gmail_password)
                smtp.send_message(message)
            print("  Gmail SMTP ok")
            log_notification(
                "morning_intel",
                "email",
                recipient,
                subject,
                plain_body[:200],
                "sent",
                "gmail_smtp",
            )
            email_sent = True
        except Exception as exc:
            print(f"  Gmail SMTP error: {type(exc).__name__}: {exc}")
    if not email_sent:
        print(
            "  Email not sent; configure Resend or "
            "GMAIL_SMTP_USER + GMAIL_APP_PASSWORD + MORNING_INTEL_TO_EMAIL"
        )
    tw_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    tw_tok = os.getenv("TWILIO_AUTH_TOKEN", "")
    tw_from = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
    tw_to = os.getenv("TWILIO_WHATSAPP_TO", "")
    if tw_sid and tw_tok and tw_to:
        body = plain_body[:1500]
        try:
            resp = requests.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{tw_sid}/Messages.json",
                auth=(tw_sid, tw_tok),
                data={"From": tw_from, "To": tw_to, "Body": body},
                timeout=15,
            )
            if resp.status_code == 201:
                sid = resp.json().get("sid")
                print(f"  Twilio WhatsApp ok sid={sid}")
                log_notification("morning_intel", "whatsapp", tw_to, subject, body[:200], "sent", sid)
            else:
                print(f"  Twilio failed {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"  Twilio error: {e}")


def run_intel(anchor, season, stage, dry_run, skip_notify, skip_claude, skip_queue=False):
    report = IntelReport(anchor_date=anchor.isoformat(), season=season)
    yesterday = anchor - timedelta(days=1)
    today = anchor
    tomorrow = anchor + timedelta(days=1)
    report.news_stories = api_mlb_news(limit=6)
    report.yesterday_results = api_game_results(yesterday)
    df = pd.DataFrame()
    indexed = iter_enriched_parquets(season, stage)
    if not indexed:
        report.notes.append(f"No pitches_enriched under {WAREHOUSE_ROOT}/{season}/{stage}")
    else:
        first_d = min(fd for _, fd in indexed)
        last_d = max(fd for _, fd in indexed)
        span = (last_d - first_d).days + 1
        if span < 14:
            report.notes.append(f"Short warehouse span (~{span}d); baselines use all earlier data in-window.")
        lo = min(first_d, anchor - timedelta(days=BASELINE_MAX_CAL_DAYS))
        paths = paths_in_calendar_range(indexed, lo, anchor)
        df = load_parquet_paths(paths)
        if df.empty:
            report.notes.append("Parquet load produced empty frame.")
        else:
            df = enrich_pitch_features(df)
            report.anomalies_pitchers = rank_unique_anomalies(
                detect_pitcher_anomalies(df, anchor),
            )
            report.anomalies_batters = rank_unique_anomalies(
                detect_batter_anomalies(df, anchor),
            )
    ids = list({int(a["player_id"]) for a in report.anomalies_pitchers + report.anomalies_batters})
    mile_ids = list(dict.fromkeys(
        [int(a["player_id"]) for a in report.anomalies_pitchers[:15]]
        + [int(a["player_id"]) for a in report.anomalies_batters[:15]]
    ))
    all_people_ids = list(dict.fromkeys(ids + mile_ids))
    people_meta = api_people_meta(all_people_ids)
    names_map = {k: v["name"] for k, v in people_meta.items()}
    teams_map = {k: v["team_id"] for k, v in people_meta.items()}
    pos_map = {k: v.get("primary_position") or "" for k, v in people_meta.items()}
    hydrate_anomaly_names(report.anomalies_pitchers, names_map)
    hydrate_anomaly_names(report.anomalies_batters, names_map)
    # Combine today + yesterday to catch intra-day moves (today first, then yesterday)
    txs_today = [(t, today) for t in api_transactions(today)]
    txs_yesterday = [(t, yesterday) for t in api_transactions(yesterday)]
    seen_tx: set[str] = set()
    tx_lines: list[str] = []
    tx_detail: list[dict] = []
    for t, day in (txs_today + txs_yesterday)[:80]:
        line = fmt_transaction(t, fallback_day=day)
        if line not in seen_tx:
            seen_tx.add(line)
            tx_lines.append(line)
            tx_detail.append({"line": line, "team_id": transaction_mlb_team_id(t)})
    report.transactions = tx_lines[:40]
    report.transactions_detail = tx_detail[:40]
    report.probables_today = api_probable_lines(today)
    report.probables_tomorrow = api_probable_lines(tomorrow)
    st = api_season_hitting_pitching(mile_ids, season)
    id_names = names_map
    mlines: list[str] = []
    mdetail: list[dict] = []
    _hitting_skip_flds = frozenset(
        {"homeRuns", "hits", "doubles", "triples", "strikeOuts"},
    )
    for pid, groups in st.items():
        for gkey_rule, fld, targets, lab, unit_phrase in MILESTONE_RULES:
            stat = groups.get(gkey_rule)
            if not stat or fld not in stat:
                continue
            # Pitching HR/hits are allowed stats — not batting-style chase milestones.
            if gkey_rule == "pitching" and fld in ("homeRuns", "hits"):
                continue
            if (
                gkey_rule == "hitting"
                and fld in _hitting_skip_flds
                and should_skip_hitting_milestones(groups, pos_map.get(pid))
            ):
                continue
            try:
                cur = int(float(stat.get(fld, 0)))
            except (TypeError, ValueError):
                continue
            for tgt in targets:
                if not _season_milestone_eligible(cur, lab, tgt):
                    continue
                need = tgt - cur
                pname = id_names.get(pid, str(pid))
                mlines.append(_fmt_season_milestone_line(pname, cur, lab, tgt))
                mdetail.append({
                    "player_id": pid,
                    "player_name": pname,
                    "stat": lab,
                    "group": gkey_rule,
                    "current": cur,
                    "target": tgt,
                    "need": need,
                    "unit": unit_phrase,
                    "team_id": teams_map.get(pid),
                })
                break
    mlines, mdetail = diversify_milestones_by_stat(mlines, mdetail, cap=45)
    report.milestones = mlines
    report.milestones_detail = mdetail
    findings = build_findings_blob(report)
    if skip_claude:
        report.tweet_drafts = ["(Claude skipped)"]
    else:
        report.tweet_drafts = generate_tweet_drafts_claude(findings, n=5)
    if dry_run or skip_queue:
        reason = "DRY RUN" if dry_run else "READ-ONLY NEWSLETTER"
        print(f"\n[{reason}] Skip queue inserts")
    else:
        season_y = anchor.year
        for i, tw in enumerate(report.tweet_drafts):
            if tw.startswith("("):
                continue
            qid = queue_text_draft(
                f"intel_draft_{anchor.isoformat()}_{i+1}", tw, anchor.isoformat(), season_y,
                meta={"source": "morning_intel", "draft_index": i + 1},
            )
            if qid:
                report.queue_ids.append(qid)
    INTEL_OUT.mkdir(parents=True, exist_ok=True)
    snap_path = INTEL_OUT / f"intel_{anchor.isoformat()}.json"
    snap_path.write_text(json.dumps({
        "anchor": report.anchor_date,
        "news_stories": report.news_stories,
        "yesterday_results": report.yesterday_results,
        "transactions": report.transactions,
        "transactions_detail": report.transactions_detail,
        "probables_today": report.probables_today,
        "probables_tomorrow": report.probables_tomorrow,
        "milestones": report.milestones,
        "milestones_detail": report.milestones_detail,
        "anomalies_pitchers": report.anomalies_pitchers,
        "anomalies_batters": report.anomalies_batters,
        "tweet_drafts": report.tweet_drafts,
        "queue_ids": report.queue_ids,
        "notes": report.notes,
    }, indent=2, default=str), encoding="utf-8")
    print(f"  Wrote {snap_path}")
    plain = render_digest_plain(report)
    newsletter_html = render_digest_html(report)
    html_path = INTEL_OUT / f"intel_{anchor.isoformat()}.html"
    html_path.write_text(newsletter_html, encoding="utf-8")
    print(f"  Wrote {html_path}")
    print("\n" + plain)
    if not skip_notify:
        send_resend_twilio(
            f"Morning Intel | {anchor.strftime('%b %-d')}",
            newsletter_html,
            plain,
            dry=dry_run,
        )
    return report


def main():
    parser = argparse.ArgumentParser(description="Mallitalytics morning intel job")
    parser.add_argument("--date", default=None, help="Anchor YYYY-MM-DD (default yesterday UTC)")
    parser.add_argument("--season", type=int, default=None, help="Warehouse season year")
    parser.add_argument("--stage", default="regular_season", help="Warehouse stage folder")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-notify", action="store_true")
    parser.add_argument("--skip-claude", action="store_true")
    parser.add_argument(
        "--skip-queue",
        action="store_true",
        help="Send/write the briefing without inserting generated drafts into the content queue.",
    )
    parser.add_argument(
        "--skip-cards",
        action="store_true",
        help="Deprecated compatibility flag; Morning Intel no longer generates watchlist cards.",
    )
    args = parser.parse_args()
    anchor = date.fromisoformat(args.date) if args.date else date.today()
    season = args.season or anchor.year
    print(f"\n{'='*60}\n  Morning Intel — anchor={anchor} season={season}\n{'='*60}")
    run_intel(
        anchor,
        season,
        args.stage,
        args.dry_run,
        args.skip_notify,
        args.skip_claude,
        skip_queue=args.skip_queue,
    )


if __name__ == "__main__":
    main()
