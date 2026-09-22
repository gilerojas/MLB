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
from datetime import date, datetime, timedelta, timezone
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
from morning_intel import persistence as _persist  # noqa: E402
from morning_intel import scouting as _scout  # noqa: E402
from morning_intel import signal_stats as _sig  # noqa: E402
from src import batter_recent as _br  # noqa: E402
from src import pitcher_recent as _pr  # noqa: E402
from src.editorial_system import mallitalytics_editorial_contract  # noqa: E402

# The VPS holds the live warehouse at /data/warehouse/mlb inside the api container.
# Overriding this is what lets the job run next to the data instead of pulling
# 1,500+ parquet files from Drive on every run.
WAREHOUSE_ROOT = Path(os.getenv("MLB_WAREHOUSE_ROOT") or (REPO_ROOT / "data" / "warehouse" / "mlb"))
OUTPUTS_ROOT = REPO_ROOT / "outputs"
INTEL_OUT = Path(os.getenv("MLB_INTEL_SNAPSHOT_DIR") or (_INTEL_DIR / "snapshots"))
SCOUTING_LEDGER = Path(os.getenv("MLB_SCOUTING_LEDGER") or (_INTEL_DIR / "scouting" / "ledger.jsonl"))
# Email-optimised brand mark. Gmail strips data: URIs on <img>, so the mail
# carries this as an inline CID attachment; the saved preview inlines it instead.
LOGO_EMAIL_PATH = Path(os.getenv("MLB_INTEL_LOGO") or (REPO_ROOT / "assets" / "brand" / "logo_email_horizontal.png"))
LOGO_CID = "mallilogo"
# Postseason lands in its own warehouse stage. Reading only regular_season means
# the engine silently freezes in October instead of failing loudly.
STAGE_ORDER = ("regular_season", "playoffs")
STATS_BASE = "https://statsapi.mlb.com/api/v1"
MLB_NEWS_RSS = "https://www.mlb.com/feeds/news/rss.xml"
SPORT_ID = 1
PARQUET_NAME_RE = re.compile(r"game_(\d+)_(\d{8})_pitches_enriched\.parquet$", re.I)
READ_COLS = [
    "game_pk", "game_date", "pitcher", "batter", "player_name", "pitch_type",
    "release_speed", "description", "zone", "type", "launch_speed",
    "launch_speed_angle", "estimated_woba_using_speedangle",
    "at_bat_number", "stand",
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
# Opportunity-based windows have no clock of their own. A player who stops
# playing keeps his "last 10 BBE" forever, so the same frozen line gets reported
# every morning — Stanton's window sat on Apr 21-24 and was still being published
# in mid-May, and would have published today, 150 days stale. Worse, a frozen
# signal repeats daily and would collect the maximum persistence bonus.
# A window must therefore be recent, and it must not be stretched across months.
MAX_WINDOW_STALENESS_DAYS = 10
MAX_BBE_WINDOW_SPAN_DAYS = 45
MAX_STARTS_WINDOW_SPAN_DAYS = 45
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
    # Publishable findings vs signals only being tracked until they repeat.
    leads: list[dict] = field(default_factory=list)
    watch: list[dict] = field(default_factory=list)
    # Wider pool persisted purely so tomorrow's run can measure repeats.
    signal_pool: list[dict] = field(default_factory=list)
    scouting_entries: list[dict] = field(default_factory=list)
    editorial_brief: str = ""
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
    """
    Indexed enriched files for a stage, or for every stage when ``stage`` is "all".

    Duplicate game files (the warehouse has picked up iCloud copies before) are
    collapsed by game_pk. A doubled game inflates every window count, which would
    understate the standard errors the ranking now depends on.
    """
    stages = STAGE_ORDER if stage == "all" else (stage,)
    by_game: dict[str, tuple[Path, date]] = {}
    for st in stages:
        base = WAREHOUSE_ROOT / str(season) / st
        if not base.exists():
            continue
        for path in sorted(base.rglob("game_*_pitches_enriched.parquet")):
            fd = _parse_file_date(path)
            if fd is None:
                continue
            m = PARQUET_NAME_RE.search(path.name)
            game_pk = m.group(1) if m else str(path)
            if game_pk not in by_game:
                by_game[game_pk] = (path, fd)
    return sorted(by_game.values(), key=lambda pair: pair[1])


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
    df["stand"] = df["stand"].fillna("").astype(str).str.upper().str[:1]
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


def _expected_mix_for_split(base_df, stand_shares: dict[str, float]) -> pd.Series:
    """
    Baseline mix reweighted to the handedness the pitcher actually faced recently.

    A pitcher facing a lefty-heavy lineup throws more changeups. That is a change
    in the opponent, not in the pitcher, and it was the single largest source of
    false mix alarms.
    """
    parts = []
    weights = []
    for hand, share in stand_shares.items():
        pool = base_df[base_df["stand"] == hand]
        if len(pool) < 20:
            continue
        parts.append(pool["pitch_type"].value_counts(normalize=True))
        weights.append(share)
    if not parts or sum(weights) <= 0:
        return base_df["pitch_type"].value_counts(normalize=True)
    total = sum(weights)
    expected = None
    for series, weight in zip(parts, weights):
        scaled = series * (weight / total)
        expected = scaled if expected is None else expected.add(scaled, fill_value=0.0)
    return expected


def _outing_mix_rates(window_df, pitch_type: str) -> list[float]:
    """Usage rate of one pitch type in each qualifying outing of the window."""
    rates = []
    for _, outing in window_df.groupby("game_pk", sort=False):
        if len(outing) < 12:
            continue
        rates.append(float((outing["pitch_type"] == pitch_type).mean()))
    return rates


def _mix_holds_across_starts(rates: list[float], base_rate: float, direction: int) -> int:
    """How many of the window's outings move the same way. Guards one-start blips."""
    if direction > 0:
        return sum(1 for r in rates if r > base_rate)
    return sum(1 for r in rates if r < base_rate)


def _dominant_mix_shift(window_df, base_df, pid):
    """
    Largest usage shift against a handedness-adjusted baseline.

    Returns ``(pitch_type, expected_pct, observed_pct, n_window, holds, n_outings,
    is_new_pitch, outing_rates)`` or None. Usage is compared to what this pitcher
    throws against the handedness he just faced, and the shift must show up in more
    than one outing before it counts.
    """
    w = window_df[_pid_match_series(window_df["pitcher"], pid)]
    b = base_df[_pid_match_series(base_df["pitcher"], pid)]
    if len(w) < MIN_PITCHES_WINDOW or len(b) < MIN_PITCHES_BASELINE:
        return None

    stand_counts = w["stand"].value_counts(normalize=True).to_dict()
    stand_shares = {h: s for h, s in stand_counts.items() if h in ("L", "R")}
    expected = _expected_mix_for_split(b, stand_shares)
    observed = w["pitch_type"].value_counts(normalize=True)

    n_outings = int(w.groupby("game_pk").size().ge(12).sum())
    best = None
    for k in set(observed.index) | set(expected.index):
        pv = float(observed.get(k, 0.0))
        pe = float(expected.get(k, 0.0))
        if pe < 0.05 and pv < 0.05:
            continue
        delta = pv - pe
        if best is None or abs(delta) > abs(best[2] - best[1]):
            best = (k, pe, pv)
    if best is None:
        return None

    k, pe, pv = best
    if abs(pv - pe) < 0.08:
        return None
    direction = 1 if pv > pe else -1
    rates = _outing_mix_rates(w, k)
    holds = _mix_holds_across_starts(rates, pe, direction)
    # A genuinely new pitch is a story; a one-outing usage wobble is not.
    is_new_pitch = pe < 0.02 and pv >= 0.10
    if holds < 2 and not is_new_pitch:
        return None
    return (k, pe * 100, pv * 100, len(w), holds, n_outings, is_new_pitch, rates)


def _anomaly_pitcher_label() -> str:
    return f"last {RECENT_STARTS} vs prior {BASELINE_STARTS} starts"


def _window_dates(frame) -> tuple[Optional[date], Optional[date]]:
    """First and last game date present in a window, for staleness reporting."""
    if frame is None or len(frame) == 0 or "gd" not in frame:
        return None, None
    series = frame["gd"].dropna()
    if series.empty:
        return None, None
    return series.min(), series.max()


def _window_is_current(frame, anchor: date, max_span_days: int) -> bool:
    """
    Whether a window describes recent form rather than a frozen snapshot.

    Rejects two distinct failures: a player who has not appeared lately, whose
    window never advances, and a window whose events are so spread out that
    "last 10" spans a third of a season and means nothing about form.
    """
    start, end = _window_dates(frame)
    if start is None or end is None:
        return False
    if (anchor - end).days > MAX_WINDOW_STALENESS_DAYS:
        return False
    return (end - start).days <= max_span_days


def _pitch_item(pid, wlab, metric, window_val, base_val, delta, n_w, n_b, counts, se,
                window_start=None, window_end=None):
    """One pitcher anomaly, annotated with its own standard error."""
    return _sig.annotate({
        "player_id": pid,
        "role": "pitcher",
        "window_days": None,
        "window_kind": "starts",
        "window_starts": RECENT_STARTS,
        "baseline_starts": BASELINE_STARTS,
        "window_label": wlab,
        "metric": metric,
        "window": window_val,
        "baseline": base_val,
        "delta": delta,
        "n_window": n_w,
        "n_baseline": n_b,
        "counts": counts,
        "window_start": window_start,
        "window_end": window_end,
    }, se)


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
        # A pitcher on the IL keeps his last three starts indefinitely.
        if not _window_is_current(wpool, anchor, MAX_STARTS_WINDOW_SPAN_DAYS):
            continue
        counts = {"pitches_recent": len(wpool), "pitches_baseline": len(bpool)}
        w_start, w_end = _window_dates(wpool)

        w_vel = float(wpool["release_speed"].mean())
        b_vel = float(bpool["release_speed"].mean())
        velo_d = w_vel - b_vel
        if abs(velo_d) >= 1.2:
            se = _sig.mean_diff_se(
                float(wpool["release_speed"].std(ddof=1)), int(wpool["release_speed"].count()),
                float(bpool["release_speed"].std(ddof=1)), int(bpool["release_speed"].count()),
                metric="avg_velo_mph",
            )
            out.append(_pitch_item(
                pid, wlab, "avg_velo_mph", round(w_vel, 2), round(b_vel, 2),
                round(velo_d, 2), len(wpool), len(bpool), counts, se, w_start, w_end,
            ))

        w_wh = float(wpool["whiff"].mean())
        b_wh = float(bpool["whiff"].mean())
        d_wh = (w_wh - b_wh) * 100.0
        if abs(d_wh) >= 5.0:
            se = _sig.rate_se_from_pcts(w_wh * 100.0, len(wpool), b_wh * 100.0, len(bpool))
            out.append(_pitch_item(
                pid, wlab, "whiff_pct", round(w_wh * 100.0, 1), round(b_wh * 100.0, 1),
                round(d_wh, 1), len(wpool), len(bpool), counts, se, w_start, w_end,
            ))

        w_ch = float(wpool["chase"].mean())
        b_ch = float(bpool["chase"].mean())
        d_ch = (w_ch - b_ch) * 100.0
        if abs(d_ch) >= 5.0:
            se = _sig.rate_se_from_pcts(w_ch * 100.0, len(wpool), b_ch * 100.0, len(bpool))
            out.append(_pitch_item(
                pid, wlab, "chase_pct", round(w_ch * 100.0, 1), round(b_ch * 100.0, 1),
                round(d_ch, 1), len(wpool), len(bpool), counts, se, w_start, w_end,
            ))

        wb = wpool[wpool["bip"]]
        bb = bpool[bpool["bip"]]
        if len(wb) >= 6 and len(bb) >= 15:
            xw_w = float(wb["estimated_woba_using_speedangle"].mean())
            xw_b = float(bb["estimated_woba_using_speedangle"].mean())
            d_xw = xw_w - xw_b
            if abs(d_xw) >= 0.04:
                se = _sig.mean_diff_se(
                    float(wb["estimated_woba_using_speedangle"].std(ddof=1)),
                    int(wb["estimated_woba_using_speedangle"].count()),
                    float(bb["estimated_woba_using_speedangle"].std(ddof=1)),
                    int(bb["estimated_woba_using_speedangle"].count()),
                    metric="xwoba_on_BIP",
                )
                out.append(_pitch_item(
                    pid, wlab, "xwoba_on_BIP", round(xw_w, 3), round(xw_b, 3),
                    round(d_xw, 3), len(wb), len(bb),
                    {"bip_recent": len(wb), "bip_baseline": len(bb)}, se, w_start, w_end,
                ))

        mix = _dominant_mix_shift(wpool, bpool, pid)
        if mix:
            name, pe, pv, n_mix, holds, n_outings, is_new, rates = mix
            # Between-outing spread, not per-pitch binomial: the outing is the
            # unit at which a pitcher actually changes his usage.
            se = _sig.cluster_se([r * 100.0 for r in rates], pe)
            item = _pitch_item(
                pid, wlab, f"mix_{name}_pct", round(pv, 1), round(pe, 1),
                round(pv - pe, 1), n_mix, len(bpool),
                {**counts, "outings_holding": holds, "outings": n_outings}, se,
                w_start, w_end,
            )
            item["baseline_kind"] = "handedness_adjusted"
            item["new_pitch"] = bool(is_new)
            out.append(item)
    return out

def _bat_item(bid, wlab, kind, win_n, base_n, metric, window_val, base_val, delta, n_w, n_b, counts, se,
              window_start=None, window_end=None):
    """One batter anomaly, annotated with its own standard error."""
    return _sig.annotate({
        "player_id": bid,
        "role": "batter",
        "window_days": None,
        "window_kind": kind,
        "window_n": win_n,
        "baseline_n": base_n,
        "window_label": wlab,
        "metric": metric,
        "window": window_val,
        "baseline": base_val,
        "delta": delta,
        "n_window": n_w,
        "n_baseline": n_b,
        "counts": counts,
        "window_start": window_start,
        "window_end": window_end,
    }, se)


def _std(series) -> float:
    try:
        return float(pd.to_numeric(series, errors="coerce").std(ddof=1))
    except (TypeError, ValueError):
        return float("nan")


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
            # Without this a benched or injured hitter's window never advances
            # and the identical line publishes every morning indefinitely.
            if not _window_is_current(wr, anchor, MAX_BBE_WINDOW_SPAN_DAYS):
                continue
            bw_start, bw_end = _window_dates(wr)
            sw = _br.summarize_bip_pool(wr)
            sb = _br.summarize_bip_pool(br)
            wlab = f"last {RECENT_BBE} vs prior {BASELINE_BBE} BBE"
            counts = {
                "barrels_recent": sw.get("barrels", 0),
                "barrels_baseline": sb.get("barrels", 0),
            }

            ev_w = sw.get("avg_ev")
            ev_b = sb.get("avg_ev")
            if ev_w is not None and ev_b is not None:
                ev_d = float(ev_w) - float(ev_b)
                if abs(ev_d) >= 2.0:
                    se = _sig.mean_diff_se(
                        _std(wr["launch_speed"]), sw["n"], _std(br["launch_speed"]), sb["n"],
                        metric="avg_EV_mph",
                    )
                    out.append(_bat_item(
                        bid, wlab, "bbe", RECENT_BBE, BASELINE_BBE, "avg_EV_mph",
                        round(float(ev_w), 2), round(float(ev_b), 2), round(ev_d, 2),
                        sw["n"], sb["n"], counts, se, bw_start, bw_end,
                    ))

            br_w = float(sw.get("barrel_pct") or 0.0)
            br_b = float(sb.get("barrel_pct") or 0.0)
            br_d = br_w - br_b
            if abs(br_d) >= 4.0:
                se = _sig.rate_se_from_pcts(br_w, sw["n"], br_b, sb["n"])
                out.append(_bat_item(
                    bid, wlab, "bbe", RECENT_BBE, BASELINE_BBE, "barrel_pct",
                    round(br_w, 1), round(br_b, 1), round(br_d, 1),
                    sw["n"], sb["n"], counts, se, bw_start, bw_end,
                ))

            xw_w = sw.get("xwoba")
            xw_b = sb.get("xwoba")
            if xw_w is not None and xw_b is not None:
                d_xw = float(xw_w) - float(xw_b)
                if abs(d_xw) >= 0.04:
                    se = _sig.mean_diff_se(
                        _std(wr["estimated_woba_using_speedangle"]), sw["n"],
                        _std(br["estimated_woba_using_speedangle"]), sb["n"],
                        metric="xwoba_on_BIP",
                    )
                    out.append(_bat_item(
                        bid, wlab, "bbe", RECENT_BBE, BASELINE_BBE, "xwoba_on_BIP",
                        round(float(xw_w), 3), round(float(xw_b), 3), round(d_xw, 3),
                        sw["n"], sb["n"], counts, se, bw_start, bw_end,
                    ))

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
        if not _window_is_current(wr, anchor, MAX_BBE_WINDOW_SPAN_DAYS):
            continue
        pw_start, pw_end = _window_dates(wr)
        sw = _br.summarize_pa_pool(wr)
        sb = _br.summarize_pa_pool(br)
        w_wh = sw.get("whiff_per_pitch")
        b_wh = sb.get("whiff_per_pitch")
        if w_wh is not None and b_wh is not None:
            d = float(w_wh) - float(b_wh)
            if abs(d) >= 5.0:
                se = _sig.rate_se_from_pcts(
                    float(w_wh), sw["n_pa_pitch_rows"], float(b_wh), sb["n_pa_pitch_rows"],
                )
                out.append(_bat_item(
                    bid,
                    (
                        f"last {RECENT_PA_PITCH_ROWS} vs prior {BASELINE_PA_PITCH_ROWS} "
                        "PA (pitch rows)"
                    ),
                    "pa", RECENT_PA_PITCH_ROWS, BASELINE_PA_PITCH_ROWS, "whiff_pct",
                    round(float(w_wh), 1), round(float(b_wh), 1), round(d, 1),
                    sw["n_pa_pitch_rows"], sb["n_pa_pitch_rows"], {}, se, pw_start, pw_end,
                ))
    return out

# Roughly 1,500 player-metric comparisons run every morning, so |z| > 2.5 still
# yields on the order of fifteen hits by chance alone. That is why repeats and
# corroborating reporting carry weight in the score rather than z standing alone.
LEAD_Z = 2.5
WATCH_Z = 1.5
MAX_LEADS = 8
MAX_LEADS_PER_FAMILY = 3
_WATCH_SHOWN = 8
FDR_Q = 0.05


def anomaly_score(item: dict) -> float:
    """
    Rank by surprise, not by raw size.

    Starts at |z| — the delta measured against its own sampling error — then
    rewards a signal that repeated across days and one that has reporting behind
    it. A smaller move with a known mechanism outranks a larger unexplained one.
    """
    score = abs(float(item.get("z") or 0.0))
    streak = _persist.confirmed_streak(item.get("persistence"))
    score *= 1.0 + 0.15 * min(streak, 4)
    corroboration = item.get("corroboration") or []
    if any(int(c.get("tier") or 9) <= _scout.TIER_REPORTING for c in corroboration):
        score += 1.0
    if item.get("new_pitch"):
        score += 0.5
    return score


def rank_unique_anomalies(items, top=25):
    """Best row per player-metric, ordered by composite score."""
    best = {}
    for it in items:
        key = (it["player_id"], it["role"], it["metric"])
        cur = best.get(key)
        if cur is None or abs(float(it.get("z") or 0.0)) > abs(float(cur.get("z") or 0.0)):
            best[key] = it
    for it in best.values():
        it["score"] = round(anomaly_score(it), 3)
    ranked = sorted(best.values(), key=lambda x: x["score"], reverse=True)
    return ranked[:top]


def mark_significance(items: list[dict], q: float = 0.05) -> list[dict]:
    """Flag which signals survive false-discovery control across the whole slate."""
    p_values = [_sig.two_sided_p(float(it.get("z") or 0.0)) for it in items]
    for it, p, keep in zip(items, p_values, _sig.benjamini_hochberg(p_values, q=q)):
        it["p"] = round(p, 6)
        it["fdr_significant"] = bool(keep)
    return items


def split_leads_and_watch(items: list[dict], max_leads: int = MAX_LEADS) -> tuple[list[dict], list[dict]]:
    """
    Partition ranked anomalies into what is publishable and what is only tracked.

    A lead has to clear false-discovery control, or be a repeat or a corroborated
    signal that clears the softer bar. Sub-threshold signals used to be discarded
    every morning; holding them in a watch list is what lets a quiet velocity
    drift graduate once it repeats.
    """
    leads, watch = [], []
    for it in items:
        z = abs(float(it.get("z") or 0.0))
        streak = _persist.confirmed_streak(it.get("persistence"))
        corroborated = bool(it.get("corroboration"))
        significant = bool(it.get("fdr_significant"))
        if significant or (z >= WATCH_Z and (streak >= 2 or corroborated)):
            leads.append(it)
        elif z >= WATCH_Z:
            watch.append(it)
    # The newsletter wants a handful of distinct angles. Without a per-family cap
    # one metric monopolizes the list even when each finding is individually sound.
    kept, spill = [], []
    family_counts: dict[str, int] = defaultdict(int)
    for it in leads:
        family = "mix" if str(it["metric"]).startswith("mix_") else str(it["metric"])
        if len(kept) < max_leads and family_counts[family] < MAX_LEADS_PER_FAMILY:
            family_counts[family] += 1
            kept.append(it)
        else:
            spill.append(it)
    return kept, spill + watch


# Metrics stable enough to carry a prior season forward. Pitch usage is excluded:
# a pitcher's mix is a plan that genuinely resets between seasons.
PRIOR_METRICS = {
    "pitcher": ("avg_velo_mph", "whiff_pct", "chase_pct", "xwoba_on_BIP"),
    "batter": ("avg_EV_mph", "barrel_pct", "xwoba_on_BIP"),
}
PRIORS_DIR = Path(os.getenv("MLB_INTEL_PRIORS_DIR") or (_INTEL_DIR / "priors"))


def priors_path(season: int) -> Path:
    return PRIORS_DIR / f"priors_{season}.json"


def build_prior_summary(season: int, stage: str = "all") -> dict:
    """
    Per-player season aggregates used as an early-season baseline.

    Built once per season by ``--build-priors``, never during the daily run: it
    reads the whole season and the morning job has no business paying that cost.
    """
    indexed = iter_enriched_parquets(season, stage)
    if not indexed:
        return {}
    df = enrich_pitch_features(load_parquet_paths([p for p, _ in indexed]))
    if df.empty:
        return {}

    out: dict[str, dict] = {}
    pit = df[df["pitcher"].notna()]
    for pid, grp in pit.groupby(pd.to_numeric(pit["pitcher"], errors="coerce")):
        if not np.isfinite(pid) or len(grp) < 200:
            continue
        bip = grp[grp["bip"]]
        out[f"pitcher:{int(pid)}"] = {
            "n": int(len(grp)),
            "avg_velo_mph": float(grp["release_speed"].mean()),
            "whiff_pct": float(grp["whiff"].mean()) * 100.0,
            "chase_pct": float(grp["chase"].mean()) * 100.0,
            "xwoba_on_BIP": (
                float(bip["estimated_woba_using_speedangle"].mean()) if len(bip) >= 25 else None
            ),
        }

    bat = df[df["batter"].notna() & df["bip"]]
    for bid, grp in bat.groupby(pd.to_numeric(bat["batter"], errors="coerce")):
        if not np.isfinite(bid) or len(grp) < 50:
            continue
        barrels = int((grp["launch_speed_angle"] == 6).sum())
        out[f"batter:{int(bid)}"] = {
            "n": int(len(grp)),
            "avg_EV_mph": float(grp["launch_speed"].mean()),
            "barrel_pct": barrels / len(grp) * 100.0,
            "xwoba_on_BIP": float(grp["estimated_woba_using_speedangle"].mean()),
        }
    return out


def load_priors(season: int) -> dict:
    path = priors_path(season)
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}


def apply_prior_season(items: list[dict], season: int, anchor: date, stage: str, report) -> list[dict]:
    """
    Pull thin in-season baselines toward the player's prior season.

    In April a "baseline" of 40 batted balls is barely a baseline at all. Blending
    it with last season by the same stabilization weight used elsewhere means one
    formula covers the whole calendar instead of April needing its own rules.
    """
    priors = load_priors(season - 1)
    if not priors:
        report.notes.append(
            f"No {season - 1} priors cached — early-season baselines are unblended. "
            "Run --build-priors once that season is in the warehouse."
        )
        return items

    blended = 0
    for item in items:
        role = item.get("role")
        metric = str(item.get("metric") or "")
        if metric not in PRIOR_METRICS.get(role, ()):
            continue
        prior = priors.get(f"{role}:{int(item['player_id'])}")
        if not prior or prior.get(metric) is None:
            continue
        n_b = int(item.get("n_baseline") or 0)
        new_base = _sig.blend_prior(float(item["baseline"]), n_b, float(prior[metric]), metric)
        if abs(new_base - float(item["baseline"])) < 1e-9:
            continue
        item["baseline_unblended"] = item["baseline"]
        item["baseline"] = round(new_base, 3)
        item["baseline_kind"] = f"blended_with_{season - 1}"
        item["delta"] = round(float(item["window"]) - new_base, 3)
        _sig.annotate(item, item.get("se"))
        blended += 1
    if blended:
        report.notes.append(f"Blended {blended} baselines with {season - 1} priors.")
    return items


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


def api_mlb_news(limit: int = 6) -> tuple[list[dict], str]:
    """
    MLB.com headlines, plus why they are missing when they are.

    mlb.com refuses datacenter traffic outright — from the VPS this returns 403
    regardless of user agent, while statsapi.mlb.com is unaffected. Returning the
    reason keeps that in the pipeline notes instead of silently emptying the
    section, which is how it would otherwise read as "no news today".
    """
    try:
        response = requests.get(MLB_NEWS_RSS, timeout=20)
        response.raise_for_status()
        return parse_mlb_news_rss(response.text, limit=limit), ""
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        return [], f"MLB.com news feed unavailable (HTTP {code}); scouting ledger still applies."
    except requests.RequestException as exc:
        return [], f"MLB.com news feed unreachable ({type(exc).__name__})."


def _stories_from_ledger(entries: list[dict], limit: int = 5) -> list[dict]:
    """
    Stand in for the headline feed using sourced ledger claims.

    mlb.com refuses the VPS, so on that host the feed is always empty. The ledger
    already holds dated, attributed reporting, which is closer to what the section
    was for than a list of headlines was.
    """
    # Official transactions already have their own panel, and most of them are
    # affiliate roster moves rather than league news, so this draws on reporting.
    # Newest first, and a hinge outranks a routine move.
    priority = {"injury": 0, "role_change": 1, "quote": 2, "performance": 3}
    candidates = [
        e for e in entries
        if e.get("url") and e.get("claim")
        and str(e.get("source") or "") != "mlb_transactions"
    ]
    # Newest first, then a stable pass puts the more newsworthy kinds on top.
    candidates.sort(key=lambda e: str(e.get("ts") or ""), reverse=True)
    ranked = sorted(
        candidates,
        key=lambda e: (
            priority.get(str(e.get("event_type") or ""), 4),
            int(e.get("tier") or 9),
        ),
    )
    seen: set[str] = set()
    stories: list[dict] = []
    for entry in ranked:
        claim = str(entry["claim"]).strip()
        if claim in seen:
            continue
        seen.add(claim)
        stories.append({
            "title": claim,
            "url": str(entry["url"]),
            "author": str(entry.get("source") or "ledger"),
            "published_at": entry.get("ts") or "",
            "image_url": "",
            "player_ids": entry.get("player_ids") or [],
        })
        if len(stories) >= limit:
            break
    return stories


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
        # Leads carry their own confidence and any reporting that explains them,
        # so the editorial model can pair a number with a mechanism instead of
        # guessing at one.
        "leads": [
            {
                "player": a.get("player_name"),
                "role": a.get("role"),
                "metric": a.get("metric"),
                "window": a.get("window"),
                "baseline": a.get("baseline"),
                "delta": a.get("delta"),
                "z": a.get("z"),
                "confidence": _confidence_tag(a),
                "sample": a.get("n_window"),
                "window_label": a.get("window_label"),
                "persistence": a.get("persistence_label"),
                "corroboration": a.get("corroboration") or [],
            }
            for a in report.leads
        ],
        "watch": [
            {
                "player": a.get("player_name"),
                "metric": a.get("metric"),
                "delta": a.get("delta"),
                "z": a.get("z"),
                "persistence": a.get("persistence_label"),
            }
            for a in report.watch[:10]
        ],
        "notes": report.notes,
    }
    return json.dumps(blob, indent=2, default=str)


def generate_editorial_glm(findings_summary: str, n: int = 5) -> tuple[str, list[str]]:
    """Create the grounded editorial brief and private tweet notebook with GLM."""
    key = os.getenv("GLM_API_KEY", "").strip()
    if not key:
        return "", ["(Set GLM_API_KEY for the editorial brief and tweet drafts.)"]
    model = os.getenv("GLM_MODEL", "glm-5.2").strip() or "glm-5.2"
    base_url = os.getenv("GLM_BASE_URL", "https://api.z.ai/api/coding/paas/v4").rstrip("/")
    editorial_contract = mallitalytics_editorial_contract("newsletter")
    system_prompt = f"""You are the editor of Mallitalytics Morning Intel, a concise daily MLB newsletter for Gilberto Rojas.

Voice: analytical, useful, baseball-literate, and direct. Write like a stable professional publication, not an AI assistant.

{editorial_contract}

Grounding rules:
- Use only facts explicitly present in the supplied JSON.
- News headlines are link context only. Do not infer article details from a headline.
- Never invent injuries, trades, scores, trends, causes, quotations, or statistics.
- Distinguish observed Statcast changes from explanations. A change is not automatically causal.
- Lead with "leads". "watch" entries have not cleared the bar — never state one as a finding.
- Respect each lead's confidence field. A "tentative" lead is phrased as a question worth
  following, never as an established trend.
- Corroboration is reporting, not measurement. Label it as such and keep it separate from the
  Statcast number it sits beside; a hinge explains a signal, it does not confirm it.
- If the slate is empty or data is missing, say so plainly rather than filling space.
- A quiet day with no leads is a legitimate edition. Say there is nothing rather than promoting
  a weak signal to fill the space.
- Avoid hype, engagement bait, rhetorical questions, and generic baseball cliches.

Return only valid JSON with keys morning_brief and tweet_drafts."""
    user_prompt = f"""Build today's private Mallitalytics edition from this verified input:

{findings_summary}

Output requirements:
- morning_brief: 90-140 words, two short paragraphs separated by a blank line. Synthesize what deserves attention today and why, without repeating every section below it.
- tweet_drafts: exactly {n} distinct plain-text drafts, each under 280 characters. Prefer measured Mallitalytics signals for statistical claims. At most one draft may use a hashtag.

Return JSON only:
{{"morning_brief":"...","tweet_drafts":["...","..."]}}"""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    def request_completion(request_messages: list[dict]) -> str:
        response = requests.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": request_messages,
                "max_tokens": 4096,
                "temperature": 0.25,
                "stream": False,
            },
            timeout=90,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        if isinstance(content, list):
            content = "".join(
                part.get("text", "") if isinstance(part, dict) else str(part)
                for part in content
            )
        return str(content).strip()

    def parse_payload(text: str) -> dict:
        decoder = json.JSONDecoder()
        for start, char in enumerate(text):
            if char != "{":
                continue
            try:
                payload, _ = decoder.raw_decode(text[start:])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and (
                "morning_brief" in payload or "tweet_drafts" in payload
            ):
                return payload
        raise json.JSONDecodeError("No editorial JSON object found", text, 0)

    try:
        content = request_completion(messages)
        try:
            payload = parse_payload(content)
        except json.JSONDecodeError:
            repair_messages = messages + [
                {"role": "assistant", "content": content},
                {
                    "role": "user",
                    "content": (
                        "Your response was not valid JSON. Reformat the same grounded content "
                        "as one valid JSON object only, with morning_brief as a string and "
                        "tweet_drafts as an array of strings. Do not add markdown or commentary."
                    ),
                },
            ]
            payload = parse_payload(request_completion(repair_messages))
    except Exception as exc:
        print(f"  GLM editorial unavailable ({type(exc).__name__}); newsletter will continue.")
        return "", ["(AI editorial unavailable for this edition.)"]
    brief = str(payload.get("morning_brief") or "").strip()
    drafts = payload.get("tweet_drafts") or []
    if not isinstance(drafts, list):
        drafts = []
    clean_drafts = [str(item).strip()[:280] for item in drafts if str(item).strip()]
    if not clean_drafts:
        clean_drafts = ["(AI tweet drafts unavailable for this edition.)"]
    return brief, clean_drafts[:n]


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


_METRIC_LABELS = {
    "avg_velo_mph": "avg velo",
    "whiff_pct": "whiff%",
    "chase_pct": "chase%",
    "xwoba_on_BIP": "xwOBA/BIP",
    "avg_EV_mph": "avg EV",
    "barrel_pct": "barrel%",
}


def _metric_label(metric: str) -> str:
    if metric.startswith("mix_") and metric.endswith("_pct"):
        return f"{metric[4:-4]} usage"
    return _METRIC_LABELS.get(metric, metric)


def _confidence_tag(a: dict) -> str:
    """How much weight this finding carries, in plain words rather than a z-score."""
    z = abs(float(a.get("z") or 0.0))
    if a.get("fdr_significant") and z >= 4.0:
        return "strong"
    if a.get("fdr_significant"):
        return "solid"
    return "tentative"


def _lead_line(a: dict) -> str:
    """One publishable finding: the stat, its confidence, and any human hinge."""
    name = a.get("player_name") or a.get("player_id")
    bits = [
        f"{name} · {_metric_label(a['metric'])} "
        f"{a['window']} vs {a['baseline']} (Δ{a['delta']:+g}, z={a.get('z')}, {_confidence_tag(a)})",
        f"    {_anomaly_window_phrase(a)}"
        + (f" · n={a.get('n_window')}" if a.get("n_window") else "")
        + (f" · through {a['window_end']}" if a.get("window_end") else "")
        + (f" · {a['persistence_label']}" if a.get("persistence_label") else ""),
    ]
    if a.get("baseline_kind") == "handedness_adjusted":
        counts = a.get("counts") or {}
        holds, outings = counts.get("outings_holding"), counts.get("outings")
        detail = "baseline adjusted for batter handedness"
        if holds and outings:
            detail += f"; held in {holds} of {outings} outings"
        bits.append(f"    {detail}")
    for hit in (a.get("corroboration") or [])[:2]:
        tier = "official" if hit.get("tier") == _scout.TIER_OFFICIAL else "reporting"
        bits.append(f"    ↳ [{tier}] {hit.get('event_type')}: {hit.get('claim')} ({hit.get('url')})")
    return "\n".join(bits)


def _watch_line(a: dict) -> str:
    name = a.get("player_name") or a.get("player_id")
    tail = f" · {a['persistence_label']}" if a.get("persistence_label") else ""
    return (
        f"{name} · {_metric_label(a['metric'])} "
        f"{a['window']} vs {a['baseline']} (z={a.get('z')}){tail}"
    )


def render_digest_plain(report: IntelReport) -> str:
    lines = [
        f"MALLITALYTICS MORNING INTEL | {report.anchor_date}", "",
    ]
    if report.editorial_brief:
        lines += ["THE READ", report.editorial_brief, ""]
    lines += ["TODAY'S LEADS"]
    if report.leads:
        for lead in report.leads:
            lines.append(_lead_line(lead))
    else:
        lines.append("No finding cleared the bar today. That is a result, not a gap.")
    lines += ["", f"WATCH ({len(report.watch)} tracked, not yet publishable)"]
    lines += [_watch_line(a) for a in report.watch[:_WATCH_SHOWN]] or ["(none)"]
    lines += ["", "THE LEADOFF"]
    lines += [f"- {s['title']} — {s['url']}" for s in report.news_stories[:6]] or ["No headlines available."]
    lines += ["", "LAST NIGHT"]
    lines += report.yesterday_results or ["No final games."]
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


# Brand palette (MALLITALYTICS_BRAND.md section 3). Orange is the attention
# colour and appears at most once per edition; it fails contrast as text on the
# cream surface (2.28:1), so it is only ever a fill behind dark ink.
BRAND_INK = "#2E3A43"
BRAND_GREEN = "#4E7B62"
BRAND_OLIVE = "#A5B884"
BRAND_ORANGE = "#F97D34"
BRAND_CREAM = "#F2EFE9"
BRAND_RULE = "#DCD7CC"
BRAND_TRACK = "#E3DFD6"
BRAND_MUTED = "#6F7B84"
# Montserrat is the brand face; email clients that refuse webfonts fall back.
# No quotes in these stacks: a quoted family inside a single-quoted style
# attribute closes the attribute early and the whole declaration is dropped.
FONT_STACK = "Montserrat,Helvetica,Arial,sans-serif"
FONT_DATA = "JetBrains Mono,Menlo,Consolas,monospace"

# Confidence is a status encoding, so every chip carries its word as well as its
# colour. Verified contrast: ink on orange 4.44:1, ink on olive 5.43:1.
_CONFIDENCE_CHIP = {
    "strong": (BRAND_OLIVE, BRAND_INK),
    "solid": (BRAND_CREAM, BRAND_INK),
    "tentative": (BRAND_CREAM, BRAND_MUTED),
}


def _band(title: str, fill: str = BRAND_INK) -> str:
    """A section band. Carries the hierarchy on its own — no decorative eyebrow."""
    return (
        f"<table role='presentation' width='100%' cellspacing='0' cellpadding='0'><tr>"
        f"<td style='background:{fill};padding:7px 14px'>"
        f"<div style=\"font-family:{FONT_STACK};font-size:12px;font-weight:700;"
        f"letter-spacing:.1em;color:{BRAND_CREAM};text-transform:uppercase\">"
        f"{html.escape(title)}</div></td></tr></table>"
    )


def _meter(z: float, max_z: float = 8.0) -> str:
    """
    A thin magnitude bar for |z|, drawn as table cells so Outlook keeps it.

    One hue on a light track, the value labelled in ink beside it — the bar is
    for scanning eight leads at a glance, not for reading a number off.
    """
    pct = max(6, min(100, round(abs(z) / max_z * 100)))
    return (
        "<table role='presentation' cellspacing='0' cellpadding='0' width='100%' "
        "style='margin-top:7px'><tr>"
        f"<td width='{pct}%' style='background:{BRAND_GREEN};height:6px;"
        "font-size:0;line-height:0;border-radius:3px'>&nbsp;</td>"
        f"<td style='background:{BRAND_TRACK};height:6px;font-size:0;line-height:0;"
        "border-radius:3px'>&nbsp;</td>"
        "</tr></table>"
    )


def _lead_rows_html(items: list[dict]) -> str:
    """Publishable findings: the number as hero, then what stands behind it."""
    if not items:
        return (
            f"<tr><td style='padding:18px 16px;font-family:{FONT_STACK};font-size:14px;"
            f"color:{BRAND_MUTED};background:#ffffff;border:1px solid {BRAND_RULE}'>"
            "No finding cleared the bar today. That is a result, not a gap.</td></tr>"
        )
    rows = []
    for idx, item in enumerate(items):
        name = html.escape(str(item.get("player_name") or item.get("player_id") or "Unknown"))
        metric = html.escape(_metric_label(str(item.get("metric") or "")))
        tag = _confidence_tag(item)
        chip_bg, chip_ink = _CONFIDENCE_CHIP[tag]
        # Solid sits on the cream surface, so its outline is what separates it
        # from tentative; fill alone would make the two identical.
        chip_border = BRAND_RULE if tag == "tentative" else BRAND_GREEN
        delta = float(item.get("delta") or 0.0)
        arrow = "&#9650;" if delta > 0 else "&#9660;"
        context = html.escape(_anomaly_window_phrase(item))
        if item.get("n_window"):
            context += f" &middot; n={item['n_window']}"
        if item.get("window_end"):
            context += f" &middot; through {html.escape(str(item['window_end']))}"
        if item.get("persistence_label"):
            context += f" &middot; {html.escape(str(item['persistence_label']))}"
        note = ""
        if item.get("baseline_kind") == "handedness_adjusted":
            counts = item.get("counts") or {}
            holds, outings = counts.get("outings_holding"), counts.get("outings")
            detail = "baseline adjusted for batter handedness"
            if holds and outings:
                detail += f"; held in {holds} of {outings} outings"
            note = (
                f"<div style='font-family:{FONT_STACK};font-size:11px;color:{BRAND_MUTED};"
                f"margin-top:5px'>{detail}</div>"
            )
        hinges = "".join(
            f"<div style='margin-top:7px;padding-left:9px;border-left:2px solid {BRAND_OLIVE};"
            f"font-family:{FONT_STACK};font-size:12px;line-height:1.45;color:{BRAND_INK}'>"
            f"<span style='font-weight:700'>"
            f"{html.escape(str(hit.get('event_type') or 'note'))}</span> &middot; "
            f"{html.escape(str(hit.get('claim') or ''))} "
            f"<a href='{html.escape(str(hit.get('url') or ''), quote=True)}' "
            f"style='color:{BRAND_GREEN};text-decoration:underline'>source</a></div>"
            for hit in (item.get("corroboration") or [])[:2]
        )
        edge = (
            f"border-left:4px solid {BRAND_ORANGE}" if idx == 0
            else f"border-left:1px solid {BRAND_RULE}"
        )
        rows.append(
            f"<tr><td style='padding:0 0 10px'>"
            f"<table role='presentation' width='100%' cellspacing='0' cellpadding='0' "
            f"style='background:#ffffff;border:1px solid {BRAND_RULE};{edge}'>"
            f"<tr><td style='padding:13px 15px'>"
            # name + confidence chip
            f"<table role='presentation' width='100%' cellspacing='0' cellpadding='0'><tr>"
            f"<td style=\"font-family:{FONT_STACK};font-size:15px;font-weight:700;"
            f"color:{BRAND_INK}\">{name}</td>"
            f"<td align='right'><span style=\"font-family:{FONT_STACK};font-size:10px;"
            f"font-weight:700;letter-spacing:.08em;text-transform:uppercase;"
            f"color:{chip_ink};background:{chip_bg};border:1px solid {chip_border};"
            f"padding:3px 8px\">{tag}</span></td>"
            f"</tr></table>"
            # the number as hero
            f"<div style=\"font-family:{FONT_DATA};font-size:21px;font-weight:700;"
            f"color:{BRAND_INK};margin-top:7px\">{html.escape(str(item.get('window')))}"
            f"<span style='font-size:13px;color:{BRAND_MUTED};font-weight:400'> from "
            f"{html.escape(str(item.get('baseline')))}</span></div>"
            f"<div style=\"font-family:{FONT_STACK};font-size:12px;color:{BRAND_INK};"
            f"margin-top:2px\">{arrow} {metric} &middot; "
            f"<span style='font-family:{FONT_DATA}'>"
            f"z={html.escape(str(item.get('z')))}</span></div>"
            f"{_meter(float(item.get('z') or 0.0))}"
            f"<div style='font-family:{FONT_STACK};font-size:11px;color:{BRAND_MUTED};"
            f"margin-top:6px'>{context}</div>"
            f"{note}{hinges}"
            f"</td></tr></table></td></tr>"
        )
    return "".join(rows)


def _watch_rows_html(items: list[dict], limit: int = _WATCH_SHOWN) -> str:
    """Signals held back until they repeat, rather than discarded overnight."""
    if not items:
        return (
            f"<tr><td style='padding:10px 0;font-family:{FONT_STACK};font-size:12px;"
            f"color:{BRAND_MUTED}'>Nothing on watch.</td></tr>"
        )
    rows = []
    for item in items[:limit]:
        tail = (
            f" &middot; {html.escape(str(item.get('persistence_label')))}"
            if item.get("persistence_label") else ""
        )
        rows.append(
            f"<tr><td style='padding:6px 0;border-bottom:1px solid {BRAND_RULE};"
            f"font-family:{FONT_STACK};font-size:12px;line-height:1.4;color:{BRAND_INK}'>"
            f"<strong>{html.escape(str(item.get('player_name') or item.get('player_id')))}"
            f"</strong> &middot; {html.escape(_metric_label(str(item.get('metric') or '')))} "
            f"<span style='font-family:{FONT_DATA}'>"
            f"{html.escape(str(item.get('window')))} from "
            f"{html.escape(str(item.get('baseline')))} &middot; "
            f"z={html.escape(str(item.get('z')))}</span>"
            f"<span style='color:{BRAND_MUTED}'>{tail}</span></td></tr>"
        )
    return "".join(rows)


def _panel_list(items: list[str], empty: str, limit: int = 8) -> str:
    """Compact boxed list for the reference panels."""
    if not items:
        return (
            f"<tr><td style='padding:8px 0;font-family:{FONT_STACK};font-size:12px;"
            f"color:{BRAND_MUTED}'>{html.escape(empty)}</td></tr>"
        )
    return "".join(
        f"<tr><td style='padding:5px 0;border-bottom:1px solid {BRAND_RULE};"
        f"font-family:{FONT_STACK};font-size:12px;line-height:1.4;color:{BRAND_INK}'>"
        f"{html.escape(str(line))}</td></tr>"
        for line in items[:limit]
    )


def _logo_data_uri() -> str:
    """Base64 logo for the saved preview file, which has no CID part to resolve."""
    try:
        import base64
        return "data:image/png;base64," + base64.b64encode(LOGO_EMAIL_PATH.read_bytes()).decode()
    except OSError:
        return ""


def render_digest_html(report: IntelReport, logo_src: str = f"cid:{LOGO_CID}") -> str:
    """
    Private morning newsletter.

    Structure is borrowed from the printed sports-newsletter form the user
    supplied — centred masthead, banded section headers, modular panels. The
    palette is not: that template is primary yellow and red, and the brand runs
    warm and analytical rather than loud, so the bands carry brand ink and the
    orange stays reserved for the strongest finding.
    """
    stories = report.news_stories[:5]
    news_rows = "".join(
        f"<tr><td style='padding:7px 0;border-bottom:1px solid {BRAND_RULE}'>"
        f"<a href='{html.escape(str(story['url']), quote=True)}' "
        f"style=\"font-family:{FONT_STACK};font-size:13px;line-height:1.4;font-weight:600;"
        f"color:{BRAND_INK};text-decoration:none\">{html.escape(str(story['title']))}</a></td></tr>"
        for story in stories
    ) or (
        f"<tr><td style='padding:8px 0;font-family:{FONT_STACK};font-size:12px;"
        f"color:{BRAND_MUTED}'>No MLB headlines retrieved.</td></tr>"
    )

    tweet_rows = "".join(
        f"<tr><td style='padding:8px 0;border-bottom:1px solid {BRAND_RULE};"
        f"font-family:{FONT_STACK};font-size:13px;line-height:1.5;color:{BRAND_INK}'>"
        f"<span style='color:{BRAND_GREEN};font-weight:700'>{idx}.</span> "
        f"{html.escape(tweet)}</td></tr>"
        for idx, tweet in enumerate(report.tweet_drafts[:5], 1)
    ) or (
        f"<tr><td style='padding:8px 0;font-family:{FONT_STACK};font-size:12px;"
        f"color:{BRAND_MUTED}'>No drafts generated.</td></tr>"
    )

    brief_paragraphs = [
        html.escape(paragraph.strip()).replace("\n", "<br>")
        for paragraph in re.split(r"\n\s*\n", report.editorial_brief)
        if paragraph.strip()
    ]
    brief_html = ""
    if brief_paragraphs:
        body = "".join(
            f"<p style=\"margin:0 0 {'9px' if i < len(brief_paragraphs) - 1 else '0'};"
            f"font-family:{FONT_STACK};font-size:14px;line-height:1.6;color:{BRAND_INK}\">"
            f"{para}</p>"
            for i, para in enumerate(brief_paragraphs)
        )
        brief_html = (
            f"<tr><td class='pad' style='padding:18px 26px 0'>"
            f"<table role='presentation' width='100%' cellspacing='0' cellpadding='0' "
            f"style='background:#ffffff;border:1px solid {BRAND_RULE};"
            f"border-left:4px solid {BRAND_ORANGE}'>"
            f"<tr><td style='padding:14px 16px'>{body}</td></tr></table></td></tr>"
        )

    notes_html = ""
    if report.notes:
        notes_html = "".join(
            f"<div style='font-family:{FONT_STACK};font-size:11px;line-height:1.5;"
            f"color:{BRAND_MUTED}'>{html.escape(str(note))}</div>"
            for note in report.notes
        )

    # Built here rather than inline: an f-string expression cannot contain a
    # backslash on Python 3.11, which is the runtime in the API container.
    band_leads = _band("Today's leads")
    band_board = _band("Today's board")
    band_watch = _band("Watch \u00b7 %d tracked" % len(report.watch), BRAND_GREEN)
    band_notebook = _band("Content notebook \u00b7 private")
    counts = (
        f"{len(report.leads)} leads &middot; {len(report.watch)} on watch &middot; "
        f"{len(report.probables_today)} probables"
    )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>@media only screen and (max-width:680px){{.shell{{width:100%!important}}.pad{{padding-left:16px!important;padding-right:16px!important}}.col{{display:block!important;width:100%!important;padding:0 0 16px 0!important}}}}</style>
</head><body style="margin:0;padding:0;background:{BRAND_CREAM}">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:{BRAND_CREAM}">
<tr><td align="center" style="padding:22px 8px">
<table role="presentation" class="shell" width="700" cellspacing="0" cellpadding="0" border="0" style="width:700px;max-width:700px;background:{BRAND_CREAM};border:1px solid {BRAND_RULE}">

<tr><td align="center" style="background:{BRAND_CREAM};padding:26px 26px 18px">
  <img src="{logo_src}" alt="Mallitalytics" width="280" style="display:block;width:280px;max-width:74%;height:auto;border:0">
</td></tr>
<tr><td align="center" style="background:{BRAND_INK};padding:11px 20px">
  <div style="font-family:{FONT_STACK};font-size:14px;font-weight:700;letter-spacing:.24em;color:{BRAND_CREAM};text-transform:uppercase">Morning Intel</div>
  <div style="font-family:{FONT_STACK};font-size:12px;color:{BRAND_OLIVE};margin-top:5px">{html.escape(_email_date(report.anchor_date))}</div>
</td></tr>
<tr><td style="background:{BRAND_ORANGE};height:4px;font-size:0;line-height:0">&nbsp;</td></tr>
<tr><td align="center" style="background:#ffffff;padding:8px 20px;border-bottom:1px solid {BRAND_RULE}">
  <div style="font-family:{FONT_DATA};font-size:11px;color:{BRAND_MUTED}">{counts}</div>
</td></tr>

{brief_html}

<tr><td class="pad" style="padding:18px 26px 0">{band_leads}
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:10px">{_lead_rows_html(report.leads)}</table>
</td></tr>

<tr><td class="pad" style="padding:8px 26px 0">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>
    <td class="col" width="50%" valign="top" style="padding-right:9px">
      {band_watch}
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{_watch_rows_html(report.watch)}</table>
    </td>
    <td class="col" width="50%" valign="top" style="padding-left:9px">
      {_band("Around the league", BRAND_GREEN)}
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{news_rows}</table>
    </td>
  </tr></table>
</td></tr>

<tr><td class="pad" style="padding:16px 26px 0">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>
    <td class="col" width="50%" valign="top" style="padding-right:9px">
      {_band("Last night")}
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{_panel_list(report.yesterday_results, "No final games.", 9)}</table>
    </td>
    <td class="col" width="50%" valign="top" style="padding-left:9px">
      {band_board}
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{_panel_list(report.probables_today, "No games scheduled.", 9)}</table>
    </td>
  </tr></table>
</td></tr>

<tr><td class="pad" style="padding:16px 26px 0">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0"><tr>
    <td class="col" width="50%" valign="top" style="padding-right:9px">
      {_band("Roster wire")}
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{_panel_list(report.transactions, "No moves.", 9)}</table>
    </td>
    <td class="col" width="50%" valign="top" style="padding-left:9px">
      {_band("Milestone radar")}
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{_panel_list(report.milestones, "Nothing close.", 9)}</table>
    </td>
  </tr></table>
</td></tr>

<tr><td class="pad" style="padding:16px 26px 0">{band_notebook}
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:6px">{tweet_rows}</table>
</td></tr>

<tr><td class="pad" style="padding:18px 26px 24px">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-top:1px solid {BRAND_RULE}">
  <tr><td style="padding:12px 0 0">
    {notes_html}
    <div style="font-family:{FONT_STACK};font-size:11px;color:{BRAND_MUTED};margin-top:9px">
      Statcast via the Mallitalytics warehouse, data through {html.escape(report.anchor_date)}.
      Reporting is labelled as such and is not measurement.
    </div>
  </td></tr></table>
</td></tr>

</table></td></tr></table></body></html>"""


def _resend_logo_attachment() -> list[dict]:
    """Resend carries the same mark as an inline attachment keyed by content id."""
    try:
        import base64
        return [{
            "filename": "mallitalytics.png",
            "content": base64.b64encode(LOGO_EMAIL_PATH.read_bytes()).decode(),
            "content_id": LOGO_CID,
            "content_type": "image/png",
        }]
    except OSError:
        return []


def _attach_inline_logo(message: EmailMessage) -> None:
    """Attach the brand mark to the HTML part so `cid:` resolves in the client."""
    try:
        payload = LOGO_EMAIL_PATH.read_bytes()
    except OSError:
        print(f"  Logo missing at {LOGO_EMAIL_PATH}; sending without the mark.")
        return
    html_part = message.get_payload()[-1]
    html_part.add_related(
        payload, maintype="image", subtype="png", cid=f"<{LOGO_CID}>",
        filename="mallitalytics.png",
    )


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
                    "attachments": _resend_logo_attachment(),
                },
                timeout=20,
            )
            if resp.status_code in (200, 201):
                eid = resp.json().get("id")
                print(f"  Resend ok id={eid}")
                email_sent = True
                try:
                    log_notification("morning_intel", "email", recipient, subject, plain_body[:200], "sent", eid)
                except Exception as exc:
                    print(f"  Notification audit warning: {type(exc).__name__}: {exc}")
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
            _attach_inline_logo(message)
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=25) as smtp:
                smtp.login(gmail_user, gmail_password)
                smtp.send_message(message)
            print("  Gmail SMTP ok")
            email_sent = True
            try:
                log_notification(
                    "morning_intel",
                    "email",
                    recipient,
                    subject,
                    plain_body[:200],
                    "sent",
                    "gmail_smtp",
                )
            except Exception as exc:
                print(f"  Notification audit warning: {type(exc).__name__}: {exc}")
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
    name_index = _scout.build_name_index(WAREHOUSE_ROOT / str(season) / "players_registry.json")
    if not name_index:
        report.notes.append("No players_registry.json — news could not be linked to players.")
    report.news_stories, news_error = api_mlb_news(limit=6)
    if news_error:
        report.notes.append(news_error)
    for story in report.news_stories:
        story["player_ids"] = _scout.resolve_players(story.get("title", ""), name_index)
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
            raw = detect_pitcher_anomalies(df, anchor) + detect_batter_anomalies(df, anchor)
            raw = apply_prior_season(raw, season, anchor, stage, report)

            history = _persist.build_history(_persist.load_recent(INTEL_OUT, anchor))
            _persist.apply(raw, history)

            ledger_entries = _scout.read_since(
                SCOUTING_LEDGER,
                datetime.now(timezone.utc) - timedelta(days=14),
            )
            report.scouting_entries = ledger_entries
            _scout.corroborate(raw, ledger_entries)
            if not report.news_stories:
                report.news_stories = _stories_from_ledger(ledger_entries)

            # False-discovery control has to see every test, so rank the full
            # deduped slate before trimming to the snapshot pool.
            scored = rank_unique_anomalies(raw, top=len(raw))
            mark_significance(scored, q=FDR_Q)
            n_sig = sum(1 for a in scored if a["fdr_significant"])
            pool = scored[:_persist.SIGNAL_POOL_SIZE]
            report.signal_pool = pool
            report.leads, report.watch = split_leads_and_watch(pool)
            report.anomalies_pitchers = [a for a in pool if a["role"] == "pitcher"][:25]
            report.anomalies_batters = [a for a in pool if a["role"] == "batter"][:25]
            report.notes.append(
                f"{len(scored)} tests -> {n_sig} survive FDR q={FDR_Q} -> "
                f"{len(report.leads)} leads, {len(report.watch)} watch."
            )
    # Leads and watch are drawn from the whole ranked pool, so a lead outside the
    # per-role top 25 would otherwise render without a name.
    displayed = (
        report.anomalies_pitchers + report.anomalies_batters
        + report.leads + report.watch[:_WATCH_SHOWN]
    )
    ids = list({int(a["player_id"]) for a in displayed})
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
    hydrate_anomaly_names(report.leads, names_map)
    hydrate_anomaly_names(report.watch[:_WATCH_SHOWN], names_map)
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
        report.editorial_brief = ""
        report.tweet_drafts = ["(AI editorial skipped)"]
    else:
        report.editorial_brief, report.tweet_drafts = generate_editorial_glm(findings, n=5)
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
        "leads": report.leads,
        "watch": report.watch,
        # Read back by the next run to measure repeats — not newsletter content.
        "signal_pool": report.signal_pool,
        "editorial_brief": report.editorial_brief,
        "tweet_drafts": report.tweet_drafts,
        "queue_ids": report.queue_ids,
        "notes": report.notes,
    }, indent=2, default=str), encoding="utf-8")
    print(f"  Wrote {snap_path}")
    plain = render_digest_plain(report)
    newsletter_html = render_digest_html(report)
    html_path = INTEL_OUT / f"intel_{anchor.isoformat()}.html"
    # The preview is opened in a browser, where a cid: reference resolves to nothing.
    html_path.write_text(render_digest_html(report, logo_src=_logo_data_uri()), encoding="utf-8")
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
    parser.add_argument(
        "--stage",
        default="all",
        help="Warehouse stage folder, or 'all' to include postseason alongside the regular season.",
    )
    parser.add_argument(
        "--build-priors",
        action="store_true",
        help="Aggregate the season into a prior-season baseline cache, then exit. Run once per season.",
    )
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
    if args.build_priors:
        summary = build_prior_summary(season, args.stage)
        PRIORS_DIR.mkdir(parents=True, exist_ok=True)
        priors_path(season).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Wrote {priors_path(season)} ({len(summary)} player-seasons)")
        return
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
