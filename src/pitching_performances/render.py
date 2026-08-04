"""Branded daily pitching performances table.

Combines pitch-level enriched parquets with feed_live boxscores:
- parquets provide whiffs, CSW, and xwOBA allowed
- raw feeds provide names, teams, opponents, K, BB, IP, ER, H, and batters faced
"""

from __future__ import annotations

import gzip
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from PIL import Image, ImageDraw, ImageFont

try:
    from ..mallitalytics_style import MALLITALYTICS
    from .malli_score import OutingRawMetrics, refine_league_norms, score_outing_dict
except ImportError:
    import sys

    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    from src.mallitalytics_style import MALLITALYTICS
    from src.pitching_performances.malli_score import OutingRawMetrics, refine_league_norms, score_outing_dict

_PITCH_RE = re.compile(r"^game_(\d+)_(\d{8})_pitches_enriched\.parquet$")
_RAW_RE = re.compile(r"^game_(\d+)_(\d{8})_feed_live\.json(?:\.gz)?$")

_WHIFF_DESCS = frozenset({"swinging_strike", "swinging_strike_blocked", "foul_tip"})
_SWING_DESCS = frozenset(
    {
        "foul_bunt",
        "foul",
        "hit_into_play",
        "swinging_strike",
        "foul_tip",
        "swinging_strike_blocked",
        "missed_bunt",
        "bunt_foul_tip",
    }
)
_CSW_DESCS = frozenset({"called_strike", "swinging_strike", "swinging_strike_blocked", "foul_tip"})
_CALLED_STRIKE_DESCS = frozenset({"called_strike"})


@dataclass(frozen=True)
class PitcherBoxLine:
    player_id: int
    player_name: str
    team: str
    opponent: str
    game_pk: int
    pitches: int
    ip: str
    er: int
    strikeouts: int
    walks: int
    hits: int
    home_runs: int
    hit_by_pitch: int
    batters_faced: int
    games_started: int


def _hex_to_rgb(hex_s: str) -> tuple[int, int, int]:
    h = hex_s.strip().lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _lerp(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    t = t * t * (3.0 - 2.0 * t)
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    import sys as _sys

    if _sys.platform == "darwin":
        paths = (
            "/Library/Fonts/Arial Bold.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        ) if bold else (
            "/Library/Fonts/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        )
    elif _sys.platform == "win32":
        paths = ("C:/Windows/Fonts/arialbd.ttf",) if bold else ("C:/Windows/Fonts/arial.ttf",)
    else:
        paths = (
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ) if bold else (
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        )
    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _text_w(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), str(text), font=font)
    return box[2] - box[0]


def _fit_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> str:
    s = str(text)
    if _text_w(draw, s, font) <= max_width:
        return s
    ell = "..."
    while s and _text_w(draw, s + ell, font) > max_width:
        s = s[:-1]
    return (s + ell) if s else ell


def _row_float(row: dict[str, Any], key: str, default: float = math.nan) -> float:
    try:
        raw = row.get(key, default)
        if raw in (None, ""):
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def _row_int(row: dict[str, Any], key: str, default: int | None = None) -> int | None:
    try:
        raw = row.get(key)
        if raw in (None, ""):
            return default
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def _open_json(path: Path) -> dict[str, Any]:
    if path.name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _safe_int(v: Any) -> int:
    try:
        if v is None or v == "":
            return 0
        return int(v)
    except (TypeError, ValueError):
        return 0


def _outs_from_ip(ip_val: str) -> int:
    s = str(ip_val or "0").strip()
    if "." not in s:
        try:
            return int(float(s)) * 3
        except ValueError:
            return 0
    whole, frac = s.split(".", 1)
    try:
        return int(whole) * 3 + min(max(int((frac or "0")[0]), 0), 2)
    except ValueError:
        return 0


def _row_outs(row: dict[str, Any]) -> int:
    direct = _row_int(row, "outs")
    if direct is not None:
        return direct
    summary = str(row.get("summary") or "")
    ip_part = summary.split(",", 1)[0].replace(" IP", "").strip()
    return _outs_from_ip(ip_part)


def _norm_desc(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.strip().str.lower().str.replace(" ", "_", regex=False).str.replace(",", "", regex=False)
    return out.where(~out.str.contains("in_play|hit_into_play", na=False, regex=True), "hit_into_play")


def _date_token(path: Path, pattern: re.Pattern[str]) -> str | None:
    m = pattern.match(path.name)
    return m.group(2) if m else None


def latest_common_date(warehouse: Path, season: int) -> str:
    common = common_dates_for_season(warehouse, season)
    if not common:
        raise FileNotFoundError(
            f"No common raw/enriched regular-season dates under {warehouse / str(season) / 'regular_season'}"
        )
    return common[-1]


def common_dates_for_season(warehouse: Path, season: int) -> list[str]:
    """YYYY-MM-DD dates with both feed_live raw and pitches_enriched parquets."""
    base = warehouse / str(season) / "regular_season"
    pq_dates = {
        d
        for p in (base / "pitches_enriched").glob("game_*_pitches_enriched.parquet")
        if (d := _date_token(p, _PITCH_RE))
    }
    raw_dates = {
        d
        for p in (base / "raw").glob("game_*_feed_live.json*")
        if (d := _date_token(p, _RAW_RE))
    }
    return [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in sorted(pq_dates & raw_dates)]


def _raw_paths_for_date(raw_dir: Path, ymd: str) -> list[Path]:
    paths = sorted(raw_dir.glob(f"game_*_{ymd}_feed_live.json*"))
    by_game: dict[str, Path] = {}
    for p in paths:
        m = _RAW_RE.match(p.name)
        if not m:
            continue
        game_pk = m.group(1)
        cur = by_game.get(game_pk)
        if cur is None or (p.suffix == ".json" and cur.name.endswith(".gz")):
            by_game[game_pk] = p
    return sorted(by_game.values())


def _pitch_paths_for_date(pitch_dir: Path, ymd: str) -> list[Path]:
    return sorted(pitch_dir.glob(f"game_*_{ymd}_pitches_enriched.parquet"))


def _box_lines_for_date(raw_dir: Path, ymd: str) -> dict[tuple[int, int], PitcherBoxLine]:
    out: dict[tuple[int, int], PitcherBoxLine] = {}
    for path in _raw_paths_for_date(raw_dir, ymd):
        m = _RAW_RE.match(path.name)
        if not m:
            continue
        game_pk = int(m.group(1))
        feed = _open_json(path)
        gd = feed.get("gameData") or {}
        teams = gd.get("teams") or {}
        away_abbr = str((teams.get("away") or {}).get("abbreviation") or "?").upper()
        home_abbr = str((teams.get("home") or {}).get("abbreviation") or "?").upper()
        box = ((feed.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
        for side, opponent in (("away", home_abbr), ("home", away_abbr)):
            team = box.get(side) or {}
            team_abbr = away_abbr if side == "away" else home_abbr
            for player in (team.get("players") or {}).values():
                person = player.get("person") or {}
                pid = person.get("id")
                pit = ((player.get("stats") or {}).get("pitching") or {})
                if not pid or not pit:
                    continue
                pitches = _safe_int(pit.get("numberOfPitches"))
                if pitches <= 0:
                    continue
                line = PitcherBoxLine(
                    player_id=int(pid),
                    player_name=str(person.get("fullName") or ""),
                    team=team_abbr,
                    opponent=opponent,
                    game_pk=game_pk,
                    pitches=pitches,
                    ip=str(pit.get("inningsPitched") or "0.0"),
                    er=_safe_int(pit.get("earnedRuns")),
                    strikeouts=_safe_int(pit.get("strikeOuts")),
                    walks=_safe_int(pit.get("baseOnBalls")),
                    hits=_safe_int(pit.get("hits")),
                    home_runs=_safe_int(pit.get("homeRuns")),
                    hit_by_pitch=_safe_int(pit.get("hitBatsmen")),
                    batters_faced=_safe_int(pit.get("battersFaced")),
                    games_started=_safe_int(pit.get("gamesStarted")),
                )
                out[(game_pk, int(pid))] = line
    return out


def _pitch_metrics_for_date(pitch_dir: Path, ymd: str) -> dict[tuple[int, int], dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    needed = [
        "game_pk",
        "pitcher",
        "description",
        "at_bat_number",
        "pitch_number",
        "zone",
        "estimated_woba_using_speedangle",
        "woba_value",
        "woba_denom",
    ]
    for path in _pitch_paths_for_date(pitch_dir, ymd):
        df = pd.read_parquet(path)
        df = df[[c for c in needed if c in df.columns]]
        frames.append(df)
    if not frames:
        return {}
    df = pd.concat(frames, ignore_index=True)
    df = df[df["pitcher"].notna() & df["game_pk"].notna()].copy()
    df["game_pk"] = pd.to_numeric(df["game_pk"], errors="coerce")
    df["pitcher"] = pd.to_numeric(df["pitcher"], errors="coerce")
    df = df[df["game_pk"].notna() & df["pitcher"].notna()]
    df["description"] = _norm_desc(df["description"])
    df["is_whiff"] = df["description"].isin(_WHIFF_DESCS)
    df["is_swing"] = df["description"].isin(_SWING_DESCS)
    df["is_csw"] = df["description"].isin(_CSW_DESCS)
    df["is_called_strike"] = df["description"].isin(_CALLED_STRIKE_DESCS)
    zone = pd.to_numeric(df["zone"], errors="coerce") if "zone" in df.columns else pd.Series(pd.NA, index=df.index)
    df["out_zone"] = zone >= 11
    df["is_chase"] = df["out_zone"] & df["is_swing"]
    if "estimated_woba_using_speedangle" not in df.columns:
        df["estimated_woba_using_speedangle"] = pd.NA
    if "woba_value" not in df.columns:
        df["woba_value"] = pd.NA
    if "woba_denom" not in df.columns:
        df["woba_denom"] = pd.NA
    grouped = df.groupby(["game_pk", "pitcher"], sort=False).agg(
        pitches=("description", "size"),
        whiffs=("is_whiff", "sum"),
        swings=("is_swing", "sum"),
        csw=("is_csw", "sum"),
        called_strikes=("is_called_strike", "sum"),
        out_zone=("out_zone", "sum"),
        chases=("is_chase", "sum"),
    )
    xwoba_by_key: dict[tuple[int, int], tuple[float, int, int]] = {}
    if {"at_bat_number", "pitch_number"}.issubset(df.columns):
        pa = df.sort_values(["game_pk", "pitcher", "at_bat_number", "pitch_number"]).groupby(
            ["game_pk", "pitcher", "at_bat_number"],
            dropna=False,
        ).tail(1)
        xw = pd.to_numeric(pa["estimated_woba_using_speedangle"], errors="coerce")
        woba = pd.to_numeric(pa["woba_value"], errors="coerce")
        denom = pd.to_numeric(pa["woba_denom"], errors="coerce").fillna(0)
        pa = pa.assign(_pa_xwoba=xw.where(xw.notna(), woba.where(denom > 0)))
        for (game_pk, pitcher), sub in pa.groupby(["game_pk", "pitcher"], sort=False):
            vals = pd.to_numeric(sub["_pa_xwoba"], errors="coerce").dropna()
            if len(vals):
                damage = int((vals >= 0.350).sum())
                xwoba_by_key[(int(game_pk), int(pitcher))] = (float(vals.mean()), int(len(vals)), damage)

    out: dict[tuple[int, int], dict[str, Any]] = {}
    for (game_pk, pitcher), row in grouped.iterrows():
        pitches = int(row["pitches"])
        whiffs = int(row["whiffs"])
        csw = int(row["csw"])
        called_strikes = int(row["called_strikes"])
        out_zone = int(row["out_zone"])
        chases = int(row["chases"])
        key = (int(game_pk), int(pitcher))
        xwoba_allowed, xwoba_pa, damage_pa = xwoba_by_key.get(key, (math.nan, 0, 0))
        out[key] = {
            "pitches": pitches,
            "whiffs": whiffs,
            "swstr_pct": (whiffs / pitches * 100.0) if pitches else math.nan,
            "csw": csw,
            "called_strikes": called_strikes,
            "called_strike_pct": (called_strikes / pitches * 100.0) if pitches else math.nan,
            "csw_pct": (csw / pitches * 100.0) if pitches else math.nan,
            "out_zone": out_zone,
            "chases": chases,
            "chase_pct": (chases / out_zone * 100.0) if out_zone else math.nan,
            "xwoba_allowed": xwoba_allowed,
            "xwoba_pa": xwoba_pa,
            "damage_pa": damage_pa,
            "damage_pct": damage_pa / xwoba_pa * 100.0 if xwoba_pa else math.nan,
        }
    return out


def _outing_raw(metric: dict[str, Any], box: PitcherBoxLine, official_pitches: int) -> OutingRawMetrics | None:
    outs = _outs_from_ip(box.ip)
    ip = outs / 3.0 if outs else 0.0
    swstr_pct = float(metric["whiffs"]) / official_pitches * 100.0 if official_pitches else math.nan
    called_strike_pct = float(metric.get("called_strike_pct", math.nan))
    chase_pct = float(metric.get("chase_pct", math.nan))
    xwoba_allowed = float(metric.get("xwoba_allowed", math.nan))
    game_whip = (box.hits + box.walks) / ip if ip else math.nan
    if not all(math.isfinite(v) for v in (swstr_pct, called_strike_pct, xwoba_allowed, game_whip)):
        return None
    if not math.isfinite(chase_pct):
        chase_pct = float("nan")
    return OutingRawMetrics(
        swstr_pct=swstr_pct,
        called_strike_pct=called_strike_pct,
        chase_pct=chase_pct,
        xwoba_allowed=xwoba_allowed,
        game_whip=game_whip,
        earned_runs=int(box.er),
        home_runs=int(box.home_runs),
        pitches=official_pitches,
        outs=outs,
        batters_faced=box.batters_faced,
        hits=box.hits,
        walks=box.walks,
        hit_by_pitch=box.hit_by_pitch,
    )


def build_pitching_performance_rows(
    warehouse: Path,
    *,
    date_str: str | None = None,
    season: int | None = None,
    min_pitches: int = 30,
    top_n: int = 30,
) -> tuple[str, list[dict[str, Any]]]:
    if season is None:
        season = datetime.now().year
    if date_str is None:
        date_str = latest_common_date(warehouse, season)
    ymd = date_str.replace("-", "")
    base = warehouse / str(season) / "regular_season"
    raw_dir = base / "raw"
    pitch_dir = base / "pitches_enriched"
    box_lines = _box_lines_for_date(raw_dir, ymd)
    metrics = _pitch_metrics_for_date(pitch_dir, ymd)

    candidates: list[tuple[tuple[int, int], dict[str, Any], PitcherBoxLine, int]] = []
    raw_samples: list[OutingRawMetrics] = []
    for key, metric in metrics.items():
        box = box_lines.get(key)
        if not box or int(metric["pitches"]) < min_pitches:
            continue
        official_pitches = box.pitches or int(metric["pitches"])
        raw = _outing_raw(metric, box, official_pitches)
        if raw is None:
            continue
        candidates.append((key, metric, box, official_pitches))
        raw_samples.append(raw)

    norms = refine_league_norms(raw_samples)

    rows: list[dict[str, Any]] = []
    for _key, metric, box, official_pitches in candidates:
        kbb_pct = (box.strikeouts - box.walks) / box.batters_faced * 100.0 if box.batters_faced else math.nan
        xwoba_allowed = float(metric["xwoba_allowed"])
        chase_pct = float(metric["chase_pct"])
        components = score_outing_dict(metric, box, official_pitches, norms)
        malli_score = float(components["malli_score"])
        hr_part = f", {box.home_runs} HR" if box.home_runs else ""
        rows.append(
            {
                "player_id": box.player_id,
                "pitcher": box.player_name,
                "team": box.team,
                "opponent": box.opponent,
                "game_pk": box.game_pk,
                "pitches": official_pitches,
                "outs": _outs_from_ip(box.ip),
                "whiffs": int(metric["whiffs"]),
                "swstr_pct": components["swstr_pct"],
                "called_strikes": int(metric.get("called_strikes", 0)),
                "called_strike_pct": components["called_strike_pct"],
                "csw": int(metric["csw"]),
                "csw_pct": components["csw_pct"],
                "kbb_pct": kbb_pct,
                "k_pct": components["k_pct"],
                "bb_pct": components["bb_pct"],
                "hr_pct": components["hr_pct"],
                "game_era": components["game_era"],
                "game_whip": components["game_whip"],
                "hit_by_pitch": components["hit_by_pitch"],
                "batters_faced": components["batters_faced"],
                "reach_rate_allowed": components["reach_rate_allowed"],
                "malli_score_version": components["malli_score_version"],
                "dominance_score": round(components["dominance_score"], 1),
                "run_prevention_score": round(components["run_prevention_score"], 1),
                "core_score": round(components["core_score"], 1),
                "process_score": round(components["process_score"], 1),
                "result_score": round(components["result_score"], 1),
                "workload": round(components["workload"], 3),
                "chases": int(metric["chases"]),
                "out_zone": int(metric["out_zone"]),
                "chase_pct": chase_pct,
                "xwoba_allowed": xwoba_allowed,
                "xwoba_pa": int(metric["xwoba_pa"]),
                "damage_pa": int(metric["damage_pa"]),
                "damage_pct": float(metric["damage_pct"]),
                "home_runs": box.home_runs,
                "malli_score_sort": malli_score,
                "malli_score": round(malli_score, 1),
                "malli_score_v3": round(components["malli_score_v3"], 1),
                "malli_score_v2": round(components["malli_score_v2"], 1),
                "summary": (
                    f"{box.ip} IP, {box.er} ER, {box.strikeouts} K, "
                    f"{box.walks} BB, {box.hits} H{hr_part}"
                ),
            }
        )

    rows.sort(
        key=lambda r: (
            r.get("malli_score_sort", r["malli_score"]),
            -r["xwoba_allowed"] if math.isfinite(r["xwoba_allowed"]) else -99,
            r["kbb_pct"] if math.isfinite(r["kbb_pct"]) else -99,
            r["whiffs"],
        ),
        reverse=True,
    )
    rows = rows[:top_n]
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return date_str, rows


def _metric_fill(
    value: float,
    values: list[float],
    *,
    low: tuple[int, int, int],
    high: tuple[int, int, int],
    lower_is_better: bool = False,
) -> tuple[int, int, int]:
    if not math.isfinite(value):
        return low
    clean = [v for v in values if math.isfinite(v)]
    if not clean:
        return low
    lo, hi = min(clean), max(clean)
    t = 0.5 if hi <= lo else (value - lo) / (hi - lo)
    if lower_is_better:
        t = 1.0 - t
    return _lerp(low, high, t)


def render_pitching_performance_table(
    rows: list[dict[str, Any]],
    date_str: str,
    out_path: Path,
    *,
    generated_on: str | None = None,
) -> Path:
    width = 1400
    row_h = 38
    header_h = 142
    footer_h = 52
    table_header_h = 36
    height = header_h + table_header_h + row_h * max(1, len(rows)) + footer_h

    brand = {k: _hex_to_rgb(v) for k, v in MALLITALYTICS.items()}
    bg = brand["warm_cream"]
    text = brand["dark_teal"]
    slate = brand["slate"]
    light_green = brand["light_green"]
    orange = brand["burnt_orange"]
    off_white = brand["off_white"]
    row_alt = _lerp(bg, off_white, 0.64)
    grid = _lerp(slate, bg, 0.62)

    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)
    font_title = _load_font(42, bold=True)
    font_subtitle = _load_font(20)
    font_header = _load_font(17, bold=True)
    font_cell = _load_font(18)
    font_cell_bold = _load_font(18, bold=True)
    font_footer = _load_font(15)

    try:
        day_label = datetime.strptime(date_str, "%Y-%m-%d").strftime("%b %-d, %Y")
    except ValueError:
        day_label = date_str

    margin = 42
    draw.rectangle((0, 0, width, 96), fill=text)
    draw.rectangle((0, 96, width, 104), fill=orange)
    draw.text((margin, 28), "Pitching Index", fill=off_white, font=font_title)
    subtitle = f"{day_label} · MalliScore = dominance + run prevention + workload"
    draw.text((margin, 108), subtitle, fill=slate, font=font_subtitle)

    cols = [
        ("Rank", 58, "right"),
        ("Pitcher", 250, "left"),
        ("Opp.", 86, "center"),
        ("Outs", 62, "right"),
        ("Pit", 62, "right"),
        ("Whiffs", 72, "right"),
        ("Chase%", 78, "right"),
        ("Dom", 74, "right"),
        ("Run", 74, "right"),
        ("Work", 72, "right"),
        ("MalliScore", 102, "right"),
        ("Line", 326, "left"),
    ]
    x_positions = [margin]
    for _, col_w, _ in cols[:-1]:
        x_positions.append(x_positions[-1] + col_w)

    y = header_h
    draw.rectangle((margin - 14, y, width - margin + 14, y + table_header_h), fill=off_white)
    for (label, col_w, align), x in zip(cols, x_positions):
        tx = x + 8
        if align == "right":
            tx = x + col_w - 8 - _text_w(draw, label, font_header)
        elif align == "center":
            tx = x + (col_w - _text_w(draw, label, font_header)) // 2
        draw.text((tx, y + 9), label, fill=text, font=font_header)
    draw.line((margin - 14, y + table_header_h, width - margin + 14, y + table_header_h), fill=text, width=3)

    chase_values = [_row_float(r, "chase_pct") for r in rows]
    dominance_values = [_row_float(r, "dominance_score") for r in rows]
    run_values = [_row_float(r, "run_prevention_score") for r in rows]
    workload_values = [_row_float(r, "workload") for r in rows]
    score_values = [_row_float(r, "malli_score") for r in rows]
    low_fill = _lerp(off_white, bg, 0.45)
    high_fill = _lerp(orange, light_green, 0.32)

    y += table_header_h
    for idx, row in enumerate(rows):
        fill = row_alt if idx % 2 else bg
        draw.rectangle((margin - 14, y, width - margin + 14, y + row_h), fill=fill)

        metric_cols = {
            "chase_pct": 6,
            "dominance_score": 7,
            "run_prevention_score": 8,
            "workload": 9,
            "malli_score": 10,
        }
        for key, col_idx in metric_cols.items():
            value = _row_float(row, key)
            if key == "chase_pct":
                values = chase_values
            elif key == "dominance_score":
                values = dominance_values
            elif key == "run_prevention_score":
                values = run_values
            elif key == "workload":
                values = workload_values
            else:
                values = score_values
            x = x_positions[col_idx]
            col_w = cols[col_idx][1]
            draw.rectangle(
                (x + 2, y + 2, x + col_w - 2, y + row_h - 2),
                fill=_metric_fill(value, values, low=low_fill, high=high_fill),
            )

        chase_pct = _row_float(row, "chase_pct")
        dominance_score = _row_float(row, "dominance_score")
        run_prevention_score = _row_float(row, "run_prevention_score")
        workload = _row_float(row, "workload")
        malli_score = _row_float(row, "malli_score")
        summary = str(row.get("summary") or "")
        values = [
            str(row["rank"]),
            f"{row['pitcher']} ({row['team']})",
            f"vs {row['opponent']}",
            str(_row_outs(row)),
            str(_row_int(row, "pitches", 0) or "--"),
            str(_row_int(row, "whiffs", 0) or "--"),
            f"{chase_pct:.1f}%" if math.isfinite(chase_pct) else "--",
            f"{dominance_score:.1f}" if math.isfinite(dominance_score) else "--",
            f"{run_prevention_score:.1f}" if math.isfinite(run_prevention_score) else "--",
            f"{workload:.2f}x" if math.isfinite(workload) else "--",
            f"{malli_score:.1f}" if math.isfinite(malli_score) else "--",
            summary,
        ]
        for (value, (label, col_w, align), x) in zip(values, cols, x_positions):
            font = font_cell_bold if label in {"Pitcher", "Dom", "Run", "Work", "MalliScore"} else font_cell
            value = _fit_text(draw, value, font, col_w - 14)
            color = text
            tx = x + 8
            if align == "right":
                tx = x + col_w - 8 - _text_w(draw, value, font)
            elif align == "center":
                tx = x + (col_w - _text_w(draw, value, font)) // 2
            draw.text((tx, y + 9), value, fill=color, font=font)
        draw.line((margin - 14, y + row_h, width - margin + 14, y + row_h), fill=grid, width=1)
        y += row_h

    if not rows:
        draw.text((margin, y + 24), "No qualifying pitching lines found.", fill=slate, font=font_cell)

    handle = "@Mallitalytics"
    handle_w = _text_w(draw, handle, font_footer)
    footer_y = height - 34
    footer = (
        f"Data: MLB · Slate: {date_str} · Dom = whiffs/called strikes/chase/weak contact · "
        "Run = WHIP/ER/HR · Work = outs + pitch efficiency"
    )
    footer = _fit_text(draw, footer, font_footer, width - (margin * 2) - handle_w - 28)
    draw.text((margin, footer_y), footer, fill=slate, font=font_footer)
    draw.text((width - margin - handle_w, footer_y), handle, fill=slate, font=font_footer)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path
