"""
Mallitalytics Daily Batter Card
===============================

Game-level batter performance card matching the Daily Pitcher Card aesthetic.
Consumes a `feed_live` JSON from the Mallitalytics MLB warehouse (or a manual
path) and outputs a 1200x675 PNG in either light or dark mode.

Sections:
  - Header:  circular headshot, game-line headline, xwOBA hero stat
  - Left:    Gaussian pitch heat map + pitch-mix strip
  - Center:  PA Log (event-by-event with EV + contact zone)
  - Right:   Batted Ball table (EV, LA, dist, trajectory, xBA)
  - Footer:  Sabermetric tile cards (BB%, K%, bat speed, swing len, RE24)

CLI (examples)
--------------
  python scripts/batter_card_daily.py --batter 656305 --feed data/warehouse/mlb/2024/regular_season/raw/game_746255_20240921_feed_live.json
      Generate a single card for batter 656305 from a specific raw feed.

  python scripts/batter_card_daily.py --batters 656305,641646 --date 2024-09-21
      Auto-locate `feed_live` raws for that date under data/warehouse/mlb and
      generate one card per batter.

  python scripts/batter_card_daily.py --batters 656305 --date yesterday --dark
      Same as above but for yesterday's games and dark / analytics theme.
"""

import argparse
import json
import os
import sys
import re
from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

if "MPLBACKEND" not in os.environ:
    os.environ["MPLBACKEND"] = "Agg"
if "MPLCONFIGDIR" not in os.environ:
    os.environ["MPLCONFIGDIR"] = "/tmp/mallitalytics_mpl"
import matplotlib
matplotlib.use(os.environ.get("MPLBACKEND", "Agg"))
import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
import seaborn as sns
import requests
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import FancyBboxPatch
from PIL import Image

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))
from src.mlb_headshot import neutralize_mlb_headshot_background

_parser = argparse.ArgumentParser(description="Mallitalytics Daily Batter Card")
_parser.add_argument(
    "--dark",
    action="store_true",
    help="Render in dark / analytics mode (default: light).",
)
_parser.add_argument(
    "--feed",
    type=str,
    default=None,
    help="Explicit path to game_*_feed_live.json (skips warehouse auto-discovery).",
)
_parser.add_argument(
    "--batter",
    type=int,
    default=None,
    help="Single batter ID (MLB playerId).",
)
_parser.add_argument(
    "--batters",
    type=str,
    default=None,
    help="Comma-separated batter IDs, e.g. 656305,641646. Requires --date when used.",
)
_parser.add_argument(
    "--date",
    type=str,
    default=None,
    help="Game date: 'yesterday' or YYYY-MM-DD. Used with --batters, or to improve filenames.",
)
_parser.add_argument(
    "--parquet",
    type=str,
    default=None,
    help="Path to game_*_*_pitches_enriched.parquet (with --batter) when feed_live is missing.",
)
_parser.add_argument(
    "--output-suffix",
    type=str,
    default=None,
    help="Optional token appended to PNG stem to avoid concurrent overwrites.",
)
_args, _ = _parser.parse_known_args()

# ─────────────────────── WBC FLAG SUPPORT ───────────────────────────────────
# WBC team abbreviation → ISO 3166-1 alpha-2 code for flagcdn.com
_WBC_FLAG_ISO = {
    "DOM": "do", "DR":  "do",
    "NED": "nl",
    "PUR": "pr",
    "USA": "us",
    "MEX": "mx",
    "VEN": "ve",
    "CUB": "cu",
    "PAN": "pa",
    "COL": "co",
    "GBR": "gb", "GRB": "gb",
    "ITA": "it",
    "NIC": "ni",
    "ISR": "il",
    "BRA": "br",
    "AUS": "au",
    "KOR": "kr",
    "JPN": "jp",
    "TPE": "tw",
    "CZE": "cz",
    "CAN": "ca",
}


def _fetch_flag_image(team_abbrev: str):
    """Download flag PNG for a WBC team abbreviation; return PIL Image or None."""
    iso = _WBC_FLAG_ISO.get((team_abbrev or "").upper().strip())
    if not iso:
        return None
    try:
        url = f"https://flagcdn.com/w160/{iso}.png"
        r = requests.get(url, timeout=10)
        if not r.ok or len(r.content) < 500:
            return None
        return Image.open(BytesIO(r.content)).convert("RGBA")
    except Exception:
        return None


# ─────────────────────────────── BRAND PALETTE ──────────────────────────────
# Matches mallitalytics_daily_card.py exactly for full visual consistency.

_PALETTE_DARK = {
    "card_bg":        "#1A2530",
    "header_bg":      "#1E3448",
    "panel_bg":       "#1F2E3D",
    "table_bg":       "#162030",
    "table_alt":      "#1A2838",
    "text_primary":   "#F5F2ED",
    "text_secondary": "#C8DCE8",
    "text_lo":        "#8FA3B8",
    "accent_orange":  "#E8712B",
    "accent_green":   "#66BB6A",
    "accent_red":     "#E74C3C",
    "accent_gold":    "#F0A830",
    "grid":           "#2C3E50",
    "border":         "#2E4A62",
    "zone_edge":      "#7A9AB5",
}

_PALETTE_LIGHT = {
    "card_bg":        "#F4F6F5",
    "header_bg":      "#FBFCFA",
    "panel_bg":       "#FFFFFF",
    "table_bg":       "#FFFFFF",
    "table_alt":      "#F7F9F8",
    "text_primary":   "#18313A",
    "text_secondary": "#52666D",
    "text_lo":        "#8B9AA0",
    "accent_orange":  "#C85F21",
    "accent_green":   "#2F8F5B",
    "accent_red":     "#C94436",
    "accent_gold":    "#B8872D",
    "grid":           "#E5EBE8",
    "border":         "#D3DDD9",
    "zone_edge":      "#60777F",
}

LIGHT_MODE = not _args.dark

PALETTE = _PALETTE_LIGHT if LIGHT_MODE else _PALETTE_DARK

BG        = PALETTE["card_bg"]
PANEL_BG  = PALETTE["panel_bg"]
TEXT_MAIN = PALETTE["text_primary"]
TEXT_SUB  = PALETTE["text_lo"]
ACCENT    = PALETTE["accent_orange"]
GREEN     = PALETTE["accent_green"]

HEAT_CMAP = (
    LinearSegmentedColormap.from_list(
        "mall_heat_light",
        ["#F1F3F5", "#C8E6C9", "#66BB6A", "#F0A830", "#E8712B", "#E74C3C"],
    ) if LIGHT_MODE else
    LinearSegmentedColormap.from_list(
        "mall_heat_dark",
        ["#1A2530", "#2E7D32", "#66BB6A", "#F0A830", "#E8712B", "#E74C3C"],
    )
)

# Pitch-type dot colors — distinct palette for light vs dark mode
_PITCH_COLORS_LIGHT = {
    "4-Seam Fastball": "#C53030",
    "Sinker":          "#C05621",
    "Cutter":          "#B7791F",
    "Slider":          "#276749",
    "Sweeper":         "#1D4044",
    "Curveball":       "#2A4365",
    "Changeup":        "#553C9A",
    "Splitter":        "#B83280",
    "Knuckle Curve":   "#2C7A7B",
}
_PITCH_COLORS_DARK = {
    "4-Seam Fastball": "#FC8181",
    "Sinker":          "#F6AD55",
    "Cutter":          "#F6E05E",
    "Slider":          "#68D391",
    "Sweeper":         "#4FD1C5",
    "Curveball":       "#63B3ED",
    "Changeup":        "#B794F4",
    "Splitter":        "#F687B3",
    "Knuckle Curve":   "#76E4F7",
}
PITCH_COLORS = _PITCH_COLORS_LIGHT if LIGHT_MODE else _PITCH_COLORS_DARK

_PITCH_ABBREV_MAP = {
    "4-Seam Fastball":    "FF", "Four-Seam Fastball": "FF",
    "Sinker":             "SI", "Two-Seam Fastball":  "SI",
    "Cutter":             "FC",
    "Slider":             "SL", "Sweeper":            "ST",
    "Changeup":           "CH", "Split-Finger":       "FS",
    "Curveball":          "CU", "Knuckle Curve":      "KC",
    "Splitter":           "FS", "Eephus":             "EP",
    "Screwball":          "SC",
}

def _pitch_abbrev(pt: str | None) -> str:
    if not pt or not isinstance(pt, str):
        return "?"
    return _PITCH_ABBREV_MAP.get(pt, pt[:2].upper())

ESPN_LOGOS = {
    "ARI": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos", "CHC": "chc",
    "CWS": "chw", "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det",
    "HOU": "hou", "KC":  "kc",  "LAA": "laa", "LAD": "lad", "MIA": "mia",
    "MIL": "mil", "MIN": "min", "NYM": "nym", "NYY": "nyy", "OAK": "oak",
    "PHI": "phi", "PIT": "pit", "SD":  "sd",  "SEA": "sea", "SF":  "sf",
    "STL": "stl", "TB":  "tb",  "TEX": "tex", "TOR": "tor", "WSH": "wsh",
}

# ──────────────────────────────── WAREHOUSE HELPERS ─────────────────────────

def _strip_env_quotes(raw: str) -> str:
    s = raw.strip()
    if len(s) >= 2 and ((s[0] == s[-1] == '"') or (s[0] == s[-1] == "'")):
        return s[1:-1].strip()
    return s


def _env_warehouse_is_doc_placeholder(raw: str) -> bool:
    s = raw.replace("\\", "/").strip().lower()
    if not s:
        return False
    if "path/to/your" in s:
        return True
    if "path/to/local" in s and "mirror" in s:
        return True
    return False


def _safe_exists_warehouse(path: Path) -> bool:
    try:
        return path.exists()
    except (OSError, TimeoutError):
        return False


def _warehouse_root() -> Path:
    """
    MLB warehouse root — respects MLB_WAREHOUSE_DIR env var (Google Drive mirror).
    Falls back to <repo>/data/warehouse/mlb. Matches mlbops api.paths.get_warehouse_dir rules.
    """
    raw = _strip_env_quotes(os.environ.get("MLB_WAREHOUSE_DIR", "").strip())
    if raw and not _env_warehouse_is_doc_placeholder(raw):
        return Path(raw).expanduser().resolve()
    return _PARENT / "data" / "warehouse" / "mlb"


def _parse_date_arg(raw: str | None):
    """Parse CLI --date into a date object (supports 'yesterday')."""
    if not raw:
        return None
    raw = raw.strip().lower()
    if raw == "yesterday":
        return (datetime.now() - timedelta(days=1)).date()
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit("Invalid --date: use 'yesterday' or YYYY-MM-DD'")


def find_feed_for_batter_on_date(
    batter_id: int,
    target_date,
    warehouse_root: Path | None = None,
) -> Path:
    """
    Search data/warehouse/mlb recursively for a game_*_{YYYYMMDD}_feed_live.json
    that contains this batter in allPlays.
    """
    if warehouse_root is None:
        warehouse_root = _warehouse_root()
    if not _safe_exists_warehouse(warehouse_root):
        raise FileNotFoundError(
            f"Warehouse root not found or not reachable: {warehouse_root} "
            "(sync to data/warehouse/mlb or open the Drive folder in Finder if using File Stream)."
        )

    date_str = target_date.strftime("%Y%m%d")
    pattern = f"game_*_{date_str}_feed_live.json"
    candidates = sorted(warehouse_root.rglob(pattern))
    if not candidates:
        raise FileNotFoundError(
            f"No feed_live raws found for date {target_date} under {warehouse_root}"
        )

    for path in candidates:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        all_plays = (
            data.get("liveData", {})
            .get("plays", {})
            .get("allPlays", [])
        )
        for play in all_plays:
            mid = play.get("matchup", {}).get("batter", {}).get("id")
            if mid == batter_id:
                return path

    raise FileNotFoundError(
        f"Batter {batter_id} not found in any feed_live for {target_date} "
        f"under {warehouse_root}"
    )


def find_parquet_for_batter_on_date(
    batter_id: int,
    target_date,
    warehouse_root: Path | None = None,
) -> Path:
    """Locate a pitches_enriched parquet for ``target_date`` that includes this batter."""
    if warehouse_root is None:
        warehouse_root = _warehouse_root()
    if not _safe_exists_warehouse(warehouse_root):
        raise FileNotFoundError(
            f"Warehouse root not found or not reachable: {warehouse_root}"
        )
    date_str = target_date.strftime("%Y%m%d")
    pattern = f"game_*_{date_str}_pitches_enriched.parquet"
    candidates = sorted(warehouse_root.rglob(pattern))
    if not candidates:
        raise FileNotFoundError(
            f"No pitches_enriched parquets for date {target_date} under {warehouse_root}"
        )
    for path in candidates:
        try:
            sm = pd.read_parquet(path, columns=["batter"])
            if int(batter_id) in sm["batter"].dropna().astype(int).unique():
                return path
        except Exception:
            continue
    raise FileNotFoundError(
        f"Batter {batter_id} not found in any pitches_enriched for {target_date}"
    )


_STATCAST_EVENT_TO_RESULT = {
    "single": "Single",
    "double": "Double",
    "triple": "Triple",
    "home_run": "Home Run",
    "strikeout": "Strikeout",
    "walk": "Walk",
    "intent_walk": "Intent Walk",
    "hit_by_pitch": "Hit By Pitch",
    "sac_fly": "Sac Fly",
    "sac_bunt": "Sac Bunt",
    "force_out": "Force Out",
    "grounded_into_double_play": "Grounded Into DP",
    "field_out": "Groundout",
    "fielders_choice": "Fielders Choice",
    "catcher_interf": "Catcher Interference",
    "error": "Field Error",
}


def _runs_scored_on_pa_from_scoreboard(g: pd.DataFrame) -> int:
    """Runs scored for the batting team on this PA (Statcast home_score / away_score delta)."""
    if g.empty or "home_score" not in g.columns or "away_score" not in g.columns:
        return 0
    g = g.sort_values("pitch_number")
    first, last = g.iloc[0], g.iloc[-1]
    itb = str(first.get("inning_topbot") or "").upper()
    try:
        a0, a1 = int(float(first["away_score"])), int(float(last["away_score"]))
        h0, h1 = int(float(first["home_score"])), int(float(last["home_score"]))
    except (TypeError, ValueError):
        return 0
    if itb.startswith("T"):
        return max(0, a1 - a0)
    return max(0, h1 - h0)


def _bases_loaded_first_pitch(g: pd.DataFrame) -> bool:
    if g.empty:
        return False
    row0 = g.sort_values("pitch_number").iloc[0]
    for col in ("on_1b", "on_2b", "on_3b"):
        if col not in row0.index:
            return False
        if pd.isna(row0.get(col)) or row0.get(col) in (0, "0", "", None):
            return False
    return True


def _bases_loaded_from_statcast_row(row: pd.Series) -> bool:
    """True if runners on 1st, 2nd, and 3rd are all set on this pitch row."""
    for col in ("on_1b", "on_2b", "on_3b"):
        if col not in row.index:
            return False
        v = row.get(col)
        if pd.isna(v) or v in (0, "0", "", None):
            return False
    return True


def _derive_notable_batter_events(gd: dict) -> list[dict]:
    """High-salience game beats for copy / redraft (ordered by priority, lowest first)."""
    out: list[dict] = []
    pa_log = gd.get("pa_log") or []
    hr_pas = [pa for pa in pa_log if pa.get("event") == "Home Run"]
    for pa in hr_pas:
        rbi = int(pa.get("rbi") or 0)
        inning = int(pa.get("inning") or 0)
        bases_loaded = bool(pa.get("bases_loaded"))
        gs_feed = bool(pa.get("grand_slam_feed"))
        if rbi >= 4 or bases_loaded or gs_feed:
            out.append({
                "type": "grand_slam",
                "priority": 1,
                "label": "Grand slam",
                "inning": inning,
                "rbi": rbi,
            })
            break
    if not any(e.get("type") == "grand_slam" for e in out) and len(hr_pas) >= 2:
        out.append({
            "type": "multi_homer_game",
            "priority": 2,
            "label": f"{len(hr_pas)} home runs in one game",
            "hr_count": len(hr_pas),
        })
    rbi_tot = int(gd.get("rbi") or 0)
    if rbi_tot >= 6 and not any(e.get("type") == "grand_slam" for e in out):
        out.append({
            "type": "big_rbi_night",
            "priority": 3,
            "label": f"{rbi_tot} RBI",
            "rbi": rbi_tot,
        })
    out.sort(key=lambda x: int(x.get("priority", 99)))
    return out


def parse_batter_game_from_parquet(pq_path: str, batter_id: int) -> dict:
    """
    Build the same ``gd`` shape as ``parse_batter_game`` using only Statcast parquet rows.
    Used when feed_live is absent but enriched data exists.
    """
    df = pd.read_parquet(pq_path)
    df = df[df["batter"] == batter_id].copy()
    if df.empty:
        raise ValueError(f"No rows for batter {batter_id} in {pq_path}")

    df = df.sort_values(["inning", "inning_topbot", "at_bat_number", "pitch_number"])
    game_date = str(df["game_date"].iloc[0])[:10]
    home_abb = str(df["home_team"].iloc[0])
    away_abb = str(df["away_team"].iloc[0])
    season_y = int(df["game_year"].iloc[0]) if "game_year" in df.columns else int(game_date[:4])

    pitcher_names: dict[int, str] = {}
    reg = _warehouse_root() / str(season_y) / "players_registry.json"
    try:
        raw = json.loads(reg.read_text())
        for k, v in raw.items():
            pitcher_names[int(k)] = v.get("fullName", str(k))
    except Exception:
        pass

    first = df.iloc[0]
    topbot0 = str(first.get("inning_topbot") or "")
    batter_side = "top" if topbot0.lower().startswith("top") else "bottom"
    opponent = home_abb if batter_side == "top" else away_abb
    batter_team = away_abb if batter_side == "top" else home_abb

    zone_counts: dict[int, int] = defaultdict(int)
    pa_log: list[dict] = []
    batted_balls: list[dict] = []
    pitches_data: list[dict] = []
    ab = h = hr = bb = k = 0
    tb = 0
    no_ab_events = {
        "Sac Fly", "Sac Bunt", "Hit By Pitch", "Intent Walk",
        "Catcher Interference",
    }
    walk_events = {"Walk", "Intent Walk"}

    grp_cols = ["inning", "inning_topbot", "at_bat_number"]
    for _, g in df.groupby(grp_cols, sort=False):
        g = g.sort_values("pitch_number")
        last = g.iloc[-1]
        ev_raw = str(last.get("events") or "").strip().lower()
        event = _STATCAST_EVENT_TO_RESULT.get(ev_raw, "Groundout")
        inning = int(last.get("inning") or 0)
        pitch_hand_pa = str(last.get("p_throws") or "")[:1] or None
        bat_side_pa = str(last.get("stand") or "")[:1] or None
        pid = int(last["pitcher"]) if pd.notna(last.get("pitcher")) else 0
        pitcher_name = pitcher_names.get(pid, f"Pitcher {pid}")

        rbi = int(_runs_scored_on_pa_from_scoreboard(g))
        if event not in no_ab_events:
            ab += 1
        if event in {"Single", "Double", "Triple", "Home Run"}:
            h += 1
            if event == "Single":
                tb += 1
            elif event == "Double":
                tb += 2
            elif event == "Triple":
                tb += 3
            elif event == "Home Run":
                tb += 4
        if event == "Home Run":
            hr += 1
        if event in walk_events:
            bb += 1
        if event == "Strikeout":
            k += 1

        num_pitches = len(g)
        contact_in_pa = None
        result_pitch_type = None

        for i, (_, row) in enumerate(g.iterrows()):
            pitch_type = row.get("pitch_name")
            if not isinstance(pitch_type, str) or (isinstance(pitch_type, float) and pitch_type != pitch_type):
                pitch_type = str(row.get("pitch_type") or "Unknown")
            px = row.get("plate_x")
            pz = row.get("plate_z")
            xba = row.get("estimated_ba_using_speedangle")
            if xba is not None and pd.isna(xba):
                xba = None
            desc = str(row.get("description") or "")
            zone = row.get("zone")
            if zone is not None and not (isinstance(zone, float) and pd.isna(zone)):
                try:
                    zone_counts[int(zone)] += 1
                except (TypeError, ValueError):
                    pass

            is_last = i == num_pitches - 1
            if is_last:
                result_pitch_type = pitch_type

            if px is not None and pz is not None and not (pd.isna(px) or pd.isna(pz)):
                pitches_data.append({
                    "px": float(px),
                    "pz": float(pz),
                    "pitch_type": pitch_type,
                    "desc": desc,
                    "event": event if is_last else "",
                })

            ev_mph = row.get("launch_speed")
            la = row.get("launch_angle")
            if ev_mph is not None and not pd.isna(ev_mph) and float(ev_mph) > 0:
                ball = {
                    "result": event,
                    "ev": float(ev_mph) if ev_mph is not None else None,
                    "la": float(la) if la is not None and not pd.isna(la) else None,
                    "dist": row.get("hit_distance_sc") if "hit_distance_sc" in row.index else None,
                    "traj": str(row.get("bb_type") or ""),
                    "hardness": "",
                    "zone": zone,
                    "pitch_type": pitch_type,
                    "xba": float(xba) if xba is not None else None,
                }
                batted_balls.append(ball)
                contact_in_pa = ball

        bases_ld = _bases_loaded_first_pitch(g)
        rbi = max(rbi, 4) if event == "Home Run" and bases_ld else rbi
        pa_log.append({
            "inning": inning,
            "pitcher": pitcher_name.split(" ")[-1] if " " in str(pitcher_name) else str(pitcher_name),
            "event": event,
            "rbi": rbi,
            "num_pitches": num_pitches,
            "contact": contact_in_pa,
            "pitcher_hand": pitch_hand_pa,
            "bat_side": bat_side_pa,
            "result_pitch": _pitch_abbrev(result_pitch_type),
            "bases_loaded": bases_ld,
            "grand_slam_feed": False,
        })

        if contact_in_pa is not None:
            for pd_entry in reversed(pitches_data):
                if pd_entry.get("event"):
                    pd_entry["ev"] = contact_in_pa.get("ev")
                    break

    rbi_total = sum(int(p.get("rbi") or 0) for p in pa_log)
    score_str = f"{batter_team} vs {opponent}"

    evs = [b["ev"] for b in batted_balls if b.get("ev") is not None]
    las = [b["la"] for b in batted_balls if b.get("la") is not None]
    hard_hit = [b for b in batted_balls if b.get("ev") is not None and b["ev"] >= 95]
    total_pa = len(pa_log)

    bat_spd_vals = []
    sl_vals = []
    bip_xwoba = []
    bip_woba = []
    re24_total = 0.0
    if "bat_speed" in df.columns:
        swings = df[df["bat_speed"].notna()]
        bat_spd_vals = swings["bat_speed"].tolist()
    if "swing_length" in df.columns:
        sl_vals = df["swing_length"].dropna().tolist()
    if "estimated_woba_using_speedangle" in df.columns:
        bip_xwoba = df["estimated_woba_using_speedangle"].dropna().tolist()
    if "woba_value" in df.columns:
        bip_woba = df["woba_value"].dropna().tolist()
    if "delta_run_exp" in df.columns:
        re24_total = float(df["delta_run_exp"].sum())

    pitch_mix: dict[str, int] = defaultdict(int)
    for p in pitches_data:
        abbr = _pitch_abbrev(p.get("pitch_type"))
        if abbr != "?":
            pitch_mix[abbr] += 1

    sabermetrics = {
        "avg_ev": round(np.mean(evs), 1) if evs else None,
        "max_ev": max(evs) if evs else None,
        "avg_la": round(np.mean(las), 1) if las else None,
        "hard_hit_pct": round(len(hard_hit) / len(batted_balls) * 100) if batted_balls else None,
        "bb_pct": round(bb / total_pa * 100) if total_pa else 0,
        "k_pct": round(k / total_pa * 100) if total_pa else 0,
        "contact_count": len(batted_balls),
        "hard_hit_ct": len(hard_hit),
        "p_seen": len(pitches_data),
        "p_per_pa": len(pitches_data) / total_pa if total_pa else None,
        "tb": tb,
        "hr_dist": max(
            [b["dist"] for b in batted_balls if b.get("result") == "Home Run" and b.get("dist")],
            default=None,
        ),
        "xwoba": round(float(np.mean(bip_xwoba)), 3) if bip_xwoba else None,
        "woba": round(float(np.mean(bip_woba)), 3) if bip_woba else None,
        "bat_speed": round(float(np.mean(bat_spd_vals)), 1) if bat_spd_vals else None,
        "swing_length": round(float(np.mean(sl_vals)), 1) if sl_vals else None,
        "re24": round(re24_total, 2) if re24_total != 0.0 else None,
    }

    return {
        "batter_id": batter_id,
        "game_date": game_date,
        "opponent": opponent,
        "batter_team": batter_team,
        "score_str": score_str,
        "ab": ab,
        "h": h,
        "hr": hr,
        "rbi": rbi_total,
        "bb": bb,
        "k": k,
        "pa_log": pa_log,
        "zone_counts": dict(zone_counts),
        "pitches_data": pitches_data,
        "batted_balls": batted_balls,
        "sabermetrics": sabermetrics,
        "pitch_mix": dict(pitch_mix),
        "parquet_only": True,
    }


# ──────────────────────────────── API HELPERS ────────────────────────────────

def fetch_player_bio(player_id: int) -> dict:
    """Fetch batter bio from MLB StatsAPI."""
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={player_id}&hydrate=currentTeam"
    try:
        data = requests.get(url, timeout=10).json()["people"][0]
        team_abb = "MLB"
        link = data.get("currentTeam", {}).get("link", "")
        if link:
            team_abb = requests.get(
                f"https://statsapi.mlb.com{link}", timeout=10
            ).json()["teams"][0]["abbreviation"]
        return dict(
            name=data["fullName"],
            hand=data.get("batSide", {}).get("code", "R"),
            age=data.get("currentAge", "--"),
            height=data.get("height", "--"),
            weight=data.get("weight", "--"),
            team=team_abb,
            position=data.get("primaryPosition", {}).get("abbreviation", ""),
        )
    except Exception:
        return dict(name="Unknown Batter", hand="R", age="--",
                    height="--", weight="--", team="MLB", position="")


def _apply_bio_fallback(bio: dict, gd: dict) -> dict:
    """Use parsed game metadata when external bio/assets are unavailable."""
    out = dict(bio)
    if out.get("name") == "Unknown Batter" and gd.get("batter_name"):
        out["name"] = gd["batter_name"]
    if out.get("team") in {"", "MLB"} and gd.get("batter_team"):
        out["team"] = gd["batter_team"]
    if out.get("hand") in {"", "R"} and gd.get("batter_hand"):
        out["hand"] = gd["batter_hand"]
    return out


def fetch_headshot(player_id: int):
    """Fetch MLB headshot for a player, neutralize background. Returns PIL Image or None."""
    url = (
        f"https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"d_people:generic:headshot:67:current.png/w_640,q_auto:best/"
        f"v1/people/{player_id}/headshot/silo/current.png"
    )
    try:
        resp = requests.get(url, timeout=10)
        if not resp.ok or len(resp.content) < 500:
            return None
        img = Image.open(BytesIO(resp.content))
        replace = (255, 255, 255) if LIGHT_MODE else (0x1F, 0x2E, 0x3D)
        return neutralize_mlb_headshot_background(img, replace_rgb=replace)
    except Exception:
        return None


def fetch_team_logo(team_abb: str):
    """Fetch team logo from ESPN CDN. Returns PIL Image or None."""
    key = ESPN_LOGOS.get(team_abb, team_abb.lower())
    url = (
        f"https://a.espncdn.com/combiner/i?"
        f"img=/i/teamlogos/mlb/500/scoreboard/{key}.png&h=200&w=200"
    )
    try:
        return Image.open(BytesIO(requests.get(url, timeout=10).content))
    except Exception:
        return None


# ─────────────────────────────── DATA PARSING ───────────────────────────────

def parse_batter_game(feed_path: str, batter_id: int) -> dict:
    """
    Extract batter-level data from a feed_live JSON and enriched parquet.

    Returns dict with: game_date, opponent, ab/h/hr/rbi/bb/k,
    pa_log, pitches_data (for scatter/KDE), batted_balls, sabermetrics.
    """
    with open(feed_path) as f:
        data = json.load(f)

    pq_path = feed_path.replace("raw", "pitches_enriched").replace("_feed_live.json", "_pitches_enriched.parquet")
    try:
        df_pq = pd.read_parquet(pq_path)
        df_pq = df_pq[df_pq.batter == batter_id]
    except Exception:
        df_pq = pd.DataFrame()

    game_data  = data.get("gameData", {})
    game_date  = game_data.get("datetime", {}).get("officialDate", "")
    teams      = game_data.get("teams", {})
    away_abb   = teams.get("away", {}).get("abbreviation", "???")
    home_abb   = teams.get("home", {}).get("abbreviation", "???")
    live_data  = data.get("liveData", {}) or {}
    linescore  = (live_data.get("linescore", {}) or {}).get("teams", {}) or {}
    all_plays  = live_data.get("plays", {}).get("allPlays", [])

    batter_side = None
    for play in all_plays:
        if play.get("matchup", {}).get("batter", {}).get("id") == batter_id:
            batter_side = play.get("about", {}).get("halfInning")
            break
    opponent     = home_abb if batter_side == "top" else away_abb
    batter_team  = away_abb if batter_side == "top" else home_abb

    # Game score from linescore (if available)
    bt_key = "away" if batter_side == "top" else "home"
    op_key = "home" if batter_side == "top" else "away"
    try:
        bt_runs = int((linescore.get(bt_key, {}) or {}).get("runs", 0))
        op_runs = int((linescore.get(op_key, {}) or {}).get("runs", 0))
    except Exception:
        bt_runs = op_runs = 0
    score_str = f"{batter_team} {bt_runs} - {opponent} {op_runs}" if bt_runs or op_runs else ""

    zone_counts  = defaultdict(int)
    pa_log       = []
    batted_balls = []
    pitches_data = []
    batter_name  = ""
    batter_hand  = ""
    ab = h = hr = rbi_total = bb = k = 0
    tb = 0

    no_ab_events = {
        "Walk", "Intent Walk", "Hit By Pitch",
        "Sac Fly", "Sac Bunt", "Catcher Interference",
    }
    walk_events = {"Walk", "Intent Walk"}

    for play in all_plays:
        if play.get("matchup", {}).get("batter", {}).get("id") != batter_id:
            continue

        matchup      = play.get("matchup", {}) or {}
        bat_side_pa  = (matchup.get("batSide", {}) or {}).get("code")
        pitch_hand_pa = (matchup.get("pitchHand", {}) or {}).get("code")
        pitcher_name  = matchup.get("pitcher", {}).get("fullName", "Unknown")
        inning       = play.get("about", {}).get("inning", 0)
        if not batter_name:
            batter_name = (matchup.get("batter", {}) or {}).get("fullName", "")
        if not batter_hand and bat_side_pa:
            batter_hand = bat_side_pa

        result = play.get("result", {})
        event  = result.get("event", "")
        try:
            rbi = int(result.get("rbi")) if result.get("rbi") is not None else 0
        except (TypeError, ValueError):
            rbi = 0

        if event not in no_ab_events:
            ab += 1
        if event in {"Single", "Double", "Triple", "Home Run"}:
            h += 1
            if event == "Single":
                tb += 1
            elif event == "Double":
                tb += 2
            elif event == "Triple":
                tb += 3
            elif event == "Home Run":
                tb += 4
        if event == "Home Run":
            hr += 1
        if event in walk_events:
            bb += 1
        if event == "Strikeout":
            k += 1

        pa_pitches = [ev for ev in play.get("playEvents", []) if ev.get("isPitch")]
        num_pitches = len(pa_pitches)
        contact_in_pa = None
        result_pitch_type = None

        bases_loaded_pa = False
        if pa_pitches and not df_pq.empty and "play_id" in df_pq.columns:
            first_id = pa_pitches[0].get("playId") or pa_pitches[0].get("play_id")
            if first_id:
                try:
                    m = df_pq[df_pq["play_id"].astype(str) == str(first_id)]
                    if not m.empty:
                        bases_loaded_pa = _bases_loaded_from_statcast_row(m.iloc[0])
                except Exception:
                    bases_loaded_pa = False
        desc_l = (result.get("description") or "").lower()
        grand_slam_feed = "grand slam" in desc_l

        # Feed `result.rbi` is often missing or 0 for big flies; Statcast score deltas on this PA are a solid proxy.
        rbi_statcast = None
        if pa_pitches and not df_pq.empty and "play_id" in df_pq.columns:
            ids: list[str] = []
            for ev in pa_pitches:
                pid = ev.get("playId") or ev.get("play_id")
                if pid:
                    ids.append(str(pid))
            if ids:
                try:
                    sub = df_pq[df_pq["play_id"].astype(str).isin(ids)]
                    if (
                        not sub.empty
                        and "home_score" in sub.columns
                        and "away_score" in sub.columns
                    ):
                        rbi_statcast = _runs_scored_on_pa_from_scoreboard(sub)
                except Exception:
                    rbi_statcast = None
        if rbi_statcast is not None and int(rbi_statcast) > 0:
            if rbi == 0:
                rbi = int(rbi_statcast)
            elif event == "Home Run" and int(rbi_statcast) > rbi:
                rbi = int(rbi_statcast)
        if event == "Home Run" and (grand_slam_feed or bases_loaded_pa):
            rbi = max(rbi, 4)

        for i, ev in enumerate(pa_pitches):
            play_event_id = ev.get("playId", "")

            if not df_pq.empty and play_event_id in df_pq.play_id.values:
                pq_row = df_pq[df_pq.play_id == play_event_id].iloc[0]
                pitch_type = pq_row.pitch_name
                px = pq_row.plate_x
                pz = pq_row.plate_z
                xba = pq_row.estimated_ba_using_speedangle if pd.notna(pq_row.estimated_ba_using_speedangle) else None
            else:
                pitch_type = ev.get("details", {}).get("type", {}).get("description", "Unknown")
                px = ev.get("pitchData", {}).get("coordinates", {}).get("pX")
                pz = ev.get("pitchData", {}).get("coordinates", {}).get("pZ")
                xba = None

            # Fallback to raw feed when parquet values are NaN (e.g. Spring Training)
            def _is_nan(v):
                try:
                    return v != v  # NaN != NaN
                except Exception:
                    return False

            if not isinstance(pitch_type, str) or _is_nan(pitch_type):
                pitch_type = ev.get("details", {}).get("type", {}).get("description", "Unknown")
            if px is None or _is_nan(px):
                px = ev.get("pitchData", {}).get("coordinates", {}).get("pX")
            if pz is None or _is_nan(pz):
                pz = ev.get("pitchData", {}).get("coordinates", {}).get("pZ")

            desc = ev.get("details", {}).get("description", "")
            zone = ev.get("pitchData", {}).get("zone")

            if zone is not None:
                zone_counts[zone] += 1

            is_last = (i == len(pa_pitches) - 1)
            if is_last:
                result_pitch_type = pitch_type

            if px is not None and pz is not None:
                pitches_data.append({
                    "px": px,
                    "pz": pz,
                    "pitch_type": pitch_type,
                    "desc": desc,
                    "event": event if is_last else "",
                })

            hit_d = ev.get("hitData")
            if hit_d:
                ball = {
                    "result":   event,
                    "ev":       hit_d.get("launchSpeed"),
                    "la":       hit_d.get("launchAngle"),
                    "dist":     hit_d.get("totalDistance"),
                    "traj":     hit_d.get("trajectory", ""),
                    "hardness": hit_d.get("hardness", ""),
                    "zone":     zone,
                    "pitch_type": pitch_type,
                    "xba":      xba
                }
                batted_balls.append(ball)
                contact_in_pa = ball

        pa_log.append({
            "inning":        inning,
            "pitcher":       pitcher_name.split(" ")[-1],
            "event":         event,
            "rbi":           rbi,
            "num_pitches":   num_pitches,
            "contact":       contact_in_pa,
            "pitcher_hand":  pitch_hand_pa,
            "bat_side":      bat_side_pa,
            "result_pitch":  _pitch_abbrev(result_pitch_type),
            "bases_loaded":  bases_loaded_pa,
            "grand_slam_feed": grand_slam_feed,
        })

        # Annotate the result pitch entry in pitches_data with EV for heatmap markers
        if contact_in_pa is not None:
            for pd_entry in reversed(pitches_data):
                if pd_entry.get("event"):
                    pd_entry["ev"] = contact_in_pa.get("ev")
                    break

    # Always match game RBI to PA log (feed API RBI is unreliable before Statcast merge).
    rbi_total = sum(int(p.get("rbi") or 0) for p in pa_log)

    evs      = [b["ev"] for b in batted_balls if b["ev"] is not None]
    las      = [b["la"] for b in batted_balls if b["la"] is not None]
    hard_hit = [b for b in batted_balls if b["ev"] is not None and b["ev"] >= 95]
    total_pa = len(pa_log)

    # Additional Statcast metrics from the enriched parquet
    bat_spd_vals, sl_vals, bip_xwoba, bip_woba, re24_total = [], [], [], [], 0.0
    if not df_pq.empty:
        swings = df_pq[df_pq["bat_speed"].notna()]
        bat_spd_vals = swings["bat_speed"].tolist()
        sl_vals      = swings["swing_length"].dropna().tolist()
        bip_xwoba    = df_pq["estimated_woba_using_speedangle"].dropna().tolist()
        if "woba_value" in df_pq.columns:
            bip_woba = df_pq["woba_value"].dropna().tolist()
        if "delta_run_exp" in df_pq.columns:
            re24_total = float(df_pq["delta_run_exp"].sum())

    # Pitch mix (abbreviated names)
    pitch_mix: dict[str, int] = defaultdict(int)
    for p in pitches_data:
        abbr = _pitch_abbrev(p.get("pitch_type"))
        if abbr != "?":
            pitch_mix[abbr] += 1

    sabermetrics = {
        "avg_ev":        round(np.mean(evs), 1) if evs else None,
        "max_ev":        max(evs) if evs else None,
        "avg_la":        round(np.mean(las), 1) if las else None,
        "hard_hit_pct":  round(len(hard_hit) / len(batted_balls) * 100) if batted_balls else None,
        "bb_pct":        round(bb / total_pa * 100) if total_pa else 0,
        "k_pct":         round(k  / total_pa * 100) if total_pa else 0,
        "contact_count": len(batted_balls),
        "hard_hit_ct":   len(hard_hit),
        "p_seen":        len(pitches_data),
        "p_per_pa":      len(pitches_data) / total_pa if total_pa else None,
        "tb":            tb,
        "hr_dist":       max([b["dist"] for b in batted_balls if b.get("result") == "Home Run" and b.get("dist")], default=None),
        # Enriched Statcast extras
        "xwoba":         round(float(np.mean(bip_xwoba)), 3) if bip_xwoba else None,
        "woba":          round(float(np.mean(bip_woba)),  3) if bip_woba  else None,
        "bat_speed":     round(float(np.mean(bat_spd_vals)), 1) if bat_spd_vals else None,
        "swing_length":  round(float(np.mean(sl_vals)), 1) if sl_vals else None,
        "re24":          round(re24_total, 2) if re24_total != 0.0 else None,
    }

    return {
        "batter_id":    batter_id,
        "batter_name":  batter_name,
        "batter_hand":  batter_hand,
        "game_date":    game_date,
        "opponent":     opponent,
        "batter_team":  batter_team,
        "score_str":    score_str,
        "ab":           ab,
        "h":            h,
        "hr":           hr,
        "rbi":          rbi_total,
        "bb":           bb,
        "k":            k,
        "pa_log":       pa_log,
        "zone_counts":  dict(zone_counts),
        "pitches_data": pitches_data,
        "batted_balls": batted_balls,
        "sabermetrics": sabermetrics,
        "pitch_mix":    dict(pitch_mix),
    }


# ──────────────────────────────── RENDER HELPERS ────────────────────────────

def _clean(ax, bg=None):
    ax.set_facecolor(bg or PALETTE["panel_bg"])
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])


def _border(ax):
    for sp in ax.spines.values():
        sp.set_visible(True)
        sp.set_edgecolor(PALETTE["border"])
        sp.set_linewidth(1.5)


def _lum(hex_color):
    r, g, b = mpl.colors.to_rgb(hex_color)
    return 0.299 * r + 0.587 * g + 0.114 * b


def _event_symbol(event: str) -> tuple:
    """Return (short_label, fill_color) for a PA result."""
    H   = PALETTE["accent_green"]
    OUT = PALETTE["text_secondary"]
    K   = PALETTE["accent_red"]
    HR  = PALETTE["accent_orange"]
    BB  = PALETTE["accent_gold"]
    lookup = {
        "Single":          ("1B",  H),
        "Double":          ("2B",  H),
        "Triple":          ("3B",  H),
        "Home Run":        ("HR",  HR),
        "Strikeout":       ("K",   K),
        "Groundout":       ("GO",  OUT),
        "Flyout":          ("FO",  OUT),
        "Lineout":         ("LO",  OUT),
        "Pop Out":         ("PO",  OUT),
        "Force Out":       ("FO",  OUT),
        "Forceout":        ("FO",  OUT),
        "Double Play":          ("GDP", K),
        "Grounded Into DP":     ("GDP", K),
        "Fielders Choice":      ("FC",  OUT),
        "Fielders Choice Out":  ("FC",  OUT),
        "Field Error":          ("E",   BB),
        "Sac Fly":              ("SF",  OUT),
        "Sac Fly Double Play":  ("SF",  OUT),
        "Sac Bunt":             ("SB",  OUT),
        "Sac Bunt Double Play": ("SB",  OUT),
        "Walk":                 ("BB",  BB),
        "Intent Walk":          ("IBB", BB),
        "Hit By Pitch":         ("HBP", BB),
        "Caught Stealing 2B":   ("CS",  K),
        "Caught Stealing 3B":   ("CS",  K),
        "Caught Stealing Home": ("CS",  K),
        "Pickoff 1B":           ("PK",  K),
        "Pickoff 2B":           ("PK",  K),
        "Pickoff 3B":           ("PK",  K),
        "Runner Out":           ("RO",  OUT),
        "Batter Interference":  ("INT", OUT),
        "Fan Interference":     ("INT", OUT),
    }
    label = lookup.get(event)
    if label:
        return label
    # Partial-match fallbacks for uncommon wordings
    el = event.lower()
    if "strikeout" in el:  return ("K",   K)
    if "home run"  in el:  return ("HR",  HR)
    if "double"    in el and "play" in el: return ("GDP", K)
    if "caught"    in el:  return ("CS",  K)
    if "walk"      in el:  return ("BB",  BB)
    if "groundout" in el or "ground" in el: return ("GO", OUT)
    if "flyout"    in el or "fly"    in el: return ("FO", OUT)
    if "lineout"   in el or "line"   in el: return ("LO", OUT)
    if "pop"       in el:  return ("PO",  OUT)
    return (event[:3].upper() if event else "?", OUT)


# ──────────────────────────────── PANEL 1 — HEADER ──────────────────────────

def plot_header(ax, bio, gd, headshot, logo, is_flag: bool = False):
    _clean(ax, PALETTE["header_bg"])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    sm = gd["sabermetrics"]

    if logo:
        al = ax.inset_axes([0.66, -0.08, 0.34, 1.18])
        al.imshow(np.array(logo), alpha=0.055)
        al.axis("off")

    if headshot:
        ai = ax.inset_axes([0.012, 0.09, 0.118, 0.80])
        ai.set_facecolor(PALETTE["header_bg"])
        img_arr = np.array(headshot)
        h_px, w_px = img_arr.shape[:2]
        cx, cy = w_px / 2, h_px / 2
        r = min(cx, cy) * 0.94
        Y_grid, X_grid = np.ogrid[:h_px, :w_px]
        mask = (X_grid - cx) ** 2 + (Y_grid - cy) ** 2 > r ** 2
        bg_rgb = (248, 249, 250) if LIGHT_MODE else (30, 52, 72)
        img_arr = img_arr.copy()
        img_arr[mask] = bg_rgb
        ai.imshow(img_arr, extent=[0, 1, 0, 1], origin="upper", aspect="auto", zorder=1)
        theta = np.linspace(0, 2 * np.pi, 300)
        ai.plot(
            0.5 + 0.47 * np.cos(theta),
            0.5 + 0.47 * np.sin(theta),
            color=PALETTE["accent_orange"], linewidth=2.0,
            transform=ai.transAxes, zorder=2,
        )
        ai.set_xlim(0, 1); ai.set_ylim(0, 1); ai.axis("off")

    lx = 0.152
    team_tag = gd.get("batter_team") or bio["team"]
    score_str = gd.get("score_str") or f"vs {gd['opponent']}"

    ax.text(lx, 0.90, bio["name"].upper(),
            color=PALETTE["text_primary"], fontsize=28, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)

    ax.text(lx, 0.67, f"{team_tag}  \u00b7  {bio['position']}",
            color=PALETTE["accent_orange"], fontsize=12.5, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)

    parts = [f"{gd['h']}-for-{gd['ab']}"]
    if gd["hr"]:
        parts.append(f"{gd['hr']} HR")
    if gd["rbi"]:
        parts.append(f"{gd['rbi']} RBI")
    if gd["bb"]:
        parts.append(f"{gd['bb']} BB")
    if gd["k"]:
        parts.append(f"{gd['k']} K")
    game_line = "   \u00b7   ".join(parts)
    ax.text(0.50, 0.50, game_line,
            color=PALETTE["text_primary"], fontsize=22, fontweight="black",
            ha="center", va="center", transform=ax.transAxes)
    ax.text(0.50, 0.31, f"{gd['game_date']}   \u00b7   {score_str}",
            color=PALETTE["text_secondary"], fontsize=12, fontweight="bold",
            ha="center", va="center", transform=ax.transAxes)

    total_pitches = len(gd.get("pitches_data", []))
    pa_count      = len(gd.get("pa_log", []))
    hand_label = {"R": "Bats R", "L": "Bats L", "S": "Switch"}.get(bio["hand"], f"B:{bio['hand']}")
    chips = [f"{hand_label}", f"Age {bio['age']}", f"{bio['height']} / {bio['weight']} lbs"]
    if total_pitches and pa_count:
        chips.append(f"{total_pitches} pitches")
    elif total_pitches:
        chips.append(f"{total_pitches} pitches")
    chip_text = "   \u2022   ".join(chips)
    ax.text(lx, 0.12, chip_text,
            color=PALETTE["text_lo"], fontsize=9.8, fontweight="bold",
            ha="left", va="center", transform=ax.transAxes)

    rx  = 0.815

    def _fmt_rate(v):
        if v is None:
            return "\u2014"
        s = f"{v:.3f}"
        return s[1:] if s.startswith("0") else s

    avg_ev  = sm.get("avg_ev")
    hh_pct  = sm.get("hard_hit_pct")

    if avg_ev is not None and avg_ev >= 100:
        hero_label = "AVG EV"
        hero_val   = f"{avg_ev:.1f}"
    elif sm.get("xwoba") is not None:
        hero_label = "xwOBA"
        hero_val   = _fmt_rate(sm["xwoba"])
    elif sm.get("woba") is not None:
        hero_label = "wOBA"
        hero_val   = _fmt_rate(sm["woba"])
    elif hh_pct is not None:
        hero_label = "HH%"
        hero_val   = f"{hh_pct}%"
    else:
        hero_label = "xwOBA"
        hero_val   = "\u2014"

    ax.text(rx, 0.86, hero_label,
            color=PALETTE["text_secondary"], fontsize=10.5, fontweight="black",
            ha="center", va="top", transform=ax.transAxes)
    ax.text(rx, 0.70, hero_val,
            color=PALETTE["accent_orange"], fontsize=34, fontweight="black",
            ha="center", va="top", transform=ax.transAxes)

    support = []
    if sm.get("max_ev") is not None:
        support.append(("MAX EV", f'{sm["max_ev"]:.0f}'))
    if sm.get("tb"):
        support.append(("TB", f'{sm["tb"]}'))
    if sm.get("p_per_pa"):
        support.append(("P/PA", f'{sm["p_per_pa"]:.1f}'))
    for i, (lbl, val) in enumerate(support[:3]):
        xp = 0.700 + i * 0.078
        ax.text(xp, 0.31, val,
                color=PALETTE["text_primary"], fontsize=16, fontweight="black",
                ha="center", va="center", transform=ax.transAxes)
        ax.text(xp, 0.16, lbl,
                color=PALETTE["text_lo"], fontsize=8.5, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes)

    ax.plot([0.148, 0.965], [0.045, 0.045],
            color=PALETTE["border"], lw=0.8, alpha=0.45, transform=ax.transAxes)
    ax.plot([0, 1], [0.02, 0.02],
            color=PALETTE["accent_orange"], lw=2.5, alpha=0.8, transform=ax.transAxes)


# ──────────────────────────────── PANEL 2 — PITCH HEAT MAP ──────────────────

def plot_pitch_heatmap(ax, pitches_data: list, pitch_mix: dict):
    """
    Gaussian-smoothed density heatmap of all pitches seen, overlaid with the
    strike zone grid.  A pitch-mix strip along the bottom replaces the old
    scatter legend.
    """
    _clean(ax)
    _border(ax)

    sz_top   = 3.5
    sz_bot   = 1.5
    sz_left  = -0.71  # 17 inches / 2 = 8.5 inches = 0.708 ft
    sz_right =  0.71
    sz_w     = sz_right - sz_left
    sz_h     = sz_top   - sz_bot

    # Extend y-limits downward to leave room for the pitch-mix strip.
    # A real strike zone is ~17" wide by ~24" tall (ratio ~0.71).
    # To make the visual zone look taller than wide in this panel,
    # we need to adjust the data limits. The panel itself is roughly square.
    # By making the x-limits wider relative to the zone width, the zone shrinks horizontally.
    ax.set_xlim(-2.35, 2.35)
    ax.set_ylim(0.05, 5.05)
    ax.set_aspect("equal", adjustable="box")

    # ── Build 2-D histogram and smooth with Gaussian kernel ─────────────────
    n_grid = 72
    gx = np.linspace(-2.35, 2.35, n_grid)
    gz = np.linspace(0.05, 5.05, n_grid)
    H  = np.zeros((n_grid, n_grid))
    dx = gx[1] - gx[0]
    dz = gz[1] - gz[0]

    for p in pitches_data:
        px, pz = p.get("px"), p.get("pz")
        if px is None or pz is None:
            continue
        xi = int((px - gx[0]) / dx)
        zi = int((pz - gz[0]) / dz)
        if 0 <= xi < n_grid and 0 <= zi < n_grid:
            H[zi, xi] += 1

    try:
        from scipy.ndimage import gaussian_filter
        H_sm = gaussian_filter(H.astype(float), sigma=3.2)
    except ImportError:
        kernel = np.ones((5, 5)) / 25.0
        H_sm = np.convolve(H.astype(float).ravel(), kernel.ravel(), mode="same").reshape(H.shape)

    if H_sm.max() > 0:
        # Mask near-zero cells (Gaussian bleed into empty corners) so they
        # render as the panel background instead of a faint colour cloud.
        threshold = H_sm.max() * 0.15
        H_plot = np.where(H_sm >= threshold, H_sm, np.nan)
        cmap_copy = HEAT_CMAP.copy()
        cmap_copy.set_bad(color=PALETTE["panel_bg"], alpha=0)
        ax.imshow(
            H_plot,
            extent=[-2.35, 2.35, 0.05, 5.05],
            origin="lower",
            cmap=cmap_copy,
            aspect="auto",
            alpha=0.70,
            vmin=threshold,
            vmax=H_sm.max() * 0.92,
            zorder=1,
        )

    # Keep raw pitch locations present but quiet; the heat and zone carry the panel.
    all_x = [p.get("px") for p in pitches_data if p.get("px") is not None and p.get("pz") is not None]
    all_z = [p.get("pz") for p in pitches_data if p.get("px") is not None and p.get("pz") is not None]
    if all_x:
        ax.scatter(
            all_x,
            all_z,
            s=7,
            c=PALETTE["text_secondary"],
            alpha=0.12,
            linewidths=0.0,
            zorder=2,
        )

    # ── Strike zone border ───────────────────────────────────────────────────
    ax.add_patch(mpatches.Rectangle(
        (sz_left, sz_bot), sz_w, sz_h,
        linewidth=0, facecolor=PALETTE["panel_bg"], alpha=0.52, zorder=2,
    ))
    ax.add_patch(mpatches.Rectangle(
        (sz_left, sz_bot), sz_w, sz_h,
        linewidth=2.8, edgecolor=PALETTE["text_primary"],
        facecolor="none", linestyle="-", zorder=3,
    ))

    # Inner 3×3 grid
    zone_w, zone_h = sz_w / 3, sz_h / 3
    for i in range(1, 3):
        ax.plot(
            [sz_left + i * zone_w, sz_left + i * zone_w], [sz_bot, sz_top],
            color=PALETTE["zone_edge"], lw=0.75, linestyle="-", zorder=3, alpha=0.38,
        )
        ax.plot(
            [sz_left, sz_right], [sz_bot + i * zone_h, sz_bot + i * zone_h],
            color=PALETTE["zone_edge"], lw=0.75, linestyle="-", zorder=3, alpha=0.38,
        )

    # ── Hard contact markers (EV ≥ 95) with special highlight for HR ────────
    hard_hits = [
        p
        for p in pitches_data
        if p.get("ev") is not None
        and p["ev"] >= 95
        and p.get("px") is not None
        and p.get("pz") is not None
    ]
    if hard_hits:
        hr_hits = [p for p in hard_hits if (p.get("event") or "").lower() == "home run"]
        other_hard = [p for p in hard_hits if p not in hr_hits]

        # Scale star size by EV so nukes stand out
        def _size_for_ev(ev: float) -> float:
            base = 95.0
            return base + max(0.0, ev - 95.0) * 5.0

        if other_hard:
            hx = [p["px"] for p in other_hard]
            hz = [p["pz"] for p in other_hard]
            sizes = [_size_for_ev(p["ev"]) for p in other_hard]
            ax.scatter(
                hx,
                hz,
                marker="o",
                s=sizes,
                color=PALETTE["panel_bg"],
                edgecolors=PALETTE["text_primary"],
                linewidths=1.1,
                zorder=6,
                alpha=0.96,
            )
            ax.scatter(
                hx,
                hz,
                marker="o",
                s=[max(26, size * 0.38) for size in sizes],
                color=PALETTE["accent_orange"],
                edgecolors="none",
                zorder=7,
                alpha=0.92,
            )

        if hr_hits:
            hx_hr = [p["px"] for p in hr_hits]
            hz_hr = [p["pz"] for p in hr_hits]
            sizes_hr = [_size_for_ev(p["ev"]) * 1.3 for p in hr_hits]
            ax.scatter(
                hx_hr,
                hz_hr,
                marker="o",
                s=sizes_hr,
                color=PALETTE["accent_orange"],
                edgecolors=PALETTE["text_primary"],
                linewidths=1.4,
                zorder=7,
                alpha=0.98,
            )
            for x, z in zip(hx_hr, hz_hr):
                ax.text(x, z, "HR", color="#FFFFFF", fontsize=7.5,
                        fontweight="black", ha="center", va="center", zorder=8)

    # ── Pitch-mix strip at bottom ─────────────────────────────────────────────
    # Sorted by frequency descending; each type gets abbr (colored) + % on same line
    total_pm  = sum(pitch_mix.values()) or 1
    sorted_pm = sorted(pitch_mix.items(), key=lambda x: -x[1])
    n_types   = len(sorted_pm)
    if n_types:
        # Solid background strip so labels sit on clean canvas, independent of KDE
        ax.add_patch(
            FancyBboxPatch(
                (0.0, 0.0),
                1.0,
                0.13,
                boxstyle="square,pad=0",
                lw=0,
                facecolor=PALETTE["card_bg"],
                transform=ax.transAxes,
                zorder=3,
            )
        )

        # Show at most the top 5 pitch types to avoid crowding on the right edge
        max_types = 5
        shown_pm = sorted_pm[:max_types]
        n_types = len(shown_pm)
        start_x, end_x = 0.08, 0.92
        width = end_x - start_x
        step = width / max(n_types, 1)
        for k_idx, (abbr, cnt) in enumerate(shown_pm):
            full_name = next((k for k, v in _PITCH_ABBREV_MAP.items() if v == abbr), None)
            col  = PITCH_COLORS.get(full_name, PALETTE["text_secondary"])
            pct  = int(round(cnt / total_pm * 100))
            xpos = start_x + step * k_idx + step / 2
            # Abbr in pitch color, pct in muted — same vertical band, two rows
            ax.text(xpos, 0.064, abbr,
                    color=col, fontsize=10, fontweight="black",
                    ha="center", va="center", transform=ax.transAxes, zorder=4)
            ax.text(xpos, 0.023, f"{pct}%",
                    color=PALETTE["text_secondary"], fontsize=8.5, fontweight="bold",
                    ha="center", va="center", transform=ax.transAxes, zorder=4)

    # Thin separator above the strip
    ax.axhline(
        0.05 + (5.05 - 0.05) * 0.085,
        color=PALETTE["border"], lw=0.7, alpha=0.5, zorder=4,
    )

    ax.text(0.5, 0.965, "Catcher's view",
            color=PALETTE["text_lo"], fontsize=8.5, fontstyle="italic",
            ha="center", va="top", transform=ax.transAxes)

    ax.set_title("PITCHES SEEN",
                 color=PALETTE["text_secondary"], fontsize=13,
                 fontweight="black", pad=10)


# ──────────────────────────────── PANEL 3 — PA LOG ──────────────────────────

def plot_pa_log(ax, pa_log: list):
    _clean(ax)
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    COLS   = ["INN", "PITCHER", "RESULT", "PITCH", "#P"]
    WIDTHS = [0.10, 0.30, 0.26, 0.16, 0.18]
    HDR_Y  = 0.955
    SEP_Y  = 0.885
    n_rows = len(pa_log)
    ROW_H  = (SEP_Y - 0.04) / max(n_rows, 1)
    y_top  = SEP_Y - ROW_H * 0.45

    hdr_kw = dict(color=PALETTE["text_lo"], fontsize=9.5,
                  fontweight="black", ha="center", va="top",
                  transform=ax.transAxes)
    xp = 0.02
    for col, w in zip(COLS, WIDTHS):
        ax.text(xp + w / 2, HDR_Y, col, **hdr_kw)
        xp += w
    ax.plot([0.02, 0.98], [SEP_Y, SEP_Y],
            color=PALETTE["border"], lw=1.0, transform=ax.transAxes)

    for ri, pa in enumerate(pa_log):
        yc = y_top - ri * ROW_H
        if yc < 0.03:
            break

        symbol, scol = _event_symbol(pa["event"])

        # Slightly tint HR rows so they pop in mobile screenshots
        if symbol == "HR":
            bg = "#FFF1E6" if LIGHT_MODE else "#3A2414"
        else:
            bg = PALETTE["table_alt"] if ri % 2 == 0 else PALETTE["table_bg"]
        ax.add_patch(FancyBboxPatch(
            (0.025, yc - ROW_H * 0.44), 0.95, ROW_H * 0.88,
            boxstyle="round,pad=0.004", lw=0, facecolor=bg,
            transform=ax.transAxes, zorder=1,
        ))

        contact = pa.get("contact")
        pitch_hand = pa.get("pitcher_hand")
        xp = 0.02

        # Accent bar
        ax.add_patch(FancyBboxPatch(
            (0.025, yc - ROW_H * 0.34), 0.005, ROW_H * 0.68,
            boxstyle="round,pad=0.002", lw=0, facecolor=scol, alpha=0.80,
            transform=ax.transAxes, zorder=2,
        ))

        # INN
        ax.text(xp + WIDTHS[0] / 2, yc, str(pa.get("inning", "-")),
                color=PALETTE["text_lo"], fontsize=10.5, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[0]

        # PITCHER
        pitcher = pa.get("pitcher", "")
        if pitch_hand in {"R", "L"}:
            pitcher_text = f"{pitcher} ({pitch_hand})"
        else:
            pitcher_text = pitcher
        if len(pitcher_text) > 18:
            pitcher_text = pitcher_text[:17] + "."
        ax.text(xp + 0.012, yc, pitcher_text,
                color=PALETTE["text_primary"], fontsize=9.6, fontweight="bold",
                ha="left", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[1]

        # RESULT pill
        pw, ph = WIDTHS[2] * 0.70, ROW_H * 0.58
        ax.add_patch(FancyBboxPatch(
            (xp + WIDTHS[2] / 2 - pw / 2, yc - ph / 2), pw, ph,
            boxstyle="round,pad=0.010", lw=1.0, edgecolor=scol,
            facecolor=scol if symbol in {"HR", "2B", "3B"} else PALETTE["panel_bg"],
            transform=ax.transAxes, zorder=2,
        ))
        tc  = "#111111" if symbol in {"HR", "2B", "3B"} and _lum(scol) > 0.50 else (
            "#FFFFFF" if symbol in {"HR", "2B", "3B"} else scol
        )
        rbi = pa.get("rbi", 0)
        lbl = symbol if rbi == 0 else f"{symbol} ({rbi} RBI)"
        ax.text(xp + WIDTHS[2] / 2, yc, lbl,
                color=tc, fontsize=10.5, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[2]

        # PITCH — type that ended the PA, colored by pitch type
        rp = pa.get("result_pitch", "?")
        full_name = next(
            (k for k, v in _PITCH_ABBREV_MAP.items() if v == rp), None
        )
        pt_col = PITCH_COLORS.get(full_name, PALETTE["text_secondary"]) if full_name else PALETTE["text_secondary"]
        ax.text(xp + WIDTHS[3] / 2, yc, rp,
                color=pt_col, fontsize=10.5, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[3]

        # #P — pitch count for this PA
        np_val = pa.get("num_pitches", 0)
        np_col = PALETTE["accent_orange"] if np_val >= 6 else PALETTE["text_primary"]
        ax.text(xp + WIDTHS[4] / 2, yc, str(np_val) if np_val else "\u2014",
                color=np_col, fontsize=10.5, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)

    ax.set_title("PLATE APPEARANCES",
                 color=PALETTE["text_secondary"], fontsize=13,
                 fontweight="black", pad=10)


# ──────────────────────────────── PANEL 4 — BATTED BALL ─────────────────────

def plot_batted_ball(ax, batted_balls: list):
    _clean(ax)
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    has_xba = any(b.get("xba") is not None for b in batted_balls)
    if has_xba:
        COLS   = ["RES", "PITCH", "EV", "LA", "TRAJ", "xBA"]
        WIDTHS = [0.17, 0.16, 0.15, 0.14, 0.21, 0.17]
    else:
        COLS   = ["RES", "PITCH", "EV", "LA", "TRAJ"]
        WIDTHS = [0.18, 0.20, 0.16, 0.16, 0.30]

    _TRAJ_MAP = {
        "ground_ball": ("GB", PALETTE["text_lo"]),
        "fly_ball":    ("FB", PALETTE["accent_orange"]),
        "line_drive":  ("LD", PALETTE["accent_green"]),
        "popup":       ("PU", PALETTE["accent_red"]),
        "bunt_grounder": ("BG", PALETTE["text_lo"]),
    }
    HDR_Y  = 0.955
    SEP_Y  = 0.885
    n_data = len(batted_balls) + (1 if batted_balls else 0)
    ROW_H  = (SEP_Y - 0.04) / max(n_data, 1)
    y_top  = SEP_Y - ROW_H * 0.45

    hdr_kw = dict(color=PALETTE["text_lo"], fontsize=9.5,
                  fontweight="black", ha="center", va="top",
                  transform=ax.transAxes)
    xp = 0.02
    for col, w in zip(COLS, WIDTHS):
        ax.text(xp + w / 2, HDR_Y, col, **hdr_kw)
        xp += w
    ax.plot([0.02, 0.98], [SEP_Y, SEP_Y],
            color=PALETTE["border"], lw=1.0, transform=ax.transAxes)

    for ri, ball in enumerate(batted_balls):
        yc = y_top - ri * ROW_H
        if yc < 0.03:
            break

        is_hr = ball.get("result") == "Home Run"
        bg = "#FFF1E6" if is_hr and LIGHT_MODE else (
            "#3A2414" if is_hr else (PALETTE["table_alt"] if ri % 2 == 0 else PALETTE["table_bg"])
        )
        ax.add_patch(FancyBboxPatch(
            (0.025, yc - ROW_H * 0.44), 0.95, ROW_H * 0.88,
            boxstyle="round,pad=0.004", lw=0, facecolor=bg,
            transform=ax.transAxes, zorder=1,
        ))

        symbol, scol = _event_symbol(ball["result"])
        xp = 0.02

        # Accent strip
        ax.add_patch(FancyBboxPatch(
            (0.025, yc - ROW_H * 0.34), 0.005, ROW_H * 0.68,
            boxstyle="round,pad=0.002", lw=0, facecolor=scol, alpha=0.82,
            transform=ax.transAxes, zorder=2,
        ))

        # RES — short result tag (ties back to PA log)
        res_col = PALETTE["accent_orange"] if symbol == "HR" else PALETTE["text_primary"]
        ax.text(xp + WIDTHS[0] / 2, yc, symbol,
                color=res_col, fontsize=11.2, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[0]

        # PITCH — abbreviated + colored by pitch type
        pitch_type = ball.get("pitch_type", "Unknown")
        pt_short   = _pitch_abbrev(pitch_type)
        pt_col     = PITCH_COLORS.get(pitch_type, PALETTE["text_secondary"])
        ax.text(xp + WIDTHS[1] / 2, yc, pt_short,
                color=pt_col, fontsize=10.5, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[1]

        # EV — tiered highlight; GB contact stays muted even when hard
        if ball.get("ev") is not None:
            ev = ball["ev"]
            traj_raw = (ball.get("traj") or "").lower()
            is_ground = traj_raw == "ground_ball"
            if ev >= 100 and not is_ground:
                ev_col = PALETTE["accent_orange"]
            elif ev >= 95 and not is_ground:
                ev_col = PALETTE["accent_gold"]
            else:
                ev_col = PALETTE["text_primary"]
            ax.text(xp + WIDTHS[2] / 2, yc, f"{ev:.0f}",
                    color=ev_col, fontsize=11, fontweight="black" if ev >= 95 else "bold",
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        else:
            ax.text(xp + WIDTHS[2] / 2, yc, "\u2014",
                    color=PALETTE["text_lo"], fontsize=10.5,
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[2]

        # LA
        if ball.get("la") is not None:
            la_col = PALETTE["text_primary"]
            sign = "+" if ball["la"] > 0 else ""
            ax.text(xp + WIDTHS[3] / 2, yc, f'{sign}{ball["la"]:.0f}\u00b0',
                    color=la_col, fontsize=10.5,
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        else:
            ax.text(xp + WIDTHS[3] / 2, yc, "\u2014",
                    color=PALETTE["text_lo"], fontsize=10.5,
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[3]

        # TRAJ + distance  (HR always shows distance only, no traj label)
        traj_raw  = ball.get("traj", "")
        dist_val  = ball.get("dist")
        if is_hr:
            traj_text = f"HR  {dist_val:.0f}ft" if dist_val is not None else "HR"
            t_col     = PALETTE["accent_orange"]
        else:
            traj_lbl, traj_col = _TRAJ_MAP.get(traj_raw, ("\u2014", PALETTE["text_lo"]))
            if dist_val is not None and traj_lbl != "\u2014":
                traj_text = f"{traj_lbl}  {dist_val:.0f}ft"
                t_col     = PALETTE["text_primary"]
            elif dist_val is not None:
                traj_text = f"{dist_val:.0f}ft"
                t_col     = PALETTE["text_primary"]
            else:
                traj_text = traj_lbl
                t_col     = PALETTE["text_primary"]
        ax.text(xp + WIDTHS[4] / 2, yc, traj_text,
                color=t_col, fontsize=10.3, fontweight="black" if is_hr else "bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[4]

        # xBA (optional; only when feed provides it)
        if has_xba:
            if ball.get("xba") is not None:
                xba_col = PALETTE["accent_orange"] if ball["xba"] >= 0.500 else PALETTE["text_primary"]
                ax.text(xp + WIDTHS[5] / 2, yc,
                        f'.{str(ball["xba"]).split(".")[-1][:3].ljust(3, "0")}',
                        color=xba_col, fontsize=10.5, fontweight="bold",
                        ha="center", va="center", transform=ax.transAxes, zorder=3)
            else:
                ax.text(xp + WIDTHS[5] / 2, yc, "\u2014",
                        color=PALETTE["text_lo"], fontsize=10.5,
                        ha="center", va="center", transform=ax.transAxes, zorder=3)

    # AVG footer row
    if batted_balls:
        evs  = [b["ev"]  for b in batted_balls if b.get("ev")  is not None]
        las  = [b["la"]  for b in batted_balls if b.get("la")  is not None]
        xbas = [b["xba"] for b in batted_balls if b.get("xba") is not None]
        ri_avg = len(batted_balls)
        yc = y_top - ri_avg * ROW_H
        if yc > 0.03:
            ax.plot([0.03, 0.97], [yc + ROW_H * 0.62, yc + ROW_H * 0.62],
                    color=PALETTE["border"], lw=1.0, alpha=0.9, transform=ax.transAxes)
            ax.add_patch(FancyBboxPatch(
                (0.025, yc - ROW_H * 0.44), 0.95, ROW_H * 0.88,
                boxstyle="round,pad=0.004", lw=0, facecolor=PALETTE["card_bg"],
                transform=ax.transAxes, zorder=1,
            ))
            ax.text(0.02 + WIDTHS[0] / 2, yc, "AVG",
                    color=PALETTE["text_secondary"], fontsize=10.5, fontweight="black",
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
            xp = 0.02 + WIDTHS[0]
            xp += WIDTHS[1]  # skip PITCH column
            if evs:
                ax.text(xp + WIDTHS[2] / 2, yc, f'{np.mean(evs):.1f}',
                        color=PALETTE["accent_orange"], fontsize=10.5, fontweight="black",
                        ha="center", va="center", transform=ax.transAxes, zorder=3)
            xp += WIDTHS[2]
            if las:
                sign = "+" if np.mean(las) > 0 else ""
                ax.text(xp + WIDTHS[3] / 2, yc, f'{sign}{np.mean(las):.1f}\u00b0',
                        color=PALETTE["text_secondary"], fontsize=10.5,
                        ha="center", va="center", transform=ax.transAxes, zorder=3)
            xp += WIDTHS[3]
            xp += WIDTHS[4]  # skip TRAJ column
            if xbas and has_xba:
                ax.text(xp + WIDTHS[5] / 2, yc,
                        f'.{str(round(np.mean(xbas), 3)).split(".")[-1].ljust(3, "0")}',
                        color=PALETTE["accent_orange"], fontsize=10.5, fontweight="black",
                        ha="center", va="center", transform=ax.transAxes, zorder=3)

    ax.set_title("BATTED BALLS",
                 color=PALETTE["text_secondary"], fontsize=13,
                 fontweight="black", pad=10)


# ──────────────────────────────── PANEL 5 — FOOTER ──────────────────────────

def plot_footer(ax, sabermetrics):
    """
    Curated game-level metrics rendered as a clean closing strip. Keep this
    selective so the footer reads as editorial context rather than inventory.
    """
    _clean(ax, PALETTE["card_bg"])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    sm = sabermetrics

    def _fmt_rate(v):
        if v is None:
            return None
        s = f"{v:.3f}"
        return s[1:] if s.startswith("0") else s

    ax.axhline(0.94, color=PALETTE["border"], linewidth=0.9, alpha=0.9)

    items: list[tuple[str, str, str]] = []

    def _add(label: str, value: str | None, highlight: str = "neutral"):
        if value is not None and len(items) < 5:
            items.append((label, value, highlight))

    if sm.get("tb"):
        _add("TOTAL BASES", f'{sm["tb"]} TB')
    if sm.get("max_ev") is not None:
        _add("MAX EV", f'{sm["max_ev"]:.0f} mph', "impact")
    if sm.get("hard_hit_ct") is not None:
        hh_val = f'{sm["hard_hit_ct"]}'
        if sm.get("hard_hit_pct") is not None:
            hh_val = f'{hh_val} / {sm["hard_hit_pct"]}%'
        _add("HARD HIT", hh_val)
    if sm.get("hr_dist"):
        _add("HR DIST", f'{sm["hr_dist"]:.0f} ft', "impact")
    if sm.get("xwoba") is not None:
        _add("xwOBA", _fmt_rate(sm["xwoba"]), "impact")
    elif sm.get("woba") is not None:
        _add("wOBA", _fmt_rate(sm["woba"]), "impact")
    if sm.get("re24") is not None:
        sign = "+" if sm["re24"] > 0 else ""
        hl   = "positive" if sm["re24"] > 0 else "negative"
        _add("RE24", f'{sign}{sm["re24"]:.2f}', hl)
    if sm.get("bat_speed") is not None:
        _add("BAT SPEED", f'{sm["bat_speed"]} mph')
    if sm.get("p_per_pa"):
        _add("P/PA", f'{sm["p_per_pa"]:.1f}')
    if sm.get("p_seen"):
        _add("PITCHES", f'{sm["p_seen"]}')

    n           = max(len(items), 1)
    branding_w  = 0.23
    avail_w     = 1.0 - branding_w - 0.025
    tile_gap    = 0.014
    tile_w      = (avail_w - tile_gap * (n - 1)) / n
    tile_y0     = 0.16
    tile_h      = 0.66

    for j, (label, val, highlight) in enumerate(items):
        x0 = 0.012 + j * (tile_w + tile_gap)

        if highlight == "positive":
            tile_bg   = "#1A3D1A" if not LIGHT_MODE else "#EEF8F1"
            tile_edge = PALETTE["accent_green"]
            v_col     = PALETTE["accent_green"]
        elif highlight == "negative":
            tile_bg   = "#3D1A1A" if not LIGHT_MODE else "#FFF2F0"
            tile_edge = PALETTE["accent_red"]
            v_col     = PALETTE["accent_red"]
        elif highlight == "impact":
            tile_bg   = "#3A2414" if not LIGHT_MODE else "#FFF1E6"
            tile_edge = PALETTE["accent_orange"]
            v_col     = PALETTE["accent_orange"]
        else:
            tile_bg   = PALETTE["panel_bg"]
            tile_edge = PALETTE["border"]
            v_col     = PALETTE["text_primary"]

        ax.add_patch(FancyBboxPatch(
            (x0, tile_y0), tile_w, tile_h,
            boxstyle="round,pad=0.012",
            lw=1.0, edgecolor=tile_edge,
            facecolor=tile_bg,
            transform=ax.transAxes, zorder=1,
        ))

        ax.text(x0 + tile_w / 2, tile_y0 + tile_h * 0.60, val,
                color=v_col, fontsize=14.5, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=2)

        ax.text(x0 + tile_w / 2, tile_y0 + tile_h * 0.24, label,
                color=PALETTE["text_lo"], fontsize=8.0, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=2)

    bx = 1.0 - branding_w / 2
    ax.text(bx, 0.62, "@Mallitalytics",
            color=PALETTE["accent_orange"], fontsize=13, fontweight="black",
            ha="center", va="center", transform=ax.transAxes)
    ax.text(bx, 0.34, "MLB  |  Statcast",
            color=PALETTE["text_secondary"], fontsize=8.6, fontweight="bold",
            ha="center", va="center", transform=ax.transAxes)


# ──────────────────────────────── CARD SNAPSHOT (queue / AI redraft) ─────────

def _build_batter_card_snapshot(
    batter_id: int,
    output_path: str,
    bio: dict,
    gd: dict,
    *,
    feed_path: str | None = None,
    parquet_path: str | None = None,
) -> dict:
    """Richer JSON for queue / redraft (schema v2 when parquet or extended fields present)."""
    src = feed_path or parquet_path or ""
    fn = Path(src).name if src else Path(output_path).name
    game_pk = None
    m = re.search(r"game_(\d+)_(\d{8})_", fn)
    if m:
        try:
            game_pk = int(m.group(1))
        except ValueError:
            pass
    pm = gd.get("pitch_mix") or {}
    pitch_mix = {str(k): int(v) for k, v in pm.items()}
    line = {
        "ab": gd.get("ab"),
        "h": gd.get("h"),
        "hr": gd.get("hr"),
        "rbi": gd.get("rbi"),
        "bb": gd.get("bb"),
        "k": gd.get("k"),
    }
    bbe = gd.get("batted_balls") or []
    mix_sorted = sorted(pitch_mix.items(), key=lambda kv: -kv[1])[:5]
    sab = gd.get("sabermetrics") or {}
    notable = _derive_notable_batter_events(gd)
    recent_context = {
        "bbe_in_game": len(bbe),
        "xwoba_game": sab.get("xwoba"),
        "hard_hit_pct": sab.get("hard_hit_pct"),
    }
    batter_tweet_context = {
        "top_pitch_types_seen": [f"{a} ({n})" for a, n in mix_sorted[:3]],
        "parquet_only": bool(gd.get("parquet_only")),
        "notable_game_events": notable,
        "hero_headline": notable[0]["label"] if notable else None,
        "max_ev_mph": sab.get("max_ev"),
        "avg_ev_mph": sab.get("avg_ev"),
        "bat_speed_mph": sab.get("bat_speed"),
        "re24": sab.get("re24"),
    }
    return {
        "schema_version": 2,
        "card_type": "batter_card",
        "batter_id": batter_id,
        "player_name": bio.get("name"),
        "team": bio.get("team"),
        "bat_side": bio.get("hand"),
        "game_date": gd.get("game_date"),
        "opponent": gd.get("opponent"),
        "batter_team": gd.get("batter_team"),
        "score": gd.get("score_str"),
        "line": line,
        "box": line,
        "pa_log": (gd.get("pa_log") or [])[:30],
        "batted_balls": bbe[:35],
        "recent_context": recent_context,
        "batter_tweet_context": batter_tweet_context,
        "notable_game_events": notable,
        "sabermetrics": gd.get("sabermetrics"),
        "pitch_mix": pitch_mix,
        "game_pk": game_pk,
        "source_feed": Path(feed_path).name if feed_path else None,
        "source_parquet": Path(parquet_path).name if parquet_path else None,
        "output_image": Path(output_path).name,
        "recent_outings": [],
    }


# ──────────────────────────────── MAIN RENDER ────────────────────────────────

def generate_batter_card(
    batter_id: int,
    output_path: str = "batter_card.png",
    *,
    feed_path: str | None = None,
    parquet_path: str | None = None,
):
    """
    Generate a Mallitalytics batter card PNG from feed_live and/or Statcast parquet.

    Exactly one of ``feed_path`` or ``parquet_path`` must be set.
    """
    if (feed_path is None) == (parquet_path is None):
        raise ValueError("Provide exactly one of feed_path or parquet_path")

    mpl.rcParams["figure.dpi"]  = 200
    mpl.rcParams["font.family"] = "DejaVu Sans"

    print(f"  Parsing {'feed' if feed_path else 'parquet'} for player {batter_id}...")
    if feed_path:
        gd = parse_batter_game(feed_path, batter_id)
    else:
        gd = parse_batter_game_from_parquet(parquet_path, batter_id)

    # Aggregate pitcher handedness for header summary
    hands = {
        pa.get("pitcher_hand")
        for pa in gd["pa_log"]
        if pa.get("pitcher_hand") in {"R", "L"}
    }
    gd["pitcher_hand_summary"] = list(hands)[0] if len(hands) == 1 else None

    print("  Fetching bio + assets...")
    bio      = fetch_player_bio(batter_id)
    bio      = _apply_bio_fallback(bio, gd)
    headshot = fetch_headshot(batter_id)

    # Prefer national flag for WBC games; fall back to MLB team logo
    batter_team = gd.get("batter_team", "")
    flag_img    = _fetch_flag_image(batter_team)
    if flag_img:
        logo    = flag_img
        is_flag = True
        print(f"  Flag: {batter_team}")
    else:
        logo    = fetch_team_logo(bio["team"])
        is_flag = False

    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(PALETTE["card_bg"])

    # Taller header (1.9) + taller footer (1.25) relative to original
    gs = gridspec.GridSpec(
        4, 3, figure=fig,
        height_ratios=[1.9, 0.04, 3.9, 1.25],
        width_ratios=[1.15, 1.0, 0.85],
        hspace=0.08, wspace=0.06,
        left=0.02, right=0.98, top=0.98, bottom=0.03,
    )

    ax_hdr  = fig.add_subplot(gs[0, :])
    ax_sep  = fig.add_subplot(gs[1, :])
    ax_zone = fig.add_subplot(gs[2, 0])
    ax_pa   = fig.add_subplot(gs[2, 1])
    ax_bb   = fig.add_subplot(gs[2, 2])
    ax_foot = fig.add_subplot(gs[3, :])

    _clean(ax_sep, PALETTE["card_bg"])

    plot_header(ax_hdr, bio, gd, headshot, logo, is_flag=is_flag)
    plot_pitch_heatmap(ax_zone, gd["pitches_data"], gd["pitch_mix"])
    plot_pa_log(ax_pa, gd["pa_log"])
    plot_batted_ball(ax_bb, gd["batted_balls"])
    plot_footer(ax_foot, gd["sabermetrics"])

    from pathlib import Path as _Path
    _Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    fig.savefig(output_path, dpi=200, bbox_inches="tight",
                facecolor=PALETTE["card_bg"], edgecolor="none")

    snapshot = _build_batter_card_snapshot(
        batter_id,
        output_path,
        bio,
        gd,
        feed_path=feed_path,
        parquet_path=parquet_path,
    )
    outp = Path(output_path)
    json_sidecar = outp.parent / f"{outp.stem}_card.json"
    try:
        json_sidecar.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
    except OSError:
        pass
    print("--- Card JSON ---")
    print(json.dumps(snapshot, default=str))
    print("--- End Card JSON ---")

    plt.close()
    print(f"  \u2192 Saved: {output_path}")


# ─────────────────────────────────── CLI ────────────────────────────────────

if __name__ == "__main__":
    def _out_stem(safe_nm: str, game_date: str) -> str:
        mode_sfx = "" if LIGHT_MODE else "_dark"
        base = f"batter_card_{safe_nm}_{game_date}{mode_sfx}"
        suf = (_args.output_suffix or "").strip()
        return f"{base}_{suf}" if suf else base

    # Priority 1: explicit feed + batter (power user / notebooks)
    if _args.feed and _args.batter:
        feed_path = _args.feed
        batter_id = int(_args.batter)
        try:
            gd = parse_batter_game(feed_path, batter_id)
        except Exception as exc:
            raise SystemExit(f"Failed to parse feed {feed_path} for batter {batter_id}: {exc}")

        bio = _apply_bio_fallback(fetch_player_bio(batter_id), gd)
        safe_nm = (
            bio["name"]
            .lower()
            .replace(", ", "_")
            .replace(",", "_")
            .replace(" ", "_")
            .replace(".", "")
            .replace("'", "")
        )
        game_date = gd.get("game_date") or "unknown_date"
        out_dir = _PARENT / "outputs" / "batter_cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{_out_stem(safe_nm, game_date)}.png"

        print(f"  Feed    : {feed_path}")
        print(f"  Batter  : {bio['name']} (ID {batter_id})")
        print(f"  Date    : {game_date}")
        print(f"  Output  : {out_path}\n")
        generate_batter_card(batter_id, str(out_path), feed_path=feed_path)

    # Priority 1b: parquet + batter (no feed_live on disk)
    elif _args.parquet and _args.batter:
        pq = _args.parquet
        batter_id = int(_args.batter)
        try:
            gd = parse_batter_game_from_parquet(pq, batter_id)
        except Exception as exc:
            raise SystemExit(f"Failed to parse parquet {pq} for batter {batter_id}: {exc}")
        bio = _apply_bio_fallback(fetch_player_bio(batter_id), gd)
        safe_nm = (
            bio["name"]
            .lower()
            .replace(", ", "_")
            .replace(",", "_")
            .replace(" ", "_")
            .replace(".", "")
            .replace("'", "")
        )
        game_date = gd.get("game_date") or "unknown_date"
        out_dir = _PARENT / "outputs" / "batter_cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{_out_stem(safe_nm, game_date)}.png"
        print(f"  Parquet : {pq}")
        print(f"  Batter  : {bio['name']} (ID {batter_id})")
        print(f"  Date    : {game_date}")
        print(f"  Output  : {out_path}\n")
        generate_batter_card(batter_id, str(out_path), parquet_path=pq)

    # Priority 2: multiple batters + date → search warehouse raws (daily workflow)
    elif _args.batters:
        target_date = _parse_date_arg(_args.date)
        if target_date is None:
            raise SystemExit("--batters requires --date (yesterday or YYYY-MM-DD)")

        batter_ids = [
            int(x.strip())
            for x in _args.batters.split(",")
            if x.strip()
        ]
        if not batter_ids:
            raise SystemExit("No valid batter IDs parsed from --batters.")

        out_dir = _PARENT / "outputs" / "batter_cards"
        out_dir.mkdir(parents=True, exist_ok=True)

        for bid in batter_ids:
            feed_path = None
            pq_path = None
            try:
                feed_path = find_feed_for_batter_on_date(bid, target_date)
            except FileNotFoundError:
                try:
                    pq_path = find_parquet_for_batter_on_date(bid, target_date)
                except FileNotFoundError as exc:
                    print(f"  Batter {bid}: {exc}")
                    continue

            try:
                if feed_path is not None:
                    gd = parse_batter_game(str(feed_path), bid)
                else:
                    gd = parse_batter_game_from_parquet(str(pq_path), bid)
            except Exception as exc:
                src = feed_path.name if feed_path is not None else Path(pq_path).name
                print(f"  Batter {bid}: failed to parse {src}: {exc}")
                continue

            bio = _apply_bio_fallback(fetch_player_bio(bid), gd)
            safe_nm = (
                bio["name"]
                .lower()
                .replace(", ", "_")
                .replace(",", "_")
                .replace(" ", "_")
                .replace(".", "")
                .replace("'", "")
            )
            game_date = gd.get("game_date") or target_date.isoformat()
            out_path = out_dir / f"{_out_stem(safe_nm, game_date)}.png"

            print(f"  Batter  : {bio['name']} (ID {bid})")
            print(f"  Date    : {game_date}")
            if feed_path is not None:
                print(f"  Feed    : {feed_path.name}")
            else:
                print(f"  Parquet : {Path(pq_path).name}")
            print(f"  Output  : {out_path}")
            if feed_path is not None:
                generate_batter_card(bid, str(out_path), feed_path=str(feed_path))
            else:
                generate_batter_card(bid, str(out_path), parquet_path=str(pq_path))
            print()

    # Priority 3: legacy defaults
    else:
        default_feed   = "game_746255_20240921_feed_live.json"
        default_batter = 656305  # Matt Chapman
        feed_path = _args.feed or default_feed
        batter_id = int(_args.batter or default_batter)
        if not Path(feed_path).exists():
            local_fallback = _PARENT / "data" / "warehouse" / "mlb" / "2025" / "all_star" / "raw" / "game_778566_20250715_feed_live.json"
            if local_fallback.exists() and not _args.feed and not _args.batter:
                feed_path = str(local_fallback)
                batter_id = 624413  # Pete Alonso; HR sample for visual smoke tests.

        try:
            gd = parse_batter_game(feed_path, batter_id)
        except Exception:
            game_date = "sample"
        else:
            game_date = gd.get("game_date") or "sample"

        bio = _apply_bio_fallback(fetch_player_bio(batter_id), gd if "gd" in locals() else {})
        safe_nm = (
            bio["name"]
            .lower()
            .replace(", ", "_")
            .replace(",", "_")
            .replace(" ", "_")
            .replace(".", "")
            .replace("'", "")
        )
        mode_sfx = "" if LIGHT_MODE else "_dark"
        out_dir = _PARENT / "outputs" / "batter_cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{_out_stem(safe_nm, game_date)}.png"

        print(f"  (Default) Feed   : {feed_path}")
        print(f"  (Default) Batter : {bio['name']} (ID {batter_id})")
        print(f"  (Default) Output : {out_path}\n")
        generate_batter_card(batter_id, str(out_path), feed_path=feed_path)
