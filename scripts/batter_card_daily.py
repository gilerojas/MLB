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
from collections import defaultdict
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

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

if "MPLBACKEND" not in os.environ:
    os.environ["MPLBACKEND"] = "Agg"

_PARENT = Path(__file__).resolve().parent.parent

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
    "card_bg":        "#F8F9FA",
    "header_bg":      "#FFFFFF",
    "panel_bg":       "#FFFFFF",
    "table_bg":       "#FFFFFF",
    "table_alt":      "#F1F3F5",
    "text_primary":   "#1A202C",
    "text_secondary": "#4A5568",
    "text_lo":        "#A0AEC0",
    "accent_orange":  "#DD6B20",
    "accent_green":   "#38A169",
    "accent_red":     "#E53E3E",
    "accent_gold":    "#B7791F",
    "grid":           "#E2E8F0",
    "border":         "#CBD5E0",
    "zone_edge":      "#718096",
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

def _warehouse_root() -> Path:
    """
    Default MLB warehouse root (mirrors pitcher card + load_mlb_warehouse):
    <repo>/data/warehouse/mlb
    """
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
    if not warehouse_root.exists():
        raise FileNotFoundError(
            f"Warehouse root not found: {warehouse_root} "
            "(expected data/warehouse/mlb relative to repo root)."
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


def _neutralize_headshot_bg(img, replace_rgb=(0x1F, 0x2E, 0x3D)):
    """Replace green/teal MLB headshot background with a neutral color."""
    arr = np.array(img)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return img
    r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
    green_bg = (
        (g > r) & (g > b) & (g > 80) &
        (np.abs(g.astype(int) - r) + np.abs(g.astype(int) - b) > 40)
    )
    arr[green_bg, 0] = replace_rgb[0]
    arr[green_bg, 1] = replace_rgb[1]
    arr[green_bg, 2] = replace_rgb[2]
    return Image.fromarray(arr)


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
        img = Image.open(BytesIO(resp.content)).convert("RGB")
        replace = (255, 255, 255) if LIGHT_MODE else (0x1F, 0x2E, 0x3D)
        return _neutralize_headshot_bg(img, replace_rgb=replace)
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

        result = play.get("result", {})
        event  = result.get("event", "")
        rbi    = result.get("rbi", 0)

        rbi_total += rbi
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
        })

        # Annotate the result pitch entry in pitches_data with EV for heatmap markers
        if contact_in_pa is not None:
            for pd_entry in reversed(pitches_data):
                if pd_entry.get("event"):
                    pd_entry["ev"] = contact_in_pa.get("ev")
                    break

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
        "hr_dist":       max([b["dist"] for b in batted_balls if b.get("result") == "Home Run" and b.get("dist")]) if batted_balls else None,
        # Enriched Statcast extras
        "xwoba":         round(float(np.mean(bip_xwoba)), 3) if bip_xwoba else None,
        "woba":          round(float(np.mean(bip_woba)),  3) if bip_woba  else None,
        "bat_speed":     round(float(np.mean(bat_spd_vals)), 1) if bat_spd_vals else None,
        "swing_length":  round(float(np.mean(sl_vals)), 1) if sl_vals else None,
        "re24":          round(re24_total, 2) if re24_total != 0.0 else None,
    }

    return {
        "batter_id":    batter_id,
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

    # ── Faded logo / flag watermark behind the right stats block ─────────────
    if logo:
        al = ax.inset_axes([0.60, 0.00, 0.40, 1.00])
        al.imshow(np.array(logo), alpha=0.07)
        al.axis("off")

    # ── Circular headshot (far left) ─────────────────────────────────────────
    if headshot:
        ai = ax.inset_axes([0.005, 0.04, 0.135, 0.92])
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
        # Orange circle border drawn as a line plot in axes fraction space
        theta = np.linspace(0, 2 * np.pi, 300)
        ai.plot(
            0.5 + 0.47 * np.cos(theta),
            0.5 + 0.47 * np.sin(theta),
            color=PALETTE["accent_orange"], linewidth=2.8,
            transform=ai.transAxes, zorder=2,
        )
        ai.set_xlim(0, 1); ai.set_ylim(0, 1); ai.axis("off")

    # ── Left text block ──────────────────────────────────────────────────────
    lx = 0.158  # left anchor for text

    # Player name — large, bold, uppercase
    ax.text(lx, 0.95, bio["name"].upper(),
            color=PALETTE["text_primary"], fontsize=25, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)

    # Team · position tag (orange accent) — prefer game context (WBC team) over MLB club
    team_tag = gd.get("batter_team") or bio["team"]
    ax.text(lx, 0.67,
            f"{team_tag}  ·  {bio['position']}",
            color=PALETTE["accent_orange"], fontsize=13, fontweight="bold",
            ha="left", va="top", transform=ax.transAxes)

    # Game Line — the batter's headline for the night
    parts = [f"{gd['h']}-for-{gd['ab']}"]
    if gd["hr"]:
        parts.append(f"{gd['hr']} HR")
    if gd["rbi"]:
        parts.append(f"{gd['rbi']} RBI")
    if gd["bb"]:
        parts.append(f"{gd['bb']} BB")
    if gd["k"]:
        parts.append(f"{gd['k']} K")
    game_line = "   ·   ".join(parts)
    ax.text(lx, 0.48, game_line,
            color=PALETTE["text_primary"], fontsize=19, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)

    # Date + scoreline context
    score_str = gd.get("score_str") or f"vs {gd['opponent']}"
    ax.text(lx, 0.27,
            f"{gd['game_date']}   ·   {score_str}",
            color=PALETTE["text_secondary"], fontsize=12, fontweight="bold",
            ha="left", va="top", transform=ax.transAxes)

    # Bio chips row — two small labeled segments
    total_pitches = len(gd.get("pitches_data", []))
    pa_count      = len(gd.get("pa_log", []))
    hand_label = {"R": "Bats R", "L": "Bats L", "S": "Switch"}.get(bio["hand"], f"B:{bio['hand']}")
    chips = [f"{hand_label}   \u00b7   Age {bio['age']}", f"{bio['height']}  {bio['weight']} lbs"]
    if total_pitches and pa_count:
        pitches_per_pa = total_pitches / pa_count
        chips.append(f"{total_pitches} pitches")
    elif total_pitches:
        chips.append(f"{total_pitches} pitches")
    chip_text = "   \u2022   ".join(chips)
    ax.text(lx, 0.09, chip_text,
            color=PALETTE["text_lo"], fontsize=9.5,
            ha="left", va="top", transform=ax.transAxes)

    # ── Vertical divider ─────────────────────────────────────────────────────
    ax.plot([0.615, 0.615], [0.06, 0.94],
            color=PALETTE["border"], lw=1.2, alpha=0.5, transform=ax.transAxes)

    # ── Right block — hero quality metric + supporting stats ─────────────────
    sm  = gd["sabermetrics"]
    rx  = 0.810  # center of right block

    def _fmt_rate(v):
        if v is None:
            return "\u2014"
        s = f"{v:.3f}"
        return s[1:] if s.startswith("0") else s

    avg_ev  = sm.get("avg_ev")
    hh_pct  = sm.get("hard_hit_pct")

    # Hero: contextual cascade — loud hard contact first, then xwOBA/wOBA
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

    ax.text(rx, 0.93, hero_label,
            color=PALETTE["text_lo"], fontsize=11, fontweight="bold",
            ha="center", va="top", transform=ax.transAxes)
    ax.text(rx, 0.78, hero_val,
            color=PALETTE["accent_orange"], fontsize=42, fontweight="black",
            ha="center", va="top", transform=ax.transAxes)

    # Supporting stats line — complementary to hero, no duplication
    max_ev   = sm.get("max_ev")
    tb       = sm.get("tb")
    p_per_pa = sm.get("p_per_pa")
    parts_s  = []
    if max_ev is not None:
        parts_s.append(f"MAX EV {max_ev:.0f}")
    if tb:
        parts_s.append(f"TB {tb}")
    if p_per_pa:
        parts_s.append(f"{p_per_pa:.1f} P/PA")
    support_text = "   \u00b7   ".join(parts_s) if parts_s else "\u2014"
    ax.text(rx, 0.27,
            support_text,
            color=PALETTE["text_secondary"], fontsize=11, fontweight="bold",
            ha="center", va="top", transform=ax.transAxes)

    # Orange accent bottom line
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
    ax.set_xlim(-2.5, 2.5)
    ax.set_ylim(0.10, 5.10)
    ax.set_aspect("equal", adjustable="box")

    # ── Build 2-D histogram and smooth with Gaussian kernel ─────────────────
    n_grid = 72
    gx = np.linspace(-2.5, 2.5, n_grid)
    gz = np.linspace(0.10, 5.10, n_grid)
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
        threshold = H_sm.max() * 0.07
        H_plot = np.where(H_sm >= threshold, H_sm, np.nan)
        cmap_copy = HEAT_CMAP.__class__(
            HEAT_CMAP.name, HEAT_CMAP._segmentdata, HEAT_CMAP.N
        )
        cmap_copy.set_bad(color=PALETTE["panel_bg"], alpha=0)
        ax.imshow(
            H_plot,
            extent=[-2.5, 2.5, 0.10, 5.10],
            origin="lower",
            cmap=cmap_copy,
            aspect="auto",
            alpha=0.92,
            vmin=threshold,
            vmax=H_sm.max() * 0.82,
            zorder=1,
        )

    # Overlay actual pitch locations at low opacity so viewers can see the raw data
    all_x = [p.get("px") for p in pitches_data if p.get("px") is not None and p.get("pz") is not None]
    all_z = [p.get("pz") for p in pitches_data if p.get("px") is not None and p.get("pz") is not None]
    if all_x:
        ax.scatter(
            all_x,
            all_z,
            s=16,
            c=PALETTE["text_primary"],
            alpha=0.25,
            linewidths=0.0,
            zorder=2,
        )

    # ── Strike zone border ───────────────────────────────────────────────────
    ax.add_patch(mpatches.Rectangle(
        (sz_left, sz_bot), sz_w, sz_h,
        linewidth=2.2, edgecolor=PALETTE["text_primary"],
        facecolor="none", linestyle="-", zorder=3,
    ))

    # Inner 3×3 grid
    zone_w, zone_h = sz_w / 3, sz_h / 3
    for i in range(1, 3):
        ax.plot(
            [sz_left + i * zone_w, sz_left + i * zone_w], [sz_bot, sz_top],
            color=PALETTE["zone_edge"], lw=0.9, linestyle="--", zorder=3, alpha=0.60,
        )
        ax.plot(
            [sz_left, sz_right], [sz_bot + i * zone_h, sz_bot + i * zone_h],
            color=PALETTE["zone_edge"], lw=0.9, linestyle="--", zorder=3, alpha=0.60,
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
            base = 140.0
            return base + max(0.0, ev - 95.0) * 6.0

        if other_hard:
            hx = [p["px"] for p in other_hard]
            hz = [p["pz"] for p in other_hard]
            sizes = [_size_for_ev(p["ev"]) for p in other_hard]
            ax.scatter(
                hx,
                hz,
                marker="*",
                s=sizes,
                color=PALETTE["accent_orange"],
                edgecolors=PALETTE["text_primary"],
                linewidths=0.7,
                zorder=6,
                alpha=0.9,
            )

        if hr_hits:
            hx_hr = [p["px"] for p in hr_hits]
            hz_hr = [p["pz"] for p in hr_hits]
            sizes_hr = [_size_for_ev(p["ev"]) * 1.3 for p in hr_hits]
            ax.scatter(
                hx_hr,
                hz_hr,
                marker="*",
                s=sizes_hr,
                color=PALETTE["accent_red"],
                edgecolors=PALETTE["text_primary"],
                linewidths=1.1,
                zorder=7,
                alpha=0.98,
            )

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
                facecolor=PALETTE["panel_bg"],
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
            ax.text(xpos, 0.062, abbr,
                    color=col, fontsize=10, fontweight="black",
                    ha="center", va="center", transform=ax.transAxes, zorder=4)
            ax.text(xpos, 0.022, f"{pct}%",
                    color=PALETTE["text_secondary"], fontsize=8.5, fontweight="bold",
                    ha="center", va="center", transform=ax.transAxes, zorder=4)

    # Thin separator above the strip
    ax.axhline(
        0.10 + (5.10 - 0.10) * 0.085,
        color=PALETTE["border"], lw=0.7, alpha=0.5, zorder=4,
    )

    ax.text(0.5, 0.975, "Catcher's View",
            color=PALETTE["text_lo"], fontsize=8, fontstyle="italic",
            ha="center", va="top", transform=ax.transAxes)

    # Subtle in-panel brand mark for X crops
    ax.text(0.03, 0.96, "@Mallitalytics",
            color=PALETTE["text_lo"], fontsize=7.5, fontweight="bold",
            ha="left", va="top", transform=ax.transAxes, alpha=0.75)

    ax.set_title("PITCH HEAT MAP",
                 color=PALETTE["text_secondary"], fontsize=13,
                 fontweight="black", pad=10)


# ──────────────────────────────── PANEL 3 — PA LOG ──────────────────────────

def plot_pa_log(ax, pa_log: list):
    _clean(ax)
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    COLS   = ["INN", "PITCHER", "RESULT", "PITCH", "#P"]
    WIDTHS = [0.10, 0.26, 0.28, 0.18, 0.18]
    HDR_Y  = 0.965
    SEP_Y  = 0.900
    n_rows = len(pa_log)
    ROW_H  = (SEP_Y - 0.04) / max(n_rows, 1)
    y_top  = SEP_Y - ROW_H * 0.45

    hdr_kw = dict(color=PALETTE["text_secondary"], fontsize=11,
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
            bg = "#FFF4E6" if LIGHT_MODE else "#3A2414"
        else:
            bg = PALETTE["table_alt"] if ri % 2 == 0 else PALETTE["table_bg"]
        ax.add_patch(FancyBboxPatch(
            (0.02, yc - ROW_H * 0.52), 0.96, ROW_H,
            boxstyle="square,pad=0", lw=0, facecolor=bg,
            transform=ax.transAxes, zorder=1,
        ))

        contact = pa.get("contact")
        pitch_hand = pa.get("pitcher_hand")
        xp = 0.02

        # Accent bar
        ax.add_patch(FancyBboxPatch(
            (0.02, yc - ROW_H * 0.52), 0.006, ROW_H,
            boxstyle="square,pad=0", lw=0, facecolor=scol, alpha=0.90,
            transform=ax.transAxes, zorder=2,
        ))

        # INN
        ax.text(xp + WIDTHS[0] / 2, yc, str(pa.get("inning", "-")),
                color=PALETTE["text_lo"], fontsize=11, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[0]

        # PITCHER
        pitcher = pa.get("pitcher", "")
        if pitch_hand in {"R", "L"}:
            pitcher_text = f"{pitcher} ({pitch_hand})"
        else:
            pitcher_text = pitcher
        ax.text(xp + WIDTHS[1] / 2, yc, pitcher_text,
                color=PALETTE["text_primary"], fontsize=10, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[1]

        # RESULT pill
        pw, ph = WIDTHS[2] * 0.80, ROW_H * 0.76
        ax.add_patch(FancyBboxPatch(
            (xp + WIDTHS[2] / 2 - pw / 2, yc - ph / 2), pw, ph,
            boxstyle="round,pad=0.006", lw=0, facecolor=scol,
            transform=ax.transAxes, zorder=2,
        ))
        tc  = "#111111" if _lum(scol) > 0.50 else "#FFFFFF"
        rbi = pa.get("rbi", 0)
        lbl = symbol if rbi == 0 else f"{symbol} ({rbi} RBI)"
        ax.text(xp + WIDTHS[2] / 2, yc, lbl,
                color=tc, fontsize=11, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[2]

        # PITCH — type that ended the PA, colored by pitch type
        rp = pa.get("result_pitch", "?")
        full_name = next(
            (k for k, v in _PITCH_ABBREV_MAP.items() if v == rp), None
        )
        pt_col = PITCH_COLORS.get(full_name, PALETTE["text_secondary"]) if full_name else PALETTE["text_secondary"]
        ax.text(xp + WIDTHS[3] / 2, yc, rp,
                color=pt_col, fontsize=11, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[3]

        # #P — pitch count for this PA
        np_val = pa.get("num_pitches", 0)
        np_col = PALETTE["accent_orange"] if np_val >= 6 else PALETTE["text_primary"]
        ax.text(xp + WIDTHS[4] / 2, yc, str(np_val) if np_val else "\u2014",
                color=np_col, fontsize=11, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)

    ax.set_title("PA LOG",
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
        WIDTHS = [0.16, 0.18, 0.14, 0.14, 0.20, 0.18]
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
    HDR_Y  = 0.965
    SEP_Y  = 0.900
    n_data = len(batted_balls) + (1 if batted_balls else 0)
    ROW_H  = (SEP_Y - 0.04) / max(n_data, 1)
    y_top  = SEP_Y - ROW_H * 0.45

    hdr_kw = dict(color=PALETTE["text_secondary"], fontsize=11,
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

        bg = PALETTE["table_alt"] if ri % 2 == 0 else PALETTE["table_bg"]
        ax.add_patch(FancyBboxPatch(
            (0.02, yc - ROW_H * 0.52), 0.96, ROW_H,
            boxstyle="square,pad=0", lw=0, facecolor=bg,
            transform=ax.transAxes, zorder=1,
        ))

        symbol, scol = _event_symbol(ball["result"])
        xp = 0.02

        # Accent strip
        ax.add_patch(FancyBboxPatch(
            (0.02, yc - ROW_H * 0.52), 0.006, ROW_H,
            boxstyle="square,pad=0", lw=0, facecolor=scol, alpha=0.90,
            transform=ax.transAxes, zorder=2,
        ))

        # RES — short result tag (ties back to PA log)
        ax.text(xp + WIDTHS[0] / 2, yc, symbol,
                color=PALETTE["text_primary"], fontsize=11, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[0]

        # PITCH — abbreviated + colored by pitch type
        pitch_type = ball.get("pitch_type", "Unknown")
        pt_short   = _pitch_abbrev(pitch_type)
        pt_col     = PITCH_COLORS.get(pitch_type, PALETTE["text_secondary"])
        ax.text(xp + WIDTHS[1] / 2, yc, pt_short,
                color=pt_col, fontsize=11, fontweight="black",
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
                    color=ev_col, fontsize=11, fontweight="bold",
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        else:
            ax.text(xp + WIDTHS[2] / 2, yc, "\u2014",
                    color=PALETTE["text_lo"], fontsize=11,
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[2]

        # LA
        if ball.get("la") is not None:
            la_col = PALETTE["text_primary"]
            sign = "+" if ball["la"] > 0 else ""
            ax.text(xp + WIDTHS[3] / 2, yc, f'{sign}{ball["la"]:.0f}\u00b0',
                    color=la_col, fontsize=11,
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        else:
            ax.text(xp + WIDTHS[3] / 2, yc, "\u2014",
                    color=PALETTE["text_lo"], fontsize=11,
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[3]

        # TRAJ + distance  (HR always shows distance only, no traj label)
        traj_raw  = ball.get("traj", "")
        dist_val  = ball.get("dist")
        is_hr     = ball.get("result") == "Home Run"
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
                color=t_col, fontsize=10, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)
        xp += WIDTHS[4]

        # xBA (optional; only when feed provides it)
        if has_xba:
            if ball.get("xba") is not None:
                xba_col = PALETTE["accent_orange"] if ball["xba"] >= 0.500 else PALETTE["text_primary"]
                ax.text(xp + WIDTHS[5] / 2, yc,
                        f'.{str(ball["xba"]).split(".")[-1][:3].ljust(3, "0")}',
                        color=xba_col, fontsize=11, fontweight="bold",
                        ha="center", va="center", transform=ax.transAxes, zorder=3)
            else:
                ax.text(xp + WIDTHS[5] / 2, yc, "\u2014",
                        color=PALETTE["text_lo"], fontsize=11,
                        ha="center", va="center", transform=ax.transAxes, zorder=3)

    # AVG footer row
    if batted_balls:
        evs  = [b["ev"]  for b in batted_balls if b.get("ev")  is not None]
        las  = [b["la"]  for b in batted_balls if b.get("la")  is not None]
        xbas = [b["xba"] for b in batted_balls if b.get("xba") is not None]
        ri_avg = len(batted_balls)
        yc = y_top - ri_avg * ROW_H
        if yc > 0.03:
            ax.plot([0.02, 0.98], [yc + ROW_H * 0.60, yc + ROW_H * 0.60],
                    color=PALETTE["border"], lw=0.6, alpha=0.7, transform=ax.transAxes)
            ax.add_patch(FancyBboxPatch(
                (0.02, yc - ROW_H * 0.52), 0.96, ROW_H,
                boxstyle="square,pad=0", lw=0, facecolor=PALETTE["card_bg"],
                transform=ax.transAxes, zorder=1,
            ))
            ax.text(0.02 + WIDTHS[0] / 2, yc, "AVG",
                    color=PALETTE["text_secondary"], fontsize=11, fontweight="black",
                    ha="center", va="center", transform=ax.transAxes, zorder=3)
            xp = 0.02 + WIDTHS[0]
            xp += WIDTHS[1]  # skip PITCH column
            if evs:
                ax.text(xp + WIDTHS[2] / 2, yc, f'{np.mean(evs):.1f}',
                        color=PALETTE["accent_orange"], fontsize=11, fontweight="bold",
                        ha="center", va="center", transform=ax.transAxes, zorder=3)
            xp += WIDTHS[2]
            if las:
                sign = "+" if np.mean(las) > 0 else ""
                ax.text(xp + WIDTHS[3] / 2, yc, f'{sign}{np.mean(las):.1f}\u00b0',
                        color=PALETTE["text_secondary"], fontsize=11,
                        ha="center", va="center", transform=ax.transAxes, zorder=3)
            xp += WIDTHS[3]
            xp += WIDTHS[4]  # skip TRAJ column
            if xbas and has_xba:
                ax.text(xp + WIDTHS[5] / 2, yc,
                        f'.{str(round(np.mean(xbas), 3)).split(".")[-1].ljust(3, "0")}',
                        color=PALETTE["accent_orange"], fontsize=11, fontweight="bold",
                        ha="center", va="center", transform=ax.transAxes, zorder=3)

    ax.set_title("BATTED BALL LOG",
                 color=PALETTE["text_secondary"], fontsize=13,
                 fontweight="black", pad=10)


# ──────────────────────────────── PANEL 5 — FOOTER ──────────────────────────

def plot_footer(ax, sabermetrics):
    """
    Sabermetric stats rendered as individual rounded tile cards for visual
    separation and readability.  RE24 receives a green/red tint based on sign.
    """
    _clean(ax, PALETTE["card_bg"])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    sm = sabermetrics

    # Build items: (label, display_value, highlight_type)
    # Focus on always-available, game-level context first.
    items: list[tuple[str, str, str]] = []
    if sm.get("tb"):
        items.append(("TOTAL BASES", f'{sm["tb"]} TB', "neutral"))
    if sm.get("p_seen"):
        items.append(("PITCHES SEEN", f'{sm["p_seen"]}', "neutral"))
    if sm.get("hard_hit_ct") is not None:
        items.append(("HARD HIT", f'{sm["hard_hit_ct"]}', "neutral"))
    # HR distance when applicable, otherwise pitches per PA if available
    if sm.get("hr_dist"):
        items.append(("HR DIST", f'{sm["hr_dist"]:.0f} ft', "neutral"))
    elif sm.get("p_per_pa"):
        items.append(("P/PA", f'{sm["p_per_pa"]:.1f}', "neutral"))
    # Optional enriched metrics if present
    if sm.get("bat_speed") is not None:
        items.append(("BAT SPEED", f'{sm["bat_speed"]} mph', "neutral"))
    if sm.get("swing_length") is not None:
        items.append(("SWING LEN", f'{sm["swing_length"]} ft', "neutral"))
    if sm.get("re24") is not None:
        sign = "+" if sm["re24"] > 0 else ""
        hl   = "positive" if sm["re24"] > 0 else "negative"
        items.append(("RE24", f'{sign}{sm["re24"]:.2f}', hl))

    n           = len(items)
    branding_w  = 0.20
    avail_w     = 1.0 - branding_w - 0.015
    tile_gap    = 0.012
    tile_w      = (avail_w - tile_gap * (n - 1)) / max(n, 1)
    tile_y0     = 0.10
    tile_h      = 0.80

    for j, (label, val, highlight) in enumerate(items):
        x0 = 0.005 + j * (tile_w + tile_gap)

        if highlight == "positive":
            tile_bg   = "#1a3d1a" if not LIGHT_MODE else "#EBF8EE"
            tile_edge = PALETTE["accent_green"]
            v_col     = PALETTE["accent_green"]
        elif highlight == "negative":
            tile_bg   = "#3d1a1a" if not LIGHT_MODE else "#FFF5F5"
            tile_edge = PALETTE["accent_red"]
            v_col     = PALETTE["accent_red"]
        else:
            tile_bg   = PALETTE["panel_bg"]
            tile_edge = PALETTE["border"]
            v_col     = PALETTE["text_primary"]

        # Tile background
        ax.add_patch(FancyBboxPatch(
            (x0, tile_y0), tile_w, tile_h,
            boxstyle="round,pad=0.018",
            lw=1.3, edgecolor=tile_edge,
            facecolor=tile_bg,
            transform=ax.transAxes, zorder=1,
        ))

        # Value — large, prominent
        ax.text(x0 + tile_w / 2, tile_y0 + tile_h * 0.64, val,
                color=v_col, fontsize=16, fontweight="black",
                ha="center", va="center", transform=ax.transAxes, zorder=2)

        # Label — small, muted, below value
        ax.text(x0 + tile_w / 2, tile_y0 + tile_h * 0.22, label,
                color=PALETTE["text_lo"], fontsize=8.5, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=2)

    # ── Branding block (right side, outside tiles) ───────────────────────────
    bx = 1.0 - branding_w / 2
    ax.text(bx, 0.65, "@Mallitalytics",
            color=PALETTE["accent_orange"], fontsize=13, fontweight="black",
            ha="center", va="center", transform=ax.transAxes)
    ax.text(bx, 0.28, "Data: MLB \u00b7 Statcast",
            color=PALETTE["text_secondary"], fontsize=8.5, fontweight="bold",
            ha="center", va="center", transform=ax.transAxes)


# ──────────────────────────────── MAIN RENDER ────────────────────────────────

def generate_batter_card(
    feed_path: str,
    batter_id: int,
    output_path: str = "batter_card.png",
):
    """
    Generate a Mallitalytics batter card PNG from a feed_live JSON.

    Parameters
    ----------
    feed_path   : path to game_{pk}_{date}_feed_live.json
    batter_id   : MLB player ID integer
    output_path : save destination
    """
    mpl.rcParams["figure.dpi"]  = 200
    mpl.rcParams["font.family"] = "DejaVu Sans"

    print(f"  Parsing feed for player {batter_id}...")
    gd = parse_batter_game(feed_path, batter_id)

    # Aggregate pitcher handedness for header summary
    hands = {
        pa.get("pitcher_hand")
        for pa in gd["pa_log"]
        if pa.get("pitcher_hand") in {"R", "L"}
    }
    gd["pitcher_hand_summary"] = list(hands)[0] if len(hands) == 1 else None

    print("  Fetching bio + assets...")
    bio      = fetch_player_bio(batter_id)
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
    plt.close()
    print(f"  \u2192 Saved: {output_path}")


# ─────────────────────────────────── CLI ────────────────────────────────────

if __name__ == "__main__":
    # Priority 1: explicit feed + batter (power user / notebooks)
    if _args.feed and _args.batter:
        feed_path = _args.feed
        batter_id = int(_args.batter)
        try:
            gd = parse_batter_game(feed_path, batter_id)
        except Exception as exc:
            raise SystemExit(f"Failed to parse feed {feed_path} for batter {batter_id}: {exc}")

        bio = fetch_player_bio(batter_id)
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
        mode_sfx = "" if LIGHT_MODE else "_dark"
        out_dir = _PARENT / "outputs" / "batter_cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"batter_card_{safe_nm}_{game_date}{mode_sfx}.png"

        print(f"  Feed    : {feed_path}")
        print(f"  Batter  : {bio['name']} (ID {batter_id})")
        print(f"  Date    : {game_date}")
        print(f"  Output  : {out_path}\n")
        generate_batter_card(feed_path, batter_id, str(out_path))

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
        mode_sfx = "" if LIGHT_MODE else "_dark"

        for bid in batter_ids:
            try:
                feed_path = find_feed_for_batter_on_date(bid, target_date)
            except FileNotFoundError as exc:
                print(f"  Batter {bid}: {exc}")
                continue

            try:
                gd = parse_batter_game(str(feed_path), bid)
            except Exception as exc:
                print(f"  Batter {bid}: failed to parse {feed_path.name}: {exc}")
                continue

            bio = fetch_player_bio(bid)
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
            out_path = out_dir / f"batter_card_{safe_nm}_{game_date}{mode_sfx}.png"

            print(f"  Batter  : {bio['name']} (ID {bid})")
            print(f"  Date    : {game_date}")
            print(f"  Feed    : {feed_path.name}")
            print(f"  Output  : {out_path}")
            generate_batter_card(str(feed_path), bid, str(out_path))
            print()

    # Priority 3: legacy defaults
    else:
        default_feed   = "game_746255_20240921_feed_live.json"
        default_batter = 656305  # Matt Chapman
        feed_path = _args.feed or default_feed
        batter_id = int(_args.batter or default_batter)

        try:
            gd = parse_batter_game(feed_path, batter_id)
        except Exception:
            game_date = "sample"
        else:
            game_date = gd.get("game_date") or "sample"

        bio = fetch_player_bio(batter_id)
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
        out_path = out_dir / f"batter_card_{safe_nm}_{game_date}{mode_sfx}.png"

        print(f"  (Default) Feed   : {feed_path}")
        print(f"  (Default) Batter : {bio['name']} (ID {batter_id})")
        print(f"  (Default) Output : {out_path}\n")
        generate_batter_card(feed_path, batter_id, str(out_path))
