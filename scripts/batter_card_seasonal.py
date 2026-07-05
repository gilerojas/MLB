"""
Mallitalytics Seasonal Batter Profile Card
==========================================

Aggregates enriched parquet data across multiple games for a batter and
renders a vertical season/tournament profile card.

Sections:
  - Header:  headshot · name · season context · primary production line
  - Story:   rolling xwOBA + batted-ball quality
  - Field:   full-width spray chart
  - Attack:  most common pitch types received and hitter performance vs each
  - Footer:  counting snapshot · Mallitalytics branding

CLI examples
------------
  python scripts/batter_card_seasonal.py --batter 656305 --season 2025
  python scripts/batter_card_seasonal.py --batter 660670 --season 2025 --dark
  python scripts/batter_card_seasonal.py --batter 660670 --parquet-dir data/warehouse/mlb/2026/wbc/pitches_enriched --context "WBC 2026" --dark
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from io import BytesIO
from pathlib import Path

import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.font_manager as font_manager
import numpy as np
import pandas as pd
import requests
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
from matplotlib.patches import FancyBboxPatch
from PIL import Image, ImageDraw

if "MPLBACKEND" not in os.environ:
    os.environ["MPLBACKEND"] = "Agg"

_PARENT = Path(__file__).resolve().parent.parent
if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

def _pixel_is_studio_backdrop(r: int, g: int, b: int) -> bool:
    if (g > r) and (g > b) and (g > 80) and (abs(int(g) - r) + abs(int(g) - b) > 40):
        return True
    mx, mn = max(r, g, b), min(r, g, b)
    return mx - mn <= 42 and (r + g + b) / 3.0 < 72


def neutralize_mlb_headshot_background(
    img: Image.Image, replace_rgb: tuple[int, int, int] = (255, 255, 255)
) -> Image.Image:
    """Replace MLB green/charcoal silo backdrops with the card background color."""
    if img.mode == "RGBA":
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[3])
        im = bg
    else:
        im = img.convert("RGB")

    arr = np.array(im, dtype=np.uint8)
    r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
    green_bg = (g > r) & (g > b) & (g > 80) & (
        np.abs(g.astype(int) - r) + np.abs(g.astype(int) - b) > 40
    )
    arr[green_bg, 0] = replace_rgb[0]
    arr[green_bg, 1] = replace_rgb[1]
    arr[green_bg, 2] = replace_rgb[2]
    im = Image.fromarray(arr, mode="RGB")

    w, h = im.size
    seeds = [
        (0, 0), (w - 1, 0), (0, h - 1), (w - 1, h - 1),
        (w // 2, 0), (w // 2, h - 1), (0, h // 2), (w - 1, h // 2),
    ]
    for xy in seeds:
        try:
            p = im.getpixel(xy)
            if isinstance(p, tuple) and len(p) >= 3 and _pixel_is_studio_backdrop(p[0], p[1], p[2]):
                ImageDraw.floodfill(im, xy, replace_rgb, thresh=40)
        except (ValueError, OSError, IndexError, TypeError):
            continue
    return im

# ─────────────────────────────── CLI ────────────────────────────────────────

_parser = argparse.ArgumentParser(description="Mallitalytics Seasonal Batter Profile Card")
_parser.add_argument("--dark", action="store_true", help="Dark / analytics theme.")
_parser.add_argument("--batter", type=int, default=660670, help="MLB player ID (default: Ronald Acuña Jr. 660670).")
_parser.add_argument("--season", type=int, default=2025, help="Season year (default 2025).")
_parser.add_argument(
    "--context", type=str, default=None,
    help='Context label shown in header, e.g. "2025 Regular Season" or "WBC 2026".',
)
_parser.add_argument(
    "--parquet-dir", type=str, default=None,
    help="Explicit directory of pitches_enriched parquet files (overrides auto-discovery).",
)
_parser.add_argument("--output", type=str, default=None, help="Output PNG path.")
_parser.add_argument(
    "--baseline-path",
    type=str,
    default=None,
    help="CSV path for league stat baselines used by stat highlighting.",
)
_parser.add_argument(
    "--debug-highlights",
    action="store_true",
    help="Print percentile/classification records for highlighted stat values.",
)
_args, _ = _parser.parse_known_args()

# ─────────────────────────────── WBC FLAGS ──────────────────────────────────

_WBC_FLAG_ISO = {
    "DOM": "do", "DR":  "do", "NED": "nl", "PUR": "pr", "USA": "us",
    "MEX": "mx", "VEN": "ve", "CUB": "cu", "PAN": "pa", "COL": "co",
    "GBR": "gb", "GRB": "gb", "ITA": "it", "NIC": "ni", "ISR": "il",
    "BRA": "br", "AUS": "au", "KOR": "kr", "JPN": "jp", "TPE": "tw",
    "CZE": "cz", "CAN": "ca",
}


def _fetch_flag_image(team_abbrev: str):
    iso = _WBC_FLAG_ISO.get((team_abbrev or "").upper().strip())
    if not iso:
        return None
    try:
        r = requests.get(f"https://flagcdn.com/w160/{iso}.png", timeout=10)
        if not r.ok or len(r.content) < 500:
            return None
        return Image.open(BytesIO(r.content)).convert("RGBA")
    except Exception:
        return None


# ─────────────────────────────── PALETTE ────────────────────────────────────

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
    "card_bg":        "#F3EFE6",
    "header_bg":      "#FCFAF5",
    "panel_bg":       "#FFFCF7",
    "table_bg":       "#FFFCF7",
    "table_alt":      "#EEE8DC",
    "text_primary":   "#1F1A14",
    "text_secondary": "#5B5448",
    "text_lo":        "#9B9385",
    "accent_orange":  "#C96A2B",
    "accent_green":   "#2F7A52",
    "accent_red":     "#C8534D",
    "accent_gold":    "#B6872B",
    "grid":           "#E2D9CA",
    "border":         "#D7CEBF",
    "zone_edge":      "#766F63",
}

LIGHT_MODE = not _args.dark
PALETTE    = _PALETTE_LIGHT if LIGHT_MODE else _PALETTE_DARK

# Seasonal differentiator: forest-green accent (daily uses orange)
SEASONAL_ACCENT = "#2E7D32" if LIGHT_MODE else "#43A047"

# Zone damage colormap: blue (struggles) → neutral → red (dominates)
_ZONE_CMAP = LinearSegmentedColormap.from_list(
    "zone_damage",
    ["#2166AC", "#F7F7F7" if LIGHT_MODE else "#3A4A5A", "#D73027"],
)

_PITCH_COLORS_LIGHT = {
    "4-Seam Fastball": "#C53030", "Sinker":        "#C05621",
    "Cutter":          "#B7791F", "Slider":        "#345C8C",
    "Sweeper":         "#394B63", "Curveball":     "#2A4365",
    "Changeup":        "#553C9A", "Splitter":      "#B83280",
    "Knuckle Curve":   "#365F91",
}
_PITCH_COLORS_DARK = {
    "4-Seam Fastball": "#FC8181", "Sinker":        "#F6AD55",
    "Cutter":          "#F6E05E", "Slider":        "#8EB5E5",
    "Sweeper":         "#A7B4C8", "Curveball":     "#63B3ED",
    "Changeup":        "#B794F4", "Splitter":      "#F687B3",
    "Knuckle Curve":   "#9BC1EF",
}
PITCH_COLORS = _PITCH_COLORS_LIGHT if LIGHT_MODE else _PITCH_COLORS_DARK

_FONT_PATH_CANDIDATES = [
    "/Library/Fonts/Montserrat-Regular.ttf",
    "/Library/Fonts/Montserrat-Bold.ttf",
    "/System/Library/Fonts/Supplemental/Montserrat-Regular.ttf",
    "/System/Library/Fonts/Supplemental/Montserrat-Bold.ttf",
    "/usr/share/fonts/truetype/montserrat/Montserrat-Regular.ttf",
    "/usr/share/fonts/truetype/montserrat/Montserrat-Bold.ttf",
]


def _configure_brand_fonts():
    for path in _FONT_PATH_CANDIDATES:
        if Path(path).exists():
            try:
                font_manager.fontManager.addfont(path)
            except Exception:
                pass
    try:
        font_manager.findfont("Montserrat", fallback_to_default=False)
        brand_font = "Montserrat"
    except Exception:
        brand_font = "DejaVu Sans"
    mpl.rcParams["font.family"] = brand_font
    mpl.rcParams["font.sans-serif"] = [brand_font, "DejaVu Sans"]


_PITCH_ABBREV_MAP = {
    "4-Seam Fastball": "FF", "Four-Seam Fastball": "FF",
    "Sinker":          "SI", "Two-Seam Fastball":  "SI",
    "Cutter":          "FC", "Slider":             "SL",
    "Sweeper":         "ST", "Changeup":           "CH",
    "Split-Finger":    "FS", "Curveball":          "CU",
    "Knuckle Curve":   "KC", "Splitter":           "FS",
}

ESPN_LOGOS = {
    "ARI": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos", "CHC": "chc",
    "CWS": "chw", "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det",
    "HOU": "hou", "KC":  "kc",  "LAA": "laa", "LAD": "lad", "MIA": "mia",
    "MIL": "mil", "MIN": "min", "NYM": "nym", "NYY": "nyy", "OAK": "oak",
    "PHI": "phi", "PIT": "pit", "SD":  "sd",  "SEA": "sea", "SF":  "sf",
    "STL": "stl", "TB":  "tb",  "TEX": "tex", "TOR": "tor", "WSH": "wsh",
}


def _pitch_abbrev(pt: str | None) -> str:
    if not pt or not isinstance(pt, str):
        return "?"
    return _PITCH_ABBREV_MAP.get(pt, pt[:2].upper())


# ─────────────────────────────── API HELPERS ────────────────────────────────

def fetch_player_bio(player_id: int) -> dict:
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


def fetch_headshot(player_id: int):
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
    key = ESPN_LOGOS.get(team_abb, team_abb.lower())
    url = (
        f"https://a.espncdn.com/combiner/i?"
        f"img=/i/teamlogos/mlb/500/scoreboard/{key}.png&h=200&w=200"
    )
    try:
        return Image.open(BytesIO(requests.get(url, timeout=10).content))
    except Exception:
        return None


def fetch_live_hitting_totals(player_id: int, season: int) -> dict:
    """Fetch current MLB Stats API season hitting totals for production stats."""
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
    params = {"stats": "season", "group": "hitting", "season": season, "sportId": 1}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        splits = resp.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        stat = splits[0].get("stat", {})
        doubles = int(stat.get("doubles", 0) or 0)
        triples = int(stat.get("triples", 0) or 0)
        hr = int(stat.get("homeRuns", 0) or 0)
        return {
            "total_pa": int(stat.get("plateAppearances", 0) or 0),
            "ab": int(stat.get("atBats", 0) or 0),
            "h": int(stat.get("hits", 0) or 0),
            "doubles": doubles,
            "triples": triples,
            "hr": hr,
            "xbh": doubles + triples + hr,
            "bb": int(stat.get("baseOnBalls", 0) or 0),
            "k": int(stat.get("strikeOuts", 0) or 0),
            "avg": float(str(stat.get("avg", "0")).replace("--", "0")),
            "obp": float(str(stat.get("obp", "0")).replace("--", "0")),
            "slg": float(str(stat.get("slg", "0")).replace("--", "0")),
            "ops": float(str(stat.get("ops", "0")).replace("--", "0")),
            "batting_line_source": "mlb_stats_api",
        }
    except Exception:
        return {}


def fetch_live_home_run_spray(player_id: int, season: int) -> pd.DataFrame:
    """Fetch current-season HR hit coordinates from MLB live feeds."""
    game_log_url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
    params = {"stats": "gameLog", "group": "hitting", "season": season, "sportId": 1}
    rows: list[dict] = []
    try:
        resp = requests.get(game_log_url, params=params, timeout=15)
        resp.raise_for_status()
        splits = resp.json().get("stats", [{}])[0].get("splits", [])
    except Exception:
        return pd.DataFrame()

    hr_games = [
        int(s.get("game", {}).get("gamePk"))
        for s in splits
        if int(s.get("stat", {}).get("homeRuns", 0) or 0) > 0 and s.get("game", {}).get("gamePk")
    ]
    for game_pk in hr_games:
        try:
            feed = requests.get(
                f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live",
                timeout=15,
            ).json()
        except Exception:
            continue
        for play in feed.get("liveData", {}).get("plays", {}).get("allPlays", []):
            if play.get("matchup", {}).get("batter", {}).get("id") != player_id:
                continue
            if play.get("result", {}).get("eventType") != "home_run":
                continue
            in_play = next(
                (ev for ev in play.get("playEvents", [])
                 if ev.get("details", {}).get("isInPlay") and ev.get("hitData")),
                None,
            )
            if not in_play:
                continue
            hit = in_play.get("hitData", {})
            coords = hit.get("coordinates", {})
            coord_x = coords.get("coordX")
            coord_y = coords.get("coordY")
            if coord_x is None or coord_y is None:
                continue
            rows.append({
                "game_pk": game_pk,
                "at_bat_number": play.get("about", {}).get("atBatIndex"),
                "events": "home_run",
                "type": "X",
                "hc_x": float(coord_x),
                "hc_y": float(coord_y),
                "hit_distance_sc": hit.get("totalDistance"),
                "launch_speed": hit.get("launchSpeed"),
                "launch_angle": hit.get("launchAngle"),
                "play_id": in_play.get("playId"),
                "spray_source": "mlb_stats_api_live",
            })

    if not rows:
        return pd.DataFrame()
    return _transform_statcast_spray_coords(pd.DataFrame(rows))


# ─────────────────────────────── DATA LOADING & AGGREGATION ─────────────────

_NO_AB_EVENTS = {
    "walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt",
    "sac_fly_double_play", "sac_bunt_double_play", "catcher_interf",
}
_HIT_EVENTS = {"single", "double", "triple", "home_run"}
_TB_MAP     = {"single": 1, "double": 2, "triple": 3, "home_run": 4}

_SWING_DESCS = {
    "swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
    "foul_bunt", "missed_bunt", "bunt_foul_tip",
    "hit_into_play", "hit_into_play_no_out", "hit_into_play_score",
}
_WHIFF_DESCS = {"swinging_strike", "swinging_strike_blocked"}

_STATCAST_HOME_X = 125.42
_STATCAST_HOME_Y = 198.27
_STATCAST_FT_PER_PX = 2.5


def _transform_statcast_spray_coords(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Statcast hc_x/hc_y into field feet with home plate at (0, 0).
    Formula follows baseball-field-viz: x=2.5*(hc_x-125.42), y=2.5*(198.27-hc_y).
    """
    out = df.copy()
    out["spray_x"] = _STATCAST_FT_PER_PX * (out["hc_x"] - _STATCAST_HOME_X)
    out["spray_y"] = _STATCAST_FT_PER_PX * (_STATCAST_HOME_Y - out["hc_y"])
    return out


def load_batter_seasonal_data(
    batter_id: int,
    parquet_dir: Path | None = None,
    season: int = 2025,
) -> pd.DataFrame:
    """
    Load all enriched parquet rows for the given batter.
    Searches the warehouse under MLB/data/warehouse/mlb/{season}/ or an explicit dir.
    """
    if parquet_dir is not None:
        pdir = Path(parquet_dir)
        files = sorted(pdir.rglob("*pitches_enriched*.parquet"))
        if not files:
            files = sorted(pdir.rglob("*.parquet"))
    else:
        root = _PARENT / "data" / "warehouse" / "mlb"
        season_root = root / str(season)
        if season_root.exists():
            files = sorted(season_root.rglob("*pitches_enriched*.parquet"))
        else:
            files = sorted(root.rglob(f"*{season}*pitches_enriched*.parquet"))

    if not files:
        return pd.DataFrame()

    frames = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            sub = df[df["batter"] == batter_id]
            if not sub.empty:
                frames.append(sub)
        except Exception:
            continue

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _ops_from_pa_events(events: pd.Series) -> float | None:
    """OBP + SLG for a PA subset (final pitch events)."""
    if events is None or events.empty:
        return None
    ab = int((~events.isin(_NO_AB_EVENTS)).sum())
    if ab == 0:
        return None
    h = int(events.isin(_HIT_EVENTS).sum())
    bb = int(events.isin({"walk", "intent_walk"}).sum())
    hbp = int((events == "hit_by_pitch").sum())
    sf = int(events.isin({"sac_fly", "sac_fly_double_play"}).sum())
    tb = int(events.map(_TB_MAP).fillna(0).sum())
    obp_denom = ab + bb + hbp + sf
    obp = (h + bb + hbp) / obp_denom if obp_denom > 0 else 0.0
    slg = tb / ab if ab > 0 else 0.0
    return round(obp + slg, 3)


def _pa_stat_line(pa_subset: pd.DataFrame) -> dict:
    """Return compact batter line stats for a final-pitch PA subset."""
    if pa_subset is None or pa_subset.empty or "events" not in pa_subset.columns:
        return {
            "pa": 0, "ab": 0, "h": 0, "hr": 0, "xbh": 0, "k": 0,
            "avg": None, "obp": None, "slg": None, "ops": None, "k_pct": None,
        }
    events = pa_subset["events"]
    pa = int(len(pa_subset))
    ab = int((~events.isin(_NO_AB_EVENTS)).sum())
    h = int(events.isin(_HIT_EVENTS).sum())
    doubles = int((events == "double").sum())
    triples = int((events == "triple").sum())
    hr = int((events == "home_run").sum())
    bb = int(events.isin({"walk", "intent_walk"}).sum())
    hbp = int((events == "hit_by_pitch").sum())
    sf = int(events.isin({"sac_fly", "sac_fly_double_play"}).sum())
    k = int((events == "strikeout").sum())
    tb = int(events.map(_TB_MAP).fillna(0).sum())
    obp_denom = ab + bb + hbp + sf
    avg = h / ab if ab else None
    obp = (h + bb + hbp) / obp_denom if obp_denom else None
    slg = tb / ab if ab else None
    return {
        "pa": pa,
        "ab": ab,
        "h": h,
        "hr": hr,
        "xbh": int(doubles + triples + hr),
        "k": k,
        "avg": round(avg, 3) if avg is not None else None,
        "obp": round(obp, 3) if obp is not None else None,
        "slg": round(slg, 3) if slg is not None else None,
        "ops": round(obp + slg, 3) if obp is not None and slg is not None else None,
        "k_pct": round(k / pa * 100, 1) if pa else None,
    }


BASELINE_DEFAULT_PATH = _PARENT / "data" / "processed" / "league_stat_baselines.csv"
BASELINE_REQUIRED_STATS = {
    "avg", "obp", "slg", "ops", "k_pct",
    "avg_ev", "max_ev", "hard_pct", "barrel_pct", "swsp_pct", "avg_dist",
    "pitch_ops", "pitch_xwoba_con", "pitch_whiff_pct", "pitch_hard_pct", "pitch_barrel_pct",
}
STAT_DIRECTIONS: dict[str, str] = {
    "avg": "higher_is_better",
    "obp": "higher_is_better",
    "slg": "higher_is_better",
    "ops": "higher_is_better",
    "avg_ev": "higher_is_better",
    "max_ev": "higher_is_better",
    "hard_pct": "higher_is_better",
    "barrel_pct": "higher_is_better",
    "swsp_pct": "higher_is_better",
    "avg_dist": "higher_is_better",
    "pitch_ops": "higher_is_better",
    "pitch_xwoba_con": "higher_is_better",
    "pitch_hard_pct": "higher_is_better",
    "pitch_barrel_pct": "higher_is_better",
    "k_pct": "lower_is_better",
    "pitch_whiff_pct": "lower_is_better",
}
PERFORMANCE_GRADIENT_LIGHT = [
    (0.0, "#2F6597"),
    (25.0, "#5F94A3"),
    (50.0, "#8B8173"),
    (75.0, "#B6872B"),
    (90.0, "#C96A2B"),
    (100.0, "#B33F2F"),
]
PERFORMANCE_GRADIENT_DARK = [
    (0.0, "#63A6E8"),
    (25.0, "#79B7C6"),
    (50.0, "#AFA79B"),
    (75.0, "#F0A830"),
    (90.0, "#F6AD55"),
    (100.0, "#FF806E"),
]
PERFORMANCE_GRADIENT = PERFORMANCE_GRADIENT_LIGHT if LIGHT_MODE else PERFORMANCE_GRADIENT_DARK
_BASELINE_QUANTILES = {
    "p10": 0.10, "p25": 0.25, "p40": 0.40, "p50": 0.50,
    "p60": 0.60, "p75": 0.75, "p90": 0.90, "p95": 0.95,
}


def _baseline_path(path: str | None = None) -> Path:
    """Resolve the reusable league-baseline CSV path."""
    return Path(path) if path else BASELINE_DEFAULT_PATH


def _league_files_for_baselines(parquet_dir: Path | None, season: int) -> list[Path]:
    """Find season parquet files for league baseline creation."""
    if parquet_dir is not None:
        pdir = Path(parquet_dir)
        files = sorted(pdir.rglob("*pitches_enriched*.parquet"))
        return files or sorted(pdir.rglob("*.parquet"))

    root = _PARENT / "data" / "warehouse" / "mlb"
    primary = root / str(season) / "regular_season" / "pitches_enriched"
    if primary.exists():
        return sorted(primary.rglob("*pitches_enriched*.parquet"))
    season_root = root / str(season)
    if season_root.exists():
        return sorted(season_root.rglob("*pitches_enriched*.parquet"))
    return sorted(root.rglob(f"*{season}*pitches_enriched*.parquet"))


def _read_league_pitch_data(files: list[Path]) -> pd.DataFrame:
    """Read only the columns needed for baseline calculations."""
    needed = [
        "batter", "events", "description", "type", "pitch_name", "game_year",
        "launch_speed", "launch_angle", "launch_speed_angle", "hit_distance_sc",
        "estimated_woba_using_speedangle",
    ]
    frames: list[pd.DataFrame] = []
    for path in files:
        try:
            frame = pd.read_parquet(path, columns=needed)
        except Exception:
            try:
                frame = pd.read_parquet(path)
                frame = frame[[c for c in needed if c in frame.columns]]
            except Exception:
                continue
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def _baseline_row(season: int, stat_name: str, values: pd.Series, source: str) -> dict | None:
    """Summarize a league stat distribution for percentile scoring."""
    vals = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(vals) < 5:
        return None
    row = {
        "season": int(season),
        "stat_name": stat_name,
        "sample_size": int(len(vals)),
        "mean": float(vals.mean()),
        "median": float(vals.median()),
        "std": float(vals.std(ddof=0)),
        "min": float(vals.min()),
        "max": float(vals.max()),
        "source": source,
    }
    for label, q in _BASELINE_QUANTILES.items():
        row[label] = float(vals.quantile(q))
    return row


def _build_player_metric_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create player-level seasonal metric rows from enriched pitch data."""
    rows: list[dict] = []
    pa_df = df[df["events"].notna()].copy() if "events" in df.columns else pd.DataFrame()
    contact = df[df["type"] == "X"].copy() if "type" in df.columns else pd.DataFrame()

    if not pa_df.empty:
        for batter, grp in pa_df.groupby("batter"):
            line = _pa_stat_line(grp)
            if line.get("pa", 0) >= 50:
                rows.append({
                    "batter": batter,
                    "avg": line.get("avg"),
                    "obp": line.get("obp"),
                    "slg": line.get("slg"),
                    "ops": line.get("ops"),
                    "k_pct": line.get("k_pct"),
                })

    if not contact.empty:
        for batter, grp in contact.groupby("batter"):
            ev = pd.to_numeric(grp.get("launch_speed"), errors="coerce").dropna()
            la = pd.to_numeric(grp.get("launch_angle"), errors="coerce").dropna()
            lsa = pd.to_numeric(grp.get("launch_speed_angle"), errors="coerce").dropna()
            dist = pd.to_numeric(grp.get("hit_distance_sc"), errors="coerce").dropna()
            if len(ev) < 30:
                continue
            rows.append({
                "batter": batter,
                "avg_ev": float(ev.mean()) if len(ev) else np.nan,
                "max_ev": float(ev.max()) if len(ev) else np.nan,
                "hard_pct": float(ev.ge(95).mean() * 100) if len(ev) else np.nan,
                "barrel_pct": float(lsa.eq(6).mean() * 100) if len(lsa) else np.nan,
                "swsp_pct": float(((la >= 8) & (la <= 32)).mean() * 100) if len(la) else np.nan,
                "avg_dist": float(dist.mean()) if len(dist) else np.nan,
            })

    return pd.DataFrame(rows)


def _build_pitch_metric_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Create batter-by-pitch-type split rows for pitch table baselines."""
    if not {"batter", "pitch_name"}.issubset(df.columns):
        return pd.DataFrame()
    rows: list[dict] = []
    for (batter, pitch_name), grp in df[df["pitch_name"].notna()].groupby(["batter", "pitch_name"]):
        if len(grp) < 40:
            continue
        swings = grp["description"].isin(_SWING_DESCS) | (grp["type"] == "X")
        whiffs = grp["description"].isin(_WHIFF_DESCS)
        contact = grp[grp["type"] == "X"]
        ev = pd.to_numeric(contact.get("launch_speed"), errors="coerce").dropna()
        lsa = pd.to_numeric(contact.get("launch_speed_angle"), errors="coerce").dropna()
        xw = pd.to_numeric(contact.get("estimated_woba_using_speedangle"), errors="coerce").dropna()
        pa_grp = grp[grp["events"].notna()] if "events" in grp.columns else pd.DataFrame()
        rows.append({
            "batter": batter,
            "pitch_name": pitch_name,
            "pitch_ops": _ops_from_pa_events(pa_grp["events"]) if len(pa_grp) >= 8 else np.nan,
            "pitch_xwoba_con": float(xw.mean()) if len(xw) >= 5 else np.nan,
            "pitch_whiff_pct": float(whiffs.sum() / swings.sum() * 100) if int(swings.sum()) >= 10 else np.nan,
            "pitch_hard_pct": float(ev.ge(95).mean() * 100) if len(ev) >= 5 else np.nan,
            "pitch_barrel_pct": float(lsa.eq(6).mean() * 100) if len(lsa) >= 5 else np.nan,
        })
    return pd.DataFrame(rows)


def build_league_baselines_if_missing(
    season: int,
    parquet_dir: Path | None = None,
    baseline_path: str | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Build and save league percentile baselines when the CSV is missing or incomplete."""
    path = _baseline_path(baseline_path)
    existing = pd.DataFrame()
    if path.exists() and not force:
        existing = pd.read_csv(path)
        if {"season", "stat_name"}.issubset(existing.columns):
            present = set(existing.loc[existing["season"] == season, "stat_name"].astype(str))
            if BASELINE_REQUIRED_STATS.issubset(present):
                return existing[existing["season"] == season].copy()

    files = _league_files_for_baselines(parquet_dir, season)
    if not files:
        if not existing.empty:
            fallback = existing.sort_values("season").groupby("stat_name", as_index=False).tail(1)
            if not fallback.empty:
                print("  Highlight baselines: using most recent available baseline season")
                return fallback
        return pd.DataFrame()

    print(f"  Highlight baselines: building {season} league context from {len(files):,} parquet files")
    league_df = _read_league_pitch_data(files)
    if league_df.empty:
        return pd.DataFrame()

    player_rows = _build_player_metric_rows(league_df)
    pitch_rows = _build_pitch_metric_rows(league_df)
    rows: list[dict] = []
    for stat in ["avg", "obp", "slg", "ops", "k_pct", "avg_ev", "max_ev", "hard_pct", "barrel_pct", "swsp_pct", "avg_dist"]:
        if stat in player_rows.columns:
            row = _baseline_row(season, stat, player_rows[stat], "player_season")
            if row:
                rows.append(row)
    for stat in ["pitch_ops", "pitch_xwoba_con", "pitch_whiff_pct", "pitch_hard_pct", "pitch_barrel_pct"]:
        if stat in pitch_rows.columns:
            row = _baseline_row(season, stat, pitch_rows[stat], "batter_pitch_type_split")
            if row:
                rows.append(row)

    built = pd.DataFrame(rows)
    if built.empty:
        return pd.DataFrame()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not existing.empty and {"season", "stat_name"}.issubset(existing.columns):
        keep = existing[~((existing["season"] == season) & existing["stat_name"].isin(built["stat_name"]))]
        out = pd.concat([keep, built], ignore_index=True, sort=False)
    else:
        out = built
    out.to_csv(path, index=False)
    print(f"  Highlight baselines: saved {path}")
    return built


def load_league_baselines(
    season: int,
    parquet_dir: Path | None = None,
    baseline_path: str | None = None,
) -> pd.DataFrame:
    """Load existing league baselines or build them from available data."""
    return build_league_baselines_if_missing(
        season=season,
        parquet_dir=parquet_dir,
        baseline_path=baseline_path,
        force=False,
    )


def compute_percentile_score(
    stat_name: str,
    raw_value: float | int | None,
    baseline: dict | pd.Series | None,
) -> dict | None:
    """Compute league-relative percentile, z-score, and favorable direction."""
    if raw_value is None or baseline is None:
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    if np.isnan(value):
        return None
    row = baseline.to_dict() if isinstance(baseline, pd.Series) else dict(baseline)
    x_points = [row.get("min"), row.get("p10"), row.get("p25"), row.get("p40"), row.get("p50"),
                row.get("p60"), row.get("p75"), row.get("p90"), row.get("p95"), row.get("max")]
    y_points = [0, 10, 25, 40, 50, 60, 75, 90, 95, 100]
    pairs = sorted(
        [(float(x), y) for x, y in zip(x_points, y_points) if x is not None and not pd.isna(x)],
        key=lambda item: item[0],
    )
    if len(pairs) < 2:
        return None
    xs = np.array([p[0] for p in pairs], dtype=float)
    ys = np.array([p[1] for p in pairs], dtype=float)
    raw_pct = float(np.interp(value, xs, ys, left=0, right=100))
    direction = STAT_DIRECTIONS.get(stat_name, "higher_is_better")
    favorable_pct = 100.0 - raw_pct if direction == "lower_is_better" else raw_pct
    mean = row.get("mean")
    std = row.get("std")
    z_score = None
    if mean is not None and std not in (None, 0) and not pd.isna(std):
        z_score = (value - float(mean)) / float(std)
    return {
        "stat": stat_name,
        "raw_value": value,
        "league_percentile": round(favorable_pct, 1),
        "raw_percentile": round(raw_pct, 1),
        "z_score": round(z_score, 3) if z_score is not None else None,
        "direction": direction,
    }


def classify_stat_value(score: dict | None) -> str | None:
    """Classify a percentile score into a readable performance bucket."""
    if not score:
        return None
    pct = score.get("league_percentile")
    if pct is None:
        return None
    if pct >= 90:
        return "Elite"
    if pct >= 75:
        return "Great"
    if pct >= 60:
        return "Above Average"
    if pct >= 40:
        return "Average"
    if pct >= 25:
        return "Below Average"
    return "Poor"


def get_performance_color(percentile: float | int | None) -> str:
    """Return a smooth cold-to-warm performance color from a favorable percentile."""
    if percentile is None:
        return PALETTE["text_secondary"]
    try:
        pct = float(percentile)
    except (TypeError, ValueError):
        return PALETTE["text_secondary"]
    if np.isnan(pct):
        return PALETTE["text_secondary"]
    pct = min(100.0, max(0.0, pct))
    stops = PERFORMANCE_GRADIENT
    if pct <= stops[0][0]:
        return stops[0][1]
    if pct >= stops[-1][0]:
        return stops[-1][1]
    for (left_pct, left_color), (right_pct, right_color) in zip(stops, stops[1:]):
        if left_pct <= pct <= right_pct:
            span = right_pct - left_pct
            t = 0.0 if span <= 0 else (pct - left_pct) / span
            left_rgb = np.array(mpl.colors.to_rgb(left_color))
            right_rgb = np.array(mpl.colors.to_rgb(right_color))
            mixed = left_rgb + (right_rgb - left_rgb) * t
            return mpl.colors.to_hex(mixed)
    return PALETTE["text_secondary"]


def apply_stat_highlight_style(
    sd: dict,
    stat_name: str,
    raw_value: float | int | None,
    label: str | None = None,
) -> str:
    """Return a numeric stat color using league-relative performance context."""
    lookup = sd.get("_baseline_lookup") or {}
    score = compute_percentile_score(stat_name, raw_value, lookup.get(stat_name))
    classification = classify_stat_value(score)
    color = get_performance_color(score.get("league_percentile") if score else None)
    if sd.get("_debug_highlights") and score:
        record = {
            "stat": label or stat_name,
            "value": score["raw_value"],
            "percentile": score["league_percentile"],
            "z_score": score["z_score"],
            "classification": classification,
            "color": color,
            "direction": score["direction"],
        }
        print("  Highlight:", json.dumps(record, ensure_ascii=False))
    return color


def compute_season_stats(df: pd.DataFrame) -> dict:
    """Aggregate all seasonal stats from a batter's enriched parquet rows."""
    if df.empty:
        return {}

    df = df.copy()
    if "description" in df.columns:
        desc = (
            df["description"].astype(str)
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace(",", "", regex=False)
        )
        bip_like = desc.str.contains("in_play|hit_into_play", na=False, regex=True)
        df["description"] = desc.where(~bip_like, "hit_into_play").replace("nan", pd.NA)
    if "type" in df.columns:
        df["type"] = df["type"].astype(str).str.strip().str.upper().replace("NAN", pd.NA)
    if "bb_type" in df.columns:
        df["bb_type"] = (
            df["bb_type"].astype(str)
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .replace({"nan": pd.NA, "": pd.NA})
        )

    df = df.drop_duplicates(
        subset=["game_pk", "at_bat_number", "pitch_number"], keep="last"
    )

    # ── PA-level: events only on the final pitch of each PA ─────────────────
    pa_df = df[df["events"].notna() & (df["events"] != "")].copy()
    total_pa = len(pa_df)

    h   = pa_df["events"].isin(_HIT_EVENTS).sum()
    singles = (pa_df["events"] == "single").sum()
    doubles = (pa_df["events"] == "double").sum()
    triples = (pa_df["events"] == "triple").sum()
    hr  = (pa_df["events"] == "home_run").sum()
    xbh = int(doubles + triples + hr)
    bb  = pa_df["events"].isin({"walk", "intent_walk"}).sum()
    hbp = (pa_df["events"] == "hit_by_pitch").sum()
    sf  = pa_df["events"].isin({"sac_fly", "sac_fly_double_play"}).sum()
    k   = (pa_df["events"] == "strikeout").sum()
    ab  = int((~pa_df["events"].isin(_NO_AB_EVENTS)).sum())
    tb  = int(pa_df["events"].map(_TB_MAP).fillna(0).sum())

    avg = h / ab if ab > 0 else 0.0
    obp = (h + bb + hbp) / (ab + bb + hbp + sf) if (ab + bb + hbp + sf) > 0 else 0.0
    slg = tb / ab if ab > 0 else 0.0

    games          = int(df["game_pk"].nunique())
    total_pitches  = len(df)

    # ── Plate discipline ─────────────────────────────────────────────────────
    is_swing    = df["description"].isin(_SWING_DESCS) | (df["type"] == "X")
    is_whiff    = df["description"].isin(_WHIFF_DESCS)
    is_in_zone  = df["zone"].between(1, 9, inclusive="both")
    is_out_zone = df["zone"].isin([11, 12, 13, 14])

    total_swings     = int(is_swing.sum())
    swinging_strikes = int(is_whiff.sum())
    zone_pitches     = int(is_in_zone.sum())
    out_zone_pitches = int(is_out_zone.sum())
    chase_swings     = int((is_swing & is_out_zone).sum())

    def _pct(num, den):
        return round(num / den * 100, 1) if den > 0 else None

    in_zone_swings = int((is_swing & is_in_zone).sum())
    in_zone_contacts = int((((df["type"] == "X") | df["description"].isin({"foul", "foul_tip", "foul_bunt", "bunt_foul_tip"})) & is_in_zone).sum())
    out_zone_contacts = int((((df["type"] == "X") | df["description"].isin({"foul", "foul_tip", "foul_bunt", "bunt_foul_tip"})) & is_out_zone).sum())

    chase_pct  = _pct(chase_swings,     out_zone_pitches)
    whiff_pct  = _pct(swinging_strikes, total_swings)
    swstr_pct  = _pct(swinging_strikes, total_pitches)
    zone_pct   = _pct(zone_pitches,     total_pitches)
    k_pct      = _pct(int(k),           total_pa)
    bb_pct     = _pct(int(bb),          total_pa)
    contact_pct = _pct(int((df["type"] == "X").sum()), total_swings)
    z_swing_pct = _pct(in_zone_swings, zone_pitches)
    o_contact_pct = _pct(out_zone_contacts, chase_swings)
    z_contact_pct = _pct(in_zone_contacts, in_zone_swings)

    # ── Batted ball types ────────────────────────────────────────────────────
    bip = df[df["bb_type"].notna() & (df["bb_type"] != "")] if "bb_type" in df.columns else pd.DataFrame()
    total_bip = len(bip)
    gb_pct = _pct(int((bip["bb_type"] == "ground_ball").sum()), total_bip) if total_bip else None
    fb_pct = _pct(int((bip["bb_type"] == "fly_ball").sum()),    total_bip) if total_bip else None
    ld_pct = _pct(int((bip["bb_type"] == "line_drive").sum()),  total_bip) if total_bip else None
    pu_pct = _pct(int((bip["bb_type"] == "popup").sum()),       total_bip) if total_bip else None

    # ── Statcast metrics — BIP (type=="X") only for EV/barrel metrics ─────────
    bip_contact = df[df["type"] == "X"]
    evs      = bip_contact["launch_speed"].dropna()
    avg_ev   = round(float(evs.mean()), 1) if len(evs) else None
    max_ev   = round(float(evs.max()),  1) if len(evs) else None
    hard_pct = round((evs >= 95).sum() / len(evs) * 100, 1) if len(evs) else None
    hard_hit_ct = int((evs >= 95).sum()) if len(evs) else 0

    # xwOBA: mean across all pitches that have a value (Savant methodology)
    xwoba_v = df["estimated_woba_using_speedangle"].dropna()
    xwoba   = round(float(xwoba_v.mean()), 3) if len(xwoba_v) else None

    woba_v = df["woba_value"].dropna()
    woba   = round(float(woba_v.mean()), 3) if len(woba_v) else None

    bat_spd_v  = bip_contact["bat_speed"].dropna()    if "bat_speed"    in df.columns else pd.Series(dtype=float)
    sl_v       = bip_contact["swing_length"].dropna() if "swing_length" in df.columns else pd.Series(dtype=float)
    bat_speed    = round(float(bat_spd_v.mean()), 1) if len(bat_spd_v) else None
    swing_length = round(float(sl_v.mean()),      1) if len(sl_v)      else None

    re24 = round(float(df["delta_run_exp"].sum()), 2) if "delta_run_exp" in df.columns else None

    barrel_pct = None
    barrel_ct = 0
    swsp_pct = None
    if "launch_speed_angle" in df.columns:
        lsa = bip_contact[bip_contact["launch_speed_angle"].notna()]
        if len(lsa):
            barrel_ct = int((lsa["launch_speed_angle"] == 6).sum())
            barrel_pct = round(barrel_ct / len(lsa) * 100, 1)

    if "launch_angle" in df.columns:
        las = bip_contact["launch_angle"].dropna()
        if len(las):
            swsp_pct = round(((las >= 8) & (las <= 32)).sum() / len(las) * 100, 1)

    # ── Zone damage map — xwOBA on contact (type=="X") per zone ──────────────
    zone_damage: dict[int, dict] = {}
    for z in range(1, 15):
        zrows = bip_contact[
            (bip_contact["zone"] == z) &
            bip_contact["estimated_woba_using_speedangle"].notna()
        ]
        zone_damage[z] = {
            "xwoba":     round(float(zrows["estimated_woba_using_speedangle"].mean()), 3) if len(zrows) else None,
            "n":         int(len(zrows)),
            "n_pitches": int((df["zone"] == z).sum()),
        }

    # Chase zone combined xwOBA (contact on out-of-zone pitches)
    chase_rows = bip_contact[is_out_zone[bip_contact.index] & bip_contact["estimated_woba_using_speedangle"].notna()]
    chase_xwoba = round(float(chase_rows["estimated_woba_using_speedangle"].mean()), 3) if len(chase_rows) else None

    # ── Four-part outside-zone damage map ─────────────────────────────────────
    outer_damage: dict[str, dict] = {k: {"xwoba": None, "n": 0, "n_pitches": 0} for k in ("up_left", "up_right", "down_left", "down_right")}
    attack_damage: dict[str, dict] = {k: {"xwoba": None, "n": 0, "n_pitches": 0} for k in ("heart", "shadow", "chase", "waste")}
    median_sz_top = float(df["sz_top"].dropna().median()) if "sz_top" in df.columns and df["sz_top"].notna().any() else _SZ_TOP
    median_sz_bot = float(df["sz_bot"].dropna().median()) if "sz_bot" in df.columns and df["sz_bot"].notna().any() else _SZ_BOT
    if {"plate_x", "plate_z"}.issubset(df.columns):
        loc_df = df[df["plate_x"].notna() & df["plate_z"].notna()].copy()
        if not loc_df.empty:
            loc_sz_top = loc_df["sz_top"].fillna(median_sz_top) if "sz_top" in loc_df.columns else pd.Series(median_sz_top, index=loc_df.index)
            loc_sz_bot = loc_df["sz_bot"].fillna(median_sz_bot) if "sz_bot" in loc_df.columns else pd.Series(median_sz_bot, index=loc_df.index)
            loc_df["attack_zone"] = [
                _attack_zone_key(px, pz, bot, top)
                for px, pz, bot, top in zip(loc_df["plate_x"], loc_df["plate_z"], loc_sz_bot, loc_sz_top)
            ]
            for key in attack_damage:
                attack_damage[key]["n_pitches"] = int((loc_df["attack_zone"] == key).sum())

            contact_attack = loc_df[
                (loc_df["type"] == "X") &
                loc_df["attack_zone"].notna() &
                loc_df["estimated_woba_using_speedangle"].notna()
            ]
            for key, grp in contact_attack.groupby("attack_zone"):
                if key in attack_damage:
                    attack_damage[key] = {
                        "xwoba": round(float(grp["estimated_woba_using_speedangle"].mean()), 3),
                        "n": int(len(grp)),
                        "n_pitches": attack_damage[key]["n_pitches"],
                    }

        px_all = df["plate_x"]
        pz_all = df["plate_z"]
        sz_top_all = df["sz_top"].fillna(_SZ_TOP) if "sz_top" in df.columns else pd.Series(_SZ_TOP, index=df.index)
        sz_bot_all = df["sz_bot"].fillna(_SZ_BOT) if "sz_bot" in df.columns else pd.Series(_SZ_BOT, index=df.index)
        mid_z_all = (sz_top_all + sz_bot_all) / 2
        out_mask_all = px_all.notna() & pz_all.notna() & (
            (px_all < _SZ_LEFT) | (px_all > _SZ_RIGHT) | (pz_all < sz_bot_all) | (pz_all > sz_top_all)
        )
        side_left_all = px_all <= 0
        upper_all = pz_all >= mid_z_all

        outer_pitch_masks = {
            "up_left": out_mask_all & side_left_all & upper_all,
            "up_right": out_mask_all & (~side_left_all) & upper_all,
            "down_left": out_mask_all & side_left_all & (~upper_all),
            "down_right": out_mask_all & (~side_left_all) & (~upper_all),
        }

        contact_outer = bip_contact[
            bip_contact["plate_x"].notna() &
            bip_contact["plate_z"].notna() &
            bip_contact["estimated_woba_using_speedangle"].notna()
        ].copy()
        if not contact_outer.empty:
            sz_top_c = contact_outer["sz_top"].fillna(_SZ_TOP) if "sz_top" in contact_outer.columns else pd.Series(_SZ_TOP, index=contact_outer.index)
            sz_bot_c = contact_outer["sz_bot"].fillna(_SZ_BOT) if "sz_bot" in contact_outer.columns else pd.Series(_SZ_BOT, index=contact_outer.index)
            mid_z_c = (sz_top_c + sz_bot_c) / 2
            px_c = contact_outer["plate_x"]
            pz_c = contact_outer["plate_z"]
            out_mask_c = (px_c < _SZ_LEFT) | (px_c > _SZ_RIGHT) | (pz_c < sz_bot_c) | (pz_c > sz_top_c)
            side_left_c = px_c <= 0
            upper_c = pz_c >= mid_z_c
            outer_contact_masks = {
                "up_left": out_mask_c & side_left_c & upper_c,
                "up_right": out_mask_c & (~side_left_c) & upper_c,
                "down_left": out_mask_c & side_left_c & (~upper_c),
                "down_right": out_mask_c & (~side_left_c) & (~upper_c),
            }
            for key, mask in outer_contact_masks.items():
                grp = contact_outer[mask]
                outer_damage[key] = {
                    "xwoba": round(float(grp["estimated_woba_using_speedangle"].mean()), 3) if len(grp) else None,
                    "n": int(len(grp)),
                    "n_pitches": int(outer_pitch_masks[key].sum()),
                }
        else:
            for key in outer_damage:
                outer_damage[key]["n_pitches"] = int(outer_pitch_masks[key].sum())

    # ── xwOBA by pitch type (contact only) ────────────────────────────────────
    xwoba_by_pitch: dict[str, dict] = {}
    if "pitch_name" in df.columns:
        for pt, grp in bip_contact[bip_contact["estimated_woba_using_speedangle"].notna()].groupby("pitch_name"):
            abbr = _pitch_abbrev(str(pt))
            if abbr != "?":
                xwoba_by_pitch[abbr] = {
                    "xwoba": round(float(grp["estimated_woba_using_speedangle"].mean()), 3),
                    "n":     int(len(grp)),
                }

    pitch_profile: list[dict] = []
    if "pitch_name" in df.columns:
        for pt, grp in df[df["pitch_name"].notna()].groupby("pitch_name"):
            abbr = _pitch_abbrev(str(pt))
            if abbr == "?":
                continue
            swings_pt = grp["description"].isin(_SWING_DESCS) | (grp["type"] == "X")
            out_zone_pt = grp["zone"].isin([11, 12, 13, 14])
            bip_pt = grp[grp["type"] == "X"]
            ev_pt = bip_pt["launch_speed"].dropna()
            xw_pt = bip_pt["estimated_woba_using_speedangle"].dropna()
            lsa_pt = bip_pt["launch_speed_angle"].dropna() if "launch_speed_angle" in bip_pt.columns else pd.Series(dtype=float)
            pa_pt = pa_df[pa_df["pitch_name"] == pt] if "pitch_name" in pa_df.columns else pd.DataFrame()
            ops_pt = _ops_from_pa_events(pa_pt["events"]) if not pa_pt.empty else None
            pitch_profile.append({
                "name": str(pt),
                "abbr": abbr,
                "count": int(len(grp)),
                "usage_pct": round(len(grp) / total_pitches * 100, 1) if total_pitches else 0.0,
                "ops": ops_pt,
                "xwoba": round(float(xw_pt.mean()), 3) if len(xw_pt) else None,
                "whiff_pct": _pct(int(grp["description"].isin(_WHIFF_DESCS).sum()), int(swings_pt.sum())),
                "chase_pct": _pct(int((swings_pt & out_zone_pt).sum()), int(out_zone_pt.sum())),
                "avg_ev": round(float(ev_pt.mean()), 1) if len(ev_pt) else None,
                "hard_hit_pct": _pct(int((ev_pt >= 95).sum()), int(len(ev_pt))),
                "barrel_pct": _pct(int((lsa_pt == 6).sum()), int(len(lsa_pt))),
            })
        pitch_profile = sorted(pitch_profile, key=lambda x: (-x["count"], x["abbr"]))

    # ── Rolling xwOBA / wOBA (game-by-game, 10-game rolling mean) ──────────────
    rolling_xwoba: list[tuple] = []   # list of (game_pk, rolling_xwoba)
    rolling_woba: list[tuple] = []
    if "game_pk" in df.columns and "estimated_woba_using_speedangle" in df.columns:
        game_xw = (
            df[df["estimated_woba_using_speedangle"].notna()]
            .groupby("game_pk")["estimated_woba_using_speedangle"]
            .mean()
            .sort_index()
        )
        if len(game_xw) >= 2:
            roll = game_xw.rolling(window=min(10, len(game_xw)), min_periods=1).mean()
            rolling_xwoba = list(zip(range(len(roll)), roll.values.tolist()))
    if "game_pk" in df.columns and "woba_value" in df.columns:
        game_woba = (
            df[df["woba_value"].notna()]
            .groupby("game_pk")["woba_value"]
            .mean()
            .sort_index()
        )
        if len(game_woba) >= 2:
            woba_roll = game_woba.rolling(window=min(10, len(game_woba)), min_periods=1).mean()
            rolling_woba = list(zip(range(len(woba_roll)), woba_roll.values.tolist()))

    # ── Recent form + handedness splits ──────────────────────────────────────
    game_sort_cols = [c for c in ("game_date", "game_pk") if c in pa_df.columns]
    if "game_pk" in pa_df.columns and game_sort_cols:
        game_order = (
            pa_df[game_sort_cols]
            .drop_duplicates()
            .sort_values(game_sort_cols)["game_pk"]
            .tolist()
        )
    else:
        game_order = []
    if len(game_order) >= 14:
        recent_game_ids = game_order[-14:]
        recent_label = "LAST 14 GAMES"
    elif len(game_order) >= 6:
        recent_game_ids = game_order
        recent_label = f"RECENT {len(game_order)} GAMES"
    else:
        recent_game_ids = game_order
        recent_label = "EARLY MLB SAMPLE"

    recent_pa = pa_df[pa_df["game_pk"].isin(recent_game_ids)] if recent_game_ids and "game_pk" in pa_df.columns else pa_df
    hand_splits: dict[str, dict] = {}
    if "p_throws" in pa_df.columns:
        throws = pa_df["p_throws"].astype(str).str.upper().str.strip()
        hand_splits["RHP"] = _pa_stat_line(pa_df[throws.eq("R")])
        hand_splits["LHP"] = _pa_stat_line(pa_df[throws.eq("L")])
    form_splits = {
        "recent_label": recent_label,
        "recent_games": len(recent_game_ids) if recent_game_ids else int(games),
        "recent": _pa_stat_line(recent_pa),
        "hand_splits": hand_splits,
    }

    # ── Spray chart data (batted balls only) ──────────────────────────────────
    spray_df = df[
        (df["type"] == "X") &
        df["hc_x"].notna() & df["hc_y"].notna() &
        (df["hc_x"] > 0)
    ].copy()
    spray_summary = {
        "pull_pct": None,
        "center_pct": None,
        "oppo_pct": None,
        "avg_dist": None,
    }
    if not spray_df.empty:
        spray_df = _transform_statcast_spray_coords(spray_df)
        spray_df["hard_hit"] = spray_df["launch_speed"].fillna(0).ge(95)
        batter_stand = str(df["stand"].dropna().mode().iloc[0]) if "stand" in df.columns and df["stand"].dropna().any() else "R"
        x = spray_df["spray_x"]
        if batter_stand == "L":
            spray_df["direction_bucket"] = np.where(x >= 45, "pull", np.where(x <= -45, "oppo", "center"))
        else:
            spray_df["direction_bucket"] = np.where(x <= -45, "pull", np.where(x >= 45, "oppo", "center"))
        direction_rates = spray_df["direction_bucket"].value_counts(normalize=True) * 100
        spray_summary = {
            "pull_pct": round(float(direction_rates.get("pull", 0.0)), 1),
            "center_pct": round(float(direction_rates.get("center", 0.0)), 1),
            "oppo_pct": round(float(direction_rates.get("oppo", 0.0)), 1),
            "avg_dist": round(float(spray_df["hit_distance_sc"].dropna().mean()), 1) if spray_df["hit_distance_sc"].notna().any() else None,
        }

    # ── Pitch mix ─────────────────────────────────────────────────────────────
    pitch_mix: dict[str, int] = defaultdict(int)
    if "pitch_name" in df.columns:
        for pt in df["pitch_name"].dropna():
            pitch_abbr = _pitch_abbrev(pt)
            if pitch_abbr != "?":
                pitch_mix[pitch_abbr] += 1

    # ── Batter's team (take first non-null from correct half-inning) ──────────
    batter_team = ""
    if "inning_topbot" in df.columns:
        top = df[df["inning_topbot"] == "Top"]["away_team"].dropna()
        bot = df[df["inning_topbot"] == "Bot"]["home_team"].dropna()
        all_teams = pd.concat([top, bot])
        if len(all_teams):
            batter_team = str(all_teams.mode().iloc[0])

    return {
        "games":       games,
        "total_pa":    total_pa,
        "total_pitches": total_pitches,
        "batter_team": batter_team,
        # Slash line + counting
        "ab": ab, "h": int(h), "hr": int(hr), "bb": int(bb),
        "k": int(k), "tb": tb,
        "doubles": int(doubles), "triples": int(triples), "xbh": xbh,
        "avg": round(avg, 3), "obp": round(obp, 3), "slg": round(slg, 3),
        "ops": round(obp + slg, 3),
        # Statcast
        "xwoba": xwoba, "woba": woba,
        "avg_ev": avg_ev, "max_ev": max_ev,
        "hard_pct": hard_pct, "hard_hit_ct": hard_hit_ct,
        "bat_speed": bat_speed, "swing_length": swing_length,
        "re24": re24, "barrel_pct": barrel_pct, "barrel_ct": barrel_ct, "swsp_pct": swsp_pct,
        # Plate discipline
        "k_pct": k_pct, "bb_pct": bb_pct,
        "chase_pct": chase_pct, "whiff_pct": whiff_pct,
        "swstr_pct": swstr_pct, "zone_pct": zone_pct,
        "z_swing_pct": z_swing_pct, "o_contact_pct": o_contact_pct, "z_contact_pct": z_contact_pct,
        "contact_pct": contact_pct,
        # Batted ball
        "gb_pct": gb_pct, "fb_pct": fb_pct,
        "ld_pct": ld_pct, "pu_pct": pu_pct,
        "total_bip": total_bip,
        # Zone map
        "zone_damage": zone_damage,
        "outer_damage": outer_damage,
        "chase_xwoba": chase_xwoba,
        "attack_damage": attack_damage,
        "median_sz_top": median_sz_top,
        "median_sz_bot": median_sz_bot,
        # Spray
        "spray_df": spray_df,
        "spray_summary": spray_summary,
        # Pitch mix
        "pitch_mix": dict(pitch_mix),
        # xwOBA by pitch type
        "xwoba_by_pitch": xwoba_by_pitch,
        "pitch_profile": pitch_profile,
        # Rolling xwOBA
        "rolling_xwoba": rolling_xwoba,
        "rolling_woba": rolling_woba,
        "form_splits": form_splits,
    }


# ─────────────────────────────── RENDER HELPERS ─────────────────────────────

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


def _lum(hex_color: str) -> float:
    r, g, b = mpl.colors.to_rgb(hex_color)
    return 0.299 * r + 0.587 * g + 0.114 * b


def _fmt_slash(v: float) -> str:
    """Format a rate stat: .287 → '.287', 1.000 → '1.000'"""
    s = f"{v:.3f}"
    return s if v >= 1.0 else s[1:]  # strip leading zero below 1.000


def _fmt_pct(v: float | None, digits: int = 1) -> str:
    return "—" if v is None else f"{v:.{digits}f}%"


def _panel_title(ax, title: str, subtitle: str | None = None, watermark: bool = False):
    ax.set_title("")
    ax.text(
        0.026, 0.948, title,
        color=PALETTE["text_secondary"], fontsize=9.2, fontweight="black",
        ha="left", va="top", transform=ax.transAxes, zorder=20,
    )
    if subtitle:
        ax.text(
            0.974, 0.948, subtitle,
            color=PALETTE["text_lo"], fontsize=6.8, fontweight="bold",
            ha="right", va="top", transform=ax.transAxes, zorder=20,
        )


def _metric_color(label: str, val: float | None, low_is_good: bool, refs: dict[str, tuple[float, float]]) -> str:
    if val is None:
        return PALETTE["text_secondary"]
    lo, hi = refs.get(label, (20.0, 30.0))
    if low_is_good:
        if val <= lo:
            return PALETTE["accent_green"]
        if val >= hi:
            return PALETTE["accent_red"]
        return PALETTE["accent_gold"]
    if val >= hi:
        return PALETTE["accent_green"]
    if val <= lo:
        return PALETTE["accent_red"]
    return PALETTE["accent_gold"]


# ─────────────────────────────── PANEL 1 — HEADER ───────────────────────────

def plot_header(ax, bio: dict, sd: dict, headshot, logo, context_label: str, is_flag: bool = False):
    _clean(ax, PALETTE["header_bg"])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    # Faded logo / flag watermark
    if logo:
        al = ax.inset_axes([0.60, 0.00, 0.40, 1.00])
        al.imshow(np.array(logo), alpha=0.07)
        al.axis("off")

    # Circular headshot
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
        theta = np.linspace(0, 2 * np.pi, 300)
        # Seasonal green circle instead of orange
        ai.plot(
            0.5 + 0.47 * np.cos(theta),
            0.5 + 0.47 * np.sin(theta),
            color=SEASONAL_ACCENT, linewidth=2.8,
            transform=ai.transAxes, zorder=2,
        )
        ai.set_xlim(0, 1); ai.set_ylim(0, 1); ai.axis("off")

    lx = 0.158
    team_tag = sd.get("batter_team") or bio["team"]
    hand_label = {"R": "Bats R", "L": "Bats L", "S": "Switch"}.get(bio["hand"], f"B:{bio['hand']}")

    ax.add_patch(FancyBboxPatch(
        (lx, 0.90), 0.135, 0.065,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        lw=0, facecolor=PALETTE["table_alt"], transform=ax.transAxes, zorder=2,
    ))
    ax.text(lx + 0.0675, 0.932, "SEASON PROFILE",
            color=SEASONAL_ACCENT, fontsize=9.5, fontweight="black",
            ha="center", va="center", transform=ax.transAxes, zorder=3)

    ax.text(lx, 0.84, bio["name"].upper(),
            color=PALETTE["text_primary"], fontsize=25, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)
    ax.text(lx, 0.63,
            f"{team_tag}  ·  {bio['position']}  ·  {hand_label}  ·  Age {bio['age']}",
            color=SEASONAL_ACCENT, fontsize=12, fontweight="bold",
            ha="left", va="top", transform=ax.transAxes)

    games = sd.get("games", 0)
    total_pa = sd.get("total_pa", 0)
    ax.text(lx, 0.49, f"{context_label}  ·  {games} G  ·  {total_pa} PA",
            color=PALETTE["text_primary"], fontsize=17, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)

    slash = (
        f"{_fmt_slash(sd.get('avg', 0))} / "
        f"{_fmt_slash(sd.get('obp', 0))} / "
        f"{_fmt_slash(sd.get('slg', 0))}"
    )
    ax.text(lx, 0.30, slash,
            color=PALETTE["text_secondary"], fontsize=14.5, fontweight="bold",
            ha="left", va="top", transform=ax.transAxes)

    story_parts = []
    if sd.get("hr"): story_parts.append(f"{sd['hr']} HR")
    if sd.get("xbh"): story_parts.append(f"{sd['xbh']} XBH")
    if sd.get("hard_pct") is not None: story_parts.append(f"{sd['hard_pct']:.1f}% HH")
    if sd.get("barrel_pct") is not None: story_parts.append(f"{sd['barrel_pct']:.1f}% Brl")
    ax.text(lx, 0.12, "   ·   ".join(story_parts),
            color=PALETTE["text_primary"], fontsize=14, fontweight="black",
            ha="left", va="top", transform=ax.transAxes)

    ax.plot([0.61, 0.61], [0.08, 0.92],
            color=PALETTE["border"], lw=1.0, alpha=0.65, transform=ax.transAxes)

    def _fmt_rate(v):
        if v is None:
            return "—"
        s = f"{v:.3f}"
        return s[1:] if s.startswith("0") else s

    rx = 0.815
    ax.text(rx, 0.89, "xwOBA",
            color=PALETTE["text_lo"], fontsize=11.5, fontweight="bold",
            ha="center", va="top", transform=ax.transAxes)
    ax.text(rx, 0.81, _fmt_rate(sd.get("xwoba")),
            color=SEASONAL_ACCENT, fontsize=38, fontweight="black",
            ha="center", va="top", transform=ax.transAxes)

    support = []
    if sd.get("woba") is not None:
        support.append(f"wOBA {_fmt_rate(sd['woba'])}")
    if sd.get("ops") is not None:
        support.append(f"OPS {_fmt_rate(sd['ops'])}")
    if sd.get("avg_ev") is not None:
        support.append(f"EV {sd['avg_ev']}")
    ax.text(rx, 0.18, "   ·   ".join(support),
            color=PALETTE["text_secondary"], fontsize=10.5, fontweight="bold",
            ha="center", va="top", transform=ax.transAxes)

    chip_h = 0.065
    chip_y = 0.44
    chips = [
        (0.70, f"Chase {_fmt_pct(sd.get('chase_pct'))}"),
        (0.81, f"Hard Hit {_fmt_pct(sd.get('hard_pct'))}"),
        (0.92, f"Sweet Spot {_fmt_pct(sd.get('swsp_pct'))}"),
    ]
    for x_center, text in chips:
        ax.add_patch(FancyBboxPatch(
            (x_center - 0.06, chip_y - chip_h / 2), 0.12, chip_h,
            boxstyle="round,pad=0.01,rounding_size=0.02",
            lw=1.0, edgecolor=PALETTE["border"], facecolor=PALETTE["table_alt"],
            transform=ax.transAxes, zorder=2,
        ))
        ax.text(x_center, chip_y, text,
                color=PALETTE["text_primary"], fontsize=8.0, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes, zorder=3)

    ax.plot([0, 1], [0.02, 0.02],
            color=SEASONAL_ACCENT, lw=3.0, alpha=0.85, transform=ax.transAxes)


# ─────────────────────────────── PANEL 2 — ZONE DAMAGE MAP ──────────────────

# Zone index → (row, col) in the 3×3 grid
# Row 0 = bottom (zones 1-3), Row 2 = top (zones 7-9)
_ZONE_RC = {
    1: (0, 0), 2: (0, 1), 3: (0, 2),
    4: (1, 0), 5: (1, 1), 6: (1, 2),
    7: (2, 0), 8: (2, 1), 9: (2, 2),
}

# Strike zone boundaries (feet)
_SZ_LEFT  = -0.71
_SZ_RIGHT =  0.71
_SZ_BOT   =  1.5
_SZ_TOP   =  3.5
_SZ_W     = _SZ_RIGHT - _SZ_LEFT
_SZ_H     = _SZ_TOP   - _SZ_BOT

_ATTACK_ZONE_TEMPLATE = {
    "heart": (-0.558, 0.558, 1.833, 3.166),
    "shadow": (-1.108, 1.108, 1.166, 3.833),
    "chase": (-1.666, 1.666, 0.500, 4.500),
}


def _scale_attack_zone_bounds(bounds: tuple[float, float, float, float], sz_bot: float, sz_top: float) -> tuple[float, float, float, float]:
    """Scale Statcast attack-zone template coordinates to the hitter's median zone height."""
    x0, x1, y0, y1 = bounds
    template_mid = (_SZ_TOP + _SZ_BOT) / 2
    hitter_mid = (sz_top + sz_bot) / 2
    scale = (sz_top - sz_bot) / (_SZ_TOP - _SZ_BOT)
    return (
        x0,
        x1,
        hitter_mid + (y0 - template_mid) * scale,
        hitter_mid + (y1 - template_mid) * scale,
    )


def _attack_zone_key(px: float, pz: float, sz_bot: float, sz_top: float) -> str | None:
    heart = _scale_attack_zone_bounds(_ATTACK_ZONE_TEMPLATE["heart"], sz_bot, sz_top)
    shadow = _scale_attack_zone_bounds(_ATTACK_ZONE_TEMPLATE["shadow"], sz_bot, sz_top)
    chase = _scale_attack_zone_bounds(_ATTACK_ZONE_TEMPLATE["chase"], sz_bot, sz_top)
    if heart[0] <= px <= heart[1] and heart[2] <= pz <= heart[3]:
        return "heart"
    if shadow[0] <= px <= shadow[1] and shadow[2] <= pz <= shadow[3]:
        return "shadow"
    if chase[0] <= px <= chase[1] and chase[2] <= pz <= chase[3]:
        return "chase"
    return "waste"


def _draw_home_plate_axes(ax, y0: float = 0.08, line_color: str | None = None):
    line_color = line_color or PALETTE["text_secondary"]
    plate = np.array([
        [-0.708, y0],
        [0.708, y0],
        [0.708, y0 - 0.30],
        [0.0, y0 - 0.60],
        [-0.708, y0 - 0.30],
        [-0.708, y0],
    ])
    ax.plot(plate[:, 0], plate[:, 1], color=line_color, lw=1.0, zorder=8)


def _draw_attack_zone_frames(ax, sz_bot: float, sz_top: float):
    colors = {
        "chase": PALETTE["text_lo"],
        "shadow": PALETTE["accent_orange"],
        "heart": SEASONAL_ACCENT,
    }
    for key in ("chase", "shadow", "heart"):
        x0, x1, y0, y1 = _scale_attack_zone_bounds(_ATTACK_ZONE_TEMPLATE[key], sz_bot, sz_top)
        ax.add_patch(mpatches.Rectangle(
            (x0, y0), x1 - x0, y1 - y0,
            facecolor="none",
            edgecolor=colors[key],
            linewidth=2.4 if key == "heart" else 1.7,
            alpha=0.88 if key == "heart" else 0.58,
            zorder=7,
        ))


def plot_zone_damage_map(
    ax,
    zone_damage: dict,
    outer_damage: dict,
    attack_damage: dict | None = None,
    sz_top: float = _SZ_TOP,
    sz_bot: float = _SZ_BOT,
):
    _clean(ax)
    _border(ax)
    _panel_title(ax, "ATTACK ZONE DAMAGE", "xwOBA on contact · catcher view")

    ax.set_xlim(-1.85, 1.85)
    ax.set_ylim(-0.62, 4.68)

    zone_w = _SZ_W / 3
    zone_h = (sz_top - sz_bot) / 3

    inner_vals = [
        zone_damage[z]["xwoba"]
        for z in range(1, 10)
        if zone_damage.get(z) and zone_damage[z]["xwoba"] is not None
    ]
    outer_vals = [v["xwoba"] for v in (outer_damage or {}).values() if v.get("xwoba") is not None]
    attack_vals = [v["xwoba"] for v in (attack_damage or {}).values() if v.get("xwoba") is not None]
    all_vals = inner_vals + outer_vals + attack_vals
    norm = TwoSlopeNorm(vmin=min(0.150, min(all_vals + [0.150])), vcenter=0.320, vmax=max(0.650, max(all_vals + [0.650]))) if all_vals else None

    cmap = LinearSegmentedColormap.from_list("malli_zone", [PALETTE["table_alt"], "#F1D6BF", PALETTE["accent_red"]])

    attack_damage = attack_damage or {}
    zone_order = ("waste", "chase", "shadow", "heart")
    for key in zone_order:
        if key == "waste":
            x0, x1, y0, y1 = -1.84, 1.84, -0.58, 4.66
        else:
            x0, x1, y0, y1 = _scale_attack_zone_bounds(_ATTACK_ZONE_TEMPLATE[key], sz_bot, sz_top)
        xw = attack_damage.get(key, {}).get("xwoba")
        color = cmap(norm(xw)) if xw is not None and norm else PALETTE["panel_bg"]
        alpha = {"waste": 0.20, "chase": 0.34, "shadow": 0.42, "heart": 0.52}[key]
        ax.add_patch(mpatches.Rectangle(
            (x0, y0), x1 - x0, y1 - y0,
            facecolor=color,
            edgecolor="none",
            alpha=alpha,
            zorder=1,
        ))

    _draw_attack_zone_frames(ax, sz_bot, sz_top)

    for z, (row, col) in _ZONE_RC.items():
        xw = zone_damage.get(z, {}).get("xwoba")
        color = cmap(norm(xw)) if xw is not None and norm else PALETTE["panel_bg"]
        zx = _SZ_LEFT + col * zone_w
        zy = sz_bot + row * zone_h
        ax.add_patch(mpatches.Rectangle(
            (zx, zy), zone_w, zone_h,
            facecolor=color, edgecolor=PALETTE["border"], linewidth=0.8, zorder=3
        ))
        if xw is not None:
            cx = zx + zone_w / 2
            cy = zy + zone_h / 2
            mapped_lum = _lum(mpl.colors.to_hex(color))
            tc = "#111111" if mapped_lum > 0.45 else "#FFFFFF"
            ax.text(cx, cy, _fmt_slash(xw),
                    color=tc, fontsize=12.5, fontweight="black",
                    ha="center", va="center", zorder=4)

    ax.add_patch(mpatches.Rectangle(
        (_SZ_LEFT, sz_bot), _SZ_W, sz_top - sz_bot,
        linewidth=2.4, edgecolor=PALETTE["text_secondary"], facecolor="none", zorder=7,
    ))

    _draw_home_plate_axes(ax)

    label_rows = [
        ("HEART", "heart", SEASONAL_ACCENT, 0.09),
        ("SHADOW", "shadow", PALETTE["accent_orange"], 0.32),
        ("CHASE", "chase", PALETTE["text_lo"], 0.57),
    ]
    for label, key, col, x in label_rows:
        item = attack_damage.get(key, {})
        text = _fmt_slash(item.get("xwoba")) if item.get("xwoba") is not None else "—"
        ax.text(x, 0.055, label, color=col, fontsize=6.6, fontweight="black",
                ha="left", va="center", transform=ax.transAxes, zorder=9)
        ax.text(x + 0.12, 0.055, text, color=PALETTE["text_primary"], fontsize=7.4, fontweight="black",
                ha="left", va="center", transform=ax.transAxes, zorder=9)


# ─────────────────────────────── PANEL 3 — BATTED BALL PROFILE ──────────────

def _spray_outcome(event: str) -> str:
    if event in ("single", "double", "triple", "home_run"):
        return event
    return "out"


def _draw_spray_field(ax, outfield_distance: float = 390):
    """Draw a home-plate-origin baseball field in feet for Statcast spray data."""
    grass_col = "#B7DED8" if LIGHT_MODE else "#244A45"
    field_col = "#A9D7D1" if LIGHT_MODE else "#203F3C"
    dirt_col = "#D6BC8C" if LIGHT_MODE else "#7E5A34"
    line_col = "#FFFFFF" if LIGHT_MODE else "#E2E8F0"
    outline_col = "#D8D8D3" if LIGHT_MODE else "#536270"
    label_col = "#9FD5D1" if LIGHT_MODE else "#75BDB7"

    anchor_angles = np.array([45, 60, 75, 90, 105, 120, 135], dtype=float)
    anchor_distances = np.array([330, 356, 387, 410, 387, 356, 330], dtype=float)
    theta_deg = np.linspace(45, 135, 360)
    wall_r = np.interp(theta_deg, anchor_angles, anchor_distances)
    theta = np.deg2rad(theta_deg)
    wall_x = wall_r * np.cos(theta)
    wall_y = wall_r * np.sin(theta)

    ax.plot(wall_x, wall_y, color=outline_col, lw=1.15, alpha=0.82, zorder=1)
    ax.fill(
        np.concatenate([[0], wall_x]),
        np.concatenate([[0], wall_y]),
        color=field_col,
        alpha=0.92,
        zorder=2,
    )

    base = 100 / np.sqrt(2)
    home = np.array([0.0, 0.0])
    first = np.array([base, base])
    second = np.array([0.0, base * 2])
    third = np.array([-base, base])

    infield_r = 162
    infield_theta = np.linspace(np.deg2rad(45), np.deg2rad(135), 180)
    ax.fill(
        np.concatenate([[0], infield_r * np.cos(infield_theta)]),
        np.concatenate([[0], infield_r * np.sin(infield_theta)]),
        color=grass_col,
        zorder=3,
    )
    ax.fill(
        [home[0], first[0], second[0], third[0], home[0]],
        [home[1], first[1], second[1], third[1], home[1]],
        color=dirt_col,
        alpha=0.95,
        zorder=4,
    )
    ax.add_patch(mpatches.Circle((0, 60.5), 9.0, facecolor=dirt_col, edgecolor=line_col, lw=0.9, zorder=5))

    for angle in (45, 135):
        foul_r = np.interp(angle, anchor_angles, anchor_distances) + 8
        ax.plot(
            [0, foul_r * np.cos(np.deg2rad(angle))],
            [0, foul_r * np.sin(np.deg2rad(angle))],
            color=outline_col,
            lw=1.15,
            alpha=0.82,
            zorder=6,
        )

    ax.plot(
        [home[0], first[0], second[0], third[0], home[0]],
        [home[1], first[1], second[1], third[1], home[1]],
        color=line_col,
        lw=1.1,
        zorder=8,
    )
    for bx, by in (first, second, third):
        ax.add_patch(mpatches.RegularPolygon(
            (bx, by), numVertices=4, radius=4.8, orientation=np.pi / 4,
            facecolor=line_col, edgecolor="none", zorder=9,
        ))
    ax.add_patch(mpatches.RegularPolygon(
        (0, 0), numVertices=5, radius=5.4, orientation=np.pi / 5,
        facecolor=line_col, edgecolor="none", zorder=9,
    ))

    for dist in (200, 300):
        ax.add_patch(mpatches.Arc(
            (0, 0), dist * 2, dist * 2,
            theta1=45, theta2=135,
            color=outline_col, lw=0.55, alpha=0.28, zorder=3,
        ))

    for angle, dist, label in [(45, 330, "330"), (70, 387, "387"), (90, 410, "410"), (110, 387, "387"), (135, 330, "330")]:
        tx = (dist + 24) * np.cos(np.deg2rad(angle))
        ty = (dist + 24) * np.sin(np.deg2rad(angle))
        ax.text(tx, ty, label, color=label_col, fontsize=10.5, fontweight="black",
                ha="center", va="center", alpha=0.68, zorder=3)


def plot_batted_ball_profile(ax_spray, ax_bars, spray_df: pd.DataFrame, sd: dict):
    _clean(ax_spray)
    _border(ax_spray)

    non_hr = spray_df[spray_df["events"] != "home_run"]
    if not non_hr.empty:
        nr = np.sqrt(non_hr["spray_x"] ** 2 + non_hr["spray_y"] ** 2)
        wall_r = float(nr.quantile(0.99)) + 30
        wall_r = max(360, min(wall_r, 430))
    else:
        wall_r = 390

    _draw_spray_field(ax_spray, outfield_distance=wall_r)

    _OUTCOME_STYLE = {
        "home_run": ("#E03282", 50, 1.0),
        "triple":   ("#F5AB00", 50, 1.0),
        "double":   ("#7D6EE7", 50, 1.0),
        "single":   ("#FF6B00", 50, 0.95),
        "out":      ("#B0B0B0", 30, 0.6),
    }

    for outcome in ("out", "single", "double", "triple", "home_run"):
        sub = spray_df[spray_df["events"].map(_spray_outcome) == outcome]
        if sub.empty:
            continue
        col, sz, al = _OUTCOME_STYLE[outcome]
        hx = sub["spray_x"]
        hy = sub["spray_y"]
        ax_spray.scatter(hx, hy, marker="o", s=sz, color=col, alpha=al,
                         linewidths=0.6 if outcome != "out" else 0,
                         edgecolors="#111111" if outcome != "out" else "none",
                         zorder=5 if outcome == "home_run" else 4)

    ax_spray.set_xlim(-365, 365)
    ax_spray.set_ylim(-25, 430)

    legend_elements = [
        ("HOME RUN", "#E03282"),
        ("TRIPLE", "#F5AB00"),
        ("DOUBLE", "#7D6EE7"),
        ("SINGLE", "#FF6B00"),
    ]
    leg_x = 228
    leg_y = 388
    for i, (lbl, col) in enumerate(legend_elements):
        ax_spray.scatter(leg_x, leg_y - i*18, marker="o", s=45, color=col, edgecolors="#111111", linewidths=0.8, zorder=6)
        ax_spray.text(leg_x + 10, leg_y - i*18, lbl, color=PALETTE["text_primary"], fontsize=8.0, fontweight="black",
                      ha="left", va="center", zorder=6)

    summary = sd.get("spray_summary", {})
    summary_text = []
    if summary.get("pull_pct") is not None:
        summary_text.append(f"Pull {_fmt_pct(summary['pull_pct'])}")
    if summary.get("center_pct") is not None:
        summary_text.append(f"Center {_fmt_pct(summary['center_pct'])}")
    if summary.get("oppo_pct") is not None:
        summary_text.append(f"Oppo {_fmt_pct(summary['oppo_pct'])}")
    subtitle = "   ·   ".join(summary_text) if summary_text else "Hits & Outs spray projection"
    _panel_title(ax_spray, "HITS SPRAY CHART", subtitle)

    _clean(ax_bars, PALETTE["panel_bg"])
    _border(ax_bars)
    ax_bars.set_xlim(0, 1)
    ax_bars.set_ylim(0, 1)
    _panel_title(ax_bars, "BATTED-BALL SHAPE", f"{sd.get('total_bip', 0)} balls in play", watermark=False)

    dir_rows = [
        ("PULL", sd.get("spray_summary", {}).get("pull_pct"), PALETTE["accent_orange"]),
        ("CENTER", sd.get("spray_summary", {}).get("center_pct"), SEASONAL_ACCENT),
        ("OPPO", sd.get("spray_summary", {}).get("oppo_pct"), PALETTE["accent_gold"]),
    ]
    bar_h = 0.09
    for i, (label, val, color) in enumerate(dir_rows):
        yc = 0.82 - i * 0.18
        ax_bars.text(0.06, yc, label, color=PALETTE["text_secondary"], fontsize=8.5, fontweight="black",
                     ha="left", va="center", transform=ax_bars.transAxes)
        ax_bars.add_patch(FancyBboxPatch(
            (0.24, yc - bar_h / 2), 0.46, bar_h,
            boxstyle="round,pad=0.006", lw=0,
            facecolor=PALETTE["table_alt"], transform=ax_bars.transAxes, zorder=1,
        ))
        if val is not None:
            ax_bars.add_patch(FancyBboxPatch(
                (0.24, yc - bar_h / 2), 0.46 * min(val / 100, 1.0), bar_h,
                boxstyle="round,pad=0.006", lw=0,
                facecolor=color, transform=ax_bars.transAxes, zorder=2, alpha=0.92,
            ))
        ax_bars.text(0.74, yc, _fmt_pct(val), color=PALETTE["text_primary"], fontsize=8.5, fontweight="bold",
                     ha="left", va="center", transform=ax_bars.transAxes)

    mini_data = [
        (f"{sd.get('hard_hit_ct', 0)}", "HH"),
        (f"{sd.get('barrel_ct', 0)}", "Brl"),
        (f"{int(round(summary['avg_dist']))} ft" if summary.get("avg_dist") is not None else "—", "Dist"),
    ]
    mini_text = "   ·   ".join(f"{v} {lbl}" for v, lbl in mini_data)
    ax_bars.text(0.50, 0.12, mini_text,
                 color=PALETTE["text_secondary"], fontsize=8.5, fontweight="bold",
                 ha="center", va="center", transform=ax_bars.transAxes)


# ─────────────────────────────── PANEL 4 — PLATE DISCIPLINE ─────────────────

def plot_plate_discipline(ax, sd: dict):
    _clean(ax)
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    DISC_BARS = [
        ("K%",      sd.get("k_pct"),     True),
        ("BB%",     sd.get("bb_pct"),    False),
        ("CHASE%",  sd.get("chase_pct"), True),
        ("WHIFF%",  sd.get("whiff_pct"), True),
        ("Z-SWING%", sd.get("z_swing_pct"), False),
        ("O-CONTACT%", sd.get("o_contact_pct"), False),
    ]
    valid = [(l, v, g) for l, v, g in DISC_BARS if v is not None]

    _MLB_AVG = {
        "K%": 21.0, "BB%": 8.1, "CHASE%": 29.0,
        "WHIFF%": 26.0, "Z-SWING%": 67.0, "O-CONTACT%": 53.0,
    }
    _REFS = {
        "K%":     (18.0, 28.0),
        "BB%":    (6.0,  10.0),
        "CHASE%": (24.0, 34.0),
        "WHIFF%": (22.0, 32.0),
        "Z-SWING%": (62.0, 72.0),
        "O-CONTACT%": (46.0, 60.0),
    }

    _MAX_FILL = {
        "K%": 45, "BB%": 20, "CHASE%": 55,
        "WHIFF%": 50, "Z-SWING%": 85, "O-CONTACT%": 85,
    }

    n       = len(valid)
    label_w = 0.38
    bar_x0  = label_w + 0.02
    bar_max = 0.36
    val_x   = bar_x0 + bar_max + 0.02
    row_h   = 0.74 / max(n, 1)
    y_top   = 0.85

    _panel_title(ax, "PLATE DISCIPLINE", "vs MLB avg")

    for i, (label, val, low_is_good) in enumerate(valid):
        yc    = y_top - i * row_h - row_h * 0.5
        mlb_avg = _MLB_AVG.get(label)
        if mlb_avg is not None:
            above_avg = val > mlb_avg
            good_above = not low_is_good
            color = "#D22D49" if (above_avg == good_above) else "#3373C4"
        else:
            color = PALETTE["text_secondary"]
        max_f = _MAX_FILL.get(label, 50)
        fill  = min(val / max_f, 1.0) * bar_max

        ax.add_patch(FancyBboxPatch(
            (bar_x0, yc - row_h * 0.32), bar_max, row_h * 0.64,
            boxstyle="round,pad=0.005", lw=0,
            facecolor=PALETTE["table_alt"], transform=ax.transAxes, zorder=1,
        ))
        if fill > 0.003:
            ax.add_patch(FancyBboxPatch(
                (bar_x0, yc - row_h * 0.32), fill, row_h * 0.64,
                boxstyle="round,pad=0.005", lw=0,
                facecolor=color, alpha=0.88, transform=ax.transAxes, zorder=2,
            ))

        mlb_avg = _MLB_AVG.get(label)
        if mlb_avg is not None:
            avg_fill = min(mlb_avg / max_f, 1.0) * bar_max
            ax.plot([bar_x0 + avg_fill, bar_x0 + avg_fill],
                    [yc - row_h * 0.38, yc + row_h * 0.38],
                    color=PALETTE["text_primary"], lw=1.5, alpha=0.65,
                    transform=ax.transAxes, zorder=3)

        ax.text(label_w - 0.01, yc, label,
                color=PALETTE["text_secondary"], fontsize=9.5, fontweight="black",
                ha="right", va="center", transform=ax.transAxes)
        ax.text(val_x, yc, f"{val:.1f}%",
                color=color, fontsize=10, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)

    ax.text(0.98, 0.04, "│ = MLB avg",
            color=PALETTE["text_lo"], fontsize=7, ha="right", va="bottom",
            transform=ax.transAxes)


# ─────────────────────────────── PANEL 5 — FOOTER ───────────────────────────

def plot_footer(ax, sd: dict):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    _panel_title(ax, "SEASON DNA", "Key Statcast indicators", watermark=False)

    def _eliteness_color(val, elite_thresh, poor_thresh):
        if val is None: return "#888888"
        if val >= elite_thresh: return "#D22D49"
        if val <= poor_thresh: return "#3373C4"
        return "#888888"

    c_ev = _eliteness_color(sd.get("avg_ev"), 91.0, 87.0)
    c_hard = _eliteness_color(sd.get("hard_pct"), 0.45, 0.33)
    c_brl = _eliteness_color(sd.get("barrel_pct"), 0.11, 0.05)
    c_val = _eliteness_color(sd.get("re24"), 10.0, 0.0)

    cards = [
        ("EV", f"{sd['avg_ev']:.1f}" if sd.get("avg_ev") is not None else "—",
         f"Max {sd['max_ev']:.1f}" if sd.get("max_ev") is not None else "Max exit velo",
         c_ev),
        ("HARD HIT%", f"{_fmt_pct(sd.get('hard_pct'))}",
         f"{sd.get('hard_hit_ct', 0)} hard hits",
         c_hard),
        ("BARREL%", f"{_fmt_pct(sd.get('barrel_pct'))}",
         f"SwSp {_fmt_pct(sd.get('swsp_pct'))} ",
         c_brl),
        ("RUN VALUE", f"{sd['re24']:+.1f}" if sd.get("re24") is not None else "—",
         f"{sd.get('hr', 0)} HR · {sd.get('xbh', 0)} XBH",
         c_val),
    ]

    card_w = 0.170
    gap = 0.02
    card_bottom, card_top = 0.20, 0.82
    card_h = card_top - card_bottom
    for i, (title, value, subtitle, color) in enumerate(cards):
        x0 = 0.02 + i * (card_w + gap)
        cx = x0 + card_w / 2
        
        # Solid colored tile
        ax.add_patch(FancyBboxPatch(
            (x0, card_bottom), card_w, card_h,
            boxstyle="round,pad=0.015",
            lw=0, facecolor=color, alpha=0.95,
            transform=ax.transAxes, zorder=1,
        ))
        
        y_title = card_top - 0.12
        y_value = card_bottom + card_h * 0.42
        y_sub   = card_bottom + card_h * 0.15
        
        tc_main = "#FFFFFF"
        tc_sub  = "#EAEAEA"

        ax.text(cx, y_title, title, color=tc_main, fontsize=8.2, fontweight="black",
                ha="center", va="center", zorder=3, transform=ax.transAxes)
        ax.text(cx, y_value, value, color=tc_main, fontsize=14.5, fontweight="black",
                ha="center", va="center", zorder=3, transform=ax.transAxes)
        ax.text(cx, y_sub, subtitle, color=tc_sub, fontsize=6.8, fontweight="bold",
                ha="center", va="center", zorder=3, transform=ax.transAxes)

    ax.text(0.88, 0.65, "@Mallitalytics",
            color=SEASONAL_ACCENT, fontsize=10, fontweight="black",
            ha="center", va="bottom", transform=ax.transAxes)
    ax.text(0.88, 0.35, "Data: MLB · Statcast",
            color=PALETTE["text_secondary"], fontsize=6.5, fontweight="bold",
            ha="center", va="top", transform=ax.transAxes)


# ─────────────────────────────── PANEL 6 — ROLLING xwOBA SPARKLINE ──────────

def plot_rolling_xwoba(
    ax,
    rolling_xwoba: list,
    xwoba_season: float | None,
    rolling_woba: list | None = None,
    woba_season: float | None = None,
):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)

    if not rolling_xwoba or len(rolling_xwoba) < 3:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.text(0.5, 0.5, "Rolling xwOBA — insufficient data",
                color=PALETTE["text_lo"], fontsize=8,
                ha="center", va="center", transform=ax.transAxes)
        return

    xs = np.arange(1, len(rolling_xwoba) + 1)
    ys = np.array([pt[1] for pt in rolling_xwoba], dtype=float)
    woba_ys = None
    if rolling_woba and len(rolling_woba) == len(rolling_xwoba):
        woba_ys = np.array([pt[1] for pt in rolling_woba], dtype=float)
        y_values = np.concatenate([ys, woba_ys])
    else:
        y_values = ys
    y_min = min(0.220, float(np.nanmin(ys)) - 0.030)
    y_max = max(0.420, float(np.nanmax(y_values)) + 0.030)
    y_range = y_max - y_min
    y_min = y_min - y_range * 0.07
    y_max = y_max + y_range * 0.15

    _panel_title(ax, "10-GAME ROLLING xwOBA / wOBA")
    ax.set_xlim(0.8, len(xs) + 0.2)
    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_label_position("left")
    ax.tick_params(axis="y", pad=2)
    ax.grid(axis="y", color=PALETTE["grid"], linewidth=0.8, alpha=0.6)
    ax.set_axisbelow(True)

    ax.axhline(0.320, color=PALETTE["text_lo"], lw=0.8, ls=(0, (2, 2)), alpha=0.45)

    hot_line = "#B33F2F" if LIGHT_MODE else "#FF806E"
    woba_line = "#365A78" if LIGHT_MODE else "#9AB7D6"
    ax.plot(xs, ys, color=hot_line, lw=2.05, zorder=3)
    ax.scatter(xs[-1], ys[-1], s=22, color=hot_line, zorder=4)

    if woba_ys is not None:
        ax.plot(xs, woba_ys, color=woba_line, lw=1.7, alpha=0.95, zorder=3)
        ax.scatter(xs[-1], woba_ys[-1], s=18, color=woba_line, zorder=4)

    ax.text(0.975, 0.88, "red xwOBA · blue wOBA", color=PALETTE["text_lo"], fontsize=6.2,
            ha="right", va="center", transform=ax.transAxes)

    mid_game = max(1, len(xs) // 2)
    ax.set_xticks([])
    ax.text(0.060, 0.030, "G1", fontsize=6.6, color=PALETTE["text_secondary"],
            ha="left", va="bottom", transform=ax.transAxes)
    ax.text(0.500, 0.030, f"G{mid_game}", fontsize=6.6, color=PALETTE["text_secondary"],
            ha="center", va="bottom", transform=ax.transAxes)
    ax.text(0.965, 0.030, f"G{len(xs)}", fontsize=6.6, color=PALETTE["text_secondary"],
            ha="right", va="bottom", transform=ax.transAxes)
    y_ticks = sorted(set([round(y_min, 3), 0.320, round(y_max, 3)]))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([_fmt_slash(v) for v in y_ticks], fontsize=6.4, color=PALETTE["text_secondary"])
    ax.margins(y=0.06)
    for sp in ax.spines.values():
        sp.set_edgecolor(PALETTE["border"])
    ax.tick_params(axis="both", colors=PALETTE["text_secondary"], length=0)
    ax.set_xlabel("")


def _metric_value_text(value, kind: str = "raw", digits: int = 1) -> str:
    if value is None:
        return "--"
    if kind == "rate":
        return _fmt_slash(float(value))
    if kind == "pct":
        return f"{float(value):.{digits}f}%"
    if kind == "mph":
        return f"{float(value):.{digits}f}"
    if kind == "signed":
        return f"{float(value):+.{digits}f}"
    return f"{value}"


def plot_seasonal_header(ax, bio: dict, sd: dict, headshot, logo, context_label: str):
    _clean(ax, PALETTE["header_bg"])
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    ax.text(
        0.040, 0.94, f"SEASON BATTER PROFILE · {context_label.upper()}",
        color=PALETTE["text_lo"], fontsize=8.2, fontweight="bold",
        ha="left", va="top", transform=ax.transAxes, zorder=3,
    )

    if logo:
        logo_ax = ax.inset_axes([0.790, 0.12, 0.150, 0.76])
        logo_ax.imshow(np.array(logo), alpha=0.13)
        logo_ax.axis("off")

    if headshot:
        img_ax = ax.inset_axes([0.050, 0.14, 0.125, 0.72])
        img = img_ax.imshow(np.array(headshot))
        clip = mpatches.Circle((0.5, 0.5), 0.49, transform=img_ax.transAxes)
        img.set_clip_path(clip)
        img_ax.add_patch(mpatches.Circle(
            (0.5, 0.5), 0.49, transform=img_ax.transAxes,
            fill=False, lw=1.2, edgecolor=PALETTE["border"],
        ))
        img_ax.axis("off")

    lx = 0.225
    ax.text(
        lx, 0.56, bio.get("name", "Unknown Batter").upper(),
        color=PALETTE["text_primary"], fontsize=24, fontweight="black",
        ha="left", va="center", transform=ax.transAxes, zorder=3,
    )

    team = sd.get("batter_team") or bio.get("team", "MLB")
    player_meta = "  ·  ".join([
        team,
        bio.get("position") or "B",
        f"Bats {bio.get('hand', '--')}",
        f"Age {bio.get('age', '--')}",
    ])
    ax.text(
        lx, 0.22, player_meta,
        color=PALETTE["text_secondary"], fontsize=9.0, fontweight="bold",
        ha="left", va="center", transform=ax.transAxes, zorder=3,
    )


def plot_batted_ball_quality(ax, sd: dict):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    _panel_title(ax, "BATTED-BALL QUALITY")
    ax.text(0.965, 0.805, f"Sample: {sd.get('total_bip', 0)} BIP",
            color=PALETTE["text_lo"], fontsize=6.8, fontweight="bold",
            ha="right", va="center", transform=ax.transAxes)

    avg_dist = sd.get("spray_summary", {}).get("avg_dist")
    metrics = [
        ("Avg EV (mph)", _metric_value_text(sd.get("avg_ev"), "mph"), "avg_ev", sd.get("avg_ev")),
        ("Max EV (mph)", _metric_value_text(sd.get("max_ev"), "mph"), "max_ev", sd.get("max_ev")),
        ("Hard Hit %", _metric_value_text(sd.get("hard_pct"), "pct"), "hard_pct", sd.get("hard_pct")),
        ("Barrel %", _metric_value_text(sd.get("barrel_pct"), "pct"), "barrel_pct", sd.get("barrel_pct")),
        ("Sweet Spot %", _metric_value_text(sd.get("swsp_pct"), "pct"), "swsp_pct", sd.get("swsp_pct")),
        ("Avg Dist (ft)", f"{int(round(avg_dist))}" if avg_dist is not None else "--", "avg_dist", avg_dist),
    ]

    ax.plot([0.045, 0.955], [0.760, 0.760], color=PALETTE["border"], lw=1.0, transform=ax.transAxes)
    for i, (label, value, metric_key, raw_value) in enumerate(metrics):
        col = i // 3
        row = i % 3
        x_label = 0.065 + col * 0.500
        y_label = 0.650 - row * 0.200
        y_value = y_label - 0.070
        if row > 0:
            ax.plot([x_label, x_label + 0.385], [y_label + 0.060, y_label + 0.060],
                    color=PALETTE["grid"], lw=0.7, alpha=0.7, transform=ax.transAxes)
        ax.text(x_label, y_label, label.upper(),
                color=PALETTE["text_lo"], fontsize=6.1, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)
        ax.text(x_label, y_value, value,
                color=apply_stat_highlight_style(sd, metric_key, raw_value, label), fontsize=11.0, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)


def plot_spray_chart_card(ax, spray_df: pd.DataFrame, sd: dict):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)

    wall_r = 380
    _draw_spray_field(ax, outfield_distance=wall_r)

    plot_df = spray_df.copy()
    plot_df["plot_x"] = plot_df["spray_x"]
    plot_df["plot_y"] = plot_df["spray_y"]
    hr_mask = (
        (plot_df["events"] == "home_run") &
        plot_df["hit_distance_sc"].notna()
    ) if "hit_distance_sc" in plot_df.columns else pd.Series(False, index=plot_df.index)
    if hr_mask.any():
        hx = plot_df.loc[hr_mask, "spray_x"]
        hy = plot_df.loc[hr_mask, "spray_y"]
        current_r = np.sqrt(hx ** 2 + hy ** 2).replace(0, np.nan)
        projected_r = plot_df.loc[hr_mask, "hit_distance_sc"].clip(lower=wall_r + 8, upper=440)
        plot_df.loc[hr_mask, "plot_x"] = hx / current_r * projected_r
        plot_df.loc[hr_mask, "plot_y"] = hy / current_r * projected_r

    outcome_style = {
        "single": ("#C96A2B", 35, 0.92, 4),
        "double": ("#4F76A3", 44, 0.95, 5),
        "triple": ("#7C5D8F", 48, 0.98, 6),
        "home_run": ("#B33F2F", 54, 1.0, 7),
    }
    for outcome in ("single", "double", "triple", "home_run"):
        sub = plot_df[plot_df["events"].map(_spray_outcome) == outcome]
        if sub.empty:
            continue
        color, size, alpha, z = outcome_style[outcome]
        ax.scatter(
            sub["plot_x"], sub["plot_y"],
            marker="o", s=size, color=color, alpha=alpha,
            linewidths=0.5 if outcome != "out" else 0,
            edgecolors=PALETTE["text_primary"] if outcome != "out" else "none",
            zorder=z,
        )

    ax.set_xlim(-372, 372)
    ax.set_ylim(-30, 462)
    ax.set_aspect("equal", adjustable="box")
    _panel_title(ax, "SPRAY CHART")

    legend = [("HR", "#B33F2F"), ("3B", "#7C5D8F"), ("2B", "#4F76A3"), ("1B", "#C96A2B")]
    legend_xs = [0.690, 0.765, 0.840, 0.915]
    for x, (label, color) in zip(legend_xs, legend):
        ax.scatter(x, 0.085, s=30, color=color, transform=ax.transAxes, zorder=9,
                   edgecolors=PALETTE["text_primary"], linewidths=0.4)
        ax.text(x + 0.014, 0.085, label, color=PALETTE["text_secondary"], fontsize=6.5,
                fontweight="black", ha="left", va="center", transform=ax.transAxes, zorder=9)


def _slash_line_text(line: dict) -> str:
    vals = [line.get("avg"), line.get("obp"), line.get("slg")]
    if any(v is None for v in vals):
        return "— / — / —"
    return " / ".join(_metric_value_text(v, "rate") for v in vals)


def plot_form_splits_card(ax, sd: dict):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    form = sd.get("form_splits") or {}
    recent = form.get("recent") or {}
    hand_splits = form.get("hand_splits") or {}
    _panel_title(ax, "FORM + SPLITS")

    ax.text(0.07, 0.825, str(form.get("recent_label") or "RECENT FORM"),
            color=PALETTE["text_lo"], fontsize=7.0, fontweight="black",
            ha="left", va="center", transform=ax.transAxes)
    ax.text(0.07, 0.745, _slash_line_text(recent),
            color=PALETTE["text_primary"], fontsize=12.5, fontweight="black",
            ha="left", va="center", transform=ax.transAxes)
    recent_detail = f"{int(recent.get('hr') or 0)} HR · {int(recent.get('xbh') or 0)} XBH"
    if recent.get("pa"):
        recent_detail += f" · {int(recent.get('pa') or 0)} PA"
    ax.text(0.07, 0.675, recent_detail,
            color=PALETTE["text_secondary"], fontsize=8.3, fontweight="bold",
            ha="left", va="center", transform=ax.transAxes)

    ax.plot([0.07, 0.93], [0.595, 0.595], color=PALETTE["border"], lw=1.0, transform=ax.transAxes)
    ax.text(0.07, 0.562, "OPS",
            color=PALETTE["text_lo"], fontsize=6.1, fontweight="black",
            ha="left", va="center", transform=ax.transAxes)

    def split_row(y: float, label: str, split: dict):
        pa = int(split.get("pa") or 0)
        ops = split.get("ops")
        k_pct = split.get("k_pct")
        hr = int(split.get("hr") or 0)
        small = pa > 0 and pa < 40
        label_color = PALETTE["text_lo"] if small else PALETTE["text_secondary"]
        ax.text(0.07, y, f"VS {label}",
                color=label_color, fontsize=7.3, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)
        if pa <= 0:
            ax.text(0.93, y, "No PA",
                    color=PALETTE["text_lo"], fontsize=8.0, fontweight="bold",
                    ha="right", va="center", transform=ax.transAxes)
            return
        pa_label = f"{pa} PA" if not small else f"{pa} PA sample"
        ax.text(0.93, y, pa_label,
                color=PALETTE["text_lo"], fontsize=7.1, fontweight="bold",
                ha="right", va="center", transform=ax.transAxes)
        ax.text(0.07, y - 0.075, _metric_value_text(ops, "rate"),
                color=apply_stat_highlight_style(sd, "ops", ops, f"VS {label} OPS"), fontsize=12.2, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)
        ax.text(0.39, y - 0.075, f"{hr} HR",
                color=PALETTE["text_primary"], fontsize=8.4, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)
        k_text = _metric_value_text(k_pct, "pct")
        ax.text(0.67, y - 0.075, f"{k_text} K",
                color=apply_stat_highlight_style(sd, "k_pct", k_pct, f"VS {label} K%"), fontsize=8.4, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)

    split_row(0.505, "RHP", hand_splits.get("RHP") or {})
    ax.plot([0.07, 0.93], [0.320, 0.320], color=PALETTE["grid"], lw=0.8, alpha=0.8, transform=ax.transAxes)
    split_row(0.230, "LHP", hand_splits.get("LHP") or {})


def _short_pitch_name(name: str | None) -> str:
    if not name:
        return "--"
    aliases = {
        "4-Seam Fastball": "4-Seam Fastball",
        "Four-Seam Fastball": "4-Seam Fastball",
        "Split-Finger": "Splitter",
        "Knuckle Curve": "Knuckle Curve",
    }
    return aliases.get(name, name)


def plot_pitch_type_performance(ax, sd: dict):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    _panel_title(ax, "PITCH TYPE PERFORMANCE")

    rows = sorted(
        [p for p in sd.get("pitch_profile", []) if p.get("count", 0) > 0],
        key=lambda p: (-p.get("count", 0), p.get("abbr", "")),
    )[:6]
    headers = [
        ("PITCH", 0.045),
        ("SEEN (%)", 0.255),
        ("OPS", 0.385),
        ("xwOBAcon", 0.485),
        ("WHIFF", 0.595),
        ("HH%", 0.715),
        ("BRL%", 0.825),
    ]
    y_header = 0.80
    for label, x in headers:
        ax.text(x, y_header, label, color=PALETTE["text_lo"], fontsize=7.0, fontweight="black",
                ha="left", va="center", transform=ax.transAxes)
    ax.plot([0.035, 0.965], [0.735, 0.735], color=PALETTE["border"], lw=1.1, transform=ax.transAxes)

    if not rows:
        ax.text(0.5, 0.46, "Pitch-type split unavailable",
                color=PALETTE["text_lo"], fontsize=11, fontweight="bold",
                ha="center", va="center", transform=ax.transAxes)
        return

    row_h = 0.105
    for i, item in enumerate(rows):
        y = 0.665 - i * row_h
        if i % 2 == 0:
            ax.add_patch(mpatches.Rectangle(
                (0.035, y - 0.042), 0.93, 0.076,
                facecolor=PALETTE["table_alt"], edgecolor="none", alpha=0.62,
                transform=ax.transAxes, zorder=0,
            ))
        full = item.get("name") or next((k for k, v in _PITCH_ABBREV_MAP.items() if v == item.get("abbr")), None)
        pitch_color = PITCH_COLORS.get(full, PALETTE["text_secondary"])
        ops = item.get("ops")
        ops_color = apply_stat_highlight_style(sd, "pitch_ops", ops, f"{item.get('name')} OPS")
        xw = item.get("xwoba")
        xw_color = apply_stat_highlight_style(sd, "pitch_xwoba_con", xw, f"{item.get('name')} xwOBAcon")
        seen_text = f"{item.get('count', 0)} ({_metric_value_text(item.get('usage_pct'), 'pct')})"
        values = [
            (_short_pitch_name(item.get("name")), 0.045, pitch_color, "black"),
            (seen_text, 0.255, PALETTE["text_primary"], "bold"),
            (_metric_value_text(ops, "rate"), 0.385, ops_color, "black"),
            (_metric_value_text(xw, "rate"), 0.485, xw_color, "black"),
            (_metric_value_text(item.get("whiff_pct"), "pct"), 0.595, apply_stat_highlight_style(sd, "pitch_whiff_pct", item.get("whiff_pct"), f"{item.get('name')} Whiff%"), "black"),
            (_metric_value_text(item.get("hard_hit_pct"), "pct"), 0.715, apply_stat_highlight_style(sd, "pitch_hard_pct", item.get("hard_hit_pct"), f"{item.get('name')} HH%"), "black"),
            (_metric_value_text(item.get("barrel_pct"), "pct"), 0.825, apply_stat_highlight_style(sd, "pitch_barrel_pct", item.get("barrel_pct"), f"{item.get('name')} BRL%"), "black"),
        ]
        for text, x, color, weight in values:
            fs = 7.7 if x == 0.045 else 8.0
            ax.text(x, y, text, color=color, fontsize=fs, fontweight=weight,
                    ha="left", va="center", transform=ax.transAxes)


def plot_counting_snapshot(ax, sd: dict):
    _clean(ax, PALETTE["panel_bg"])
    _border(ax)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    metrics = [
        ("PA", sd.get("total_pa", 0), None, None),
        ("AB", sd.get("ab", 0), None, None),
        ("H", sd.get("h", 0), None, None),
        ("2B", sd.get("doubles", 0), None, None),
        ("3B", sd.get("triples", 0), None, None),
        ("HR", sd.get("hr", 0), None, None),
        ("XBH", sd.get("xbh", 0), None, None),
        ("BB", sd.get("bb", 0), None, None),
        ("K", sd.get("k", 0), None, None),
        ("AVG", _metric_value_text(sd.get("avg"), "rate"), "avg", sd.get("avg")),
        ("OBP", _metric_value_text(sd.get("obp"), "rate"), "obp", sd.get("obp")),
        ("SLG", _metric_value_text(sd.get("slg"), "rate"), "slg", sd.get("slg")),
        ("OPS", _metric_value_text(sd.get("ops"), "rate"), "ops", sd.get("ops")),
    ]
    _panel_title(ax, "BATTING LINE")
    for i, (label, value, stat_name, raw_value) in enumerate(metrics):
        x0 = 0.030 + i * (0.940 / max(len(metrics) - 1, 1))
        value_color = (
            apply_stat_highlight_style(sd, stat_name, raw_value, label)
            if stat_name else PALETTE["text_primary"]
        )
        ax.text(x0, 0.580, str(value), color=value_color, fontsize=9.0,
                fontweight="black", ha="center", va="center", transform=ax.transAxes)
        ax.text(x0, 0.315, label, color=PALETTE["text_lo"], fontsize=5.8,
                fontweight="black", ha="center", va="center", transform=ax.transAxes)


def plot_brand_footer(ax):
    _clean(ax, PALETTE["card_bg"])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.text(0.02, 0.50, "Data: MLB Stats API · Statcast",
            color=PALETTE["text_lo"], fontsize=7.2, fontweight="bold",
            ha="left", va="center", transform=ax.transAxes)
    ax.text(0.98, 0.50, "@Mallitalytics",
            color=SEASONAL_ACCENT, fontsize=11, fontweight="black",
            ha="right", va="center", transform=ax.transAxes)


# ─────────────────────────────── MAIN RENDER ────────────────────────────────

def generate_batter_profile(
    batter_id: int,
    season: int = 2025,
    context_label: str | None = None,
    parquet_dir: str | None = None,
    output_path: str = "batter_profile.png",
):
    mpl.rcParams["figure.dpi"]  = 200
    _configure_brand_fonts()

    print(f"  Loading parquet data for batter {batter_id} ({season})…")
    pdir = Path(parquet_dir) if parquet_dir else None
    df   = load_batter_seasonal_data(batter_id, parquet_dir=pdir, season=season)

    if df.empty:
        raise SystemExit(f"No data found for batter {batter_id} in season {season}.")

    print(f"  Aggregating {len(df):,} pitch rows across {df['game_pk'].nunique()} games…")
    sd = compute_season_stats(df)
    live_totals = fetch_live_hitting_totals(batter_id, season)
    if live_totals:
        sd.update(live_totals)
        print("  Batting line source: MLB Stats API season totals")
    live_hr_spray = fetch_live_home_run_spray(batter_id, season)
    if not live_hr_spray.empty and "spray_df" in sd:
        local_non_hr = sd["spray_df"][sd["spray_df"]["events"].map(_spray_outcome) != "home_run"].copy()
        sd["spray_df"] = pd.concat([local_non_hr, live_hr_spray], ignore_index=True, sort=False)
        sd["spray_hr_plotted"] = int(len(live_hr_spray))
        print(f"  Spray chart HR source: MLB Stats API live feeds ({len(live_hr_spray)} HR)")

    baseline_df = load_league_baselines(season, parquet_dir=pdir, baseline_path=_args.baseline_path)
    if not baseline_df.empty and {"stat_name"}.issubset(baseline_df.columns):
        sd["_league_baselines"] = baseline_df
        sd["_baseline_lookup"] = baseline_df.set_index("stat_name").to_dict("index")
        print(f"  Highlight baselines: loaded {len(sd['_baseline_lookup'])} stat distributions")
    else:
        sd["_league_baselines"] = pd.DataFrame()
        sd["_baseline_lookup"] = {}
        print("  Highlight baselines: unavailable, using neutral stat colors")
    sd["_debug_highlights"] = bool(_args.debug_highlights)

    if context_label is None:
        context_label = f"{season} Regular Season"

    print("  Fetching bio + assets…")
    bio      = fetch_player_bio(batter_id)
    headshot = fetch_headshot(batter_id)

    batter_team = sd.get("batter_team") or bio["team"]
    flag_img    = _fetch_flag_image(batter_team)
    if flag_img:
        logo, is_flag = flag_img, True
        print(f"  Flag: {batter_team}")
    else:
        logo, is_flag = fetch_team_logo(bio["team"]), False

    fig = plt.figure(figsize=(8, 10))
    fig.patch.set_facecolor(PALETTE["card_bg"])

    outer_gs = gridspec.GridSpec(
        6, 1, figure=fig,
        height_ratios=[1.02, 1.55, 3.16, 2.04, 0.82, 0.28],
        hspace=0.125,
        left=0.045, right=0.955, top=0.975, bottom=0.030,
    )

    ax_hdr = fig.add_subplot(outer_gs[0])

    story_gs = gridspec.GridSpecFromSubplotSpec(
        1, 2,
        subplot_spec=outer_gs[1],
        width_ratios=[1.18, 1.0],
        wspace=0.075,
    )
    ax_spark = fig.add_subplot(story_gs[0])
    ax_quality = fig.add_subplot(story_gs[1])
    field_gs = gridspec.GridSpecFromSubplotSpec(
        1, 2,
        subplot_spec=outer_gs[2],
        width_ratios=[0.64, 1.36],
        wspace=0.075,
    )
    ax_form = fig.add_subplot(field_gs[0])
    ax_spray = fig.add_subplot(field_gs[1])
    ax_pitch = fig.add_subplot(outer_gs[3])
    ax_counts = fig.add_subplot(outer_gs[4])
    ax_brand = fig.add_subplot(outer_gs[5])

    plot_seasonal_header(ax_hdr, bio, sd, headshot, logo, context_label)
    plot_rolling_xwoba(ax_spark, sd.get("rolling_xwoba", []), sd.get("xwoba"), sd.get("rolling_woba"), sd.get("woba"))
    plot_batted_ball_quality(ax_quality, sd)
    plot_form_splits_card(ax_form, sd)
    plot_spray_chart_card(ax_spray, sd["spray_df"], sd)
    plot_pitch_type_performance(ax_pitch, sd)
    plot_counting_snapshot(ax_counts, sd)
    plot_brand_footer(ax_brand)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    fig.savefig(output_path, dpi=200, facecolor=PALETTE["card_bg"], edgecolor="none")
    plt.close()
    print(f"  → Saved: {output_path}")


# ─────────────────────────────── CLI ENTRY ──────────────────────────────────

if __name__ == "__main__":

    batter_id = _args.batter
    season    = _args.season
    ctx       = _args.context or f"{season} Regular Season"
    mode_sfx  = "" if LIGHT_MODE else "_dark"

    bio = fetch_player_bio(batter_id)
    safe_nm = (
        bio["name"]
        .lower()
        .replace(", ", "_").replace(",", "_")
        .replace(" ", "_").replace(".", "").replace("'", "")
    )

    if _args.output:
        out_path = _args.output
    else:
        out_dir  = _PARENT / "outputs" / "batter_cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = str(out_dir / f"batter_profile_{safe_nm}_{season}{mode_sfx}.png")

    print(f"  Batter  : {bio['name']} (ID {batter_id})")
    print(f"  Season  : {season}")
    print(f"  Context : {ctx}")
    print(f"  Output  : {out_path}\n")

    generate_batter_profile(
        batter_id=batter_id,
        season=season,
        context_label=ctx,
        parquet_dir=_args.parquet_dir,
        output_path=out_path,
    )
