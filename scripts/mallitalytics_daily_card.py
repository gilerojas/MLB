"""
Mallitalytics Daily Pitcher Card

Generates a single-game pitching card (PNG) from a Statcast pitches_enriched parquet:
header (name, bio, box score, Zone%, Whiffs, CSW%, GB%), hard contact heatmap, movement
profile with arm angle, pitch tendencies by count, and an arsenal table (velo, spin,
break, Chase%, Whiff%, Str%, Zone%, BS75+%, xwOBA). Chase%, Whiff%, and BS75+% use swings
as the denominator; Str% and Zone% use pitches. Uses a default game and pitcher
defined in CONFIG; use --random to pick a game from the warehouse instead.

CLI:
  python scripts/mallitalytics_daily_card.py
      Use CONFIG parquet and pitcher; light theme. Output: outputs/pitching_cards/

  python scripts/mallitalytics_daily_card.py --dark
      Same as above, dark / analytics theme.

  python scripts/mallitalytics_daily_card.py --random
      Pick a random game from data/warehouse (pitcher with most pitches, ≥50);
      light theme unless --dark is also passed.

  python scripts/mallitalytics_daily_card.py --random --dark
      Random game, dark theme.

  python scripts/mallitalytics_daily_card.py --pitchers 663460,690953,701542 --date yesterday
      Generate cards for the given pitcher IDs from yesterday's games (one card per pitcher).

  python scripts/mallitalytics_daily_card.py --pitchers 663460 --date 2025-03-22
      Generate card for pitcher 663460 from games on 2025-03-22.

To use a specific game/pitcher, edit PARQUET_PATH and PITCHER_ID in the CONFIG section.
"""

from __future__ import annotations

import warnings
warnings.filterwarnings("ignore")
import os
if "MPLBACKEND" not in os.environ:
    os.environ["MPLBACKEND"] = "Agg"
# Reducir ruido en terminal (matplotlib/fontconfig); no afecta MLB
if "MPLCONFIGDIR" not in os.environ:
    _mpl_tmp = os.path.join(os.path.expanduser("~"), ".mallitalytics_mpl")
    try:
        os.makedirs(_mpl_tmp, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = _mpl_tmp
    except Exception:
        pass

import math
import re
import sys
from collections import Counter
import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from io import BytesIO

_parser = argparse.ArgumentParser(description="Mallitalytics Daily Pitcher Card")
_parser.add_argument("--dark",     action="store_true", help="Render in dark / analytics mode")
_parser.add_argument("--random",   action="store_true", help="Pick a random game+pitcher from the warehouse")
_parser.add_argument("--pitchers", type=str, default=None, help="Comma-separated pitcher IDs (e.g. 663460,690953,701542)")
_parser.add_argument("--date",     type=str, default="yesterday", help="Game date: yesterday or YYYY-MM-DD (used with --pitchers)")
_parser.add_argument("--parquet",  type=str, default=None, help="Path to a single pitches_enriched.parquet (e.g. WBC output)")
_parser.add_argument("--pitcher",  type=int, default=None, help="Pitcher ID (required with --parquet)")
_parser.add_argument("--logo-path", type=str, default=None, help="Path to custom logo/flag PNG to show in header (overrides ESPN team logo)")
_parser.add_argument("--output-dir", type=str, default=None, help="Directory for generated pitcher card PNGs")
_parser.add_argument(
    "--output-suffix",
    type=str,
    default=None,
    help="Optional token inserted before _dark.png so concurrent runs do not clobber the same file",
)
_args, _ = _parser.parse_known_args()

import requests
import numpy as np
import pandas as pd
# Matplotlib must fully initialize before seaborn (avoids partial-init / _version errors on some setups).
import matplotlib
matplotlib.use(os.environ.get("MPLBACKEND", "Agg"))
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Ellipse
import matplotlib as mpl
import seaborn as sns
from PIL import Image

_ROOT_MLB = Path(__file__).resolve().parent.parent


def _resolved_output_dir() -> Path:
    raw = getattr(_args, "output_dir", None)
    if raw and str(raw).strip():
        return Path(str(raw)).expanduser().resolve()
    return Path(__file__).parent.parent / "outputs" / "pitching_cards"
if str(_ROOT_MLB) not in sys.path:
    sys.path.insert(0, str(_ROOT_MLB))
from src.mlb_headshot import neutralize_mlb_headshot_background

# -----------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------
# Cristopher Sánchez 2024-04-12 vs PIT (game_745596)
_PARENT = Path(__file__).resolve().parent.parent
PARQUET_PATH = _PARENT / "data" / "warehouse" / "mlb" / "2024" / "regular_season" / "pitches_enriched" / "game_745596_20240412_pitches_enriched.parquet"
PITCHER_ID  = 650911  # Cristopher Sánchez
MIN_PITCHES = 3
# Statcast: league avg bat speed ~72 mph; 75+ = "fast swing" (MLB 2024 bat tracking)
FAST_SWING_MPH = 75
# Pitch-type xwOBA beats in card JSON / redraft: ignore tiny samples (e.g. 5 pitches).
MIN_PITCHES_FOR_XWOBA_BEAT = 15
MIN_BIP_FOR_XWOBA_BEAT = 6
MIN_PITCHES_XWOBA_FALLBACK_NO_BIP = 25

# League-wide benchmark cache for gradient scaling
_BENCHMARK_CACHE = {}

def load_pitch_metric_benchmarks(season: int):
    """
    Load league-wide metric benchmarks for the given season from
    config/pitch_metric_benchmarks_<season>.json.
    Returns a dict or None if not available.
    """
    global _BENCHMARK_CACHE
    if season in _BENCHMARK_CACHE:
        return _BENCHMARK_CACHE[season]
    cfg_path = _PARENT / "config" / f"pitch_metric_benchmarks_{season}.json"
    if not cfg_path.exists():
        return None
    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        _BENCHMARK_CACHE[season] = data
        return data
    except Exception:
        return None

LIGHT_MODE  = not _args.dark

OUTPUT_PATH = (
    Path(__file__).parent.parent / "outputs" / "pitching_cards" /
    f"pitcher_card_cristopher_sánchez_2024-04-12{'_light' if LIGHT_MODE else ''}.png"
)

# -----------------------------------------------------------------
# BRAND PALETTE 
# -----------------------------------------------------------------
_PALETTE_DARK = {
    "card_bg":       "#1A2530",
    "header_bg":     "#1E3448",
    "panel_bg":      "#1F2E3D",
    "table_bg":      "#162030",
    "table_alt":     "#1A2838",
    "text_primary":  "#F5F2ED",
    "text_secondary":"#C8DCE8",
    "text_lo":       "#8FA3B8",
    "accent_orange": "#E8712B",
    "accent_green":  "#66BB6A",
    "accent_red":    "#E74C3C",
    "move_arm":      "#E8712B",
    "move_glove":    "#5BC8D5",
    "move_up":       "#66BB6A",
    "move_down":     "#8FA3B8",
    "grid":          "#2C3E50",
    "border":        "#2E4A62",
    "zone_edge":     "#7A9AB5",
}

# SaaS UI Dashboard Aesthetic
_PALETTE_LIGHT = {
    "card_bg":       "#F8F9FA",   
    "header_bg":     "#FFFFFF",   
    "panel_bg":      "#FFFFFF",   
    "table_bg":      "#FFFFFF",   
    "table_alt":     "#F1F3F5",   
    "text_primary":  "#1A202C",   
    "text_secondary":"#4A5568",   
    "text_lo":       "#A0AEC0",   
    "accent_orange": "#DD6B20",   
    "accent_green":  "#38A169",   
    "accent_red":    "#E53E3E",   
    "move_arm":      "#DD6B20",   
    "move_glove":    "#38A169",   
    "move_up":       "#38A169",   
    "move_down":     "#4A5568",   
    "grid":          "#E2E8F0",   
    "border":        "#CBD5E0",   
    "zone_edge":     "#718096",   
}

PALETTE = _PALETTE_LIGHT if LIGHT_MODE else _PALETTE_DARK

if LIGHT_MODE:
    GRAD_METRIC_LO = "#FFFBEB"   # pale cream (low efficiency)
    GRAD_METRIC_HI = "#B45309"   # deep amber (high efficiency = good for pitcher)
    GRAD_VELO_LO   = "#FDE8CC"
    GRAD_VELO_HI   = "#B84010"
    GRAD_XWOBA_LO  = "#FFFBEB"   # pale cream (low xwOBA = good, same as other metrics)
    GRAD_XWOBA_HI  = "#B45309"   # deep amber (high xwOBA = bad for pitcher)
else:
    GRAD_METRIC_LO = "#1E2A38"
    GRAD_METRIC_HI = "#DD6B20"   # warm amber (dark mode)
    GRAD_VELO_LO   = "#251A0A"
    GRAD_VELO_HI   = "#E8712B"
    GRAD_XWOBA_LO  = "#1E2A38"   # same as GRAD_METRIC_LO
    GRAD_XWOBA_HI  = "#DD6B20"   # warm amber

PITCH_COLOURS = {
    'FF': {'colour': '#C94B68', 'name': '4-Seam Fastball'},
    'FA': {'colour': '#C94B68', 'name': 'Fastball'},
    'SI': {'colour': '#A03878', 'name': 'Sinker'},
    'FC': {'colour': '#9470B8', 'name': 'Cutter'},
    'CH': {'colour': '#C97248', 'name': 'Changeup'},
    'FS': {'colour': '#B85530', 'name': 'Splitter'},
    'SC': {'colour': '#B86828', 'name': 'Screwball'},
    'FO': {'colour': '#B87830', 'name': 'Forkball'},
    'SL': {'colour': '#48A86A', 'name': 'Slider'},
    'ST': {'colour': '#26A098', 'name': 'Sweeper'},
    'SV': {'colour': '#358062', 'name': 'Slurve'},
    'KC': {'colour': '#6258B8', 'name': 'Knuckle Curve'},
    'CU': {'colour': '#4A5DB8', 'name': 'Curveball'},
    'CS': {'colour': '#5A70C2', 'name': 'Slow Curve'},
    'EP': {'colour': '#6880C0', 'name': 'Eephus'},
    'KN': {'colour': '#908018', 'name': 'Knuckleball'},
    'PO': {'colour': '#584035', 'name': 'Pitch Out'},
    'UN': {'colour': '#887868', 'name': 'Unknown'},
}

PITCH_COLOURS_LIGHT = {
    # Fastball family — spread the crimson → purple range wider
    'FF': {'colour': '#C01040', 'name': '4-Seam Fastball'},  # Slightly purer crimson
    'FA': {'colour': '#C01040', 'name': 'Fastball'},
    'SI': {'colour': '#6E0855', 'name': 'Sinker'},            # Deeper wine, more distance from FF
    'FC': {'colour': '#6A46A0', 'name': 'Cutter'},            # Keep — works well

    # Offspeed family — THIS is the key fix
    'CH': {'colour': '#D4780A', 'name': 'Changeup'},          # → brand amber, kills FS/CH merge
    'FS': {'colour': '#8A3808', 'name': 'Splitter'},          # → darker saddle brown
    'SC': {'colour': '#964810', 'name': 'Screwball'},         # Keep
    'FO': {'colour': '#965818', 'name': 'Forkball'},          # Keep

    # Breaking family — keep entirely, it's clean
    'SL': {'colour': '#1A7840', 'name': 'Slider'},
    'ST': {'colour': '#007870', 'name': 'Sweeper'},
    'SV': {'colour': '#166048', 'name': 'Slurve'},
    'KC': {'colour': '#4030A0', 'name': 'Knuckle Curve'},
    'CU': {'colour': '#2838A0', 'name': 'Curveball'},
    'CS': {'colour': '#3850B0', 'name': 'Slow Curve'},
    'EP': {'colour': '#4868B0', 'name': 'Eephus'},

    # Others — keep
    'KN': {'colour': '#706000', 'name': 'Knuckleball'},
    'PO': {'colour': '#402818', 'name': 'Pitch Out'},
    'UN': {'colour': '#685848', 'name': 'Unknown'},
}

_active_colours = PITCH_COLOURS_LIGHT if LIGHT_MODE else PITCH_COLOURS
DICT_COLOUR = {k: v['colour'] for k, v in _active_colours.items()}
DICT_PITCH  = {k: v['name']   for k, v in PITCH_COLOURS.items()}

ESPN_LOGOS = {
    "AZ":"ari","ATL":"atl","BAL":"bal","BOS":"bos","CHC":"chc","CWS":"chw",
    "CIN":"cin","CLE":"cle","COL":"col","DET":"det","HOU":"hou","KC":"kc",
    "LAA":"laa","LAD":"lad","MIA":"mia","MIL":"mil","MIN":"min","NYM":"nym",
    "NYY":"nyy","OAK":"oak","PHI":"phi","PIT":"pit","SD":"sd","SF":"sf",
    "SEA":"sea","STL":"stl","TB":"tb","TEX":"tex","TOR":"tor","WSH":"wsh",
}

PARQUET_PATTERN = re.compile(r"^game_(\d+)_(\d{8})_pitches_enriched\.parquet$")
BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
REQUEST_HEADERS = {"User-Agent": "Mallitalytics/1.0 (stats analysis)"}

# Warehouse layout: .../mlb/{year}/{stage}/pitches_enriched/
_WAREHOUSE_STAGES = ("regular_season", "postseason", "spring_training", "playoffs", "all_star")
_RECENT_OUTING_LOOKBACK_DAYS = 120
_MAX_PRIOR_OUTINGS_META = 5
_REPO_ROOT = Path(__file__).resolve().parent.parent


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


def _resolved_warehouse_root_from_env() -> Path:
    """Same rules as mlbops api.paths.get_warehouse_dir (script cannot import FastAPI app)."""
    raw = _strip_env_quotes(os.environ.get("MLB_WAREHOUSE_DIR", "").strip())
    if raw and not _env_warehouse_is_doc_placeholder(raw):
        return Path(raw).expanduser().resolve()
    return _REPO_ROOT / "data" / "warehouse" / "mlb"


def _safe_is_dir(path: Path) -> bool:
    """Google Drive File Stream can raise TimeoutError from pathlib.stat()."""
    try:
        return path.is_dir()
    except (OSError, TimeoutError):
        return False


def _warehouse_mlb_root_from_parquet(parquet_path: str) -> Path | None:
    """Resolve MLB warehouse root for prior-start scan; None if path is outside standard layout."""
    raw = _strip_env_quotes(os.environ.get("MLB_WAREHOUSE_DIR", "").strip())
    if raw and not _env_warehouse_is_doc_placeholder(raw):
        return Path(raw).expanduser().resolve()
    p = Path(parquet_path).resolve()
    parts = p.parts
    try:
        idx = parts.index("mlb")
    except ValueError:
        return _REPO_ROOT / "data" / "warehouse" / "mlb"
    return Path(*parts[: idx + 1])


def _parquet_game_pk_date(path: Path) -> tuple[int | None, date | None]:
    m = PARQUET_PATTERN.match(path.name)
    if not m:
        return None, None
    try:
        gpk = int(m.group(1))
        ymd = datetime.strptime(m.group(2), "%Y%m%d").date()
        return gpk, ymd
    except ValueError:
        return None, None


def fetch_boxscore_data(game_pk: int) -> dict | None:
    """Single MLB boxscore JSON fetch (shared by line override + team abbrev correction)."""
    try:
        r = requests.get(BOXSCORE_URL.format(game_pk=game_pk), headers=REQUEST_HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def fetch_season_pitching_stats(pitcher_id: int, season: int) -> dict | None:
    """Cumulative season pitching line from MLB Stats API (omit on failure).

    Numbers reflect whatever the API returns at request time and may lag same-day games.
    """
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{int(pitcher_id)}/stats",
            params={"stats": "season", "group": "pitching", "season": str(int(season))},
            headers=REQUEST_HEADERS,
            timeout=12,
        )
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return None

    stat_block: dict | None = None
    for block in payload.get("stats") or []:
        grp = (block.get("group") or {}).get("displayName") or ""
        if str(grp).lower() != "pitching":
            continue
        splits = block.get("splits") or []
        if splits:
            st = splits[0].get("stat")
            if isinstance(st, dict) and st:
                stat_block = st
                break
    if not stat_block:
        return None

    def _to_float(x) -> float | None:
        if x is None:
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    def _to_int(x) -> int | None:
        if x is None:
            return None
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    ip_raw = stat_block.get("inningsPitched")
    return {
        "season": int(season),
        "era": _to_float(stat_block.get("era")),
        "whip": _to_float(stat_block.get("whip")),
        "innings_pitched": str(ip_raw) if ip_raw is not None and str(ip_raw).strip() else None,
        "games_played": _to_int(stat_block.get("gamesPlayed")),
        "games_started": _to_int(stat_block.get("gamesStarted")),
        "strike_outs": _to_int(stat_block.get("strikeOuts")),
        "base_on_balls": _to_int(stat_block.get("baseOnBalls")),
        "hits": _to_int(stat_block.get("hits")),
        "home_runs": _to_int(stat_block.get("homeRuns")),
        "note": "Cumulative MLB season pitching stats from Stats API at card generation time; may lag same-day games.",
    }


def teams_from_boxscore_json(data: dict) -> tuple[str | None, str | None]:
    teams = data.get("teams") or {}
    th = (teams.get("home") or {}).get("team") or {}
    ta = (teams.get("away") or {}).get("team") or {}
    ha = th.get("abbreviation") or th.get("fileCode")
    aa = ta.get("abbreviation") or ta.get("fileCode")
    if ha and aa:
        return str(ha).strip().upper(), str(aa).strip().upper()
    return None, None


def fetch_boxscore_team_abbrevs(game_pk: int, box_data: dict | None = None) -> tuple[str | None, str | None]:
    """Official home/away abbreviations from boxscore (optionally reuse pre-fetched JSON)."""
    data = box_data if box_data is not None else fetch_boxscore_data(game_pk)
    if not data:
        return None, None
    return teams_from_boxscore_json(data)


def _infer_opponent_team(
    df: pd.DataFrame,
    pitcher_mlb_team: str,
    game_pk: int | None = None,
    box_data: dict | None = None,
) -> str:
    """
    Opponent team abbrev for the card header and JSON.
    Uses per-pitch defensive team (Top half = home pitches, Bottom = away pitches) by majority vote,
    case-insensitive inning_topbot. Falls back to bio vs home/away. Optionally corrects home/away from MLB boxscore.
    """
    def _clean_abb(s: str) -> str:
        s = (s or "").strip()
        if not s or s.lower() in ("--", "nan", "none"):
            return ""
        return s

    home_team = _clean_abb(str(df["home_team"].iloc[0]) if "home_team" in df.columns else "")
    away_team = _clean_abb(str(df["away_team"].iloc[0]) if "away_team" in df.columns else "")

    if game_pk is not None:
        bh, ba = fetch_boxscore_team_abbrevs(game_pk, box_data=box_data)
        if bh and ba and bh != ba:
            if (not home_team or not away_team) or (home_team.upper() == away_team.upper()):
                home_team, away_team = bh, ba

    if not home_team or not away_team:
        return away_team or home_team or "--"

    if home_team.upper() == away_team.upper():
        return "--"

    hu, au = home_team.upper(), away_team.upper()
    bt = (pitcher_mlb_team or "").strip()
    btu = bt.upper()

    defensive: list[str] = []
    if "inning_topbot" in df.columns:
        for _, row in df.iterrows():
            raw = row.get("inning_topbot")
            if raw is None:
                continue
            try:
                if isinstance(raw, float) and pd.isna(raw):
                    continue
            except TypeError:
                pass
            v = str(raw).strip().upper()
            if not v:
                continue
            if v.startswith("T") and not v.startswith("TW"):
                defensive.append(home_team)
            elif v.startswith("B"):
                defensive.append(away_team)

    if defensive:
        pit_team, _cnt = Counter(defensive).most_common(1)[0]
        opp = away_team if pit_team.upper() == hu else home_team
        if btu and btu in (hu, au) and opp.upper() == btu:
            opp = away_team if btu == hu else home_team
        return opp

    if btu == hu:
        return away_team
    if btu == au:
        return home_team

    if "inning_topbot" in df.columns:
        itb = df["inning_topbot"].astype(str).str.strip().str.upper()
        n_top = int(itb.str.match(r"^T", na=False).sum())
        n_bot = int(itb.str.match(r"^B", na=False).sum())
        if n_top or n_bot:
            return away_team if n_top >= n_bot else home_team

    return away_team


def _mean_velo(df: pd.DataFrame) -> float | None:
    if "release_speed" not in df.columns or df["release_speed"].isna().all():
        return None
    v = float(df["release_speed"].mean())
    return v if not math.isnan(v) else None


def _summarize_one_outing(parquet_path: Path, pitcher_id: int, bio: dict) -> dict | None:
    """Box + process stats for one game file and pitcher (for meta JSON / tweet context)."""
    try:
        df_raw = load_game(str(parquet_path), pitcher_id)
    except Exception:
        return None
    df = process_pitches(df_raw)
    box = compute_box_score(df)
    gpk, gdate = _parquet_game_pk_date(parquet_path)
    if gpk is not None:
        try:
            official = fetch_box_score_line(gpk, pitcher_id)
            if official:
                box["ip"] = official["ip"]
                box["h"] = official["h"]
                box["er"] = official.get("er", box.get("er", 0))
                box["k"] = official["k"]
                box["bb"] = official["bb"]
                box["hr"] = official["hr"]
        except Exception:
            pass
    opp = _infer_opponent_team(df, str(bio.get("team") or ""), game_pk=gpk)
    game_date_s = gdate.isoformat() if gdate is not None else ""
    if not game_date_s and "game_date" in df.columns:
        gd = df["game_date"].iloc[0]
        game_date_s = gd.strftime("%Y-%m-%d") if hasattr(gd, "strftime") else str(gd)[:10]
    velo = _mean_velo(df)
    return {
        "game_date": game_date_s,
        "game_pk": gpk,
        "opponent": opp,
        "ip": box.get("ip"),
        "k": int(box.get("k", 0)),
        "bb": int(box.get("bb", 0)),
        "er": int(box.get("er", 0)),
        "h": int(box.get("h", 0)),
        "hr": int(box.get("hr", 0)),
        "pitches": int(box.get("total_pitches") or box.get("n") or len(df)),
        "whiffs": int(box.get("whiffs", 0)),
        "csw_pct": _json_scalar_float(box.get("csw_pct"), 2),
        "zone_pct": _json_scalar_float(box.get("zone_pct"), 2),
        "avg_velo_mph": _json_scalar_float(velo, 2) if velo is not None else None,
    }


def _collect_recent_pitcher_outings(
    *,
    warehouse_root: Path,
    pitcher_id: int,
    card_game_date: str,
    current_game_pk: int | None,
    bio: dict,
    max_prior: int = _MAX_PRIOR_OUTINGS_META,
    lookback_days: int = _RECENT_OUTING_LOOKBACK_DAYS,
) -> list[dict]:
    """
    Prior starts for the same pitcher before this game, by scanning warehouse parquets
    day-by-day (cheap globs). Newest first.
    """
    try:
        anchor = datetime.strptime(card_game_date.strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return []
    if not _safe_is_dir(warehouse_root):
        return []

    seen_pk: set[int] = set()
    if current_game_pk is not None:
        seen_pk.add(int(current_game_pk))

    out: list[dict] = []
    for day_offset in range(1, lookback_days + 1):
        if len(out) >= max_prior:
            break
        d = anchor - timedelta(days=day_offset)
        date_str = d.strftime("%Y%m%d")
        for yr in (d.year, d.year - 1):
            for stage in _WAREHOUSE_STAGES:
                enriched = warehouse_root / str(yr) / stage / "pitches_enriched"
                if not _safe_is_dir(enriched):
                    continue
                try:
                    paths_day = sorted(enriched.glob(f"game_*_{date_str}_pitches_enriched.parquet"))
                except (OSError, TimeoutError):
                    continue
                for path in paths_day:
                    if len(out) >= max_prior:
                        break
                    gpk, _gd = _parquet_game_pk_date(path)
                    if gpk is not None and gpk in seen_pk:
                        continue
                    try:
                        col = "pitcher" if "pitcher" in pd.read_parquet(path, columns=[]).columns else None
                    except Exception:
                        continue
                    try:
                        probe = pd.read_parquet(path, columns=["pitcher"] if col else ["pitcher_id"])
                        pcol = "pitcher" if "pitcher" in probe.columns else "pitcher_id"
                        n = int((probe[pcol] == pitcher_id).sum())
                    except Exception:
                        continue
                    if n < MIN_PITCHES:
                        continue
                    summ = _summarize_one_outing(path, pitcher_id, bio)
                    if not summ or not summ.get("game_date"):
                        continue
                    gpk2 = summ.get("game_pk")
                    if gpk2 is not None:
                        if gpk2 in seen_pk:
                            continue
                        seen_pk.add(int(gpk2))
                    out.append(summ)
        if len(out) >= max_prior:
            break
    return out


def _outing_context_vs_last(
    current_box: dict,
    current_df: pd.DataFrame,
    recent_outings: list[dict],
) -> dict | None:
    """Deltas vs most recent prior start (tweet / redraft context)."""
    if not recent_outings:
        return None
    last = recent_outings[0]
    cur_velo = _mean_velo(current_df)
    lv = last.get("avg_velo_mph")
    cv = float(cur_velo) if cur_velo is not None else None
    ctx: dict = {"vs_last_start": {}}
    vs = ctx["vs_last_start"]
    if cv is not None and lv is not None:
        vs["avg_velo_delta_mph"] = round(cv - float(lv), 2)
    lc = last.get("csw_pct")
    cc = current_box.get("csw_pct")
    if lc is not None and cc is not None:
        vs["csw_pct_delta"] = round(float(cc) - float(lc), 2)
    lz = last.get("zone_pct")
    cz = current_box.get("zone_pct")
    if lz is not None and cz is not None:
        vs["zone_pct_delta"] = round(float(cz) - float(lz), 2)
    le = last.get("er")
    ce = current_box.get("er")
    if le is not None and ce is not None:
        vs["er_delta"] = int(ce) - int(le)
    return ctx if vs else None


def _recent_prior_summary(recent: list[dict]) -> dict | None:
    """Simple means over prior starts in meta (tweet context); not full season stats."""
    if not recent:
        return None

    def _mean_int(key: str) -> float | None:
        xs = [int(r[key]) for r in recent if r.get(key) is not None]
        return round(sum(xs) / len(xs), 2) if xs else None

    def _mean_float(key: str) -> float | None:
        xs = [float(r[key]) for r in recent if r.get(key) is not None]
        return round(sum(xs) / len(xs), 2) if xs else None

    return {
        "prior_starts_in_window": len(recent),
        "avg_velo_mph_mean": _mean_float("avg_velo_mph"),
        "csw_pct_mean": _mean_float("csw_pct"),
        "zone_pct_mean": _mean_float("zone_pct"),
        "k_mean": _mean_int("k"),
        "bb_mean": _mean_int("bb"),
        "er_mean": _mean_int("er"),
    }


def fetch_box_score_line(game_pk: int, pitcher_id: int, box_data: dict | None = None) -> dict | None:
    """
    Fetch official box score and return this pitcher's line: ip, h, r, k, bb, hr.
    Use this to override pitch-derived stats when Statcast only has events on the final pitch of each PA
    (so reliever-recorded outs are missing from the pitcher's filtered data). Returns None on failure.
    Pass box_data to avoid a duplicate HTTP call when the box JSON was already loaded.
    """
    data = box_data if box_data is not None else fetch_boxscore_data(game_pk)
    if not data:
        return None
    pid_str = str(pitcher_id)
    for side in ("home", "away"):
        teams = data.get("teams") or {}
        team = teams.get(side) or {}
        players = team.get("players") or {}
        for key, obj in players.items():
            if not isinstance(obj, dict):
                continue
            person_id = str((obj.get("person") or {}).get("id", ""))
            if key != f"ID{pid_str}" and person_id != pid_str:
                continue
            stats = obj.get("stats")
            if isinstance(stats, list):
                for s in stats:
                    if (s.get("group") or {}).get("displayName") == "pitching":
                        stats = s
                        break
                else:
                    stats = None
            elif isinstance(stats, dict):
                stats = stats.get("pitching") or stats
            if not stats or not isinstance(stats, dict):
                continue
            ip_raw = stats.get("inningsPitched") or stats.get("innings") or "0"
            try:
                ip_float = float(ip_raw)
                ip = f"{int(ip_float)}.{int(round((ip_float % 1) * 10))}" if ip_float % 1 else f"{int(ip_float)}.0"
            except (TypeError, ValueError):
                ip = str(ip_raw)
            return {
                "ip": ip,
                "h": int(stats.get("hits") or stats.get("hit") or 0),
                "er": int(stats.get("earnedRuns") or stats.get("r") or 0),
                "k": int(stats.get("strikeOuts") or stats.get("strikeOut") or 0),
                "bb": int(stats.get("baseOnBalls") or stats.get("walks") or 0),
                "hr": int(stats.get("homeRuns") or stats.get("homeRun") or 0),
            }
    return None


def _pitcher_person_from_boxscore(box_data: dict, pitcher_id: int) -> dict | None:
    """Return the Stats API `person` object for this pitcher from a boxscore payload."""
    pid_str = str(pitcher_id)
    for side in ("home", "away"):
        team = (box_data.get("teams") or {}).get(side) or {}
        players = team.get("players") or {}
        for key, obj in players.items():
            if not isinstance(obj, dict):
                continue
            person = obj.get("person") or {}
            if key == f"ID{pid_str}" or str(person.get("id", "")) == pid_str:
                return person if isinstance(person, dict) else None
    return None


def fetch_person_mlb_debut_date(pitcher_id: int) -> str | None:
    """`mlbDebutDate` from `/people/{id}` (YYYY-MM-DD). None if missing or request fails."""
    try:
        r = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{int(pitcher_id)}",
            headers=REQUEST_HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        people = r.json().get("people") or []
        if not people:
            return None
        d = people[0].get("mlbDebutDate")
        return str(d).strip() if d else None
    except Exception:
        return None


def build_pitcher_source_metadata(
    *,
    game_pk: int | None,
    pitcher_id: int,
    game_date: str,
    box_data: dict | None = None,
) -> dict:
    """Provenance for card JSON: compare Stats API `mlbDebutDate` to this card's game date."""
    debut: str | None = None
    source: str | None = None
    bd = box_data
    if bd is None and game_pk is not None:
        bd = fetch_boxscore_data(game_pk)
    if bd:
        person = _pitcher_person_from_boxscore(bd, pitcher_id)
        if person:
            raw = person.get("mlbDebutDate")
            if raw:
                debut = str(raw).strip()
                source = "statsapi_boxscore"
    if debut is None:
        fallback = fetch_person_mlb_debut_date(pitcher_id)
        if fallback:
            debut = fallback
            source = "statsapi_people"
    gd = (game_date or "").strip()[:10]
    is_debut = bool(debut and gd and debut == gd)
    return {
        "game_official_date": gd or None,
        "mlb_debut_date": debut,
        "is_mlb_debut_game": is_debut,
        "mlb_debut_date_source": source,
    }


# -----------------------------------------------------------------
# DATA PIPELINE (Unchanged)
# -----------------------------------------------------------------
def load_game(parquet_path, pitcher_id):
    df  = pd.read_parquet(parquet_path)
    col = "pitcher" if "pitcher" in df.columns else "pitcher_id"
    if col not in df.columns: raise ValueError(f"No pitcher column. Columns: {list(df.columns)}")
    df = df[df[col] == pitcher_id].copy()
    if df.empty:
        avail = pd.read_parquet(parquet_path)[col].unique()
        raise ValueError(f"Pitcher {pitcher_id} not found. Available IDs: {avail}")
    return df

def _normalize_for_card(df: pd.DataFrame) -> None:
    """
    Normalize description, type, and bb_type so strike/BIP/hard-hit logic works for both
    Statcast CSV format (e.g. 'Hit Into Play', 'Called Strike', 'Ground Ball') and
    feed/warehouse format (e.g. 'hit_into_play', 'called_strike', 'ground_ball').
    Modifies df in place.
    """
    # description: lowercase, spaces -> underscores; map BIP variants to hit_into_play
    if 'description' in df.columns:
        d = df['description'].astype(str).str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace(',', '')
        # Statcast "Hit Into Play" -> "hit_into_play"; "In play, out(s)" -> "in_play_out(s)" -> treat as BIP
        bip_like = d.str.contains('in_play|hit_into_play', na=False, regex=True)
        df['description'] = d.where(~bip_like, 'hit_into_play')
    # type: canonical S/B so is_strike works (Statcast uses 'S','B','X'; only 'S' is strike)
    if 'type' in df.columns:
        t = df['type'].astype(str).str.strip().str.upper()
        df['type'] = t
    # bb_type: lowercase, spaces -> underscores for is_gb_bip
    if 'bb_type' in df.columns:
        b = df['bb_type'].astype(str).str.strip().str.lower().str.replace(' ', '_', regex=False)
        df['bb_type'] = b.replace('nan', '').replace('', np.nan)


def process_pitches(df):
    swing_codes = ['foul_bunt','foul','hit_into_play','swinging_strike','foul_tip','swinging_strike_blocked','missed_bunt','bunt_foul_tip']
    whiff_codes = ['swinging_strike','foul_tip','swinging_strike_blocked']
    df = df.copy()
    _normalize_for_card(df)
    df['pitch_type'] = df['pitch_type'].fillna('UN').astype(str)
    df['stand']      = df['stand'].fillna('R').astype(str)
    df['swing']      = df['description'].isin(swing_codes)
    df['whiff']      = df['description'].isin(whiff_codes)
    if 'zone' not in df.columns or df['zone'].isna().all():
        df['zone'] = 14
    df['in_zone']    = (df['zone'] < 10) & (df['zone'] > 0)
    df['out_zone']   = (df['zone'] > 10) | (df['zone'] == 0)
    df['chase']      = (~df['in_zone']) & df['swing']
    # Strikes: 'S' (called/swinging) or 'X' (in play) — both count as strikes per Savant Str%
    df['is_strike']  = df['type'].astype(str).str.strip().str.upper().isin(('S', 'X'))
    if 'pfx_z' in df.columns and df['pfx_z'].notna().any():
        df['pfx_z_in'] = df['pfx_z'].fillna(0) * 12
    else:
        df['pfx_z_in'] = np.nan
    if 'pfx_x' in df.columns and df['pfx_x'].notna().any():
        df['pfx_x_in'] = df['pfx_x'].fillna(0) * 12
    else:
        df['pfx_x_in'] = np.nan

    # Hard hit and damage are strictly for balls actually put into play
    df['is_bip'] = (df['description'] == 'hit_into_play')
    df['is_gb_bip'] = (df['bb_type'].fillna('').astype(str) == 'ground_ball') & df['is_bip']
    if 'launch_speed' in df.columns: df['hard_hit'] = (df['launch_speed'] >= 95.0) & df['is_bip']
    else: df['hard_hit'] = False
    if 'estimated_woba_using_speedangle' in df.columns: df['is_damage'] = (df['hard_hit'] | (df['estimated_woba_using_speedangle'] >= 0.350)) & df['is_bip']
    else: df['is_damage'] = df['hard_hit']
    return df

def compute_box_score(df):
    out_events = ['strikeout','field_out','force_out','grounded_into_double_play','double_play','fielders_choice_out','sac_fly','sac_bunt','strikeout_double_play','other_out']
    # Normalize events: feed uses "Field Out" (capitalized+spaces); Statcast uses "field_out"
    def _norm_ev(s):
        return s.astype(str).str.lower().str.replace(' ', '_', regex=False).replace('nan', pd.NA)
    events_norm = _norm_ev(df['events'])

    # One event per plate appearance: use last pitch of each PA so we don't over/undercount when events are duplicated or only on final pitch
    group_cols = [c for c in ['inning', 'at_bat_number'] if c in df.columns]
    sort_cols = [c for c in ['inning', 'at_bat_number', 'pitch_number'] if c in df.columns]
    if len(group_cols) >= 2 and len(sort_cols) >= 2:
        last_per_pa = df.sort_values(sort_cols).groupby(group_cols, dropna=False).tail(1)
        pa_events = _norm_ev(last_per_pa['events'])
    else:
        pa_events = events_norm

    outs = int(pa_events.isin(out_events).sum())
    k  = int(pa_events.isin(['strikeout','strikeout_double_play']).sum())
    bb = int(pa_events.isin(['walk','intent_walk']).sum())
    hr = int(pa_events.eq('home_run').sum())
    h  = int(pa_events.isin(['single','double','triple','home_run']).sum())
    n  = len(df)
    csw_codes = ['called_strike', 'swinging_strike', 'swinging_strike_blocked', 'foul_tip']
    csw       = df['description'].isin(csw_codes).sum()

    # Runs allowed: opponent score (batting team) at end minus at start of outing
    r = 0
    if 'home_score' in df.columns and 'away_score' in df.columns and 'inning_topbot' in df.columns:
        try:
            t = df.sort_values(['inning', 'at_bat_number', 'pitch_number']) if 'inning' in df.columns else df
            opp = np.where(t['inning_topbot'].astype(str).str.upper().str.startswith('T'), t['away_score'], t['home_score'])
            opp = pd.Series(opp).astype(float)
            r = max(0, int(opp.iloc[-1]) - int(opp.iloc[0]))
        except Exception:
            pass

    bip = int(df['is_bip'].sum())
    gb  = int(df['is_gb_bip'].sum())
    has_bb_type = 'bb_type' in df.columns and df['bb_type'].notna().any()
    gb_pct = (gb / bip) if (bip >= 5 and has_bb_type) else None

    fast_swing_pct = None
    if 'bat_speed' in df.columns and df['swing'].any():
        n_sw = int(df['swing'].sum())
        if n_sw >= 1:
            fast_swing_pct = float((df['swing'] & (df['bat_speed'] >= FAST_SWING_MPH)).sum()) / float(n_sw)

    zone_pct = df['in_zone'].sum() / n * 100 if n else 0
    return dict(
        ip=f"{outs//3}.{outs%3}", pa=df['at_bat_number'].nunique(),
        k=int(k), bb=int(bb), hr=int(hr), h=int(h), er=r,
        n=n, whiffs=int(df['whiff'].sum()),
        zone_pct=zone_pct, csw_pct=csw / n * 100 if n else 0, total_pitches=n,
        gb_pct=gb_pct, fast_swing_pct=fast_swing_pct,
    )

def group_arsenal(df, min_pitches=MIN_PITCHES):
    df = df.copy()
    if 'bat_speed' in df.columns:
        df['_fg75'] = df['swing'] & (df['bat_speed'] >= FAST_SWING_MPH)
    else:
        df['_fg75'] = False
    g = df.groupby('pitch_type').agg(
        count=('pitch_type','count'), velo=('release_speed','mean'), pfx_z=('pfx_z_in','mean'),
        pfx_x=('pfx_x_in','mean'), spin=('release_spin_rate','mean'), extension=('release_extension','mean'),
        rel_x=('release_pos_x','mean'), rel_z=('release_pos_z','mean'), swing=('swing','sum'),
        whiff=('whiff','sum'), in_zone=('in_zone','sum'), out_zone=('out_zone','sum'), chase=('chase','sum'),
        fast_ge_75=('_fg75', 'sum'),
        xwoba=('estimated_woba_using_speedangle','mean'), delta_re=('delta_run_exp','sum'),
        gb=('is_gb_bip', 'sum'), bip=('is_bip', 'sum'), hard_hit=('hard_hit', 'sum'),
    ).reset_index()
    # No minimum pitch-count filter per pitch type: even a single pitch of a given type should appear.
    # min_pitches is kept only for other callers (e.g. random card selection), not for this grouping.
    g = g.copy()
    if 'is_strike' in df.columns:
        g['strikes'] = g['pitch_type'].map(df.groupby('pitch_type')['is_strike'].sum())
    else:
        g['strikes'] = np.nan
    total = len(df)
    g['usage_pct']    = g['count'] / total
    sw = g['swing'].replace(0, np.nan)
    # Whiff% / Chase% / BS75+%: denominator = swings (Str% / Zone% still per pitch)
    g['whiff_pct']    = g['whiff'] / sw
    g['str_pct']      = g['strikes'] / g['count']   # strikes / pitches
    g['zone_pct']     = g['in_zone'] / g['count']   # in-zone / pitches
    g['chase_pct']    = g['chase'] / sw
    g['rv100']        = -g['delta_re'] / g['count'] * 100
    gb_denom = g['bip'].replace(0, np.nan)
    # GB%: ground balls as a share of balls IN PLAY (not all pitches)
    g['gb_pct']       = g['gb'] / gb_denom
    # Don't show GB% when fewer than 5 balls in play to avoid noisy 0%/100% from tiny samples
    g.loc[g['bip'] < 5, 'gb_pct'] = np.nan
    g['hard_hit_pct'] = (g['hard_hit'] / gb_denom).fillna(0.0).clip(upper=1.0)

    # BS75+%: swings with bat speed ≥ FAST_SWING_MPH, as a share of all swings on that pitch type
    g['fast_swing_pct'] = g['fast_ge_75'] / sw

    g['name']   = g['pitch_type'].map(DICT_PITCH).fillna(g['pitch_type'])
    g['colour'] = g['pitch_type'].map(DICT_COLOUR).fillna('#9C8975')
    g = g.drop(columns=['fast_ge_75'], errors='ignore')
    return g.sort_values('count', ascending=False).reset_index(drop=True)

def fetch_player_bio(pitcher_id):
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={pitcher_id}&hydrate=currentTeam"
    try:
        data = requests.get(url, timeout=10).json()['people'][0]
        team_abb, link = "MLB", data.get('currentTeam', {}).get('link', '')
        if link: team_abb = requests.get(f"https://statsapi.mlb.com{link}", timeout=10).json()['teams'][0]['abbreviation']
        return dict(name=data['fullName'], hand=data['pitchHand']['code'], age=data.get('currentAge','--'), height=data.get('height','--'), weight=data.get('weight','--'), team=team_abb)
    except Exception: return dict(name="Unknown Pitcher", hand="R", age="--", height="--", weight="--", team="MLB")

def fetch_headshot(pitcher_id):
    pid = int(pitcher_id)
    url = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_640,q_auto:best/v1/people/{pid}/headshot/silo/current.png"
    try:
        r = requests.get(url, timeout=10)
        if not r.ok or len(r.content) < 500:
            return None
        img = Image.open(BytesIO(r.content))
        # Green/teal and black/charcoal silo backdrops → neutral (see src/mlb_headshot.py)
        replace = (255, 255, 255) if LIGHT_MODE else (0x1F, 0x2E, 0x3D)
        return neutralize_mlb_headshot_background(img, replace_rgb=replace)
    except Exception:
        return None

def fetch_team_logo(team_abb):
    key = ESPN_LOGOS.get(team_abb, team_abb.lower())
    url = f"https://a.espncdn.com/combiner/i?img=/i/teamlogos/mlb/500/scoreboard/{key}.png&h=200&w=200"
    try: return Image.open(BytesIO(requests.get(url, timeout=10).content))
    except Exception: return None

# -----------------------------------------------------------------
# PLOT HELPERS
# -----------------------------------------------------------------
def _clean(ax, bg=None):
    ax.set_facecolor(bg or PALETTE["panel_bg"])
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.set_xticks([]); ax.set_yticks([])

def _border(ax):
    for sp in ax.spines.values():
        sp.set_visible(True)
        sp.set_edgecolor(PALETTE["border"])
        sp.set_linewidth(1.5) # Thicker, structural frame

def _lum(hex_color):
    r, g, b = mpl.colors.to_rgb(hex_color)
    return 0.299*r + 0.587*g + 0.114*b

def _grad_color(val, vmin, vmax, lo_hex, hi_hex, invert=False):
    try: t = 0.5 if np.isnan(float(val)) or vmax == vmin else float(np.clip((val - vmin) / (vmax - vmin), 0.0, 1.0))
    except Exception: t = 0.5
    if invert: t = 1.0 - t
    lo, hi = np.array(mpl.colors.to_rgb(lo_hex)), np.array(mpl.colors.to_rgb(hi_hex))
    return mpl.colors.to_hex(lo + t * (hi - lo))

def _fmt_movement(pfx_x_in, pfx_z_in, _hand: str):
    try:
        if np.isnan(pfx_x_in) or np.isnan(pfx_z_in):
            return "--", "--"
        x = float(pfx_x_in)
        z = float(pfx_z_in)
        # Display HB = -Statcast pfx_x for both hands (see plot_movement).
        return f'{-x:+.1f}"', f'{z:+.1f}"'
    except (TypeError, ValueError):
        return "--", "--"

# -----------------------------------------------------------------
# PANELS
# -----------------------------------------------------------------
def plot_header(ax, bio, box, game_date, opp_team, headshot_img, logo_img):
    _clean(ax, PALETTE["header_bg"])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

    if headshot_img:
        ai = ax.inset_axes([0.005, 0.04, 0.115, 0.92])
        ai.set_facecolor(PALETTE["panel_bg"])
        arr = np.array(headshot_img)
        ai.imshow(arr, extent=[0, 1, 0, 1], origin="upper", aspect="auto", zorder=1)
        ai.set_xlim(0, 1)
        ai.set_ylim(0, 1)
        ai.axis("off")

    # BUMPED: Main Name to 34
    ax.text(0.135, 0.92, bio['name'], color=PALETTE["text_primary"], fontsize=34, fontweight='black', ha='left', va='top', transform=ax.transAxes)
    
    # BUMPED: Date/Opp to 16
    ax.text(0.135, 0.60, f"{game_date}   \u00b7   vs  {opp_team}", color=PALETTE["accent_orange"], fontsize=16, fontweight='black', ha='left', va='top', transform=ax.transAxes)

    # BUMPED: Bio & Game Stats
    ax.text(0.135, 0.40, f"{bio['hand']}HP  \u00b7  Age {bio['age']}  \u00b7  {bio['height']}  \u00b7  {bio['weight']} lbs", color=PALETTE["text_lo"], fontsize=12, ha='left', va='top', transform=ax.transAxes)
    summary = f"{box['total_pitches']} Pitches  \u00b7  {box['zone_pct']:.1f}% Zone  \u00b7  {box['whiffs']} Whiffs  \u00b7  CSW  {box['csw_pct']:.1f}%"
    if box.get('gb_pct') is not None:
        summary += f"  \u00b7  {box['gb_pct']*100:.0f}% GB"
    ax.text(0.135, 0.20, summary, color=PALETTE["text_primary"], fontsize=13, fontweight='bold', ha='left', va='top', transform=ax.transAxes)

    row1 = [("IP", box['ip'], PALETTE["text_primary"]), ("H", box['h'], PALETTE["text_primary"]), ("R", box.get('er', 0), PALETTE["text_primary"])]
    row2 = [("K", box['k'], PALETTE["accent_orange"]), ("BB", box['bb'], PALETTE["accent_orange"]), ("HR", box['hr'], PALETTE["accent_orange"])]
    # Stats block: 3 cols in [0.630, 0.875], logo in [0.885, 0.975]
    vline_x = 0.610
    logo_x  = 0.882
    bx0     = 0.660     # center of first column
    dx      = 0.075     # column spacing → cols at 0.660, 0.735, 0.810

    # Value (big) on top, label (small) below — clearly paired
    for i, (lbl, val, col) in enumerate(row1):
        xp = bx0 + i * dx
        ax.text(xp, 0.91, str(val), color=col, fontsize=26, fontweight='black', ha='center', va='top', transform=ax.transAxes)
        ax.text(xp, 0.63, lbl, color=PALETTE["text_lo"], fontsize=11, fontweight='bold', ha='center', va='top', transform=ax.transAxes)

    # Subtle horizontal separator between rows
    ax.plot([vline_x + 0.01, logo_x - 0.01], [0.54, 0.54], color=PALETTE["border"], lw=0.8, alpha=0.6, transform=ax.transAxes)

    for i, (lbl, val, col) in enumerate(row2):
        xp = bx0 + i * dx
        ax.text(xp, 0.50, str(val), color=col, fontsize=26, fontweight='black', ha='center', va='top', transform=ax.transAxes)
        ax.text(xp, 0.23, lbl, color=PALETTE["text_lo"], fontsize=11, fontweight='bold', ha='center', va='top', transform=ax.transAxes)

    ax.plot([vline_x, vline_x], [0.08, 0.95], color=PALETTE["border"], lw=1.2, transform=ax.transAxes)

    if logo_img:
        al = ax.inset_axes([logo_x, 0.08, 1.0 - logo_x - 0.01, 0.84])
        al.imshow(np.array(logo_img))
        al.axis('off')
    ax.plot([0, 1], [0.02, 0.02], color=PALETTE["accent_orange"], lw=2.5, alpha=0.8, transform=ax.transAxes)

def plot_damage_heatmap(ax, arsenal, df):
    # Savant-inspired: standard panel background, white strike zone, large solid dots
    bg_col   = PALETTE["panel_bg"]
    zone_bg  = "#FFFFFF" if LIGHT_MODE else "#1A2D3F"
    zone_bdr = "#4A90C4" if LIGHT_MODE else PALETTE["zone_edge"]
    ax.set_facecolor(bg_col)
    _border(ax)

    sz_top = df['sz_top'].median() if 'sz_top' in df.columns else 3.5
    sz_bot = df['sz_bot'].median() if 'sz_bot' in df.columns else 1.5
    zw     = 17 / 12

    # White fill inside the strike zone (clean Savant look)
    ax.add_patch(mpatches.Rectangle((-zw/2, sz_bot), zw, sz_top - sz_bot,
                                     fill=True, facecolor=zone_bg, alpha=0.85,
                                     edgecolor='none', zorder=1))
    # Dashed strike zone border
    ax.add_patch(mpatches.Rectangle((-zw/2, sz_bot), zw, sz_top - sz_bot,
                                     fill=False, edgecolor=zone_bdr,
                                     lw=1.8, ls='--', zorder=4))

    # Dots — desaturated when hard contact exists so stars/heatmap are the focal point
    pt_colour = dict(zip(arsenal['pitch_type'], arsenal['colour'])) if arsenal is not None and not arsenal.empty else {}
    df_dmg = df[df['is_damage'] == True].dropna(subset=['plate_x', 'plate_z'])
    no_damage = len(df_dmg) == 0
    dot_alpha = 0.30 if not no_damage else 0.75   # fade non-damage dots when there is damage
    dot_s     = 70   if not no_damage else 80
    for pt, grp in df.dropna(subset=['plate_x', 'plate_z']).groupby('pitch_type'):
        col = pt_colour.get(pt, PALETTE["text_lo"])
        ax.scatter(grp['plate_x'], grp['plate_z'], color=col, s=dot_s, alpha=dot_alpha,
                   edgecolors='none', linewidths=0, zorder=3)

    star_edge = 'white'
    if len(df_dmg) >= 4:
        try: sns.kdeplot(data=df_dmg, x='plate_x', y='plate_z',
                         fill=True, cmap='YlOrRd', alpha=0.55,
                         levels=8, ax=ax, zorder=2)
        except Exception: pass
    # Stars uniform red — pitch type already visible via dot colors
    if len(df_dmg) > 0:
        for pt, grp in df_dmg.groupby('pitch_type'):
            col = pt_colour.get(pt, PALETTE["accent_red"])
            ax.scatter(grp['plate_x'], grp['plate_z'], color=col, s=180,
                       marker='*', edgecolors=star_edge, linewidths=0.9, zorder=6)

    # Axis limits
    zone_center_z = (sz_top + sz_bot) / 2.0
    half_from_zone = max(zw / 2, (sz_top - sz_bot) / 2) + 0.55
    px = df['plate_x'].dropna()
    pz = df['plate_z'].dropna()
    if len(px) and len(pz):
        half = max(half_from_zone,
                   float(np.nanmax(np.abs(px))) + 0.45,
                   float(np.nanmax(np.abs(pz - zone_center_z))) + 0.55)
    else:
        half = half_from_zone
    ax.set_aspect('equal')
    ax.set_xlim(-half, half)
    ax.set_ylim(zone_center_z - half, zone_center_z + half)
    # Remove auto tick labels/axis labels (seaborn kdeplot sets column names as labels)
    ax.set_xticks([]); ax.set_yticks([])
    ax.set_xlabel(''); ax.set_ylabel('')

    ax.set_title("HARD CONTACT ALLOWED", color=PALETTE["text_secondary"], fontsize=13, fontweight='black', pad=10)
    ax.text(0.5, 0.015, "Catcher's View  ·  Colored by pitch type", transform=ax.transAxes,
            ha='center', va='bottom', color=PALETTE["text_lo"], fontsize=8.5, fontstyle='italic')

    # Legend — raised to avoid overlapping subtitle; only damage status shown
    import matplotlib.lines as mlines
    if no_damage:
        h_nd = mlines.Line2D([], [], marker='P', linestyle='none', markersize=9,
                             color=PALETTE["accent_green"], label="No Hard Contact")
        leg = ax.legend(handles=[h_nd], loc='lower center',
                        fontsize=8.5, frameon=True, framealpha=0.92,
                        edgecolor=PALETTE["border"], facecolor=bg_col,
                        borderpad=0.4, handletextpad=0.3,
                        bbox_to_anchor=(0.5, 0.06))
    else:
        h_dmg = mlines.Line2D([], [], marker='*', linestyle='none', markersize=11,
                               markerfacecolor=PALETTE["accent_red"], markeredgecolor=star_edge,
                               markeredgewidth=0.7, label="★ Hard contact (EV ≥95 | xwOBA ≥.350)")
        leg = ax.legend(handles=[h_dmg], loc='lower center',
                        fontsize=8.5, frameon=True, framealpha=0.92,
                        edgecolor=PALETTE["border"], facecolor=bg_col,
                        borderpad=0.4, handletextpad=0.3,
                        bbox_to_anchor=(0.5, 0.06))
    leg.set_zorder(20)
    for txt in leg.get_texts():
        txt.set_color(PALETTE["accent_green"] if no_damage else PALETTE["text_secondary"])
        txt.set_fontweight('bold')

def plot_movement(ax, arsenal, df, hand):
    _clean(ax); _border(ax)
    # Statcast pfx_x is catcher-frame (+ = toward 1B). Plot uses -pfx_x for BOTH hands so that
    # clusters sit on the same side as the Arm/Glove captions (TJ / public-analyst style):
    # RHP: glove (1B/+pfx) → left; arm (3B/-pfx) → right. LHP: arm (1B/+pfx) → left; glove → right.
    sign = -1

    kde_fill_alpha, scatter_alpha = (0.35, 0.75) if LIGHT_MODE else (0.25, 0.55)

    for _, row in arsenal.iterrows():
        mask = df['pitch_type'] == row['pitch_type']
        xs, zs = df.loc[mask, 'pfx_x_in'] * sign, df.loc[mask, 'pfx_z_in']
        if len(xs) >= 6:
            try: sns.kdeplot(x=xs, y=zs, fill=True, color=row['colour'], alpha=kde_fill_alpha, levels=8, ax=ax, zorder=2, warn_singular=False)
            except Exception: pass
        ax.scatter(xs, zs, color=row['colour'], s=25, alpha=scatter_alpha, edgecolors='none', zorder=4)
        if len(xs) >= 1:
            ax.text(xs.mean(), zs.mean(), row['pitch_type'], ha='center', va='center', color='white', fontsize=10, fontweight='bold', bbox=dict(facecolor=row['colour'], edgecolor='none', boxstyle='round,pad=0.2', alpha=0.95), zorder=6)

    # Auto-scale to actual movement range; keep x symmetric, y tight to data
    mask_all = df['pitch_type'].isin(arsenal['pitch_type'])
    xs_all = (df.loc[mask_all, 'pfx_x_in'].dropna() * sign)
    zs_all =  df.loc[mask_all, 'pfx_z_in'].dropna()
    pad = 3.0
    if len(xs_all) and len(zs_all):
        x_lim  = max(xs_all.abs().max() + pad, 10.0)
        z_lo   = min(zs_all.min() - pad, -6.0)
        z_hi   = max(zs_all.max() + pad, 10.0)
    else:
        x_lim, z_lo, z_hi = 18.0, -18.0, 18.0

    line_c  = "#A0AEC0" if LIGHT_MODE else PALETTE["text_lo"]
    grid_c  = "#CBD5E0" if LIGHT_MODE else PALETTE["grid"]

    # ±6 / ±12 inch reference grid — always drawn, matplotlib clips to axis limits
    for v in [-12, -6, 6, 12]:
        ax.axhline(v, color=grid_c, lw=0.9, alpha=0.55, ls="--", zorder=1)
        ax.axvline(v, color=grid_c, lw=0.9, alpha=0.55, ls="--", zorder=1)

    # Prominent zero crosshair
    ax.axhline(0, color=line_c, lw=1.8, alpha=0.95, zorder=1)
    ax.axvline(0, color=line_c, lw=1.8, alpha=0.95, zorder=1)

    # Arm slot reference: only when arm_angle is present in data (often missing in ST)
    # Intuitive placement: RHP → ray on the right of y-axis; LHP → ray on the left
    arm_deg = np.nan
    if 'arm_angle' in df.columns:
        arm_deg = df['arm_angle'].dropna().mean()
        if not np.isnan(arm_deg) and abs(arm_deg) < 90:
            rad = np.deg2rad(abs(arm_deg))
            L = 1.0 * min(x_lim, (z_hi - z_lo) * 0.5)
            dx_base = abs(L * np.sin(rad))
            dy = abs(L * np.cos(rad))
            dx = dx_base if hand == 'R' else -dx_base  # RHP: right of y-axis; LHP: left
            ax.plot([0, dx], [0, dy], color=PALETTE["accent_orange"], lw=2, ls='--', alpha=0.85, zorder=3)

    ax.set_xlim(-x_lim, x_lim); ax.set_ylim(z_lo, z_hi)
    ax.set_xticks([]); ax.set_yticks([])

    # Axis inch labels outside the frame — use axes-fraction coords so layout never clips them
    x_range = 2 * x_lim
    z_range = z_hi - z_lo
    label_out_kw = dict(color=PALETTE["text_lo"], fontsize=7.5, alpha=0.85,
                        clip_on=False, transform=ax.transAxes)
    for v in [-12, -6, 6, 12]:
        # Y-axis: just left of left spine (axes x = -0.05)
        if z_lo < v < z_hi:
            ax_y = (v - z_lo) / z_range
            ax.text(-0.02, ax_y, f"{v:+d}\"", va="center", ha="right", **label_out_kw)
        # X-axis: just below bottom spine (axes y = -0.02)
        if -x_lim < v < x_lim:
            ax_x = (v - (-x_lim)) / x_range
            ax.text(ax_x, -0.02, f"{v:+d}\"", va="top", ha="center", **label_out_kw)

    # Arm/Glove labels: RHP has Arm on the right; LHP has Arm on the left (same as ray)
    lkw = dict(fontsize=9, fontstyle='italic', color=PALETTE["text_secondary"],
               transform=ax.transAxes,
               bbox=dict(facecolor=PALETTE["panel_bg"], edgecolor=PALETTE["border"],
                         boxstyle='round,pad=0.20', linewidth=0.7, alpha=0.88))
    if hand == 'R':
        ax.text(0.02, 0.04, "Glove Side \u2190", ha='left', va='bottom', **lkw)
        ax.text(0.98, 0.04, "Arm Side \u2192",  ha='right', va='bottom', **lkw)
    else:
        ax.text(0.02, 0.04, "\u2190 Arm Side", ha='left', va='bottom', **lkw)
        ax.text(0.98, 0.04, "Glove Side \u2192",  ha='right', va='bottom', **lkw)

    title = "MOVEMENT PROFILE"
    if not np.isnan(arm_deg) and abs(arm_deg) < 90:
        title += f" \u2022 {arm_deg:.0f}\u00b0 Arm Angle"
    ax.set_title(title, color=PALETTE["text_secondary"], fontsize=13, fontweight='black', pad=10)

# Shared with plot_pitch_tendencies and compute_pitch_tendencies_by_situation (card JSON for tweets / redraft).
PITCH_TENDENCY_SITUATIONS = [
    ("FIRST PITCH", "0-0", [(0, 0)]),
    ("PITCHER AHEAD", "0-1  \u00b7  1-1", [(0, 1), (1, 1)]),
    ("TWO-STRIKE", "0-2  \u00b7  1-2  \u00b7  2-2", [(0, 2), (1, 2), (2, 2)]),
    ("EVEN", "1-0  \u00b7  2-1", [(1, 0), (2, 1)]),
    ("HITTER AHEAD", "2-0  \u00b7  3-0  \u00b7  3-1", [(2, 0), (3, 0), (3, 1)]),
    ("FULL COUNT", "3-2", [(3, 2)]),
]


def _tendency_balls_strikes(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Count-state columns for tendency bucketing (coerce, clip to in-play grid, round).

    Raw feeds sometimes carry floats, strings, or post-pitch walk/K counts (>3 balls / >2 strikes).
    Without this, those rows match no situation and vanish from the n= sums while still in pitch totals.
    """
    if "balls" not in df.columns or "strikes" not in df.columns:
        return pd.Series(np.nan, index=df.index), pd.Series(np.nan, index=df.index)
    b = pd.to_numeric(df["balls"], errors="coerce").clip(lower=0, upper=3).round()
    s = pd.to_numeric(df["strikes"], errors="coerce").clip(lower=0, upper=2).round()
    return b, s


def _pitch_tendency_specs(df: pd.DataFrame) -> list[tuple[str, str, pd.Series]]:
    """Situation label, count subtitle, boolean mask — same logic for plot + card JSON."""
    b, s = _tendency_balls_strikes(df)
    # Sentinel so NaN balls/strikes never match a legal count (avoids pd.NA in boolean masks)
    b_m = b.fillna(-1)
    s_m = s.fillna(-1)
    specs: list[tuple[str, str, pd.Series]] = []
    union = pd.Series(False, index=df.index)
    for sit_label, count_str, counts in PITCH_TENDENCY_SITUATIONS:
        m = pd.Series(False, index=df.index)
        for bb, ss in counts:
            m = m | ((b_m == bb) & (s_m == ss))
        union = union | m
        specs.append((sit_label, count_str, m))
    other = ~union
    if int(other.sum()) > 0:
        specs.append(("OTHER", "outside standard counts", other))
    return specs


def plot_pitch_tendencies(ax, arsenal, df):
    _clean(ax); _border(ax)
    ACCENTS = [PALETTE["text_lo"], PALETTE["accent_green"], "#5BC8D5", PALETTE["text_lo"], PALETTE["accent_orange"], "#BE5FA0"]

    LPAD, RPAD, TPAD, BPAD = 0.03, 0.03, 0.04, 0.04   # tight top padding: use full height
    specs = _pitch_tendency_specs(df)
    n_rows = len(specs)
    ROW_H = (1 - TPAD - BPAD) / n_rows
    LABEL_W, BAR_W, BADGE_W = 0.32, 0.44, 0.15

    for si, (sit_label, _count_str, mask) in enumerate(specs):
        sit_df, n_total = df[mask], int(mask.sum())
        y_center = 1 - TPAD - si * ROW_H - ROW_H/2

        bg = PALETTE["table_alt"] if si % 2 == 0 else PALETTE["table_bg"]
        accent = ACCENTS[si % len(ACCENTS)]
        ax.add_patch(FancyBboxPatch((LPAD, y_center - ROW_H/2 + 0.005), 1 - LPAD - RPAD, ROW_H - 0.010, boxstyle="round,pad=0.004", lw=0, facecolor=bg, transform=ax.transAxes, zorder=1))
        ax.add_patch(FancyBboxPatch((LPAD, y_center - ROW_H/2 + 0.005), 0.008, ROW_H - 0.010, boxstyle="square,pad=0", lw=0, facecolor=accent, alpha=0.95, transform=ax.transAxes, zorder=2))

        ax.text(LPAD + 0.025, y_center + ROW_H * 0.14, sit_label,
                ha='left', va='center', transform=ax.transAxes,
                color=PALETTE["text_primary"], fontsize=10.5, fontweight='black', zorder=3)
        n_note = f"n = {n_total}" if sit_label != "OTHER" else f"n = {n_total}  ·  non-grid / missing count"
        ax.text(LPAD + 0.025, y_center - ROW_H * 0.25, n_note,
                ha='left', va='center', transform=ax.transAxes,
                color=PALETTE["text_primary"], fontsize=9.0, fontweight='black', zorder=3)

        bar_x, bar_y, bar_h = LPAD + LABEL_W, y_center - ROW_H * 0.28, ROW_H * 0.56
        if n_total == 0:
            ax.text(bar_x + BAR_W / 2, y_center, "\u2014  no data", ha='center', va='center', transform=ax.transAxes, color=PALETTE["text_lo"], fontsize=10, zorder=3)
            continue

        pitch_counts = sit_df['pitch_type'].value_counts()
        x_cur, dominant_pt, dominant_pct, dominant_col = bar_x, None, 0.0, PALETTE["text_lo"]

        for _, arow in arsenal.iterrows():
            pt, cnt = arow['pitch_type'], pitch_counts.get(arow['pitch_type'], 0)
            if cnt == 0: continue
            pct, seg_w = cnt / n_total, BAR_W * (cnt / n_total)
            ax.add_patch(FancyBboxPatch((x_cur, bar_y), max(seg_w, 0.0005), bar_h, boxstyle="square,pad=0", lw=0, facecolor=arow['colour'], alpha=0.95, transform=ax.transAxes, zorder=4))
            if pct >= 0.20:
                tc = '#111111' if _lum(arow['colour']) > 0.50 else '#FFFFFF'
                ax.text(x_cur + seg_w / 2, y_center, pt, ha='center', va='center', transform=ax.transAxes, color=tc, fontsize=10, fontweight='bold', zorder=5)
            if pct > dominant_pct: dominant_pt, dominant_pct, dominant_col = pt, pct, arow['colour']
            x_cur += seg_w

        if dominant_pt:
            bx, pill_w, pill_h = LPAD + LABEL_W + BAR_W + 0.020, BADGE_W - 0.015, ROW_H * 0.60
            pill_lw, pill_ec = (0.8, PALETTE["border"]) if LIGHT_MODE else (0.0, "none")
            ax.add_patch(FancyBboxPatch((bx, y_center - pill_h / 2), pill_w, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=dominant_col, alpha=0.95, transform=ax.transAxes, zorder=5))
            tc = '#111111' if _lum(dominant_col) > 0.50 else '#FFFFFF'
            ax.text(bx + pill_w / 2, y_center + ROW_H * 0.12, dominant_pt, ha='center', va='center', transform=ax.transAxes, color=tc, fontsize=11, fontweight='black', zorder=6)
            ax.text(bx + pill_w / 2, y_center - ROW_H * 0.22, f"{dominant_pct:.0%}", ha='center', va='center', transform=ax.transAxes, color=tc, fontsize=9.5, fontweight='bold', zorder=6)

    ax.set_title("PITCH TENDENCIES BY SITUATION", color=PALETTE["text_secondary"], fontsize=13, fontweight='black', pad=10)

# -----------------------------------------------------------------
# PANEL 4 - ARSENAL TABLE
# -----------------------------------------------------------------
def plot_arsenal_table(ax, arsenal, hand, box, benchmarks=None, card_flags=None):
    _clean(ax, PALETTE["card_bg"])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

    if card_flags is None:
        card_flags = {}
    _has_xwoba = card_flags.get('has_xwoba', 'xwoba' in arsenal.columns and arsenal['xwoba'].notna().any())
    _has_bs75  = card_flags.get('has_bs75',  'fast_swing_pct' in arsenal.columns and arsenal['fast_swing_pct'].notna().any())

    col11  = "BS75+%" if _has_bs75 else "Zone%"
    last_col = "xwOBA*" if _has_xwoba else "HH%*"

    COLS   = ["Pitch", "#", "Pitch%", "Velo", "Spin", "Ext.", "HB", "IVB", "Chase%", "Whiffs", "Str%", col11, last_col]
    WIDTHS = [0.178, 0.044, 0.060, 0.060, 0.056, 0.052, 0.062, 0.062, 0.064, 0.064, 0.064, 0.064, 0.082]
    WIDTHS = [w / sum(WIDTHS) for w in WIDTHS]

    HDR_Y, SEP_Y = 0.97, 0.89
    n_data_rows = len(arsenal) + 1   # pitch rows + All row

    # Row height and clear gap so header line never touches first row
    ROW_H = 0.83 / n_data_rows
    y_top = SEP_Y - (ROW_H / 2) - 0.022

    hdr_color, hdr_weight = (PALETTE["text_primary"], 'black') if LIGHT_MODE else (PALETTE["text_secondary"], 'bold')

    xp = 0.005
    for col, w in zip(COLS, WIDTHS):
        ax.text(xp + w / 2, HDR_Y, col, ha='center', va='top', transform=ax.transAxes, color=hdr_color, fontsize=12, fontweight=hdr_weight)
        xp += w
    ax.plot([0.005, 0.995], [SEP_Y, SEP_Y], color=PALETTE["border"], lw=1.0, transform=ax.transAxes)

    total = arsenal['count'].sum()
    tsw = arsenal['swing'].sum()
    # All row: whiff/chase/BS75 use total swings; str/zone/xwOBA still per pitch
    aw = arsenal['whiff'].sum() / tsw if tsw else np.nan
    astr = arsenal['strikes'].sum() / total if 'strikes' in arsenal.columns and total else np.nan  # strikes / included pitches
    az = arsenal['in_zone'].sum() / total if total else np.nan        # in-zone / included pitches
    axw = (arsenal['xwoba'] * arsenal['count']).sum() / total if total else np.nan  # weighted xwOBA (included pitches)
    ach = arsenal['chase'].sum() / tsw if tsw else np.nan
    all_fast_swing_pct = box.get('fast_swing_pct')  # game-level: fast swings / swings

    def _safe(v):
        try: return not np.isnan(float(v))
        except: return False

    # All-row hard-hit %: hard hits / balls in play across all pitch types
    total_bip = arsenal['bip'].sum() if 'bip' in arsenal.columns else 0
    total_hh  = arsenal['hard_hit'].sum() if 'hard_hit' in arsenal.columns else 0
    ahh = (total_hh / total_bip) if total_bip > 0 else np.nan

    rows = []
    for _, r in arsenal.iterrows():
        hb_str, ivb_str = _fmt_movement(r['pfx_x'], r['pfx_z'], hand)
        rows.append(dict(name=r['name'], count=int(r['count']), pct=f"{r['usage_pct']:.1%}", velo=f"{r['velo']:.1f}" if _safe(r['velo']) else "--", spin=f"{r['spin']:.0f}" if _safe(r.get('spin', np.nan)) else "--", ext=f"{r['extension']:.1f}" if _safe(r.get('extension', np.nan)) else "--", hb=hb_str, ivb=ivb_str, raw_whiff=r['whiff_pct'], whiffs=int(r.get('whiff', 0) or 0), raw_str=r.get('str_pct', np.nan), raw_chase=r.get('chase_pct', np.nan), raw_zone=r['zone_pct'], raw_xwoba=r['xwoba'], raw_hh_pct=r.get('hard_hit_pct', np.nan), raw_fast_swing=r.get('fast_swing_pct', np.nan), colour=r['colour'], is_all=False))
    # All row: show the full pitch count from the header so numbers stay consistent (even if some rare pitch types are filtered out by MIN_PITCHES)
    rows.append(dict(name='All', count=int(box['total_pitches']), pct='100%', velo='--', spin='--', ext='--', hb='--', ivb='--',
                     whiffs=int(arsenal['whiff'].sum()) if 'whiff' in arsenal.columns else int(box.get('whiffs', 0) or 0),
                     raw_whiff=aw, raw_str=astr, raw_chase=ach, raw_zone=az, raw_xwoba=axw,
                     raw_hh_pct=ahh, raw_fast_swing=all_fast_swing_pct, colour=PALETTE["text_lo"], is_all=True))

    def _range(key, league_key=None):
        # Fallback: per-game range from rows (excluding All row)
        vals = [r[key] for r in rows if not r['is_all'] and _safe(r[key])]
        if len(vals) >= 2:
            lo, hi = min(vals), max(vals)
            data_rng = (lo, hi) if hi > lo else (0.0, 1.0)
        else:
            data_rng = (0.0, 1.0)
        if not benchmarks or not league_key:
            return data_rng
        try:
            metric = benchmarks.get(league_key, {})
            # Legacy JSON used *_per_pitch; gradients are approximate until benchmarks are regenerated.
            if league_key == "whiff_per_swing" and "whiff_per_swing" not in benchmarks:
                metric = benchmarks.get("whiff_per_pitch", {})
            if league_key == "chase_per_swing" and "chase_per_swing" not in benchmarks:
                metric = benchmarks.get("chase_per_pitch", {})
            lo = metric.get("p20", metric.get("p5"))
            hi = metric.get("p80", metric.get("p95"))
            if lo is None or hi is None:
                return data_rng
            lo, hi = float(lo), float(hi)
            if hi <= lo:
                return data_rng
            return (lo, hi)
        except Exception:
            return data_rng

    # League-anchored ranges where available; fall back to game-only spread
    chase_range       = _range('raw_chase',       league_key="chase_per_swing")
    whiff_range       = _range('raw_whiff',       league_key="whiff_per_swing")
    str_range         = _range('raw_str',         league_key="strike_per_pitch")
    fast_swing_range  = _range('raw_fast_swing',   league_key="fast_swing_per_swing")
    xw_range          = _range('raw_xwoba',       league_key="xwoba_allowed")

    velo_vals = [float(r['velo']) for r in rows if not r['is_all'] and r['velo'] != '--']
    velo_data_range = (min(velo_vals), max(velo_vals)) if len(velo_vals) >= 2 else (85.0, 100.0)
    if benchmarks and "velocity_mph" in benchmarks:
        try:
            vbm = benchmarks["velocity_mph"]
            v_lo = float(vbm.get("p20", vbm.get("p5")))
            v_hi = float(vbm.get("p80", vbm.get("p95")))
            velo_range = (v_lo, v_hi) if v_hi > v_lo else velo_data_range
        except Exception:
            velo_range = velo_data_range
    else:
        velo_range = velo_data_range

    pill_lw, pill_ec = (0.8, PALETTE["border"]) if LIGHT_MODE else (0.0, "none")

    for ri, row in enumerate(rows):
        yc = y_top - ri * ROW_H
        bg = PALETTE["table_alt"] if ri % 2 == 0 else PALETTE["table_bg"]
        ax.add_patch(FancyBboxPatch((0.005, yc - ROW_H * 0.52), 0.990, ROW_H, boxstyle="square,pad=0", lw=0, facecolor=bg, transform=ax.transAxes, zorder=0))

        xp = 0.005
        for ci, (col, w) in enumerate(zip(COLS, WIDTHS)):
            xc = xp + w / 2
            
            # Narrow pills so adjacent cells have visible horizontal gap
            pill_w, pill_h = w * 0.70, ROW_H * 0.72
            pill_x, pill_y = xc - (pill_w / 2), yc - (pill_h / 2)

            if ci == 0:
                if not row['is_all']:
                    disp = row['name'] if len(row['name']) <= 16 else row['name'][:14] + '..'
                    p_name_w = w * 0.92
                    ax.add_patch(FancyBboxPatch((xc - (p_name_w/2), pill_y), p_name_w, pill_h, boxstyle="round,pad=0.006", lw=0, facecolor=row['colour'], transform=ax.transAxes, zorder=1))
                    tc = '#111111' if _lum(row['colour']) > 0.50 else '#FFFFFF'
                    ax.text(xc, yc, disp, ha='center', va='center', fontsize=11.5, fontweight='black', color=tc, transform=ax.transAxes, zorder=2)
                else:
                    ax.text(xc, yc, row['name'], ha='center', va='center', fontsize=13, fontweight='black', color=PALETTE["text_secondary"], transform=ax.transAxes)

            elif ci == 1:
                ax.text(xc, yc, str(row['count']), ha='center', va='center', fontsize=13, fontweight='bold', color=PALETTE["text_primary"], transform=ax.transAxes)

            elif ci == 2:
                ax.text(xc, yc, row['pct'], ha='center', va='center', fontsize=13, fontweight='bold', color=PALETTE["text_primary"], transform=ax.transAxes)

            elif ci == 3:
                if row['velo'] != '--' and not row['is_all']:
                    try:
                        vv = float(row['velo'])
                        bc = _grad_color(vv, velo_range[0], velo_range[1], GRAD_VELO_LO, GRAD_VELO_HI)
                        tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                        ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w, pill_h, boxstyle="round,pad=0.006", lw=0, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                        ax.text(xc, yc, row['velo'], ha='center', va='center', fontsize=13, fontweight='black', color=tc, transform=ax.transAxes, zorder=2)
                    except ValueError: ax.text(xc, yc, row['velo'], ha='center', va='center', fontsize=13, fontweight='black', color=PALETTE["accent_orange"], transform=ax.transAxes)
                else: ax.text(xc, yc, row['velo'], ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)

            elif ci == 4: ax.text(xc, yc, row['spin'], ha='center', va='center', fontsize=12, color=PALETTE["text_secondary"] if row['spin'] != '--' else PALETTE["text_lo"], transform=ax.transAxes)
            elif ci == 5: ax.text(xc, yc, row['ext'], ha='center', va='center', fontsize=12, color=PALETTE["text_secondary"] if row['ext'] != '--' else PALETTE["text_lo"], transform=ax.transAxes)

            elif ci == 6:
                val = row['hb']
                ax.text(xc, yc, val if val != '--' else "--", ha='center', va='center', fontsize=12,
                        color=PALETTE["text_secondary"] if val != '--' and not row['is_all'] else PALETTE["text_lo"],
                        transform=ax.transAxes)

            elif ci == 7:
                val = row['ivb']
                ax.text(xc, yc, val if val != '--' else "--", ha='center', va='center', fontsize=12,
                        color=PALETTE["text_secondary"] if val != '--' and not row['is_all'] else PALETTE["text_lo"],
                        transform=ax.transAxes)

            elif ci == 8:
                cv = row['raw_chase']
                vs = f"{cv:.0%}" if _safe(cv) else "--"
                if vs != "--" and not row['is_all']:
                    bc = _grad_color(cv, chase_range[0], chase_range[1], GRAD_METRIC_LO, GRAD_METRIC_HI)
                    tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                    ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                    ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='bold', color=tc, transform=ax.transAxes, zorder=2)
                else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)

            elif ci == 9:
                wv = row['raw_whiff']
                vs = str(row.get('whiffs', 0)) if _safe(wv) else "--"
                if vs != "--" and not row['is_all']:
                    bc = _grad_color(wv, whiff_range[0], whiff_range[1], GRAD_METRIC_LO, GRAD_METRIC_HI)
                    tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                    ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                    ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='bold', color=tc, transform=ax.transAxes, zorder=2)
                else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)

            elif ci == 10:
                sv = row['raw_str']
                vs = f"{sv:.0%}" if _safe(sv) else "--"
                if vs != "--" and not row['is_all']:
                    bc = _grad_color(sv, str_range[0], str_range[1], GRAD_METRIC_LO, GRAD_METRIC_HI)
                    tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                    ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                    ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='bold', color=tc, transform=ax.transAxes, zorder=2)
                else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)

            elif ci == 11:
                if _has_bs75:
                    fv = row['raw_fast_swing']
                    vs = f"{fv:.0%}" if _safe(fv) else "--"
                    if vs != "--" and not row['is_all']:
                        bc = _grad_color(fv, fast_swing_range[0], fast_swing_range[1], GRAD_METRIC_LO, GRAD_METRIC_HI)
                        tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                        ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                        ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='bold', color=tc, transform=ax.transAxes, zorder=2)
                    else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)
                else:
                    # Zone%: share of pitches in the strike zone (always available)
                    zv = row.get('raw_zone', np.nan)
                    vs = f"{zv:.0%}" if _safe(zv) else "--"
                    if vs != "--" and not row['is_all']:
                        zone_range = _range('raw_zone', league_key="zone_per_pitch")
                        bc = _grad_color(zv, zone_range[0], zone_range[1], GRAD_METRIC_LO, GRAD_METRIC_HI)
                        tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                        ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                        ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='bold', color=tc, transform=ax.transAxes, zorder=2)
                    else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)

            elif ci == 12:
                if _has_xwoba:
                    xv = row['raw_xwoba']
                    vs = f"{xv:.3f}" if _safe(xv) else "--"
                    if vs != "--" and not row['is_all']:
                        bc = _grad_color(xv, xw_range[0], xw_range[1], GRAD_XWOBA_LO, GRAD_XWOBA_HI)
                        tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                        ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w * 1.05, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                        ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='black', color=tc, transform=ax.transAxes, zorder=2)
                    else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)
                else:
                    hv = row.get('raw_hh_pct', np.nan)
                    vs = f"{hv:.0%}" if _safe(hv) else "--"
                    if vs != "--" and not row['is_all']:
                        hh_range = _range('raw_hh_pct')
                        bc = _grad_color(hv, hh_range[0], hh_range[1], GRAD_XWOBA_LO, GRAD_XWOBA_HI)
                        tc = '#111111' if _lum(bc) > 0.50 else '#FFFFFF'
                        ax.add_patch(FancyBboxPatch((pill_x, pill_y), pill_w * 1.05, pill_h, boxstyle="round,pad=0.006", lw=pill_lw, edgecolor=pill_ec, facecolor=bc, alpha=0.95, transform=ax.transAxes, zorder=1))
                        ax.text(xc, yc, vs, ha='center', va='center', fontsize=12, fontweight='black', color=tc, transform=ax.transAxes, zorder=2)
                    else: ax.text(xc, yc, vs, ha='center', va='center', fontsize=13, color=PALETTE["text_lo"], transform=ax.transAxes)

            xp += w

    ax.set_ylim(0, 1.0)

    xp = 0.005
    for w in WIDTHS[:-1]:
        xp += w
        # Adjust the separator line bottom to roughly the bottom of the table data
        line_bottom = max(0.02, y_top - len(rows) * ROW_H)
        ax.plot([xp, xp], [line_bottom, SEP_Y], color=PALETTE["border"], lw=0.6, alpha=0.60, transform=ax.transAxes)

# -----------------------------------------------------------------
# PANEL 5 - PITCH LEGEND (full-width strip between panels and table)
# -----------------------------------------------------------------
def plot_legend(ax, arsenal):
    """Horizontal row of color-coded pitch type pills — one per pitch type."""
    _clean(ax, PALETTE["card_bg"])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

    items = [(row['pitch_type'], row['colour'], row['name'])
             for _, row in arsenal.iterrows()]
    n = len(items)

    # Horizontal margins + gap between pills for breathing room
    H_MARGIN = 0.06          # left & right edge padding
    GAP      = 0.030         # space between each pill
    PAD      = 0.006         # patch padding
    slot_w   = (1.0 - 2 * H_MARGIN) / n
    pill_w   = slot_w - GAP - (2 * PAD)
    pill_h   = 0.56 - (2 * PAD)

    for i, (pt, col, nm) in enumerate(items):
        pill_x = H_MARGIN + i * slot_w + GAP / 2 + PAD
        pill_y = 0.22 + PAD
        ax.add_patch(FancyBboxPatch(
            (pill_x, pill_y), pill_w, pill_h,
            boxstyle=f"round,pad={PAD}", lw=0,
            facecolor=col, transform=ax.transAxes, zorder=2))
        tc = '#111111' if _lum(col) > 0.50 else '#FFFFFF'
        ax.text(pill_x + pill_w * 0.18, 0.50, pt,
                ha='center', va='center', transform=ax.transAxes,
                color=tc, fontsize=10, fontweight='black', zorder=3)
        ax.text(pill_x + pill_w * 0.60, 0.50, nm,
                ha='center', va='center', transform=ax.transAxes,
                color=tc, fontsize=9.5, fontweight='bold', zorder=3)

# -----------------------------------------------------------------
def plot_footer(ax, card_flags=None):
    if card_flags is None:
        card_flags = {}
    _clean(ax, PALETTE["card_bg"])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.axhline(0.97, color=PALETTE["border"], linewidth=1.0)
    ax.text(0.01, 0.50, "@Mallitalytics", color=PALETTE["accent_orange"], fontsize=12, fontweight='black', va='center', transform=ax.transAxes)
    ax.text(0.99, 0.50, "Data: MLB \u00b7 Statcast", color=PALETTE["text_secondary"], fontsize=11, fontweight='bold', ha='right', va='center', transform=ax.transAxes)

    # Build notes only for stats actually shown on this card
    notes = []
    if card_flags.get('has_xwoba', True):
        notes.append("* xwOBA: quality of contact allowed \u2014 lower is better for pitcher")
    else:
        notes.append("* HH%: hard-hit balls in play (EV \u2265 95 mph) as share of BIP \u2014 lower is better")
    notes.append("* Hard contact: EV \u2265 95 mph or xwOBA \u2265 0.350")
    notes.append("* Whiffs: pitch-level whiff count; Chase% and whiff shading use swings; Str% & Zone% use pitches")
    if card_flags.get('has_bs75', True):
        notes.append("* BS75+%: swings with bat speed \u2265 75 mph \u00f7 swings (Statcast)")

    kw = dict(color=PALETTE["text_secondary"], fontsize=9.0, ha='center', va='center', transform=ax.transAxes)
    n = len(notes)
    if n == 2:
        ys = [0.70, 0.30]
    elif n == 4:
        ys = [0.84, 0.60, 0.38, 0.16]
    else:
        ys = [0.78, 0.50, 0.22][:n]
    for note, y in zip(notes, ys):
        ax.text(0.5, y, note, **kw)

# -----------------------------------------------------------------
def _json_scalar_float(x, ndigits: int = 4):
    """JSON-safe float (None if NaN)."""
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except TypeError:
        pass
    try:
        if hasattr(x, "item"):
            x = x.item()
        v = float(x)
        if math.isnan(v):
            return None
        return round(v, ndigits)
    except (TypeError, ValueError):
        return None


def compute_pitch_tendencies_by_situation(df: pd.DataFrame, arsenal: pd.DataFrame) -> list[dict]:
    """
    Count-state pitch mix aligned with the card's PITCH TENDENCIES BY SITUATION panel.
    Serialized on the card snapshot for queue redraft / X copy.
    """
    if arsenal.empty or "balls" not in df.columns or "strikes" not in df.columns or "pitch_type" not in df.columns:
        return []
    key_from_label = {
        "FIRST PITCH": "first_pitch",
        "PITCHER AHEAD": "pitcher_ahead",
        "TWO-STRIKE": "two_strike",
        "EVEN": "even",
        "HITTER AHEAD": "hitter_ahead",
        "FULL COUNT": "full_count",
        "OTHER": "other_count",
    }
    out: list[dict] = []
    for sit_label, _count_display, mask in _pitch_tendency_specs(df):
        sit_df = df[mask]
        n_total = len(sit_df)
        sid = key_from_label.get(sit_label, sit_label.lower().replace(" ", "_"))
        row: dict = {
            "situation_key": sid,
            "situation_label": sit_label,
            "n_pitches": int(n_total),
        }
        if n_total == 0:
            row["dominant_pitch_type"] = None
            row["dominant_share"] = None
            row["top_mix"] = []
            out.append(row)
            continue
        pitch_counts = sit_df["pitch_type"].value_counts()
        dominant_pt, dominant_pct = None, 0.0
        top_mix: list[dict] = []
        for _, arow in arsenal.iterrows():
            pt = arow["pitch_type"]
            cnt = int(pitch_counts.get(pt, 0))
            if cnt == 0:
                continue
            pct = cnt / n_total
            top_mix.append({"pitch_type": str(pt), "share": _json_scalar_float(pct, 4)})
            if pct > dominant_pct:
                dominant_pt, dominant_pct = str(pt), pct
        top_mix.sort(key=lambda z: -(z["share"] or 0))
        row["dominant_pitch_type"] = dominant_pt
        row["dominant_share"] = _json_scalar_float(dominant_pct, 4)
        row["top_mix"] = top_mix[:3]
        out.append(row)
    return out


def _outs_from_ip_str(ip) -> int:
    """Convert IP string like ``8.1`` to total outs (8*3+1=25)."""
    if ip is None:
        return 0
    s = str(ip).strip()
    if not s:
        return 0
    parts = s.split(".", 1)
    try:
        inn = int(parts[0])
    except ValueError:
        return 0
    if len(parts) == 1:
        return inn * 3
    frac = (parts[1].strip()[:1] or "0")
    try:
        o = int(frac)
    except ValueError:
        o = 0
    o = min(max(o, 0), 2)
    return inn * 3 + o


def _derive_notable_pitcher_events(df: pd.DataFrame, box: dict) -> list[dict]:
    """High-salience outing beats (no-hit bids, one-hit gems) from pitch log + box."""
    out: list[dict] = []
    if df is None or df.empty:
        return out
    needed = ("inning", "at_bat_number", "pitch_number", "events")
    if not all(c in df.columns for c in needed):
        return out
    t = df.sort_values(["inning", "at_bat_number", "pitch_number"])
    hit_ev = {"single", "double", "triple", "home_run"}
    # One row per PA: any pitch in the PA can carry the hit outcome (last pitch is usual, but align with compute_box_score
    # by preferring last pitch, then scanning the PA if the last row is blank / non-hit).
    hit_innings: list[int] = []
    for (_, _), sub in t.groupby(["inning", "at_bat_number"], dropna=False):
        sub = sub.sort_values("pitch_number")
        evs = (
            sub["events"]
            .astype(str)
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace("-", "_", regex=False)
            .replace("nan", pd.NA)
        )
        row_hit = evs.isin(list(hit_ev))
        if not row_hit.any():
            continue
        try:
            inn_i = int(sub["inning"].iloc[0])
        except (TypeError, ValueError):
            continue
        hit_innings.append(inn_i)

    outs = _outs_from_ip_str(box.get("ip"))
    h_off = int(box.get("h") or 0)
    first_hit = min(hit_innings) if hit_innings else None
    hits_through_8 = sum(1 for inn in hit_innings if inn <= 8)
    late_only_hits = len(hit_innings) > 0 and hits_through_8 == 0 and min(hit_innings) >= 9

    if late_only_hits and outs >= 24:
        out.append({
            "type": "no_hitter_through_8_first_hit_late",
            "priority": 1,
            "label": "Carried a no-hit bid through 8; all hits allowed came in the 9th or later",
            "first_hit_inning": int(first_hit) if first_hit is not None else None,
            "hits_official": h_off,
        })
    elif h_off == 0 and outs >= 21 and not hit_innings:
        out.append({
            "type": "hitless_in_pitch_log",
            "priority": 1,
            "label": "No hits in pitch-by-pitch log (verify official box)",
        })
    elif (
        h_off == 1
        and outs >= 24
        and not any(e.get("type") == "no_hitter_through_8_first_hit_late" for e in out)
        and (first_hit is None or first_hit < 9)
    ):
        out.append({
            "type": "one_hit_deep_outing",
            "priority": 2,
            "label": "One-hit outing over 8 IP",
            "hits": h_off,
            "ip": str(box.get("ip") or ""),
        })
    out.sort(key=lambda x: int(x.get("priority", 99)))
    return out


def _derived_pitcher_tweet_context(
    arsenal: pd.DataFrame,
    box: dict,
    tendencies: list[dict],
    outing_context: dict | None,
    prior_summary: dict | None,
) -> dict:
    """Compact fields so redraft prompts can reason about mix, BS75+, and form without huge JSON."""
    total = int(arsenal["count"].sum()) if not arsenal.empty else 0
    derived: dict = {"total_pitches": total}
    if total <= 0:
        return derived

    sorta = arsenal.sort_values("count", ascending=False)
    top2 = sorta.head(2)
    top3 = sorta.head(3)
    c2 = int(top2["count"].sum())
    c3 = int(top3["count"].sum())
    derived["top2_usage_share"] = _json_scalar_float(c2 / total, 4)
    derived["top3_usage_share"] = _json_scalar_float(c3 / total, 4)
    p2 = "+".join(str(x) for x in top2["pitch_type"].tolist())
    pct2 = _json_scalar_float(100.0 * c2 / total, 1)
    derived["primary_pitch_line"] = f"{p2} = {pct2}% of pitches" if pct2 is not None else p2

    fs = box.get("fast_swing_pct")
    if fs is not None:
        try:
            if not pd.isna(fs):
                derived["game_fast_swing_pct"] = _json_scalar_float(fs, 4)
        except TypeError:
            derived["game_fast_swing_pct"] = _json_scalar_float(fs, 4)

    hints: list[str] = []
    vs = (outing_context or {}).get("vs_last_start") or {}
    if vs:
        erd = vs.get("er_delta")
        if erd is not None:
            if int(erd) < 0:
                hints.append("fewer_ER_than_last_start")
            elif int(erd) > 0:
                hints.append("more_ER_than_last_start")
        csw_d = vs.get("csw_pct_delta")
        if csw_d is not None:
            try:
                if float(csw_d) > 3:
                    hints.append("CSW_up_vs_last_start")
                elif float(csw_d) < -3:
                    hints.append("CSW_down_vs_last_start")
            except (TypeError, ValueError):
                pass
        vm = vs.get("avg_velo_delta_mph")
        if vm is not None:
            try:
                if float(vm) > 0.5:
                    hints.append("velo_up_vs_last_start")
                elif float(vm) < -0.5:
                    hints.append("velo_down_vs_last_start")
            except (TypeError, ValueError):
                pass

    if prior_summary and prior_summary.get("er_mean") is not None and box.get("er") is not None:
        try:
            em = float(prior_summary["er_mean"])
            be = float(box["er"])
            if be < em - 0.05:
                hints.append("ER_below_recent_prior_mean")
            elif be > em + 0.05:
                hints.append("ER_above_recent_prior_mean")
        except (TypeError, ValueError):
            pass

    derived["form_hints"] = hints[:5]

    if "xwoba" in sorta.columns and sorta["xwoba"].notna().any():
        sub = sorta[sorta["xwoba"].notna()].copy()
        if "bip" in sub.columns:
            sub = sub[
                (sub["count"] >= MIN_PITCHES_FOR_XWOBA_BEAT)
                & (sub["bip"] >= MIN_BIP_FOR_XWOBA_BEAT)
            ]
        else:
            sub = sub[sub["count"] >= MIN_PITCHES_XWOBA_FALLBACK_NO_BIP]
        if len(sub) >= 1:
            imin = sub["xwoba"].idxmin()
            imax = sub["xwoba"].idxmax()
            best = sub.loc[imin]
            worst = sub.loc[imax]
            n_bip_best = (
                int(best["bip"]) if "bip" in sub.columns and pd.notna(best["bip"]) else None
            )
            n_bip_worst = (
                int(worst["bip"]) if "bip" in sub.columns and pd.notna(worst["bip"]) else None
            )
            derived["best_xwoba_pitch"] = {
                "pitch_type": str(best["pitch_type"]),
                "xwoba": _json_scalar_float(best["xwoba"], 3),
                "n_pitches": int(best["count"]),
                "n_bip": n_bip_best,
            }
            if imin != imax:
                derived["worst_xwoba_pitch"] = {
                    "pitch_type": str(worst["pitch_type"]),
                    "xwoba": _json_scalar_float(worst["xwoba"], 3),
                    "n_pitches": int(worst["count"]),
                    "n_bip": n_bip_worst,
                }

    rich_sit = [
        t for t in tendencies
        if t.get("n_pitches", 0) >= 6
        and t.get("dominant_pitch_type")
        and t.get("dominant_share") is not None
    ]
    if rich_sit:
        def _sit_score(t: dict) -> float:
            try:
                share = float(t.get("dominant_share") or 0.0)
                n_pitches = int(t.get("n_pitches") or 0)
            except (TypeError, ValueError):
                return 0.0
            return share * math.log1p(max(n_pitches, 0))

        pick = max(rich_sit, key=_sit_score)
        try:
            dominant_share = float(pick.get("dominant_share") or 0.0)
        except (TypeError, ValueError):
            dominant_share = 0.0
        if dominant_share >= 0.50:
            derived["tendency_highlight"] = {
                "situation_key": pick.get("situation_key"),
                "n_pitches": pick.get("n_pitches"),
                "dominant_pitch_type": pick.get("dominant_pitch_type"),
                "dominant_share": pick.get("dominant_share"),
            }

    return derived


def _box_json(box: dict) -> dict:
    out = {}
    for k, v in box.items():
        if k == "ip":
            out[k] = v
        elif isinstance(v, (bool, np.bool_)):
            out[k] = bool(v)
        elif isinstance(v, (int, np.integer)):
            out[k] = int(v)
        elif v is None:
            out[k] = None
        else:
            out[k] = _json_scalar_float(v, 4)
    return out


def _arsenal_rows_json(arsenal: pd.DataFrame) -> list[dict]:
    cols = [
        "pitch_type",
        "name",
        "count",
        "whiff",
        "usage_pct",
        "velo",
        "whiff_pct",
        "zone_pct",
        "chase_pct",
        "str_pct",
        "xwoba",
        "fast_swing_pct",
        "gb_pct",
        "spin",
        "rv100",
    ]
    rows = []
    for _, row in arsenal.iterrows():
        d: dict = {}
        for c in cols:
            if c not in arsenal.columns:
                continue
            val = row[c]
            if c in ("pitch_type", "name"):
                d[c] = str(val) if val is not None and not pd.isna(val) else None
            elif c == "count":
                d[c] = int(val) if not pd.isna(val) else None
            elif c == "whiff":
                d["whiffs"] = int(val) if not pd.isna(val) else None
            elif c in ("usage_pct", "whiff_pct", "zone_pct", "chase_pct", "str_pct", "gb_pct", "fast_swing_pct"):
                d[c] = _json_scalar_float(val, 4)
            else:
                d[c] = _json_scalar_float(val, 3)
        rows.append(d)
    return rows


def _build_pitcher_card_snapshot(
    *,
    pitcher_id: int,
    parquet_path: str,
    output_path: str,
    game_date: str,
    opp_team: str,
    bio: dict,
    box: dict,
    arsenal: pd.DataFrame,
    recent_outings: list[dict] | None = None,
    outing_context: dict | None = None,
    recent_prior_summary: dict | None = None,
    pitch_tendencies_by_situation: list[dict] | None = None,
    pitcher_tweet_context: dict | None = None,
    season_pitching_stats: dict | None = None,
    notable_game_events: list[dict] | None = None,
    source_metadata: dict | None = None,
) -> dict:
    path_name = Path(parquet_path).name
    game_pk = None
    m = PARQUET_PATTERN.search(path_name)
    if m:
        try:
            game_pk = int(m.group(1))
        except ValueError:
            pass
    ro = list(recent_outings or [])
    snap: dict = {
        "schema_version": 2,
        "card_type": "pitcher_card",
        "pitcher_id": pitcher_id,
        "player_name": bio.get("name"),
        "team": bio.get("team"),
        "throws": bio.get("hand"),
        "game_date": game_date,
        "opponent": opp_team,
        "game_pk": game_pk,
        "source_parquet": path_name,
        "source_metadata": dict(source_metadata) if source_metadata else {},
        "output_image": Path(output_path).name,
        "box": _box_json(box),
        "header_summary": {
            "total_pitches": box.get("total_pitches"),
            "zone_pct": _json_scalar_float(box.get("zone_pct"), 2),
            "whiffs": box.get("whiffs"),
            "csw_pct": _json_scalar_float(box.get("csw_pct"), 2),
            "gb_pct": _json_scalar_float(box.get("gb_pct"), 3) if box.get("gb_pct") is not None else None,
        },
        "arsenal": _arsenal_rows_json(arsenal),
        "recent_outings": ro,
        "pitch_tendencies_by_situation": list(pitch_tendencies_by_situation or []),
    }
    if outing_context:
        snap["outing_context"] = outing_context
    if recent_prior_summary:
        snap["recent_prior_summary"] = recent_prior_summary
    if pitcher_tweet_context:
        snap["pitcher_tweet_context"] = pitcher_tweet_context
    if season_pitching_stats:
        snap["season_pitching_stats"] = season_pitching_stats
    if notable_game_events:
        snap["notable_game_events"] = list(notable_game_events)
    return snap


def render_card(parquet_path, pitcher_id, output_path):
    mpl.rcParams['figure.dpi']  = 200
    mpl.rcParams['font.family'] = 'DejaVu Sans'

    df_raw  = load_game(parquet_path, pitcher_id)
    df      = process_pitches(df_raw)
    box     = compute_box_score(df)
    # Override with official box score when available (IP/line often wrong from pitch-level events when reliever gets the final out)
    path_name = Path(parquet_path).name
    game_pk_match = PARQUET_PATTERN.search(path_name)
    game_pk: int | None = None
    box_data: dict | None = None
    if game_pk_match:
        try:
            game_pk = int(game_pk_match.group(1))
            box_data = fetch_boxscore_data(game_pk)
            official = (
                fetch_box_score_line(game_pk, pitcher_id, box_data=box_data)
                if box_data
                else fetch_box_score_line(game_pk, pitcher_id)
            )
            if official:
                box["ip"] = official["ip"]
                box["h"] = official["h"]
                box["er"] = official.get("er", box.get("er", 0))
                box["k"] = official["k"]
                box["bb"] = official["bb"]
                box["hr"] = official["hr"]
        except Exception:
            pass
    arsenal = group_arsenal(df)

    if arsenal.empty: raise ValueError(f"No pitch types with >= {MIN_PITCHES} pitches found.")

    gd = df["game_date"].iloc[0]
    game_date = gd.strftime("%Y-%m-%d") if hasattr(gd, "strftime") else str(gd)[:10]
    # Use game year for league benchmarks (e.g., 2024 regular season table)
    try:
        season_year = int(str(game_date)[:4])
    except Exception:
        season_year = None
    benchmarks = load_pitch_metric_benchmarks(season_year) if season_year else None
    hand = df["p_throws"].iloc[0] if "p_throws" in df.columns else "R"
    home_team = str(df["home_team"].iloc[0]) if "home_team" in df.columns else "--"
    away_team = str(df["away_team"].iloc[0]) if "away_team" in df.columns else "--"
    if game_pk is not None:
        bh, ba = fetch_boxscore_team_abbrevs(game_pk, box_data=box_data)
        if bh and ba and bh != ba:
            hu = home_team.strip().upper() if home_team and home_team != "--" else ""
            au = away_team.strip().upper() if away_team and away_team != "--" else ""
            if (not hu or not au) or (hu == au):
                home_team, away_team = bh, ba

    bio = fetch_player_bio(pitcher_id)
    opp_team = _infer_opponent_team(
        df, str(bio.get("team") or ""), game_pk=game_pk, box_data=box_data
    )
    headshot = fetch_headshot(pitcher_id)
    logo_path_arg = getattr(_args, 'logo_path', None)
    if logo_path_arg and Path(logo_path_arg).is_file():
        try:
            logo = Image.open(logo_path_arg).convert("RGBA")
        except Exception:
            logo = fetch_team_logo(bio["team"])
    else:
        logo = fetch_team_logo(bio["team"])

    n_rows = len(arsenal) + 1
    fig_h  = 7.8 + n_rows * 0.95

    fig = plt.figure(figsize=(16, fig_h))
    fig.patch.set_facecolor(PALETTE["card_bg"])

    # 6-row layout:
    #  0 Header | 1 sep-top | 2 Panels | 3 Legend strip | 4 sep-bot | 5 Table
    gs = gridspec.GridSpec(
        6, 3, figure=fig,
        height_ratios=[1.4, 0.05, 3.5, 0.50, 0.05, n_rows * 0.55],
        width_ratios=[1, 1, 1], hspace=0.10, wspace=0.06,
        left=0.02, right=0.98, top=0.98, bottom=0.07,
    )

    ax_hdr  = fig.add_subplot(gs[0, :])
    ax_st   = fig.add_subplot(gs[1, :]);  _clean(ax_st,  PALETTE["card_bg"])
    ax_dmg  = fig.add_subplot(gs[2, 0])
    ax_mov  = fig.add_subplot(gs[2, 1])
    ax_frq  = fig.add_subplot(gs[2, 2])
    ax_leg  = fig.add_subplot(gs[3, :])
    ax_sb   = fig.add_subplot(gs[4, :]);  _clean(ax_sb,  PALETTE["card_bg"])
    ax_tbl  = fig.add_subplot(gs[5, :])

    plot_header(ax_hdr, bio, box, game_date, opp_team, headshot, logo)
    plot_damage_heatmap(ax_dmg, arsenal, df)
    plot_movement(ax_mov, arsenal, df, hand)
    plot_pitch_tendencies(ax_frq, arsenal, df)
    plot_legend(ax_leg, arsenal)

    # Detect available data flavours once and pass to both table and footer
    has_xwoba = 'xwoba' in arsenal.columns and arsenal['xwoba'].notna().any()
    has_bs75  = ('fast_swing_pct' in arsenal.columns and arsenal['fast_swing_pct'].notna().any())
    card_flags = dict(has_xwoba=has_xwoba, has_bs75=has_bs75)

    plot_arsenal_table(ax_tbl, arsenal, hand, box, benchmarks, card_flags=card_flags)

    ax_ftr = fig.add_axes([0.02, 0.004, 0.96, 0.055])
    plot_footer(ax_ftr, card_flags=card_flags)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    fig.savefig(output_path, dpi=200, bbox_inches="tight", facecolor=PALETTE["card_bg"], edgecolor="none")

    wh_root = _warehouse_mlb_root_from_parquet(str(parquet_path))
    recent_meta: list[dict] = []
    if wh_root is not None and _safe_is_dir(wh_root):
        recent_meta = _collect_recent_pitcher_outings(
            warehouse_root=wh_root,
            pitcher_id=int(pitcher_id),
            card_game_date=game_date,
            current_game_pk=game_pk,
            bio=bio,
        )
    outing_ctx = _outing_context_vs_last(box, df, recent_meta)
    prior_summary = _recent_prior_summary(recent_meta)
    tendency_rows = compute_pitch_tendencies_by_situation(df, arsenal)
    ptweet_ctx = _derived_pitcher_tweet_context(arsenal, box, tendency_rows, outing_ctx, prior_summary)
    notable_events = _derive_notable_pitcher_events(df, box)
    if notable_events:
        ptweet_ctx = {**ptweet_ctx, "notable_game_events": notable_events}

    season_stats = None
    try:
        season_y = int(str(game_date).strip()[:4])
    except (TypeError, ValueError):
        season_y = date.today().year
    if season_y >= 2000:
        season_stats = fetch_season_pitching_stats(int(pitcher_id), season_y)

    src_meta = build_pitcher_source_metadata(
        game_pk=game_pk,
        pitcher_id=int(pitcher_id),
        game_date=game_date,
        box_data=box_data,
    )

    snapshot = _build_pitcher_card_snapshot(
        pitcher_id=int(pitcher_id),
        parquet_path=str(parquet_path),
        output_path=str(output_path),
        game_date=game_date,
        opp_team=str(opp_team),
        bio=bio,
        box=box,
        arsenal=arsenal,
        recent_outings=recent_meta,
        outing_context=outing_ctx,
        recent_prior_summary=prior_summary,
        pitch_tendencies_by_situation=tendency_rows,
        pitcher_tweet_context=ptweet_ctx,
        season_pitching_stats=season_stats,
        notable_game_events=notable_events,
        source_metadata=src_meta,
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
    return output_path

def _log(msg: str, flush: bool = True) -> None:
    print(msg, flush=flush)


if __name__ == "__main__":
    if _args.parquet and _args.pitcher is not None:
        pq_path = Path(_args.parquet)
        _log("Mallitalytics Daily Card (parquet mode)")
        _log("")
        if not pq_path.exists():
            print(f"ERROR: Parquet not found: {pq_path}", file=sys.stderr, flush=True)
            print("Use the full path (e.g. .../WBC/data/exhibition/processed/pitches_enriched/game_836149_20260303_pitches_enriched.parquet)", file=sys.stderr, flush=True)
            sys.exit(1)
        out_dir = _resolved_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        bio = fetch_player_bio(_args.pitcher)
        safe_nm = bio["name"].lower().replace(", ", "_").replace(",", "_").replace(" ", "_").replace(".", "").replace("'", "")
        mode_sfx = "" if LIGHT_MODE else "_dark"
        out_path = out_dir / f"pitcher_card_{safe_nm}_wbc{mode_sfx}.png"
        _log(f"  Parquet:  {pq_path}")
        _log(f"  Pitcher: {_args.pitcher} ({bio['name']})")
        _log("  Generating card (may take 20–40 s)...")
        try:
            render_card(str(pq_path), _args.pitcher, str(out_path))
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr, flush=True)
            raise
        _log(f"  -> Saved: {out_path}")
        _log("")
    elif _args.pitchers:
        pitcher_ids = [int(x.strip()) for x in _args.pitchers.split(",") if x.strip()]
        if _args.date.lower() == "yesterday":
            target_date = (datetime.now() - timedelta(days=1)).date()
        else:
            try:
                target_date = datetime.strptime(_args.date.strip()[:10], "%Y-%m-%d").date()
            except ValueError:
                sys.exit(f"Invalid --date: use yesterday or YYYY-MM-DD")
        date_str = target_date.strftime("%Y%m%d")
        warehouse_root = _resolved_warehouse_root_from_env()
        # Bounded scan only — full rglob(warehouse) can take minutes on large Drive mirrors.
        _stages = ("regular_season", "postseason", "spring_training", "playoffs", "all_star")
        y0 = target_date.year
        parquets_by_date: list[Path] = []
        for yr in (y0, y0 - 1):
            for stage in _stages:
                enriched = warehouse_root / str(yr) / stage / "pitches_enriched"
                if not _safe_is_dir(enriched):
                    continue
                try:
                    parquets_by_date.extend(
                        sorted(enriched.glob(f"game_*_{date_str}_pitches_enriched.parquet"))
                    )
                except (OSError, TimeoutError):
                    continue
        parquets_by_date = sorted(set(parquets_by_date))
        if not parquets_by_date:
            print(
                "\nNo pitches_enriched parquets for this game date on disk.\n"
                f"  Date:     {target_date}  (glob: game_*_{date_str}_pitches_enriched.parquet)\n"
                f"  Looked in: {warehouse_root}/{{year}}/{{stage}}/pitches_enriched/\n\n"
                "The hub does NOT stream from Google Drive. Drive is the source of truth; the API and\n"
                "card scripts read whatever path MLB_WAREHOUSE_DIR points to (default: repo data/warehouse/mlb).\n\n"
                "Fix:\n"
                "  1) Prefer a local mirror: ./scripts/pull_mlbops_from_drive.sh (or Hub Settings -> Drive sync),\n"
                "     then remove MLB_WAREHOUSE_DIR or set it to the repo path …/data/warehouse/mlb.\n"
                "  2) If MLB_WAREHOUSE_DIR points at Google Drive for Desktop, open MLB/warehouse/mlb in Finder first;\n"
                "     File Stream often times out until the folder is hydrated — local mirror is more reliable.\n\n"
                "Pitcher cards need Statcast pitches_enriched parquets for that game — the MLB game log alone is not enough.\n",
                file=sys.stderr,
                flush=True,
            )
            sys.exit(1)
        out_dir = _resolved_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        mode_sfx = "" if LIGHT_MODE else "_dark"
        generated = 0
        skip_msgs: list[str] = []
        for pid in pitcher_ids:
            best_path, best_count = None, 0
            for path in parquets_by_date:
                try:
                    df = pd.read_parquet(path, columns=["pitcher"])
                    n = int((df["pitcher"] == pid).sum())
                    if n >= MIN_PITCHES and n > best_count:
                        best_path, best_count = path, n
                except Exception:
                    continue
            if best_path is None:
                msg = (
                    f"Pitcher {pid}: no Statcast game with ≥{MIN_PITCHES} pitches on {target_date} "
                    f"in the local warehouse (checked {len(parquets_by_date)} parquet(s) for that date)."
                )
                skip_msgs.append(msg)
                print(f"  {msg}", file=sys.stderr, flush=True)
                continue
            bio = fetch_player_bio(pid)
            safe_nm = bio["name"].lower().replace(", ", "_").replace(",", "_").replace(" ", "_").replace(".", "").replace("'", "")
            _osuf = getattr(_args, "output_suffix", None)
            _mid = f"_{_osuf}" if (_osuf and str(_osuf).strip()) else ""
            out_path = out_dir / f"pitcher_card_{safe_nm}_{target_date.isoformat()}{_mid}{mode_sfx}.png"
            print(f"  Pitcher {pid} ({bio['name']}): {best_path.name} \u2192 {out_path.name}")
            render_card(str(best_path), pid, str(out_path))
            print(f"  \u2192 Saved: {out_path}\n")
            generated += 1
        if generated == 0:
            print(
                "\nNo pitcher card(s) were written. Common causes:\n"
                "  • pitches_enriched parquets exist for the date but this pitcher has fewer than "
                f"{MIN_PITCHES} tracked pitches in each file (injury / very short outing).\n"
                "  • Warehouse mirror is stale — sync from Drive or run ingestion for that date.\n",
                file=sys.stderr,
                flush=True,
            )
            for m in skip_msgs:
                print(f"  - {m}", file=sys.stderr, flush=True)
            sys.exit(1)
    elif _args.random:
        import random as _random

        warehouse_root = Path(__file__).parent.parent / "data" / "warehouse"
        all_parquets   = sorted(warehouse_root.rglob("*pitches_enriched.parquet"))
        if not all_parquets:
            sys.exit("No pitches_enriched parquet files found under data/warehouse/")

        # Sample up to 50 candidates and pick the one whose top pitcher has the
        # most pitches (excludes files where every pitcher threw < MIN_PITCHES).
        candidates = _random.sample(all_parquets, min(50, len(all_parquets)))
        best = None
        for path in candidates:
            try:
                _tmp = pd.read_parquet(path, columns=["pitcher", "player_name", "game_date"])
                vc   = _tmp["pitcher"].value_counts()
                if vc.iloc[0] >= 50:          # must have thrown at least 50 pitches
                    best = (path, _tmp, int(vc.index[0]))
                    break
            except Exception:
                continue

        if best is None:
            sys.exit("Could not find a suitable game in the sampled candidates — try again.")

        chosen_path, df_pick, top_id = best
        # Note: player_name in Statcast parquets is the BATTER's name.
        # Use the MLB API to get the actual pitcher's name for the filename.
        pitcher_bio = fetch_player_bio(top_id)
        player_nm   = pitcher_bio['name']
        game_date   = str(df_pick["game_date"].iloc[0])[:10] if "game_date" in df_pick.columns else "unknown"
        n_pitches   = int(df_pick[df_pick["pitcher"] == top_id].shape[0])

        out_dir = _resolved_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_nm  = player_nm.lower().replace(", ", "_").replace(",", "_").replace(" ", "_").replace(".", "").replace("'", "")
        mode_sfx = "" if LIGHT_MODE else "_dark"
        out_path = out_dir / f"pitcher_card_{safe_nm}_{game_date}{mode_sfx}.png"

        print(f"\n  Game file : {chosen_path.name}")
        print(f"  Pitcher   : {player_nm}  (ID {top_id})  —  {n_pitches} pitches")
        print(f"  Output    : {out_path}\n")
        render_card(str(chosen_path), top_id, str(out_path))
        print(f"  -> Saved: {out_path}\n")
    else:
        if not PARQUET_PATH.exists():
            sys.exit(f"Default PARQUET_PATH not found: {PARQUET_PATH}")
        render_card(str(PARQUET_PATH), PITCHER_ID, str(OUTPUT_PATH))
        print(f"\n  -> Saved: {OUTPUT_PATH}\n")
