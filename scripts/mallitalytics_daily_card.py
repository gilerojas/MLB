"""
Mallitalytics Daily Pitcher Card

Generates a single-game pitching card (PNG) from a Statcast pitches_enriched parquet:
header (name, bio, box score, Zone%, Whiffs, CSW%, GB%), hard contact heatmap, movement
profile with arm angle, pitch tendencies by count, and an arsenal table (velo, spin,
break, Chase%, Whiff%, Str%, Zone%, BS75+%, xwOBA). Uses a default game and pitcher
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

import re
import sys
import argparse
import json
from datetime import datetime, timedelta
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
_args, _ = _parser.parse_known_args()

import requests
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, Ellipse
from PIL import Image

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


def fetch_box_score_line(game_pk: int, pitcher_id: int) -> dict | None:
    """
    Fetch official box score and return this pitcher's line: ip, h, r, k, bb, hr.
    Use this to override pitch-derived stats when Statcast only has events on the final pitch of each PA
    (so reliever-recorded outs are missing from the pitcher's filtered data). Returns None on failure.
    """
    try:
        r = requests.get(BOXSCORE_URL.format(game_pk=game_pk), headers=REQUEST_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception:
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
        swung = df.loc[df['swing'] & df['bat_speed'].notna(), 'bat_speed']
        if len(swung) >= 1:
            fast_swing_pct = (swung >= FAST_SWING_MPH).mean()

    zone_pct = df['in_zone'].sum() / n * 100 if n else 0
    return dict(
        ip=f"{outs//3}.{outs%3}", pa=df['at_bat_number'].nunique(),
        k=int(k), bb=int(bb), hr=int(hr), h=int(h), er=r,
        n=n, whiffs=int(df['whiff'].sum()),
        zone_pct=zone_pct, csw_pct=csw / n * 100 if n else 0, total_pitches=n,
        gb_pct=gb_pct, fast_swing_pct=fast_swing_pct,
    )

def group_arsenal(df, min_pitches=MIN_PITCHES):
    g = df.groupby('pitch_type').agg(
        count=('pitch_type','count'), velo=('release_speed','mean'), pfx_z=('pfx_z_in','mean'),
        pfx_x=('pfx_x_in','mean'), spin=('release_spin_rate','mean'), extension=('release_extension','mean'),
        rel_x=('release_pos_x','mean'), rel_z=('release_pos_z','mean'), swing=('swing','sum'),
        whiff=('whiff','sum'), in_zone=('in_zone','sum'), out_zone=('out_zone','sum'), chase=('chase','sum'),
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
    # All rates use pitches as denominator for full consistency with the All row
    g['whiff_pct']    = g['whiff']   / g['count']   # whiffs / pitches (SwStr%)
    g['str_pct']      = g['strikes'] / g['count']   # strikes / pitches
    g['zone_pct']     = g['in_zone'] / g['count']   # in-zone / pitches
    g['chase_pct']    = g['chase']   / g['count']   # chases / pitches
    g['rv100']        = -g['delta_re'] / g['count'] * 100
    gb_denom = g['bip'].replace(0, np.nan)
    # GB%: ground balls as a share of balls IN PLAY (not all pitches)
    g['gb_pct']       = g['gb'] / gb_denom
    # Don't show GB% when fewer than 5 balls in play to avoid noisy 0%/100% from tiny samples
    g.loc[g['bip'] < 5, 'gb_pct'] = np.nan
    g['hard_hit_pct'] = (g['hard_hit'] / gb_denom).fillna(0.0).clip(upper=1.0)

    # Fast swing %: share of swings (with tracked bat speed) that were 75+ mph — show whenever we have any tracked swings; "--" only when zero
    if 'bat_speed' in df.columns and df['swing'].any():
        swung = df[df['swing'] & df['bat_speed'].notna()].copy()
        swung['fast_swing'] = swung['bat_speed'] >= FAST_SWING_MPH
        fs = swung.groupby('pitch_type')['fast_swing'].mean()  # no minimum sample; "--" only when no tracked swings
        g['fast_swing_pct'] = g['pitch_type'].map(fs).values
    else:
        g['fast_swing_pct'] = np.nan

    g['name']         = g['pitch_type'].map(DICT_PITCH).fillna(g['pitch_type'])
    g['colour']       = g['pitch_type'].map(DICT_COLOUR).fillna('#9C8975')
    return g.sort_values('count', ascending=False).reset_index(drop=True)

def fetch_player_bio(pitcher_id):
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={pitcher_id}&hydrate=currentTeam"
    try:
        data = requests.get(url, timeout=10).json()['people'][0]
        team_abb, link = "MLB", data.get('currentTeam', {}).get('link', '')
        if link: team_abb = requests.get(f"https://statsapi.mlb.com{link}", timeout=10).json()['teams'][0]['abbreviation']
        return dict(name=data['fullName'], hand=data['pitchHand']['code'], age=data.get('currentAge','--'), height=data.get('height','--'), weight=data.get('weight','--'), team=team_abb)
    except Exception: return dict(name="Unknown Pitcher", hand="R", age="--", height="--", weight="--", team="MLB")

def _neutralize_headshot_background(img, replace_rgb=(255, 255, 255)):
    """Replace green/teal MLB headshot background with a neutral color. img: PIL Image (RGB)."""
    arr = np.array(img)
    if arr.ndim != 3 or arr.shape[2] < 3:
        return img
    r, g, b = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]
    # Green-ish background: green dominant and not too dark
    green_bg = (g > r) & (g > b) & (g > 80) & (np.abs(g.astype(int) - r) + np.abs(g.astype(int) - b) > 40)
    arr[green_bg, 0], arr[green_bg, 1], arr[green_bg, 2] = replace_rgb[0], replace_rgb[1], replace_rgb[2]
    return Image.fromarray(arr)


def fetch_headshot(pitcher_id):
    pid = int(pitcher_id)
    url = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_640,q_auto:best/v1/people/{pid}/headshot/silo/current.png"
    try:
        r = requests.get(url, timeout=10)
        if not r.ok or len(r.content) < 500:
            return None
        img = Image.open(BytesIO(r.content)).convert("RGB")
        # Replace MLB's default green/teal background with neutral (white in light mode, panel in dark)
        replace = (255, 255, 255) if LIGHT_MODE else (0x1F, 0x2E, 0x3D)
        return _neutralize_headshot_background(img, replace_rgb=replace)
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

def plot_pitch_tendencies(ax, arsenal, df):
    _clean(ax); _border(ax)
    SITUATIONS = [("FIRST PITCH", "0-0", [(0, 0)]), ("PITCHER AHEAD", "0-1  \u00b7  1-1", [(0, 1), (1, 1)]), ("TWO-STRIKE", "0-2  \u00b7  1-2  \u00b7  2-2", [(0, 2), (1, 2), (2, 2)]), ("EVEN", "1-0  \u00b7  2-1", [(1, 0), (2, 1)]), ("HITTER AHEAD", "2-0  \u00b7  3-0  \u00b7  3-1", [(2, 0), (3, 0), (3, 1)]), ("FULL COUNT", "3-2", [(3, 2)])]
    ACCENTS = [PALETTE["text_lo"], PALETTE["accent_green"], "#5BC8D5", PALETTE["text_lo"], PALETTE["accent_orange"], "#BE5FA0"]

    LPAD, RPAD, TPAD, BPAD = 0.03, 0.03, 0.04, 0.04   # tight top padding: use full height
    ROW_H = (1 - TPAD - BPAD) / len(SITUATIONS)
    LABEL_W, BAR_W, BADGE_W = 0.32, 0.44, 0.15

    for si, (sit_label, count_str, counts) in enumerate(SITUATIONS):
        mask = pd.Series(False, index=df.index)
        for b, s in counts: mask = mask | ((df['balls'] == b) & (df['strikes'] == s))
        sit_df, n_total = df[mask], len(df[mask])
        y_center = 1 - TPAD - si * ROW_H - ROW_H/2

        bg = PALETTE["table_alt"] if si % 2 == 0 else PALETTE["table_bg"]
        ax.add_patch(FancyBboxPatch((LPAD, y_center - ROW_H/2 + 0.005), 1 - LPAD - RPAD, ROW_H - 0.010, boxstyle="round,pad=0.004", lw=0, facecolor=bg, transform=ax.transAxes, zorder=1))
        ax.add_patch(FancyBboxPatch((LPAD, y_center - ROW_H/2 + 0.005), 0.008, ROW_H - 0.010, boxstyle="square,pad=0", lw=0, facecolor=ACCENTS[si], alpha=0.95, transform=ax.transAxes, zorder=2))

        ax.text(LPAD + 0.025, y_center + ROW_H * 0.14, sit_label,
                ha='left', va='center', transform=ax.transAxes,
                color=PALETTE["text_primary"], fontsize=10.5, fontweight='black', zorder=3)
        ax.text(LPAD + 0.025, y_center - ROW_H * 0.25, f"n = {n_total}",
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

    COLS   = ["Pitch", "#", "Pitch%", "Velo", "Spin", "Ext.", "HB", "IVB", "Chase%", "Whiff%", "Str%", col11, last_col]
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
    # All row: from pitch-level aggregates (totals), not averages of per-pitch percentages
    aw = arsenal['whiff'].sum() / total if total else np.nan          # whiffs / included pitches
    astr = arsenal['strikes'].sum() / total if 'strikes' in arsenal.columns and total else np.nan  # strikes / included pitches
    az = arsenal['in_zone'].sum() / total if total else np.nan        # in-zone / included pitches
    axw = (arsenal['xwoba'] * arsenal['count']).sum() / total if total else np.nan  # weighted xwOBA (included pitches)
    ach = arsenal['chase'].sum() / total if total else np.nan         # chases / included pitches
    all_fast_swing_pct = box.get('fast_swing_pct')  # game-level from compute_box_score (swings with bat speed)

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
        rows.append(dict(name=r['name'], count=int(r['count']), pct=f"{r['usage_pct']:.1%}", velo=f"{r['velo']:.1f}" if _safe(r['velo']) else "--", spin=f"{r['spin']:.0f}" if _safe(r.get('spin', np.nan)) else "--", ext=f"{r['extension']:.1f}" if _safe(r.get('extension', np.nan)) else "--", hb=hb_str, ivb=ivb_str, raw_whiff=r['whiff_pct'], raw_str=r.get('str_pct', np.nan), raw_chase=r.get('chase_pct', np.nan), raw_zone=r['zone_pct'], raw_xwoba=r['xwoba'], raw_hh_pct=r.get('hard_hit_pct', np.nan), raw_fast_swing=r.get('fast_swing_pct', np.nan), colour=r['colour'], is_all=False))
    # All row: show the full pitch count from the header so numbers stay consistent (even if some rare pitch types are filtered out by MIN_PITCHES)
    rows.append(dict(name='All', count=int(box['total_pitches']), pct='100%', velo='--', spin='--', ext='--', hb='--', ivb='--',
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
    chase_range       = _range('raw_chase',       league_key="chase_per_pitch")
    whiff_range       = _range('raw_whiff',       league_key="whiff_per_pitch")
    str_range         = _range('raw_str',         league_key="strike_per_pitch")
    fast_swing_range  = _range('raw_fast_swing')  # no league benchmarks yet
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
                vs = f"{wv:.0%}" if _safe(wv) else "--"
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
    if card_flags.get('has_bs75', True):
        notes.append("* BS75+%: share of swings with bat speed \u2265 75 mph (Statcast bat tracking)")

    kw = dict(color=PALETTE["text_secondary"], fontsize=9.0, ha='center', va='center', transform=ax.transAxes)
    n = len(notes)
    ys = [0.78, 0.50, 0.22][:n]
    if n == 2:
        ys = [0.70, 0.30]
    for note, y in zip(notes, ys):
        ax.text(0.5, y, note, **kw)

# -----------------------------------------------------------------
def render_card(parquet_path, pitcher_id, output_path):
    mpl.rcParams['figure.dpi']  = 200
    mpl.rcParams['font.family'] = 'DejaVu Sans'

    df_raw  = load_game(parquet_path, pitcher_id)
    df      = process_pitches(df_raw)
    box     = compute_box_score(df)
    # Override with official box score when available (IP/line often wrong from pitch-level events when reliever gets the final out)
    path_name = Path(parquet_path).name
    game_pk_match = PARQUET_PATTERN.search(path_name)
    if game_pk_match:
        try:
            game_pk = int(game_pk_match.group(1))
            official = fetch_box_score_line(game_pk, pitcher_id)
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

    # Rival: por inning_topbot (Top = home pitcher → rival away; Bottom = away pitcher → rival home).
    # Válido para MLB y WBC; en WBC evita "vs DOM" cuando el pitcher lanza por DOM pero su equipo MLB es otro.
    if "inning_topbot" in df.columns:
        n_top = (df["inning_topbot"] == "Top").sum()
        n_bot = (df["inning_topbot"] == "Bottom").sum()
        opp_team = away_team if n_top >= n_bot else home_team
    else:
        opp_team = away_team  # fallback

    bio = fetch_player_bio(pitcher_id)
    if "inning_topbot" not in df.columns:
        opp_team = away_team if bio["team"] == home_team else home_team
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
        out_dir = Path(__file__).parent.parent / "outputs" / "pitching_cards"
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
        warehouse_root = Path(__file__).parent.parent / "data" / "warehouse"
        all_parquets = sorted(warehouse_root.rglob("*pitches_enriched.parquet"))
        # Filter to files matching target date (game_*_YYYYMMDD_*.parquet)
        parquets_by_date = [p for p in all_parquets if date_str in p.name and "pitches_enriched" in p.name]
        if not parquets_by_date:
            sys.exit(f"No parquet files found for date {target_date} under {warehouse_root}")
        out_dir = Path(__file__).parent.parent / "outputs" / "pitching_cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        mode_sfx = "" if LIGHT_MODE else "_dark"
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
                print(f"  Pitcher {pid}: no game with \u2265{MIN_PITCHES} pitches on {target_date}; skipped.")
                continue
            bio = fetch_player_bio(pid)
            safe_nm = bio["name"].lower().replace(", ", "_").replace(",", "_").replace(" ", "_").replace(".", "").replace("'", "")
            out_path = out_dir / f"pitcher_card_{safe_nm}_{target_date.isoformat()}{mode_sfx}.png"
            print(f"  Pitcher {pid} ({bio['name']}): {best_path.name} \u2192 {out_path.name}")
            render_card(str(best_path), pid, str(out_path))
            print(f"  \u2192 Saved: {out_path}\n")
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

        out_dir = Path(__file__).parent.parent / "outputs" / "pitching_cards"
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