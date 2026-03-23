"""
Mallitalytics Seasonal Batter Profile Card
==========================================

Aggregates enriched parquet data across multiple games for a batter and
renders a 1200×675 season/tournament profile card.

Sections:
  - Header:  headshot · name · context badge (WBC 2026 · 5 G · 22 PA) · xwOBA hero
  - Left:    Zone Damage Map (9-zone xwOBA grid + chase border + pitch-mix strip)
  - Center:  Batted Ball Profile (spray chart + GB/FB/LD/PU% distribution bars)
  - Right:   Plate Discipline (K%, BB%, Chase%, Whiff%, SwStr%, Zone% bars)
  - Footer:  AVG · OBP · SLG · Avg EV · Bat Speed · RE24 tiles

CLI examples
------------
  python scripts/batter_card_seasonal.py --batter 656305 --season 2025
  python scripts/batter_card_seasonal.py --batter 660670 --season 2025 --dark
  python scripts/batter_card_seasonal.py --batter 660670 --parquet-dir data/warehouse/mlb/2026/wbc/pitches_enriched --context "WBC 2026" --dark
"""

import argparse
import os
from collections import defaultdict
from io import BytesIO
from pathlib import Path

import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
import requests
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
from matplotlib.patches import FancyBboxPatch
from PIL import Image

if "MPLBACKEND" not in os.environ:
    os.environ["MPLBACKEND"] = "Agg"

_PARENT = Path(__file__).resolve().parent.parent

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
    "Cutter":          "#B7791F", "Slider":        "#276749",
    "Sweeper":         "#1D4044", "Curveball":     "#2A4365",
    "Changeup":        "#553C9A", "Splitter":      "#B83280",
    "Knuckle Curve":   "#2C7A7B",
}
_PITCH_COLORS_DARK = {
    "4-Seam Fastball": "#FC8181", "Sinker":        "#F6AD55",
    "Cutter":          "#F6E05E", "Slider":        "#68D391",
    "Sweeper":         "#4FD1C5", "Curveball":     "#63B3ED",
    "Changeup":        "#B794F4", "Splitter":      "#F687B3",
    "Knuckle Curve":   "#76E4F7",
}
PITCH_COLORS = _PITCH_COLORS_LIGHT if LIGHT_MODE else _PITCH_COLORS_DARK

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


def _neutralize_headshot_bg(img, replace_rgb=(0x1F, 0x2E, 0x3D)):
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
    key = ESPN_LOGOS.get(team_abb, team_abb.lower())
    url = (
        f"https://a.espncdn.com/combiner/i?"
        f"img=/i/teamlogos/mlb/500/scoreboard/{key}.png&h=200&w=200"
    )
    try:
        return Image.open(BytesIO(requests.get(url, timeout=10).content))
    except Exception:
        return None


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
    if {"plate_x", "plate_z"}.issubset(df.columns):
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
            pitch_profile.append({
                "abbr": abbr,
                "count": int(len(grp)),
                "usage_pct": round(len(grp) / total_pitches * 100, 1) if total_pitches else 0.0,
                "xwoba": round(float(xw_pt.mean()), 3) if len(xw_pt) else None,
                "whiff_pct": _pct(int(grp["description"].isin(_WHIFF_DESCS).sum()), int(swings_pt.sum())),
                "chase_pct": _pct(int((swings_pt & out_zone_pt).sum()), int(out_zone_pt.sum())),
                "avg_ev": round(float(ev_pt.mean()), 1) if len(ev_pt) else None,
            })
        pitch_profile = sorted(pitch_profile, key=lambda x: (-x["count"], x["abbr"]))

    # ── Rolling xwOBA (game-by-game, 10-game rolling mean) ────────────────────
    rolling_xwoba: list[tuple] = []   # list of (game_pk, rolling_xwoba)
    rolling_hard_hit: list[tuple] = []
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
    if "game_pk" in bip_contact.columns and "launch_speed" in bip_contact.columns:
        game_hh = (
            bip_contact.assign(hard_hit=bip_contact["launch_speed"].ge(95))
            .groupby("game_pk")["hard_hit"]
            .mean()
            .sort_index()
        )
        if len(game_hh) >= 2:
            hh_roll = game_hh.rolling(window=min(10, len(game_hh)), min_periods=1).mean() * 100
            rolling_hard_hit = list(zip(range(len(hh_roll)), hh_roll.values.tolist()))

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
        spray_df["spray_x"] = spray_df["hc_x"] - 125
        spray_df["spray_y"] = 200 - spray_df["hc_y"]
        spray_df["hard_hit"] = spray_df["launch_speed"].fillna(0).ge(95)
        batter_stand = str(df["stand"].dropna().mode().iloc[0]) if "stand" in df.columns and df["stand"].dropna().any() else "R"
        spray_sign = -1 if batter_stand == "R" else 1
        oriented_x = spray_df["spray_x"] * spray_sign
        spray_df["direction_bucket"] = np.where(
            oriented_x <= -18, "pull",
            np.where(oriented_x >= 18, "oppo", "center")
        )
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
            ab = _pitch_abbrev(pt)
            if ab != "?":
                pitch_mix[ab] += 1

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
        "rolling_hard_hit": rolling_hard_hit,
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
    ax.set_title(
        title,
        loc="left", fontsize=10.5, fontweight="black",
        color=PALETTE["text_secondary"], pad=5,
    )
    if subtitle:
        ax.set_title(
            subtitle,
            loc="right", fontsize=7, fontweight="normal",
            color=PALETTE["text_lo"], pad=5,
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


def plot_zone_damage_map(ax, zone_damage: dict, outer_damage: dict, pitch_profile: list[dict] | None = None):
    _clean(ax)
    _border(ax)
    _panel_title(ax, "ZONE DAMAGE MAP", "xwOBA on contact · catcher view")

    ax.set_xlim(-1.45, 1.45)
    ax.set_ylim(0.40, 4.50)

    zone_w = _SZ_W / 3
    zone_h = _SZ_H / 3

    inner_vals = [
        zone_damage[z]["xwoba"]
        for z in range(1, 10)
        if zone_damage.get(z) and zone_damage[z]["xwoba"] is not None
    ]
    outer_vals = [v["xwoba"] for v in (outer_damage or {}).values() if v.get("xwoba") is not None]
    all_vals = inner_vals + outer_vals
    norm = TwoSlopeNorm(vmin=min(0.150, min(all_vals + [0.150])), vcenter=0.320, vmax=max(0.650, max(all_vals + [0.650]))) if all_vals else None

    cmap = LinearSegmentedColormap.from_list("savant_zone", ["#3373C4", "#F2E8E8", "#D22D49"])

    chase_w = zone_w * 0.75
    chase_bot_h = zone_h
    chase_top_h = zone_h

    mid_z = (_SZ_BOT + _SZ_TOP) / 2
    out_left = _SZ_LEFT - chase_w
    out_right = _SZ_RIGHT + chase_w
    out_bot = _SZ_BOT - chase_bot_h
    out_top = _SZ_TOP + chase_top_h

    # x, y, w, h
    outer_bounds = {
        "up_left":    (out_left, mid_z, abs(out_left), out_top - mid_z),
        "up_right":   (0.0, mid_z, out_right, out_top - mid_z),
        "down_left":  (out_left, out_bot, abs(out_left), mid_z - out_bot),
        "down_right": (0.0, out_bot, out_right, mid_z - out_bot),
    }

    sz_border_col = "#A9A9A9"

    if outer_damage:
        for key, (ox, oy, w, h) in outer_bounds.items():
            xw = outer_damage.get(key, {}).get("xwoba")
            color = cmap(norm(xw)) if xw is not None and norm else PALETTE["panel_bg"]
            ax.add_patch(mpatches.Rectangle(
                (ox, oy), w, h,
                facecolor=color, edgecolor="white", linewidth=1.2, zorder=1
            ))
            if xw is not None:
                tx = out_left + chase_w / 2 if 'left' in key else _SZ_RIGHT + chase_w / 2
                ty = _SZ_TOP + chase_top_h / 2 if 'up' in key else out_bot + chase_bot_h / 2
                mapped_lum = _lum(mpl.colors.to_hex(color))
                tc = "#111111" if mapped_lum > 0.45 else "#FFFFFF"
                ax.text(tx, ty, _fmt_slash(xw), color=tc, fontsize=12.5, fontweight="black",
                        ha="center", va="center", zorder=3)

    for z, (row, col) in _ZONE_RC.items():
        xw = zone_damage.get(z, {}).get("xwoba")
        color = cmap(norm(xw)) if xw is not None and norm else PALETTE["panel_bg"]
        zx = _SZ_LEFT + col * zone_w
        zy = _SZ_BOT + row * zone_h
        ax.add_patch(mpatches.Rectangle(
            (zx, zy), zone_w, zone_h,
            facecolor=color, edgecolor=sz_border_col, linewidth=0.9, zorder=2
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
        (_SZ_LEFT, _SZ_BOT), _SZ_W, _SZ_H,
        linewidth=2.5, edgecolor=sz_border_col, facecolor="none", zorder=6,
    ))

    hp_w = 0.40
    hp_y = 0.25
    hp = mpatches.Polygon(
        [(-hp_w / 2, hp_y), (hp_w / 2, hp_y),
         (hp_w / 2, hp_y - 0.12), (0, hp_y - 0.22),
         (-hp_w / 2, hp_y - 0.12)]
    )
    hp.set_facecolor("#FFFFFF")
    hp.set_edgecolor("#999999")
    hp.set_linewidth(1.0)
    hp.set_zorder(2)
    ax.add_patch(hp)

    ribbon = ax.inset_axes([0.00, -0.08, 1.0, 0.11])
    _clean(ribbon, PALETTE["table_alt"])
    for sp in ribbon.spines.values():
        sp.set_visible(False)
    ribbon.set_xlim(0, 1)
    ribbon.set_ylim(0, 1)

    prof = pitch_profile or []
    pitch_mix = sorted(
        [v for v in prof if v.get("count", 0) > 0],
        key=lambda x: x.get("count", 0), reverse=True
    )
    total_pitches = sum(x.get("count", 0) for x in pitch_mix)

    ribbon.text(0.02, 0.5, "HOW HE DAMAGES PITCHES",
                color=PALETTE["text_lo"], fontsize=5.8, fontweight="black",
                ha="left", va="center", transform=ribbon.transAxes)

    if pitch_mix and total_pitches > 0:
        x_start = 0.41
        step = 0.58 / max(len(pitch_mix[:6]), 1)
        for i, item in enumerate(pitch_mix[:6]):
            x0 = x_start + i * step
            full = next((k for k, v in _PITCH_ABBREV_MAP.items() if v == item["abbr"]), None)
            col = PITCH_COLORS.get(full, PALETTE["text_secondary"])
            ribbon.text(x0, 0.65, item["abbr"], color=col, fontsize=10.5, fontweight="black",
                        ha="center", va="center", transform=ribbon.transAxes)
            ribbon.text(x0, 0.25, _fmt_slash(item["xwoba"]) if item.get("xwoba") is not None else "—",
                        color=PALETTE["text_primary"], fontsize=8.5, fontweight="bold",
                        ha="center", va="center", transform=ribbon.transAxes)
    else:
        ribbon.text(0.5, 0.5, "Pitch-type split unavailable",
                    color=PALETTE["text_lo"], fontsize=8,
                    ha="center", va="center", transform=ribbon.transAxes)
    for i, item in enumerate(prof):
        x0 = 0.20 + i * step
        full = next((k for k, v in _PITCH_ABBREV_MAP.items() if v == item["abbr"]), None)
        col = PITCH_COLORS.get(full, PALETTE["text_secondary"])
        ribbon.text(x0, 0.65, item["abbr"], color=col, fontsize=10.5, fontweight="black",
                    ha="center", va="center", transform=ribbon.transAxes)
        ribbon.text(x0, 0.25, _fmt_slash(item["xwoba"]) if item.get("xwoba") is not None else "—",
                    color=PALETTE["text_primary"], fontsize=8.5, fontweight="bold",
                    ha="center", va="center", transform=ribbon.transAxes)


# ─────────────────────────────── PANEL 3 — BATTED BALL PROFILE ──────────────

def _spray_outcome(event: str) -> str:
    if event in ("single", "double", "triple", "home_run"):
        return event
    return "out"


def _draw_spray_field(ax, wall_r: float = 162):
    """Draw a cleaner spray-chart field with believable geometry."""
    grass_col = "#A7D7D6" if LIGHT_MODE else "#2A4C4B"
    infield_col = "#8AC1BF" if LIGHT_MODE else "#1A3635"
    line_col = "#FFFFFF" if LIGHT_MODE else "#E2E8F0"

    b1 = np.array([63, 63])
    b2 = np.array([0, 126])
    b3 = np.array([-63, 63])
    
    foul_r = wall_r
    theta = np.linspace(np.deg2rad(45), np.deg2rad(135), 360)
    wall_x = foul_r * np.cos(theta)
    wall_y = foul_r * np.sin(theta)

    ax.fill(np.concatenate([[0], wall_x]), np.concatenate([[0], wall_y]),
            color=grass_col, zorder=0)

    infield_r = 95
    t_infield = np.linspace(np.deg2rad(45), np.deg2rad(135), 100)
    ax.fill(np.concatenate([[0], infield_r * np.cos(t_infield)]), 
            np.concatenate([[0], infield_r * np.sin(t_infield)]), 
            color=infield_col, zorder=1)

    ax.plot(wall_x, wall_y, color=line_col, lw=2.1, zorder=2)
    ax.plot([0, foul_r * np.cos(np.deg2rad(45))], [0, foul_r * np.sin(np.deg2rad(45))],
            color=line_col, lw=1.2, zorder=2)
    ax.plot([0, foul_r * np.cos(np.deg2rad(135))], [0, foul_r * np.sin(np.deg2rad(135))],
            color=line_col, lw=1.2, zorder=2)
    ax.plot([0, b1[0], b2[0], b3[0], 0], [0, b1[1], b2[1], b3[1], 0],
            color=line_col, lw=1.2, zorder=2)

    for bx, by in (b1, b2, b3):
        ax.add_patch(plt.Rectangle((bx - 2.5, by - 2.5), 5.0, 5.0, angle=45, color=line_col, zorder=3))
    ax.add_patch(mpatches.RegularPolygon((0, 0), numVertices=5, radius=4.0,
                                         orientation=np.pi / 5, color=line_col, zorder=3))


def plot_batted_ball_profile(ax_spray, ax_bars, spray_df: pd.DataFrame, sd: dict):
    _clean(ax_spray)
    _border(ax_spray)

    non_hr = spray_df[spray_df["events"] != "home_run"]
    if not non_hr.empty:
        nr = np.sqrt((non_hr["hc_x"] - 125) ** 2 + (200 - non_hr["hc_y"]) ** 2)
        wall_r = float(nr.quantile(0.99)) + 8
        wall_r = max(120, min(wall_r, 185))
    else:
        wall_r = 162

    _draw_spray_field(ax_spray, wall_r=wall_r)

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
        hx = sub["spray_x"] if "spray_x" in sub.columns else sub["hc_x"] - 125
        hy = sub["spray_y"] if "spray_y" in sub.columns else 200 - sub["hc_y"]
        ax_spray.scatter(hx, hy, marker="o", s=sz, color=col, alpha=al,
                         linewidths=0.6 if outcome != "out" else 0,
                         edgecolors="#111111" if outcome != "out" else "none",
                         zorder=5 if outcome == "home_run" else 4)

    ax_spray.set_xlim(-(wall_r * 1.15), wall_r * 1.15)
    ax_spray.set_ylim(-15, wall_r * 1.40)

    legend_elements = [
        ("HOME RUN", "#E03282"),
        ("TRIPLE", "#F5AB00"),
        ("DOUBLE", "#7D6EE7"),
        ("SINGLE", "#FF6B00"),
    ]
    leg_x = wall_r * 0.65
    leg_y = wall_r * 1.20
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

# Approximate MLB 2025 percentile thresholds for qualified batters.
# Format: [(value, percentile), ...] ascending by value.
_PCT_THRESHOLDS: dict[str, list[tuple[float, int]]] = {
    "avg_ev":    [(83.5, 10), (85.5, 25), (87.0, 40), (88.2, 50),
                  (89.2, 60), (90.2, 70), (91.2, 80), (92.2, 90), (93.2, 95)],
    "bat_speed": [(66.5, 10), (68.5, 25), (70.0, 40), (71.5, 50),
                  (72.5, 60), (73.5, 70), (74.5, 80), (75.5, 90), (76.5, 95)],
    "hard_pct":  [(28.0, 10), (33.0, 25), (37.0, 40), (40.0, 50),
                  (43.0, 60), (47.0, 70), (51.0, 80), (55.0, 90), (59.0, 95)],
    "barrel_pct":[(2.0,  10), (4.0,  25), (5.5,  40), (6.5,  50),
                  (7.5,  60), (9.0,  70), (11.0, 80), (14.0, 90), (16.0, 95)],
    "swsp_pct":  [(25.0, 10), (28.0, 25), (31.0, 40), (33.0, 50),
                  (35.0, 60), (37.0, 70), (39.0, 80), (41.0, 90), (43.0, 95)],
    "xwoba":     [(0.245,10), (0.278,25), (0.298,40), (0.312,50),
                  (0.328,60), (0.348,70), (0.368,80), (0.388,90), (0.408,95)],
}


def _percentile(metric: str, value: float | None) -> int | None:
    thresholds = _PCT_THRESHOLDS.get(metric)
    if not thresholds or value is None:
        return None
    pct = 5
    for thresh_val, thresh_pct in thresholds:
        if value >= thresh_val:
            pct = thresh_pct
        else:
            break
    return pct


def _pct_color(pct: int) -> str:
    if pct >= 80: return PALETTE["accent_green"]
    if pct >= 50: return PALETTE["accent_gold"]
    return PALETTE["accent_red"]


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

def plot_rolling_xwoba(ax, rolling_xwoba: list, xwoba_season: float | None, rolling_hard_hit: list | None = None):
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
    y_min = min(0.220, float(np.nanmin(ys)) - 0.025)
    y_max = max(0.420, float(np.nanmax(ys)) + 0.025)

    _panel_title(ax, "10-GAME ROLLING xwOBA")
    ax.set_xlim(1, len(xs))
    ax.set_ylim(y_min, y_max)
    ax.yaxis.set_label_position("left")
    ax.tick_params(axis="y", pad=2)
    ax.grid(axis="y", color=PALETTE["grid"], linewidth=0.8, alpha=0.6)
    ax.set_axisbelow(True)

    ax.axhline(0.320, color=PALETTE["text_lo"], lw=0.9, ls="--", alpha=0.75)

    ax.fill_between(xs, ys, 0.320, where=ys >= 0.320, color=SEASONAL_ACCENT, alpha=0.16)
    ax.fill_between(xs, ys, 0.320, where=ys < 0.320, color=PALETTE["accent_red"], alpha=0.14)
    ax.plot(xs, ys, color=SEASONAL_ACCENT, lw=2.2, zorder=3)
    ax.scatter(xs[-1], ys[-1], s=26, color=SEASONAL_ACCENT, zorder=4)

    if xwoba_season is not None:
        ax.axhline(xwoba_season, color=PALETTE["accent_gold"], lw=1.0, ls=":", alpha=0.9)

    if rolling_hard_hit and len(rolling_hard_hit) == len(rolling_xwoba):
        hh = np.array([pt[1] for pt in rolling_hard_hit], dtype=float)
        hh_scaled = y_min + (hh / 100.0) * (y_max - y_min) * 0.55
        ax.plot(xs, hh_scaled, color=PALETTE["accent_orange"], lw=1.2, alpha=0.75)
        ax.text(0.99, 0.88, "orange = rolling HH%", color=PALETTE["text_lo"], fontsize=7,
                ha="right", va="center", transform=ax.transAxes)

    ax.set_xticks([1, max(1, len(xs) // 2), len(xs)])
    ax.set_xticklabels([1, max(1, len(xs) // 2), len(xs)], fontsize=8, color=PALETTE["text_secondary"])
    y_ticks = sorted(set([round(y_min, 3), 0.320, round(y_max, 3)]))
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([_fmt_slash(v) for v in y_ticks], fontsize=7, color=PALETTE["text_secondary"])
    ax.margins(y=0.06)
    for sp in ax.spines.values():
        sp.set_edgecolor(PALETTE["border"])
    ax.tick_params(axis="both", colors=PALETTE["text_secondary"], length=0)
    ax.set_xlabel(f"GAMES PLAYED (1-{len(xs)})", fontsize=8.5, fontweight="bold", color=PALETTE["text_lo"], labelpad=4)


# ─────────────────────────────── MAIN RENDER ────────────────────────────────

def generate_batter_profile(
    batter_id: int,
    season: int = 2025,
    context_label: str | None = None,
    parquet_dir: str | None = None,
    output_path: str = "batter_profile.png",
):
    mpl.rcParams["figure.dpi"]  = 200
    mpl.rcParams["font.family"] = "DejaVu Sans"

    print(f"  Loading parquet data for batter {batter_id} ({season})…")
    pdir = Path(parquet_dir) if parquet_dir else None
    df   = load_batter_seasonal_data(batter_id, parquet_dir=pdir, season=season)

    if df.empty:
        raise SystemExit(f"No data found for batter {batter_id} in season {season}.")

    print(f"  Aggregating {len(df):,} pitch rows across {df['game_pk'].nunique()} games…")
    sd = compute_season_stats(df)

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

    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor(PALETTE["card_bg"])

    outer_gs = gridspec.GridSpec(
        3, 1, figure=fig,
        height_ratios=[1.65, 4.2, 1.35],
        hspace=0.08,
        left=0.02, right=0.98, top=0.97, bottom=0.03,
    )

    ax_hdr = fig.add_subplot(outer_gs[0])

    body_gs = gridspec.GridSpecFromSubplotSpec(
        1, 3,
        subplot_spec=outer_gs[1],
        width_ratios=[1.10, 1.05, 0.85],
        wspace=0.05,
    )
    ax_zone = fig.add_subplot(body_gs[0])
    center_gs = gridspec.GridSpecFromSubplotSpec(
        2, 1,
        subplot_spec=body_gs[1],
        height_ratios=[2.6, 1.1],
        hspace=0.08,
    )
    ax_spray = fig.add_subplot(center_gs[0])
    ax_bars  = fig.add_subplot(center_gs[1])
    ax_disc = fig.add_subplot(body_gs[2])

    bottom_gs = gridspec.GridSpecFromSubplotSpec(
        1, 2,
        subplot_spec=outer_gs[2],
        width_ratios=[1.25, 1.0],
        wspace=0.06,
    )
    ax_spark = fig.add_subplot(bottom_gs[0])
    ax_foot = fig.add_subplot(bottom_gs[1])

    plot_header(ax_hdr, bio, sd, headshot, logo, context_label, is_flag=is_flag)
    plot_zone_damage_map(ax_zone, sd["zone_damage"], sd.get("outer_damage", {}), sd.get("pitch_profile"))
    plot_batted_ball_profile(ax_spray, ax_bars, sd["spray_df"], sd)
    plot_plate_discipline(ax_disc, sd)
    plot_rolling_xwoba(ax_spark, sd.get("rolling_xwoba", []), sd.get("xwoba"), sd.get("rolling_hard_hit"))
    plot_footer(ax_foot, sd)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    fig.savefig(output_path, dpi=200, bbox_inches="tight",
                facecolor=PALETTE["card_bg"], edgecolor="none")
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
