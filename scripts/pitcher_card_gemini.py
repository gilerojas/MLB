"""
Mallitalytics Daily Pitcher Card (v6 - Topographic Signature Layout)
====================================================================
Generates a publication-ready daily pitching summary card.
Features:
- Fixed Camera Release Point Matrix (Biomechanics with mound context)
- Topographic KDE Movement Profile (Elegant density clouds)
- Damage Heatmap (KDE of Hard Hits/xwOBA)
"""

import warnings
warnings.filterwarnings("ignore")

import os
if "MPLBACKEND" not in os.environ:
    os.environ["MPLBACKEND"] = "Agg"

import argparse
import re
from pathlib import Path

import requests
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch
from io import BytesIO

# ─────────────────────────────────────────────────────────────────
# BRAND PALETTE & DICTIONARIES
# ─────────────────────────────────────────────────────────────────
PALETTE = {
    "card_bg":       "#131B23", 
    "header_bg":     "#1C2836",
    "panel_bg":      "#1A2430",
    "table_bg":      "#16202A",
    "table_alt":     "#1C2836",
    "text_primary":  "#F5F2ED",
    "text_secondary":"#A8BDD0",
    "text_lo":       "#5D7A93",
    "accent_orange": "#E8712B",
    "accent_green":  "#66BB6A",
    "accent_red":    "#E74C3C",
    "grid":          "#2C3E50",
    "border":        "#2E4A62",
    "zone_edge":     "#8FA3B8",
}

PITCH_COLOURS = {
    'FF': {'colour': '#FF007D', 'name': '4-Seam'},
    'SI': {'colour': '#98165D', 'name': 'Sinker'},
    'FC': {'colour': '#BE5FA0', 'name': 'Cutter'},
    'CH': {'colour': '#F79E70', 'name': 'Changeup'},
    'FS': {'colour': '#FE6100', 'name': 'Splitter'},
    'SL': {'colour': '#67E18D', 'name': 'Slider'},
    'ST': {'colour': '#1BB999', 'name': 'Sweeper'},
    'CU': {'colour': '#3025CE', 'name': 'Curveball'},
    'KC': {'colour': '#311D8B', 'name': 'Knuck. Curve'},
    'UN': {'colour': '#9C8975', 'name': 'Unknown'},
}
DICT_COLOUR = {k: v['colour'] for k, v in PITCH_COLOURS.items()}
DICT_PITCH  = {k: v['name']   for k, v in PITCH_COLOURS.items()}

# ─────────────────────────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────────────────────────
def load_game(parquet_path, pitcher_id):
    df = pd.read_parquet(parquet_path)
    col = "pitcher" if "pitcher" in df.columns else "pitcher_id"
    df = df[df[col] == pitcher_id].copy()
    if df.empty: raise ValueError(f"Pitcher {pitcher_id} not found.")
    return df

def process_pitches(df):
    swing_codes = ['foul_bunt','foul','hit_into_play','swinging_strike','foul_tip','swinging_strike_blocked','missed_bunt','bunt_foul_tip']
    whiff_codes = ['swinging_strike','foul_tip','swinging_strike_blocked']
    
    df = df.copy()
    df['swing']     = df['description'].isin(swing_codes)
    df['whiff']     = df['description'].isin(whiff_codes)
    df['in_zone']   = df['zone'] < 10
    df['out_zone']  = df['zone'] > 10
    df['chase']     = (~df['in_zone']) & df['swing']
    df['is_strike'] = df['type'] == 'S'
    df['pfx_z_in']  = df['pfx_z'] * 12
    df['pfx_x_in']  = df['pfx_x'] * 12
    
    if 'launch_speed' in df.columns: df['hard_hit'] = df['launch_speed'] >= 95.0
    else: df['hard_hit'] = False
        
    if 'estimated_woba_using_speedangle' in df.columns:
        df['is_damage'] = (df['hard_hit']) | (df['estimated_woba_using_speedangle'] >= 0.350)
    else: df['is_damage'] = df['hard_hit']
        
    return df

def compute_box_score(df):
    out_events = ['strikeout','field_out','force_out','grounded_into_double_play','double_play','fielders_choice_out','sac_fly','sac_bunt','strikeout_double_play','other_out']
    outs  = df['events'].isin(out_events).sum()
    pa_df = df.dropna(subset=['events'])
    max_ev = df['launch_speed'].max() if 'launch_speed' in df.columns else np.nan
    avg_bs = df['bat_speed'].mean() if 'bat_speed' in df.columns else np.nan

    return dict(
        ip=f"{outs//3}.{outs%3}", pa=df['at_bat_number'].nunique(),
        k=int(pa_df['events'].isin(['strikeout','strikeout_double_play']).sum()), 
        bb=int(pa_df['events'].isin(['walk','intent_walk']).sum()), 
        hr=int(pa_df['events'].eq('home_run').sum()), 
        h=int(pa_df['events'].isin(['single','double','triple','home_run']).sum()), 
        er=int(pa_df['events'].eq('home_run').sum()), 
        total_pitches=len(df), whiffs=int(df['whiff'].sum()),
        strike_pct=df['is_strike'].sum()/len(df)*100 if len(df) else 0,
        csw_pct=(df['whiff'].sum() + df['description'].eq('called_strike').sum())/len(df)*100,
        max_ev=max_ev, avg_bat_speed=avg_bs
    )

def group_arsenal(df):
    g = df.groupby('pitch_type').agg(
        count=('pitch_type','count'), velo=('release_speed','mean'),
        pfx_z=('pfx_z_in','mean'), pfx_x=('pfx_x_in','mean'),
        extension=('release_extension','mean'), arm_angle=('arm_angle','mean') if 'arm_angle' in df.columns else ('release_speed', lambda x: np.nan),
        swing=('swing','sum'), whiff=('whiff','sum'),
        in_zone=('in_zone','sum'), xwoba=('estimated_woba_using_speedangle','mean'),
    ).reset_index()
    
    total = len(df)
    g['usage_pct'] = g['count'] / total
    g['whiff_pct'] = g['whiff'] / g['swing'].replace(0, np.nan)
    g['zone_pct']  = g['in_zone'] / g['count']
    g['name']      = g['pitch_type'].map(DICT_PITCH).fillna(g['pitch_type'])
    g['colour']    = g['pitch_type'].map(DICT_COLOUR).fillna('#9C8975')
    return g.sort_values('count', ascending=False).reset_index(drop=True)

def fetch_player_bio(pitcher_id):
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={pitcher_id}&hydrate=currentTeam"
    try:
        data = requests.get(url, timeout=10).json()['people'][0]
        team_abb = "MLB"
        link = data.get('currentTeam', {}).get('link', '')
        if link:
            td = requests.get(f"https://statsapi.mlb.com{link}", timeout=10).json()
            team_abb = td['teams'][0]['abbreviation']
        return dict(name=data['fullName'], hand=data['pitchHand']['code'], age=data.get('currentAge','—'), height=data.get('height','—'), weight=data.get('weight','—'), team=team_abb)
    except Exception: return dict(name="Unknown Pitcher", hand="R", age="—", height="—", weight="—", team="MLB")

def _clean(ax, bg=None):
    ax.set_facecolor(bg or PALETTE["panel_bg"])
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.set_xticks([]); ax.set_yticks([])

def _border(ax):
    for sp in ax.spines.values():
        sp.set_visible(True); sp.set_edgecolor(PALETTE["border"]); sp.set_linewidth(0.6)

def _lum(hex_color):
    r, g, b = mpl.colors.to_rgb(hex_color)
    return 0.299*r + 0.587*g + 0.114*b

# ─────────────────────────────────────────────────────────────────
# PANELS
# ─────────────────────────────────────────────────────────────────
def plot_header(ax, bio, box, game_date, opp_team):
    _clean(ax, PALETTE["header_bg"])
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

    ax.text(0.12, 0.90, bio['name'], color=PALETTE["text_primary"], fontsize=32, fontweight='bold', ha='left', va='top', transform=ax.transAxes)
    ax.text(0.12, 0.60, f"{game_date}   ·   vs  {opp_team}", color=PALETTE["accent_orange"], fontsize=14, fontweight='bold', ha='left', va='top', transform=ax.transAxes)
    ax.text(0.12, 0.38, f"{bio['hand']}HP   ·   Age {bio['age']}   ·   {bio['height']}   ·   {bio['weight']} lbs", color=PALETTE["text_secondary"], fontsize=12, ha='left', va='top', transform=ax.transAxes)
    ax.text(0.12, 0.15, f"{box['total_pitches']} pitches  ·  {box['strike_pct']:.1f}% strikes  ·  {box['csw_pct']:.1f}% CSW", color=PALETTE["text_lo"], fontsize=11, ha='left', va='top', transform=ax.transAxes)

    row1 = [("IP", box['ip'], PALETTE["text_primary"]), ("H", box['h'], PALETTE["text_primary"]), ("R", box.get('er', 0), PALETTE["text_primary"]), ("Max EV", f"{box['max_ev']:.1f}" if pd.notnull(box['max_ev']) else "—", PALETTE["text_primary"])]
    row2 = [("K", box['k'], PALETTE["accent_orange"]), ("BB", box['bb'], PALETTE["accent_orange"]), ("Whiffs", box['whiffs'], PALETTE["accent_orange"]), ("Bat Spd", f"{box['avg_bat_speed']:.1f}" if pd.notnull(box['avg_bat_speed']) else "—", PALETTE["accent_orange"])]
    
    bx0 = 0.55; dx = 0.09
    for i, (lbl, val, col) in enumerate(row1):
        ax.text(bx0 + i*dx, 0.82, lbl, color=PALETTE["text_lo"], fontsize=10, ha='center', va='top', transform=ax.transAxes)
        ax.text(bx0 + i*dx, 0.62, str(val), color=col, fontsize=24, fontweight='bold', ha='center', va='top', transform=ax.transAxes)
    for i, (lbl, val, col) in enumerate(row2):
        ax.text(bx0 + i*dx, 0.35, lbl, color=PALETTE["text_lo"], fontsize=10, ha='center', va='top', transform=ax.transAxes)
        ax.text(bx0 + i*dx, 0.15, str(val), color=col, fontsize=24, fontweight='bold', ha='center', va='top', transform=ax.transAxes)

    ax.plot([0.50, 0.50], [0.10, 0.90], color=PALETTE["border"], lw=1.0, transform=ax.transAxes)
    ax.plot([0, 1], [0.02, 0.02], color=PALETTE["accent_orange"], lw=2.0, alpha=0.8, transform=ax.transAxes)

def plot_arsenal_table(ax, arsenal):
    _clean(ax, PALETTE["panel_bg"]); _border(ax)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)

    COLS   = ["Pitch", "%", "Velo", "iVB", "HB", "Whiff%", "xwOBA"]
    WIDTHS = [0.26, 0.09, 0.11, 0.11, 0.11, 0.14, 0.14]
    tw = sum(WIDTHS); WIDTHS = [w/tw for w in WIDTHS]

    HDR_Y = 0.90; ROW_H = 0.11; SEP_Y = HDR_Y - 0.04
    ax.add_patch(mpatches.Rectangle((0.02, SEP_Y-0.03), 0.96, 0.09, facecolor=PALETTE["header_bg"], edgecolor=PALETTE["border"], lw=0.5, transform=ax.transAxes, zorder=0))

    x = 0.02
    for col, w in zip(COLS, WIDTHS):
        ax.text(x + w/2, HDR_Y - 0.02, col, ha='center', va='center', transform=ax.transAxes, color=PALETTE["text_primary"], fontsize=9.5, fontweight='bold')
        x += w

    y_top = SEP_Y - ROW_H*0.7
    for ri, r in arsenal.iterrows():
        yc = y_top - ri*ROW_H
        bg = PALETTE["table_alt"] if ri % 2 == 0 else PALETTE["table_bg"]
        ax.add_patch(FancyBboxPatch((0.02, yc - ROW_H*0.48), 0.96, ROW_H*0.96, boxstyle="square,pad=0", lw=0, facecolor=bg, transform=ax.transAxes, zorder=0))

        x = 0.02
        for ci, (col, w) in enumerate(zip(COLS, WIDTHS)):
            xc = x + w/2
            if ci == 0: 
                ax.add_patch(FancyBboxPatch((x+0.01, yc-ROW_H*0.35), w-0.02, ROW_H*0.70, boxstyle="round,pad=0.008", lw=0, facecolor=r['colour'], transform=ax.transAxes, zorder=1))
                tc = '#111111' if _lum(r['colour']) > 0.50 else '#FFFFFF'
                ax.text(xc, yc, r['name'], ha='center', va='center', fontsize=9.5, fontweight='bold', color=tc, transform=ax.transAxes, zorder=2)
            elif ci == 1: ax.text(xc, yc, f"{r['usage_pct']:.0%}", ha='center', va='center', fontsize=10, color=PALETTE["text_primary"], transform=ax.transAxes)
            elif ci == 2: ax.text(xc, yc, f"{r['velo']:.1f}", ha='center', va='center', fontsize=10.5, fontweight='bold', color=PALETTE["accent_orange"], transform=ax.transAxes)
            elif ci == 3: ax.text(xc, yc, f"{r['pfx_z']:.1f}", ha='center', va='center', fontsize=10, color=PALETTE["text_primary"], transform=ax.transAxes)
            elif ci == 4: ax.text(xc, yc, f"{r['pfx_x']:.1f}", ha='center', va='center', fontsize=10, color=PALETTE["text_primary"], transform=ax.transAxes)
            elif ci == 5: 
                wv = r['whiff_pct']
                if pd.notnull(wv):
                    bc = PALETTE["accent_green"] if wv > 0.28 else PALETTE["accent_orange"] if wv > 0.18 else PALETTE["text_lo"]
                    ax.add_patch(FancyBboxPatch((x+0.01, yc-ROW_H*0.35), w-0.02, ROW_H*0.70, boxstyle="round,pad=0.005", lw=0, facecolor=bc, alpha=0.88, transform=ax.transAxes, zorder=1))
                    ax.text(xc, yc, f"{wv:.0%}", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#FFFFFF', transform=ax.transAxes, zorder=2)
                else: ax.text(xc, yc, "—", ha='center', va='center', color=PALETTE["text_lo"], transform=ax.transAxes)
            elif ci == 6: 
                xv = r['xwoba']
                if pd.notnull(xv):
                    bc = PALETTE["accent_green"] if xv < 0.280 else PALETTE["accent_orange"] if xv < 0.370 else PALETTE["accent_red"]
                    ax.add_patch(FancyBboxPatch((x+0.01, yc-ROW_H*0.35), w-0.02, ROW_H*0.70, boxstyle="round,pad=0.005", lw=0, facecolor=bc, alpha=0.88, transform=ax.transAxes, zorder=1))
                    ax.text(xc, yc, f"{xv:.3f}", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#FFFFFF', transform=ax.transAxes, zorder=2)
                else: ax.text(xc, yc, "—", ha='center', va='center', color=PALETTE["text_lo"], transform=ax.transAxes)
            x += w

    ax.set_title("ARSENAL SUMMARY", color=PALETTE["text_secondary"], fontsize=11, fontweight='bold', pad=10)

def plot_release_point(ax, arsenal, df, hand):
    """Release Point Matrix: Fixed perspective so the rubber is ALWAYS visible."""
    _clean(ax); _border(ax)
    
    # Anclamos la cámara para que tenga sentido real de espacio
    # X: Catcher's view (-3.5 a 3.5 pies). Z: Altura desde el suelo (3 a 7.5 pies)
    ax.set_xlim(-3.5, 3.5)
    ax.set_ylim(2.5, 7.5)

    # El Centro de la Lomita (Rubber)
    ax.axvline(0, color=PALETTE["text_lo"], lw=1.5, linestyle='--', alpha=0.6, zorder=1)
    ax.text(0, 2.7, "Centro de la Lomita", color=PALETTE["text_lo"], fontsize=8, ha='center', va='bottom', fontweight='bold')

    for _, row in arsenal.iterrows():
        mask = df['pitch_type'] == row['pitch_type']
        subset = df[mask]
        
        # Puntos crudos (fondo tenue)
        ax.scatter(subset['release_pos_x'], subset['release_pos_z'], color=row['colour'], s=20, alpha=0.25, edgecolors='none', zorder=2)
        
        # Nodo central (promedio exacto)
        if not subset.empty:
            ax.scatter(subset['release_pos_x'].mean(), subset['release_pos_z'].mean(), color=row['colour'], s=120, edgecolors='#FFFFFF', linewidths=1.2, zorder=3)

    ax.set_title("RELEASE POINT MATRIX", color=PALETTE["text_secondary"], fontsize=11, fontweight='bold', pad=10)
    ax.text(0.5, 0.95, "Catcher's View", transform=ax.transAxes, ha='center', va='top', color=PALETTE["text_primary"], fontsize=9, bbox=dict(facecolor=PALETTE["panel_bg"], edgecolor='none', alpha=0.7))

def plot_movement_topographic(ax, arsenal, df, hand):
    """Topographic KDE Movement Profile: Replaces arrows with elegant density clouds."""
    _clean(ax); _border(ax)
    sign = -1 if hand == 'R' else 1
    
    lim = 24
    ax.axhline(0, color=PALETTE["text_lo"], lw=1.0, alpha=0.5, zorder=1)
    ax.axvline(0, color=PALETTE["text_lo"], lw=1.0, alpha=0.5, zorder=1)
    
    for _, row in arsenal.iterrows():
        mask = df['pitch_type'] == row['pitch_type']
        xs = df.loc[mask, 'pfx_x_in'] * sign
        zs = df.loc[mask, 'pfx_z_in']
        
        # 1. KDE Nubes Topográficas (Si hay suficientes pitcheos)
        if len(xs) >= 4:
            sns.kdeplot(x=xs, y=zs, fill=True, color=row['colour'], alpha=0.35, levels=4, ax=ax, zorder=2, warn_singular=False)
            
        # 2. Scatter tenue para pitcheos individuales
        ax.scatter(xs, zs, color=row['colour'], s=20, alpha=0.6, edgecolors='none', zorder=3)
        
        # 3. Label del pitcheo en su centro promedio
        if len(xs) > 0:
            ax.text(xs.mean(), zs.mean(), row['pitch_type'], ha='center', va='center', color='white', fontsize=8, fontweight='bold', bbox=dict(facecolor=row['colour'], edgecolor='none', boxstyle='round,pad=0.2', alpha=0.9), zorder=5)

    ax.set_xlim(-lim, lim); ax.set_ylim(-lim, lim)

    lkw = dict(fontsize=8, fontstyle='italic', color=PALETTE["text_secondary"], bbox=dict(facecolor=PALETTE["panel_bg"], edgecolor=PALETTE["border"], boxstyle='round,pad=0.3', linewidth=0.5))
    ax.text(-lim+0.5, -lim+1.5, "← Glove", ha='left', va='bottom', **lkw)
    ax.text(lim-0.5, -lim+1.5, "Arm →", ha='right', va='bottom', **lkw)

    ax.set_title("TOPOGRAPHIC MOVEMENT PROFILE", color=PALETTE["text_secondary"], fontsize=11, fontweight='bold', pad=10)

def plot_damage_heatmap(ax, arsenal, df):
    _clean(ax); _border(ax)
    
    sz_top = df['sz_top'].median() if not df.empty and 'sz_top' in df.columns else 3.5
    sz_bot = df['sz_bot'].median() if not df.empty and 'sz_bot' in df.columns else 1.5
    zw = 17/12

    ax.add_patch(mpatches.Rectangle((-zw/2, sz_bot), zw, sz_top - sz_bot, fill=False, edgecolor=PALETTE["zone_edge"], lw=2.0, zorder=4))
    for i in range(1, 3):
        ax.plot([-zw/2 + i*zw/3, -zw/2 + i*zw/3], [sz_bot, sz_top], color=PALETTE["grid"], lw=0.5, alpha=0.4, zorder=2)
        ax.plot([-zw/2, zw/2], [sz_bot + i*(sz_top-sz_bot)/3, sz_bot + i*(sz_top-sz_bot)/3], color=PALETTE["grid"], lw=0.5, alpha=0.4, zorder=2)

    ax.scatter(df['plate_x'], df['plate_z'], color=PALETTE["text_lo"], s=10, alpha=0.2, zorder=3)

    df_damage = df[df['is_damage'] == True]
    
    if len(df_damage) >= 3:
        sns.kdeplot(data=df_damage, x='plate_x', y='plate_z', fill=True, cmap='YlOrRd', alpha=0.5, levels=6, ax=ax, zorder=2)
        ax.scatter(df_damage['plate_x'], df_damage['plate_z'], color='#FFFFFF', s=80, marker='*', edgecolors=PALETTE["accent_red"], linewidths=1.0, zorder=5)
    elif len(df_damage) > 0:
        ax.scatter(df_damage['plate_x'], df_damage['plate_z'], color='#FFFFFF', s=80, marker='*', edgecolors=PALETTE["accent_red"], linewidths=1.0, zorder=5)
    else:
        ax.text(0.5, 0.5, "DOMINANTE\nCero Hard Hits / Daño", transform=ax.transAxes, ha='center', va='center', color=PALETTE["accent_green"], fontsize=12, fontweight='bold', alpha=0.8)

    ax.set_xlim(-2.5, 2.5); ax.set_ylim(0.5, 5.5)
    ax.set_aspect('equal', adjustable='box')
    
    ax.set_title("DAMAGE ZONE (xwOBA > .350 / 95+ mph)", color=PALETTE["text_secondary"], fontsize=11, fontweight='bold', pad=10)
    ax.text(0.95, 0.05, "★ = Contacto Fuerte", transform=ax.transAxes, ha='right', va='bottom', color=PALETTE["text_primary"], fontsize=8, bbox=dict(facecolor=PALETTE["panel_bg"], edgecolor=PALETTE["border"], alpha=0.9))

def plot_footer(ax, df):
    _clean(ax, PALETTE["card_bg"])
    if 'bat_speed' in df.columns:
        best_pitch = df.groupby('pitch_type')['bat_speed'].mean().idxmin()
        best_val = df.groupby('pitch_type')['bat_speed'].mean().min()
        insight = f"Malli-Insight: Velocidad promedio del bate rival fue de solo {best_val:.1f} mph contra la {DICT_PITCH.get(best_pitch, best_pitch)}."
    else:
        insight = "Malli-Insight: Pitching Dashboard Dinámico"

    ax.text(0.01, 0.5, "@Mallitalytics", color=PALETTE["accent_orange"], fontsize=12, fontweight='bold', va='center', transform=ax.transAxes)
    ax.text(0.18, 0.5, insight, color=PALETTE["text_primary"], fontsize=10, fontstyle='italic', va='center', transform=ax.transAxes)
    ax.text(0.99, 0.5, "Data: MLB · Statcast", color=PALETTE["text_secondary"], fontsize=10, ha='right', va='center', transform=ax.transAxes)

# ─────────────────────────────────────────────────────────────────
# MASTER RENDER
# ─────────────────────────────────────────────────────────────────
def render_card(parquet_path, pitcher_id, output_path):
    mpl.rcParams['figure.dpi'] = 200; mpl.rcParams['font.family'] = 'DejaVu Sans'

    df_raw = load_game(parquet_path, pitcher_id)
    df = process_pitches(df_raw)
    box = compute_box_score(df)
    arsenal = group_arsenal(df)

    gd = df["game_date"].iloc[0]; game_date = gd.strftime("%Y-%m-%d") if hasattr(gd, "strftime") else str(gd)[:10]
    hand = df["p_throws"].iloc[0] if "p_throws" in df.columns else "R"

    bio = fetch_player_bio(pitcher_id) 

    fig = plt.figure(figsize=(18, 10))
    fig.patch.set_facecolor(PALETTE["card_bg"])

    gs = gridspec.GridSpec(
        4, 3,
        figure=fig,
        height_ratios=[1.2, 3.2, 2.0, 0.3], 
        width_ratios=[1.7, 1.4, 1.1], 
        hspace=0.20, wspace=0.08,
        left=0.02, right=0.98,
        top=0.98, bottom=0.02,
    )

    ax_hdr = fig.add_subplot(gs[0, :])
    ax_tbl = fig.add_subplot(gs[1, 0])     
    ax_rel = fig.add_subplot(gs[2, 0])     
    ax_mov = fig.add_subplot(gs[1:3, 1])   
    ax_dmg = fig.add_subplot(gs[1:3, 2])   
    ax_ftr = fig.add_subplot(gs[3, :])

    plot_header(ax_hdr, bio, box, game_date, "OPP")
    plot_arsenal_table(ax_tbl, arsenal)
    plot_release_point(ax_rel, arsenal, df, hand)
    plot_movement_topographic(ax_mov, arsenal, df, hand)
    plot_damage_heatmap(ax_dmg, arsenal, df)
    plot_footer(ax_ftr, df)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.canvas.draw()
    fig.savefig(output_path, dpi=200, bbox_inches="tight", facecolor=PALETTE["card_bg"], edgecolor="none")
    plt.close()
    return output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mallitalytics Signature Pitcher Card")
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--pitcher-id", type=int, required=False)
    parser.add_argument("--output", type=Path, required=False)
    parser.add_argument("--list-pitchers", action="store_true")
    
    args = parser.parse_args()

    if args.list_pitchers:
        df = pd.read_parquet(args.parquet)
        col = "pitcher" if "pitcher" in df.columns else "pitcher_id"
        if col in df.columns:
            conteo = df.groupby(col).size().reset_index(name='pitches').sort_values('pitches', ascending=False)
            print(conteo.to_string(index=False))
        exit(0)

    try:
        ruta = render_card(str(args.parquet.resolve()), args.pitcher_id, str(args.output.resolve()))
        print(f"✅ ¡Éxito! Tarjeta guardada en: {ruta}")
    except Exception as e:
        print(f"❌ Error: {e}")