"""
HR Tracker table image (1200×675) — adaptive layout; DIST./EV heat-mapping (batter names neutral).
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

try:
    from ..mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
    from .name_display import last_name_with_generational_suffix
except ImportError:
    import sys
    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    from src.mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
    from src.hr_tracker.name_display import last_name_with_generational_suffix

def _hex_to_rgb(hex_s: str) -> tuple[int, int, int]:
    h = hex_s.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))

def _colors_rgb() -> dict[str, tuple[int, int, int]]:
    m = MALLITALYTICS
    return {
        "charcoal": _hex_to_rgb(m["charcoal"]),
        "off_white": _hex_to_rgb(m["off_white"]),
        "slate": _hex_to_rgb(m["slate"]),
        "burnt_orange": _hex_to_rgb(m["burnt_orange"]),
        "record_highlight": _hex_to_rgb(m["muted_gold"]),
    }

def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    import sys as _sys
    if _sys.platform == "win32":
        paths = ("C:/Windows/Fonts/arialbd.ttf", "C:/Windows/Fonts/arial.ttf") if bold else ("C:/Windows/Fonts/arial.ttf",)
    elif _sys.platform == "darwin":
        paths = ("/Library/Fonts/Arial Bold.ttf", "/Library/Fonts/Arial.ttf", "/System/Library/Fonts/Supplemental/Arial Bold.ttf")
    else:
        paths = ("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf")
    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except OSError:
            continue
    return ImageFont.load_default()

def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    if not text: return 0
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]

def _hr_ev(hr: dict) -> float:
    v = hr.get("ev_mph") or hr.get("ev")
    return float(v) if v is not None else 0.0

def _hr_dist(hr: dict) -> int:
    v = hr.get("distance_ft") or hr.get("dist")
    return int(v) if v is not None else 0


def _lerp_rgb(
    a: tuple[int, int, int],
    b: tuple[int, int, int],
    t: float,
    *,
    smooth: bool = True,
) -> tuple[int, int, int]:
    """Linear RGB blend; optional smoothstep for less banding (t in 0..1)."""
    t = max(0.0, min(1.0, t))
    if smooth:
        t = t * t * (3.0 - 2.0 * t)
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _norm_range(v: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return max(0.0, min(1.0, (v - lo) / (hi - lo)))


def _heat_palette(c: dict[str, tuple[int, int, int]]):
    """Cool (low stat) → warm (high stat) using brand colors; stays readable on charcoal."""
    cold = _lerp_rgb(c["slate"], c["off_white"], 0.42, smooth=False)
    hot = c["record_highlight"]
    peak = _lerp_rgb(c["record_highlight"], c["burnt_orange"], 0.35, smooth=False)
    return cold, hot, peak


def _stat_color(
    value: float,
    lo: float,
    hi: float,
    cold: tuple[int, int, int],
    hot: tuple[int, int, int],
    peak: tuple[int, int, int],
) -> tuple[int, int, int]:
    """Seamless gradient: mostly cold↔hot, slight push toward burnt orange at the very top."""
    t = _norm_range(value, lo, hi)
    mid = _lerp_rgb(cold, hot, t, smooth=True)
    if t < 0.92:
        return mid
    u = (t - 0.92) / 0.08
    return _lerp_rgb(mid, peak, u, smooth=False)


def render_hr_tracker_image(hrs: list[dict], date_str: str, out_path: Path) -> Path:
    w, h = CARD_WIDTH_X, CARD_HEIGHT_X
    c = _colors_rgb()
    img = Image.new("RGB", (w, h), c["charcoal"])
    draw = ImageDraw.Draw(img)

    # Configuración de fuentes
    font_title = _load_font(40, bold=True)
    font_subtitle = _load_font(20)
    font_col_hdr = _load_font(16, bold=True)
    font_footer = _load_font(14)

    try:
        day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        day_fmt = date_str

    # Header
    margin_x_base = 50
    title_y = 32
    draw.text((margin_x_base, title_y), f"HR Tracker — {day_fmt}", fill=c["off_white"], font=font_title)
    draw.text((margin_x_base, title_y + 48), "Complete list of home runs — sorted by exit velocity", fill=c["slate"], font=font_subtitle)
    draw.line([(margin_x_base, title_y + 82), (w - margin_x_base, title_y + 82)], fill=c["slate"], width=1)

    if not hrs:
        draw.text((margin_x_base, title_y + 110), "No home runs recorded.", fill=c["slate"], font=_load_font(18))
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(out_path, "PNG")
        return out_path

    # Sort & value range for seamless heat (per day, not hard thresholds)
    hrs_sorted = sorted(hrs, key=_hr_ev, reverse=True)
    ev_vals = [_hr_ev(x) for x in hrs_sorted if _hr_ev(x) > 0]
    dist_vals = [float(_hr_dist(x)) for x in hrs_sorted if _hr_dist(x) > 0]
    min_ev = min(ev_vals) if ev_vals else 0.0
    max_ev = max(ev_vals) if ev_vals else 0.0
    min_dist = min(dist_vals) if dist_vals else 0.0
    max_dist = max(dist_vals) if dist_vals else 0.0
    cold_rgb, hot_rgb, peak_rgb = _heat_palette(c)
    n_hrs = len(hrs_sorted)

    # --- Adaptive layout: single wide table if few HRs ---
    use_two_cols = n_hrs > 12
    col_spacing = 60
    
    if use_two_cols:
        mid = (n_hrs + 1) // 2
        columns_data = [hrs_sorted[:mid], hrs_sorted[mid:]]
        eff_col_w = (w - (2 * margin_x_base) - col_spacing) // 2
        current_margin_x = margin_x_base
    else:
        columns_data = [hrs_sorted]
        eff_col_w = 800 # Más ancha al estar sola
        current_margin_x = (w - eff_col_w) // 2

    sub_col_widths = [
        int(eff_col_w * 0.35), # Batter
        int(eff_col_w * 0.12), # Team
        int(eff_col_w * 0.15), # Dist
        int(eff_col_w * 0.12), # EV
        int(eff_col_w * 0.26), # Pitcher
    ]

    data_start_y = title_y + 105
    max_rows = len(columns_data[0])
    row_h = min(38, (h - data_start_y - 60) // max_rows)
    font_data = _load_font(int(row_h * 0.52))
    font_data_bold = _load_font(int(row_h * 0.52), bold=True)

    def draw_col(data_list, start_x):
        curr_y = data_start_y
        # Headers
        hx = start_x
        hdrs = ["BATTER", "TEAM", "DIST.", "EV", "PITCHER"]
        for i, hdr in enumerate(hdrs):
            draw.text((hx, curr_y), hdr, fill=c["slate"], font=font_col_hdr)
            hx += sub_col_widths[i]
        
        curr_y += 25
        draw.line([(start_x, curr_y), (start_x + eff_col_w, curr_y)], fill=c["slate"], width=1)
        curr_y += 8

        for hr in data_list:
            rx = start_x
            d_v, e_v = _hr_dist(hr), _hr_ev(hr)
            is_m_dist = d_v > 0 and max_dist > 0 and float(d_v) == max_dist
            is_m_ev = e_v > 0 and max_ev > 0 and abs(e_v - max_ev) < 1e-6

            t_ev = _norm_range(e_v, min_ev, max_ev) if e_v > 0 and max_ev > 0 else 0.0
            ev_color = (
                _stat_color(e_v, min_ev, max_ev, cold_rgb, hot_rgb, peak_rgb)
                if e_v > 0
                else cold_rgb
            )
            dist_color = (
                _stat_color(float(d_v), min_dist, max_dist, cold_rgb, hot_rgb, peak_rgb)
                if d_v > 0
                else cold_rgb
            )

            prefix = "* " if (is_m_dist or is_m_ev) else ""
            b_name = prefix + last_name_with_generational_suffix(hr.get("batter", "?"))
            draw.text((rx, curr_y), b_name, fill=c["slate"], font=font_data_bold)
            rx += sub_col_widths[0]

            draw.text((rx, curr_y), str(hr.get("team_abbrev") or hr.get("team") or ""), fill=c["slate"], font=font_data)
            rx += sub_col_widths[1]

            dist_bold = d_v > 0 and _norm_range(float(d_v), min_dist, max_dist) >= 0.72
            draw.text((rx, curr_y), f"{d_v} ft", fill=dist_color, font=font_data_bold if dist_bold else font_data)
            rx += sub_col_widths[2]

            ev_bold = e_v > 0 and t_ev >= 0.72
            draw.text((rx, curr_y), f"{e_v:.1f}", fill=ev_color, font=font_data_bold if ev_bold else font_data)
            rx += sub_col_widths[3]
            
            # Pitcher
            p_name = "vs " + last_name_with_generational_suffix(hr.get("pitcher", "?"))
            draw.text((rx, curr_y), p_name, fill=c["slate"], font=font_data)
            
            curr_y += row_h

    # Draw table columns
    draw_col(columns_data[0], current_margin_x)
    if use_two_cols:
        draw_col(columns_data[1], current_margin_x + eff_col_w + col_spacing)

    # Footer
    footer_y = h - 35
    draw.text((margin_x_base, footer_y), "* Longest distance / Highest EV", fill=c["record_highlight"], font=font_footer)
    watermark = "@Mallitalytics"
    draw.text((w - margin_x_base - _text_width(draw, watermark, font_footer), footer_y), watermark, fill=c["slate"], font=font_footer)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path

render_hr_tracker_clean = render_hr_tracker_image

# --- Local test data ---
_EXAMPLE_HRS = [
    {"batter": "Rice", "team": "NYY", "dist": 410, "ev": 110.9, "pitcher": "Luzardo"},
    {"batter": "Schwarber", "team": "PHI", "dist": 405, "ev": 108.7, "pitcher": "Peterson"},
    {"batter": "Freeman", "team": "LAD", "dist": 395, "ev": 105.1, "pitcher": "Williams"},
    {"batter": "Ohtani", "team": "LAD", "dist": 420, "ev": 112.5, "pitcher": "Skenes"},
    {"batter": "Alvarez", "team": "HOU", "dist": 388, "ev": 107.8, "pitcher": "Mikolas"},
    {"batter": "Luis Robert Jr.", "team": "CWS", "dist": 415, "ev": 110.1, "pitcher": "Gausman"},
    {"batter": "Vladimir Guerrero Jr.", "team": "TOR", "dist": 390, "ev": 106.5, "pitcher": "Cole"},
    {"batter": "Judge", "team": "NYY", "dist": 435, "ev": 115.2, "pitcher": "Cease"},
    {"batter": "Harper", "team": "PHI", "dist": 402, "ev": 109.3, "pitcher": "Nola"},
    {"batter": "Machado", "team": "SD", "dist": 380, "ev": 104.9, "pitcher": "Kershaw"},
    {"batter": "Fernando Tatis Jr.", "team": "SD", "dist": 428, "ev": 111.0, "pitcher": "Burnes"},
    {"batter": "Ronald Acuna Jr.", "team": "ATL", "dist": 418, "ev": 113.8, "pitcher": "Wheeler"},
    {"batter": "Soto", "team": "NYY", "dist": 399, "ev": 108.2, "pitcher": "Webb"},
    {"batter": "Bobby Witt Jr.", "team": "KC", "dist": 385, "ev": 107.0, "pitcher": "Lopez"},
]

if __name__ == "__main__":
    _today = datetime.now().strftime("%Y-%m-%d")
    _out = Path(__file__).resolve().parents[2] / f"hr_tracker_clean_{_today.replace('-', '')}.png"
    render_hr_tracker_image(_EXAMPLE_HRS, _today, _out)
    print(f"Saved: {_out}")