"""
HR Tracker table image (1200×675) — adaptive layout; DIST./EV heat-mapping (batter names neutral).
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

try:
    from ..mallitalytics_style import (
        CARD_HEIGHT_X,
        CARD_WIDTH_X,
        MALLITALYTICS,
        MONTSERRAT_WEIGHT_BOLD,
        load_jetbrains_mono,
        load_montserrat,
    )
    from .name_display import last_name_with_generational_suffix
except ImportError:
    import sys
    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    from src.mallitalytics_style import (
        CARD_HEIGHT_X,
        CARD_WIDTH_X,
        MALLITALYTICS,
        MONTSERRAT_WEIGHT_BOLD,
        load_jetbrains_mono,
        load_montserrat,
    )
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

def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    if not text: return 0
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _fit_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    text = str(text or "")
    if _text_width(draw, text, font) <= max_width:
        return text
    ell = "..."
    if max_width <= _text_width(draw, ell, font):
        return ""
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _text_width(draw, text[:mid] + ell, font) <= max_width:
            lo = mid
        else:
            hi = mid - 1
    return text[:lo].rstrip() + ell


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


def _fmt_dist(d_v: int) -> str:
    return f"{d_v}" if d_v > 0 else "—"


def _fmt_ev(e_v: float) -> str:
    return f"{e_v:.1f}" if e_v > 0 else "—"


def _split_columns(n_hrs: int) -> int:
    """1, 2, or 3 columns depending on slate size."""
    if n_hrs > 28:
        return 3
    if n_hrs > 14:
        return 2
    return 1


def render_hr_tracker_image(hrs: list[dict], date_str: str, out_path: Path) -> Path:
    w, h = CARD_WIDTH_X, CARD_HEIGHT_X
    c = _colors_rgb()
    img = Image.new("RGB", (w, h), c["charcoal"])
    draw = ImageDraw.Draw(img)

    margin_x = 44
    title_y = 28
    font_title = load_montserrat(36, weight=MONTSERRAT_WEIGHT_BOLD)
    font_subtitle = load_montserrat(17)
    font_col_hdr = load_montserrat(14, bold=True)
    font_footer = load_montserrat(13)

    try:
        day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        day_fmt = date_str

    n_hrs = len(hrs)
    title = f"HR Tracker — {day_fmt}"
    if n_hrs:
        title += f"  ·  {n_hrs} HR"
    draw.text((margin_x, title_y), title, fill=c["off_white"], font=font_title)
    draw.text(
        (margin_x, title_y + 42),
        "Sorted by exit velocity  ·  * = longest or hardest hit",
        fill=c["slate"],
        font=font_subtitle,
    )
    header_line_y = title_y + 68
    draw.line([(margin_x, header_line_y), (w - margin_x, header_line_y)], fill=c["slate"], width=1)

    if not hrs:
        draw.text((margin_x, header_line_y + 20), "No home runs recorded.", fill=c["slate"], font=load_montserrat(18))
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(out_path, "PNG")
        return out_path

    hrs_sorted = sorted(hrs, key=_hr_ev, reverse=True)
    ev_vals = [_hr_ev(x) for x in hrs_sorted if _hr_ev(x) > 0]
    dist_vals = [float(_hr_dist(x)) for x in hrs_sorted if _hr_dist(x) > 0]
    min_ev = min(ev_vals) if ev_vals else 0.0
    max_ev = max(ev_vals) if ev_vals else 0.0
    min_dist = min(dist_vals) if dist_vals else 0.0
    max_dist = max(dist_vals) if dist_vals else 0.0
    cold_rgb, hot_rgb, peak_rgb = _heat_palette(c)

    n_cols = _split_columns(n_hrs)
    col_gap = 36 if n_cols == 3 else 48
    usable_w = w - 2 * margin_x - col_gap * (n_cols - 1)
    eff_col_w = usable_w // n_cols

    # Per-column chunk sizes (balanced)
    base = n_hrs // n_cols
    extra = n_hrs % n_cols
    columns_data: list[list[dict]] = []
    idx = 0
    for col_i in range(n_cols):
        take = base + (1 if col_i < extra else 0)
        columns_data.append(hrs_sorted[idx : idx + take])
        idx += take

    # Batter+team merged; no standalone team column. Three-column slates need
    # explicit stat gutters so DIST./EV/Pitcher do not visually collide.
    dist_ev_gap = 10 if n_cols == 3 else 8
    stat_pitcher_gap = 14 if n_cols == 3 else 16
    if n_cols == 3:
        batter_w = 140
        dist_w = 52
        ev_w = 54
        pitcher_w = max(68, eff_col_w - batter_w - dist_w - dist_ev_gap - ev_w - stat_pitcher_gap)
    else:
        batter_w = int(eff_col_w * 0.40)
        dist_w = int(eff_col_w * 0.16)
        ev_w = int(eff_col_w * 0.14)
        pitcher_w = max(90, eff_col_w - batter_w - dist_w - dist_ev_gap - ev_w - stat_pitcher_gap)
    sub_col_widths = [batter_w, dist_w, ev_w, pitcher_w]
    col_offsets = [
        0,
        batter_w,
        batter_w + dist_w + dist_ev_gap,
        batter_w + dist_w + dist_ev_gap + ev_w + stat_pitcher_gap,
    ]

    data_start_y = header_line_y + 14
    footer_reserve = 42
    max_rows = max(len(col) for col in columns_data) if columns_data else 1
    row_h = max(28, min(40, (h - data_start_y - 26 - footer_reserve - 12) // max_rows))
    data_size = max(11, min(17, int(row_h * 0.45)))
    font_data = load_montserrat(data_size)
    font_data_bold = load_montserrat(data_size, bold=True)
    font_stat = load_jetbrains_mono(data_size)
    font_stat_bold = load_jetbrains_mono(data_size, bold=True)
    font_team = load_montserrat(max(9, int(data_size * 0.72)))
    row_alt = _lerp_rgb(c["charcoal"], c["slate"], 0.12, smooth=False)

    def draw_col(data_list: list[dict], start_x: int) -> None:
        curr_y = data_start_y
        hdrs = ["BATTER", "DIST.", "EV", "PITCHER"]
        for i, hdr in enumerate(hdrs):
            hx = start_x + col_offsets[i]
            if i in {1, 2}:
                hx += sub_col_widths[i] - _text_width(draw, hdr, font_col_hdr)
            draw.text((hx, curr_y), hdr, fill=c["slate"], font=font_col_hdr)

        curr_y += 20
        draw.line([(start_x, curr_y), (start_x + eff_col_w, curr_y)], fill=c["slate"], width=1)
        curr_y += 6

        for row_i, hr in enumerate(data_list):
            row_top = curr_y
            if row_i % 2 == 1:
                draw.rectangle(
                    (start_x - 4, row_top - 2, start_x + eff_col_w + 4, row_top + row_h - 4),
                    fill=row_alt,
                )

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
            hr_n = hr.get("hr_in_stage")
            if hr_n is not None:
                b_name += f" ({hr_n})"
            team = str(hr.get("team_abbrev") or hr.get("team") or "")
            b_name = _fit_text(draw, b_name, font_data_bold, sub_col_widths[0] - 6)
            draw.text((rx, curr_y), b_name, fill=c["off_white"], font=font_data_bold)
            if team:
                draw.text((rx, curr_y + data_size + 4), team, fill=c["slate"], font=font_team)

            dist_bold = d_v > 0 and _norm_range(float(d_v), min_dist, max_dist) >= 0.72
            dist_label = _fmt_dist(d_v)
            rx = start_x + col_offsets[1]
            draw.text(
                (rx + sub_col_widths[1] - _text_width(draw, dist_label, font_stat_bold if dist_bold else font_stat), curr_y + 2),
                dist_label,
                fill=dist_color,
                font=font_stat_bold if dist_bold else font_stat,
            )

            ev_bold = e_v > 0 and t_ev >= 0.72
            ev_label = _fmt_ev(e_v)
            rx = start_x + col_offsets[2]
            draw.text(
                (rx + sub_col_widths[2] - _text_width(draw, ev_label, font_stat_bold if ev_bold else font_stat), curr_y + 2),
                ev_label,
                fill=ev_color,
                font=font_stat_bold if ev_bold else font_stat,
            )

            p_name = "vs " + last_name_with_generational_suffix(hr.get("pitcher", "?"))
            p_name = _fit_text(draw, p_name, font_data, sub_col_widths[3])
            rx = start_x + col_offsets[3]
            draw.text((rx, curr_y + 2), p_name, fill=c["slate"], font=font_data)

            curr_y += row_h

    col_x = margin_x
    for col in columns_data:
        draw_col(col, col_x)
        col_x += eff_col_w + col_gap

    footer_y = h - 30
    draw.text((margin_x, footer_y), "* Longest distance / highest EV", fill=c["record_highlight"], font=font_footer)
    watermark = "@Mallitalytics"
    draw.text((w - margin_x - _text_width(draw, watermark, font_footer), footer_y), watermark, fill=c["slate"], font=font_footer)

    out_path = Path(out_path)
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
