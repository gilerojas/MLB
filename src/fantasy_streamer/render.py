"""Bet-slip style projection card for a single fantasy streamer candidate."""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps

try:
    from ..mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
except ImportError:
    import sys

    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    from src.mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS


def _hex_to_rgb(hex_s: str) -> tuple[int, int, int]:
    h = hex_s.strip().lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _rgba(rgb: tuple[int, int, int], alpha: int = 255) -> tuple[int, int, int, int]:
    return rgb + (alpha,)


def _lerp_rgb(
    a: tuple[int, int, int],
    b: tuple[int, int, int],
    t: float,
    *,
    smooth: bool = True,
) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    if smooth:
        t = t * t * (3.0 - 2.0 * t)
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _heat_color(value: float | None, lo: float, hi: float) -> tuple[int, int, int]:
    """Cold (low) → warm (high), matching hub heatScale.ts."""
    cold = (99, 166, 232)
    warm = (240, 168, 48)
    peak = (232, 113, 43)
    if value is None or hi <= lo:
        return warm
    t = max(0.0, min(1.0, (float(value) - lo) / (hi - lo)))
    if t < 0.92:
        return _lerp_rgb(cold, warm, t)
    u = (t - 0.92) / 0.08
    return _lerp_rgb(warm, peak, u, smooth=False)


def _colors() -> dict[str, tuple[int, int, int]]:
    m = MALLITALYTICS
    bg = _hex_to_rgb(m.get("charcoal", "#1A2530"))
    accent = _hex_to_rgb(m.get("burnt_orange", "#E8712B"))
    success = _hex_to_rgb(m.get("light_green", "#66BB6A"))
    warning = _hex_to_rgb(m.get("muted_gold", "#F0A830"))
    danger = _hex_to_rgb(m.get("soft_red", "#E74C3C"))
    off_white = _hex_to_rgb(m.get("off_white", "#F5F2ED"))
    slate = _hex_to_rgb(m.get("slate", "#5D6D7E"))
    panel = (bg[0] + 22, bg[1] + 28, bg[2] + 36)
    panel_border = (bg[0] + 48, bg[1] + 56, bg[2] + 68)
    return {
        "bg": bg,
        "panel": panel,
        "panel_border": panel_border,
        "accent": accent,
        "success": success,
        "warning": warning,
        "danger": danger,
        "text": off_white,
        "muted": slate,
    }


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


def _fmt_num(value: Any, digits: int = 1) -> str:
    try:
        num = float(value)
        if num != num:
            return "—"
        if digits == 0:
            return str(int(round(num)))
        return f"{num:.{digits}f}"
    except (TypeError, ValueError):
        return "—"


def _malli_tier(score: float | None) -> str:
    if score is None:
        return "PROJECTION"
    if score >= 60:
        return "STRONG STREAM"
    if score >= 48:
        return "FRINGE STREAM"
    return "SIT / FADE"


PROJ_RANGES = {
    "malli": (35.0, 75.0),
    "ip": (4.0, 7.0),
    "k": (3.0, 10.0),
    "bb": (0.5, 3.5),
    "h": (3.0, 8.0),
    "er": (0.5, 5.0),
}


def _headshot_url(player_id: int, size: int) -> str:
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_{size},q_auto:best/v1/people/{player_id}/headshot/67/current"
    )


@lru_cache(maxsize=64)
def _fetch_headshot(player_id: int, size: int) -> Image.Image | None:
    try:
        resp = requests.get(_headshot_url(player_id, max(120, size * 3)), timeout=12)
        resp.raise_for_status()
        return Image.open(BytesIO(resp.content)).convert("RGBA")
    except Exception:
        return None


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    fitted = ImageOps.fit(img, (size, size), method=Image.Resampling.LANCZOS, centering=(0.5, 0.35))
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    fitted.putalpha(mask)
    return fitted


def _avatar(player_id: int | None, size: int, colors: dict[str, tuple[int, int, int]]) -> Image.Image:
    if not player_id:
        avatar = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        d = ImageDraw.Draw(avatar)
        d.ellipse((1, 1, size - 2, size - 2), fill=_rgba(colors["panel"]), outline=_rgba(colors["panel_border"]))
        font = _load_font(max(14, size // 2), bold=True)
        label = "?"
        box = d.textbbox((0, 0), label, font=font)
        d.text(((size - (box[2] - box[0])) / 2, (size - (box[3] - box[1])) / 2 - 1), label, fill=_rgba(colors["muted"]), font=font)
        return avatar

    raw = _fetch_headshot(int(player_id), size)
    if raw is None:
        return _avatar(None, size, colors)
    return _circle_crop(raw, size)


def _draw_prop_cell(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    w: int,
    h: int,
    label: str,
    value: str,
    colors: dict[str, tuple[int, int, int]],
    *,
    numeric: float | None = None,
    lo: float = 0.0,
    hi: float = 1.0,
) -> None:
    value_color = _heat_color(numeric, lo, hi) if numeric is not None else colors["text"]
    border = tuple(int(colors["panel_border"][i] * 0.65 + value_color[i] * 0.35) for i in range(3))
    draw.rounded_rectangle((x, y, x + w, y + h), radius=8, fill=_rgba(colors["panel"]), outline=_rgba(border), width=1)
    font_label = _load_font(16)
    font_value = _load_font(42, bold=True)
    lw = _text_w(draw, label, font_label)
    vw = _text_w(draw, value, font_value)
    draw.text((x + (w - lw) / 2, y + 16), label, fill=_rgba(colors["muted"]), font=font_label)
    draw.text((x + (w - vw) / 2, y + 42), value, fill=_rgba(value_color), font=font_value)


def render_streamer_projection(
    *,
    pitcher: str,
    opponent: str,
    game_date: str,
    projected: dict[str, Any],
    projected_malli_score: float | None = None,
    player_id: int | None = None,
    team: str | None = None,
    pitcher_hand: str | None = None,
    home_away: str | None = None,
    probable_status: str = "projected_rotation",
    venue: str | None = None,
    out_path: Path,
) -> Path:
    """Render a single-pitcher projection slip (1200×675)."""
    colors = _colors()
    width, height = CARD_WIDTH_X, CARD_HEIGHT_X
    img = Image.new("RGBA", (width, height), _rgba(colors["bg"]))
    draw = ImageDraw.Draw(img)

    margin = 48
    draw.rectangle((0, 0, width, 6), fill=_rgba(colors["accent"]))

    font_kicker = _load_font(18, bold=True)
    font_title = _load_font(34, bold=True)
    font_match = _load_font(28, bold=True)
    font_meta = _load_font(18)
    font_hero_label = _load_font(20, bold=True)
    font_hero = _load_font(88, bold=True)
    font_tier = _load_font(22, bold=True)
    font_footer = _load_font(15)

    try:
        day_label = datetime.strptime(game_date, "%Y-%m-%d").strftime("%b %-d, %Y")
    except ValueError:
        day_label = game_date

    draw.text((margin, 28), "PITCHING PROJECTION", fill=_rgba(colors["accent"]), font=font_kicker)
    date_w = _text_w(draw, day_label, font_meta)
    draw.text((width - margin - date_w, 32), day_label, fill=_rgba(colors["muted"]), font=font_meta)

    avatar_size = 108
    avatar = _avatar(player_id, avatar_size, colors)
    img.alpha_composite(avatar, (margin, 88))

    name_x = margin + avatar_size + 24
    matchup = f"{pitcher.upper()}  vs  {opponent.upper()}"
    draw.text((name_x, 96), matchup, fill=_rgba(colors["text"]), font=font_match)

    meta_bits = []
    if pitcher_hand:
        meta_bits.append(f"{pitcher_hand}HP")
    if team:
        meta_bits.append(team)
    if home_away:
        meta_bits.append(home_away.title())
    status_label = "Probable" if probable_status == "probable" else "Rotation proj." if probable_status == "projected_rotation" else title_case_status(probable_status)
    meta_bits.append(status_label)
    meta_line = " · ".join(meta_bits)
    draw.text((name_x, 138), meta_line, fill=_rgba(colors["muted"]), font=font_meta)
    if venue:
        draw.text((name_x, 164), venue, fill=_rgba(colors["muted"]), font=font_meta)

    hero_y = 210
    hero_h = 168
    draw.rounded_rectangle(
        (margin, hero_y, width - margin, hero_y + hero_h),
        radius=12,
        fill=_rgba(colors["panel"]),
        outline=_rgba(colors["panel_border"]),
        width=1,
    )

    malli = projected_malli_score
    if malli is None and isinstance(projected, dict):
        try:
            malli = float(projected.get("malli_score")) if projected.get("malli_score") is not None else None
        except (TypeError, ValueError):
            malli = None

    tier_label = _malli_tier(malli)
    malli_lo, malli_hi = PROJ_RANGES["malli"]
    malli_color = _heat_color(malli, malli_lo, malli_hi)

    hero_label = "PROJECTED MALLISCORE"
    hl_w = _text_w(draw, hero_label, font_hero_label)
    draw.text(((width - hl_w) / 2, hero_y + 18), hero_label, fill=_rgba(colors["muted"]), font=font_hero_label)

    malli_text = _fmt_num(malli, 1)
    malli_w = _text_w(draw, malli_text, font_hero)
    draw.text(((width - malli_w) / 2, hero_y + 44), malli_text, fill=_rgba(malli_color), font=font_hero)

    tier_w = _text_w(draw, tier_label, font_tier)
    draw.text(((width - tier_w) / 2, hero_y + hero_h - 38), tier_label, fill=_rgba(malli_color), font=font_tier)

    props_y = hero_y + hero_h + 28
    prop_h = 108
    gap = 14
    prop_defs = [
        ("IP", "ip", PROJ_RANGES["ip"]),
        ("K", "k", PROJ_RANGES["k"]),
        ("BB", "bb", PROJ_RANGES["bb"]),
        ("H", "h", PROJ_RANGES["h"]),
        ("ER", "er", PROJ_RANGES["er"]),
    ]
    total_gap = gap * (len(prop_defs) - 1)
    prop_w = (width - margin * 2 - total_gap) // len(prop_defs)
    for i, (label, key, (lo, hi)) in enumerate(prop_defs):
        raw = projected.get(key)
        try:
            numeric = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            numeric = None
        value = _fmt_num(raw, 1)
        x = margin + i * (prop_w + gap)
        _draw_prop_cell(draw, x, props_y, prop_w, prop_h, label, value, colors, numeric=numeric, lo=lo, hi=hi)

    whip = projected.get("whip")
    if whip is not None:
        whip_line = f"Proj. WHIP {_fmt_num(whip, 2)}"
        whip_w = _text_w(draw, whip_line, font_meta)
        draw.text(((width - whip_w) / 2, props_y + prop_h + 18), whip_line, fill=_rgba(colors["muted"]), font=font_meta)

    footer = "Mallitalytics · model projection, not a lineup lock"
    fw = _text_w(draw, footer, font_footer)
    draw.text(((width - fw) / 2, height - 34), footer, fill=_rgba(colors["muted"]), font=font_footer)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.convert("RGB").save(out_path, format="PNG", optimize=True)
    return out_path


def title_case_status(value: str) -> str:
    return value.replace("_", " ").title()
