from __future__ import annotations

from functools import lru_cache
from io import BytesIO
from datetime import datetime
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps

try:
    from ..mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
except ImportError:
    import sys

    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    # Fallback dimensions if missing
    CARD_WIDTH_X, CARD_HEIGHT_X = 1200, 675
    MALLITALYTICS = {
        "charcoal": "#0b1120",      
        "off_white": "#f8fafc",     
        "slate": "#64748b",         
        "burnt_orange": "#ea580c",  
    }


def _hex_to_rgb(hex_s: str) -> tuple[int, int, int]:
    h = hex_s.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _rgba(rgb: tuple[int, int, int], alpha: int = 255) -> tuple[int, int, int, int]:
    return rgb + (alpha,)


def _colors_rgb() -> dict[str, tuple[int, int, int]]:
    m = MALLITALYTICS
    bg_dark = _hex_to_rgb(m.get("charcoal", "#0b1120"))
    text_light = _hex_to_rgb(m.get("off_white", "#f8fafc"))
    accent = _hex_to_rgb(m.get("burnt_orange", "#ea580c"))
    
    card_bg = (bg_dark[0] + 18, bg_dark[1] + 24, bg_dark[2] + 35) 
    card_border = (bg_dark[0] + 40, bg_dark[1] + 48, bg_dark[2] + 65)

    return {
        "background": bg_dark,
        "card_bg": card_bg,
        "card_border": card_border,
        "accent": accent,
        "text_main": text_light,
        "text_muted": (148, 163, 184), 
        "text_soft": (203, 213, 225),  
        "shadow": (0, 0, 0),
    }


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    import sys as _sys

    if _sys.platform == "win32":
        paths = (
            ("C:/Windows/Fonts/arialbd.ttf", "C:/Windows/Fonts/arial.ttf")
            if bold
            else ("C:/Windows/Fonts/arial.ttf",)
        )
    elif _sys.platform == "darwin":
        paths = (
            "/Library/Fonts/Arial Bold.ttf",
            "/Library/Fonts/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        )
    else:
        paths = (
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        )

    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except OSError:
            continue

    return ImageFont.load_default()


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    if not text:
        return 0
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _truncate_to_width(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_w: int,
) -> str:
    if _text_width(draw, text, font) <= max_w:
        return text

    ell = "…"
    if _text_width(draw, ell, font) > max_w:
        return ""

    lo, hi = 0, len(text)
    best = ell

    while lo <= hi:
        mid = (lo + hi) // 2
        cand = text[:mid].rstrip() + ell
        if _text_width(draw, cand, font) <= max_w:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1

    return best


def _headshot_url(player_id: int, size: int) -> str:
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_{size},q_auto:best/v1/people/{player_id}/headshot/67/current"
    )


@lru_cache(maxsize=256)
def _fetch_headshot(player_id: int, size: int) -> Image.Image | None:
    try:
        resp = requests.get(_headshot_url(player_id, max(120, size * 3)), timeout=15)
        resp.raise_for_status()
        return Image.open(BytesIO(resp.content)).convert("RGBA")
    except Exception:
        return None


def _circle_crop(img: Image.Image, size: int) -> Image.Image:
    fitted = ImageOps.fit(
        img,
        (size, size),
        method=Image.Resampling.LANCZOS,
        centering=(0.5, 0.35),
    )
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    fitted.putalpha(mask)
    return fitted


def _placeholder_avatar(size: int, colors: dict[str, tuple[int, int, int]]) -> Image.Image:
    avatar = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(avatar)

    d.ellipse(
        (1, 1, size - 2, size - 2),
        fill=_rgba(colors["background"]),
        outline=_rgba(colors["card_border"]),
        width=1,
    )

    font = _load_font(max(12, size // 2), bold=True)
    label = "?"
    bbox = d.textbbox((0, 0), label, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]

    d.text(
        ((size - tw) / 2, (size - th) / 2 - 1),
        label,
        fill=_rgba(colors["text_muted"]),
        font=font,
    )
    return avatar


def _avatar_image(player_id: int | None, size: int, colors: dict[str, tuple[int, int, int]]) -> Image.Image:
    if player_id is None:
        return _placeholder_avatar(size, colors)

    raw = _fetch_headshot(player_id, size)
    if raw is None:
        return _placeholder_avatar(size, colors)

    avatar = _circle_crop(raw, size)
    ring = Image.new("RGBA", (size + 2, size + 2), (0, 0, 0, 0))
    d = ImageDraw.Draw(ring)

    d.ellipse(
        (0, 0, size + 1, size + 1),
        fill=_rgba(colors["background"]),
        outline=_rgba(colors["card_border"], 150),
        width=1,
    )
    ring.alpha_composite(avatar, (1, 1))
    return ring


def _draw_pill(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    text: str,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
    border: tuple[int, int, int, int],
    text_fill: tuple[int, int, int, int],
    pad_x: int = 8,
    h: int = 20,
) -> int:
    w = _text_width(draw, text, font) + pad_x * 2
    draw.rounded_rectangle(
        (x, y, x + w, y + h),
        radius=h // 2,
        fill=fill,
        outline=border,
        width=1,
    )
    bbox = draw.textbbox((0, 0), text, font=font)
    th = bbox[3] - bbox[1]
    draw.text((x + pad_x, y + (h - th) / 2 - 1), text, fill=text_fill, font=font)
    return w


def _draw_meta_line(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    record: str,
    era: str,
    font: ImageFont.ImageFont,
    colors: dict[str, tuple[int, int, int]],
) -> None:
    cursor_x = x

    draw.text((cursor_x, y), record, fill=_rgba(colors["text_muted"]), font=font)
    cursor_x += _text_width(draw, record, font)

    sep = "  |  "
    draw.text((cursor_x, y), sep, fill=_rgba(colors["card_border"]), font=font)
    cursor_x += _text_width(draw, sep, font)

    era_str = str(era)
    draw.text((cursor_x, y), era_str, fill=_rgba(colors["text_soft"]), font=font)
    cursor_x += _text_width(draw, era_str, font)

    draw.text((cursor_x, y), " ERA", fill=_rgba(colors["text_muted"]), font=font)


def _draw_pitcher_tile(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    w: int,
    h: int,
    pitcher: dict,
    colors: dict[str, tuple[int, int, int]],
    font_name: ImageFont.ImageFont,
    font_meta: ImageFont.ImageFont,
) -> None:
    # Lock avatar to exactly fit the available inner vertical space
    avatar_size = min(36, h)
    avatar = _avatar_image(pitcher.get("id"), avatar_size, colors)

    avatar_y = y + (h - avatar_size) // 2
    canvas.alpha_composite(avatar, (x, avatar_y))

    text_x = x + avatar_size + 14
    max_text_w = max(50, w - (text_x - x) - 4)

    name = _truncate_to_width(
        draw,
        pitcher.get("name") or "TBD",
        font_name,
        max_text_w,
    )

    total_text_h = 26
    text_start_y = y + (h - total_text_h) // 2

    draw.text((text_x, text_start_y), name, fill=_rgba(colors["text_main"]), font=font_name)

    record = pitcher.get("record") or "—"
    era = pitcher.get("era") or "—"
    meta_y = text_start_y + 16
    _draw_meta_line(draw, text_x, meta_y, record, str(era), font_meta, colors)


def _draw_matchup_card(
    canvas: Image.Image,
    x: int,
    y: int,
    w: int,
    h: int,
    row: dict,
    colors: dict[str, tuple[int, int, int]],
    fonts: dict[str, ImageFont.ImageFont],
) -> None:
    draw = ImageDraw.Draw(canvas)

    draw.rounded_rectangle(
        (x + 2, y + 4, x + w + 2, y + h + 4),
        radius=10,
        fill=_rgba(colors["shadow"], 25),
    )

    draw.rounded_rectangle(
        (x, y, x + w, y + h),
        radius=10,
        fill=_rgba(colors["card_bg"]),
        outline=_rgba(colors["card_border"], 180),
        width=1,
    )

    top_y = y + 8
    pill_h = 20
    chip_x = x + 14
    chip_w = _draw_pill(
        draw,
        chip_x,
        top_y,
        row["time_et"],
        fonts["meta_bold"],
        fill=_rgba(colors["background"]),
        border=_rgba(colors["card_border"], 0), 
        text_fill=_rgba(colors["text_soft"]),
        pad_x=10,
        h=pill_h,
    )

    matchup_x = chip_x + chip_w + 12
    draw.text(
        (matchup_x, top_y + 2),
        f"{row['away_abbr']} @ {row['home_abbr']}",
        fill=_rgba(colors["text_main"]),
        font=fonts["matchup"],
    )

    divider_y = top_y + pill_h + 6
    draw.line(
        [(x + 14, divider_y), (x + w - 14, divider_y)],
        fill=_rgba(colors["card_border"], 100),
        width=1,
    )

    inner_y = divider_y + 6
    inner_h = h - (inner_y - y) - 6
    
    half_gap = 20
    side_pad = 14
    tile_w = (w - (side_pad * 2) - half_gap) // 2
    tile_x1 = x + side_pad
    tile_x2 = tile_x1 + tile_w + half_gap

    _draw_pitcher_tile(canvas, draw, tile_x1, inner_y, tile_w, inner_h, row["away_pitcher"], colors, fonts["name"], fonts["meta"])
    _draw_pitcher_tile(canvas, draw, tile_x2, inner_y, tile_w, inner_h, row["home_pitcher"], colors, fonts["name"], fonts["meta"])


def render_probables_board(
    rows: list[dict],
    date_str: str,
    out_path: Path,
    *,
    heading_title: str = "PROBABLE STARTERS",
    heading_subtitle: str = "Season W-L and ERA shown for all listed pitchers",
) -> Path:
    """Render the probable-starters-style board to PNG (dynamic height)."""
    w = CARD_WIDTH_X
    colors = _colors_rgb()

    try:
        day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        day_fmt = date_str

    # --- DYNAMIC HEIGHT CALCULATION ---
    n = len(rows)
    use_two_cols = n > 7
    n_eff = (n + 1) // 2 if use_two_cols else n

    # Lock proportions so cards never squish or warp
    row_h = 82 
    row_gap = 12

    margin_x = 36
    header_top = 24
    header_bottom = 88
    body_top = header_bottom + 16
    footer_height = 36

    # Calculate exact vertical pixels required for the layout
    grid_h = (n_eff * row_h) + (max(0, n_eff - 1) * row_gap)
    required_h = body_top + grid_h + footer_height

    # If the slate needs more room than 675px, intelligently expand the canvas
    h = max(CARD_HEIGHT_X, required_h)

    # Initialize correctly sized canvas
    img = Image.new("RGBA", (w, h), _rgba(colors["background"]))
    draw = ImageDraw.Draw(img)

    # Header section
    font_title = _load_font(32, bold=True)
    font_date = _load_font(22, bold=True)
    font_sub = _load_font(14)
    font_foot = _load_font(12)

    title_y = header_top + 12
    title_text = heading_title

    draw.text(
        (margin_x + 6, title_y),
        title_text,
        fill=_rgba(colors["text_main"]),
        font=font_title,
    )

    tw = _text_width(draw, title_text, font_title)
    slash_text = "//"
    
    draw.text(
        (margin_x + 6 + tw + 12, title_y + 8),
        slash_text,
        fill=_rgba(colors["accent"]),
        font=font_date,
    )
    
    sw = _text_width(draw, slash_text, font_date)
    
    draw.text(
        (margin_x + 6 + tw + 12 + sw + 10, title_y + 8),
        day_fmt.upper(),
        fill=_rgba(colors["text_muted"]),
        font=font_date,
    )

    draw.text(
        (margin_x + 6, title_y + 44),
        heading_subtitle,
        fill=_rgba(colors["text_muted"]),
        font=font_sub,
    )

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        draw.text(
            (margin_x + 8, 120),
            "No games scheduled.",
            fill=_rgba(colors["text_muted"]),
            font=_load_font(20),
        )
        img.convert("RGB").save(out_path, "PNG")
        return out_path

    fonts = {
        "matchup": _load_font(15 if use_two_cols else 17, bold=True),
        "name": _load_font(14 if use_two_cols else 16, bold=True),
        "meta": _load_font(11),
        "meta_bold": _load_font(11, bold=True),
    }

    # Draw body grid
    if use_two_cols:
        gap = 20
        col_w = (w - 2 * margin_x - gap) // 2
        x_left = margin_x
        x_right = x_left + col_w + gap
        mid = (n + 1) // 2

        for i, row in enumerate(rows[:mid]):
            card_y = body_top + i * (row_h + row_gap)
            _draw_matchup_card(img, x_left, card_y, col_w, row_h, row, colors, fonts)

        for j, row in enumerate(rows[mid:]):
            card_y = body_top + j * (row_h + row_gap)
            _draw_matchup_card(img, x_right, card_y, col_w, row_h, row, colors, fonts)
    else:
        col_w = w - margin_x * 2
        for i, row in enumerate(rows):
            card_y = body_top + i * (row_h + row_gap)
            _draw_matchup_card(img, margin_x, card_y, col_w, row_h, row, colors, fonts)

    # Footer stays perfectly aligned to the new dynamic bottom boundary
    footer_text = "Mallitalytics"
    foot_w = _text_width(draw, footer_text, font_foot)
    draw.text(
        (w - margin_x - foot_w, h - 26),
        footer_text,
        fill=_rgba(colors["text_muted"], 120),
        font=font_foot,
    )

    img.convert("RGB").save(out_path, "PNG")
    return out_path