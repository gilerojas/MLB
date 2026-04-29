"""
Games of the day slate — same visual language as probables board (dark cards, // date header).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

try:
    from ..mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
except ImportError:
    import sys

    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
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


def _draw_pill(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    text: str,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
    border: tuple[int, int, int, int],
    text_fill: tuple[int, int, int, int],
    pad_x: int = 10,
    h: int = 22,
) -> int:
    w = _text_width(draw, text, font) + pad_x * 2
    draw.rounded_rectangle((x, y, x + w, y + h), radius=h // 2, fill=fill, outline=border, width=1)
    bbox = draw.textbbox((0, 0), text, font=font)
    th = bbox[3] - bbox[1]
    draw.text((x + pad_x, y + (h - th) / 2 - 1), text, fill=text_fill, font=font)
    return w


def _draw_game_row_card(
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
        (x + 2, y + 3, x + w + 2, y + h + 3),
        radius=10,
        fill=_rgba(colors["shadow"], 22),
    )
    draw.rounded_rectangle(
        (x, y, x + w, y + h),
        radius=10,
        fill=_rgba(colors["card_bg"]),
        outline=_rgba(colors["card_border"], 180),
        width=1,
    )
    mid_y = y + (h - 22) // 2
    chip_x = x + 14
    chip_w = _draw_pill(
        draw,
        chip_x,
        mid_y,
        row["time_et"],
        fonts["chip"],
        fill=_rgba(colors["background"]),
        border=_rgba(colors["card_border"], 0),
        text_fill=_rgba(colors["text_soft"]),
        pad_x=10,
        h=22,
    )
    mu = f"{row['away_abbr']} @ {row['home_abbr']}"
    draw.text((chip_x + chip_w + 14, mid_y + 2), mu, fill=_rgba(colors["text_main"]), font=fonts["matchup"])


def render_games_of_day_board(rows: list[dict], date_str: str, out_path: Path) -> Path:
    w = CARD_WIDTH_X
    colors = _colors_rgb()
    try:
        day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        day_fmt = date_str

    n = len(rows)
    use_two_cols = n > 10
    n_eff = (n + 1) // 2 if use_two_cols else n
    row_h = 48
    row_gap = 10
    margin_x = 36
    header_top = 24
    header_bottom = 88
    body_top = header_bottom + 14
    footer_height = 36
    grid_h = (n_eff * row_h) + (max(0, n_eff - 1) * row_gap)
    h = max(CARD_HEIGHT_X, body_top + grid_h + footer_height)

    img = Image.new("RGBA", (w, h), _rgba(colors["background"]))
    draw = ImageDraw.Draw(img)

    font_title = _load_font(32, bold=True)
    font_date = _load_font(22, bold=True)
    font_sub = _load_font(14)
    font_foot = _load_font(12)

    title_y = header_top + 12
    title_text = "GAMES OF DAY"
    draw.text((margin_x + 6, title_y), title_text, fill=_rgba(colors["text_main"]), font=font_title)
    tw = _text_width(draw, title_text, font_title)
    slash = "//"
    draw.text((margin_x + 6 + tw + 12, title_y + 8), slash, fill=_rgba(colors["accent"]), font=font_date)
    sw = _text_width(draw, slash, font_date)
    draw.text(
        (margin_x + 6 + tw + 12 + sw + 10, title_y + 8),
        day_fmt.upper(),
        fill=_rgba(colors["text_muted"]),
        font=font_date,
    )
    draw.text(
        (margin_x + 6, title_y + 44),
        "Scheduled matchups · first pitch Eastern",
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
        foot = "Mallitalytics"
        draw.text(
            (w - margin_x - _text_width(draw, foot, font_foot), h - 26),
            foot,
            fill=_rgba(colors["text_muted"], 120),
            font=font_foot,
        )
        img.convert("RGB").save(out_path, "PNG")
        return out_path

    fonts = {
        "matchup": _load_font(16 if use_two_cols else 17, bold=True),
        "chip": _load_font(11, bold=True),
    }

    if use_two_cols:
        gap = 20
        col_w = (w - 2 * margin_x - gap) // 2
        x_left = margin_x
        x_right = x_left + col_w + gap
        mid = (n + 1) // 2
        for i, row in enumerate(rows[:mid]):
            cy = body_top + i * (row_h + row_gap)
            _draw_game_row_card(img, x_left, cy, col_w, row_h, row, colors, fonts)
        for j, row in enumerate(rows[mid:]):
            cy = body_top + j * (row_h + row_gap)
            _draw_game_row_card(img, x_right, cy, col_w, row_h, row, colors, fonts)
    else:
        col_w = w - margin_x * 2
        for i, row in enumerate(rows):
            cy = body_top + i * (row_h + row_gap)
            _draw_game_row_card(img, margin_x, cy, col_w, row_h, row, colors, fonts)

    foot = "Mallitalytics"
    draw.text(
        (w - margin_x - _text_width(draw, foot, font_foot), h - 26),
        foot,
        fill=_rgba(colors["text_muted"], 120),
        font=font_foot,
    )
    img.convert("RGB").save(out_path, "PNG")
    return out_path
