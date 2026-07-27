"""Mallitalytics 1200x675 daily pitcher showdown renderer."""

from __future__ import annotations

from functools import lru_cache
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps

from src.mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X

ROOT = Path(__file__).resolve().parents[2]
LOGO_PATH = ROOT / "assets" / "brand" / "mallitalytics_horizontal_footer.png"
FONT_DIR = ROOT / "assets" / "fonts"
MONTSERRAT_REGULAR = FONT_DIR / "Montserrat-Regular.ttf"
MONTSERRAT_SEMIBOLD = FONT_DIR / "Montserrat-SemiBold.ttf"
MONTSERRAT_BOLD = FONT_DIR / "Montserrat-Bold.ttf"
JETBRAINS_MONO = FONT_DIR / "JetBrainsMono.ttf"

MONTSERRAT_WEIGHT_SEMIBOLD = 600
MONTSERRAT_WEIGHT_BOLD = 700

PAPER = "#F2EFE9"
INK = "#2E3A43"
GREEN = "#4E7B62"
OLIVE = "#A5B884"
ORANGE = "#F97D34"
MUTED = "#737872"
HAIRLINE = "#C8C0B3"
SOFT = "#E8E3DA"
AWAY_BAR = "#9AB2A2"
HOME_BAR = "#E5A17B"

ESPN_LOGOS = {
    "ARI": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos", "CHC": "chc",
    "CWS": "chw", "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det",
    "HOU": "hou", "KC": "kc", "LAA": "laa", "LAD": "lad", "MIA": "mia",
    "MIL": "mil", "MIN": "min", "NYM": "nym", "NYY": "nyy", "OAK": "oak",
    "ATH": "oak", "PHI": "phi", "PIT": "pit", "SD": "sd", "SEA": "sea",
    "SF": "sf", "STL": "stl", "TB": "tb", "TEX": "tex", "TOR": "tor",
    "WSH": "wsh",
}


def _font(size: int, weight: int | None = None) -> ImageFont.ImageFont:
    if weight is not None and weight >= MONTSERRAT_WEIGHT_BOLD:
        path = MONTSERRAT_BOLD
    elif weight is not None and weight >= MONTSERRAT_WEIGHT_SEMIBOLD:
        path = MONTSERRAT_SEMIBOLD
    else:
        path = MONTSERRAT_REGULAR
    try:
        return ImageFont.truetype(str(path), size)
    except OSError:
        return ImageFont.load_default()


def _mono(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    try:
        font = ImageFont.truetype(str(JETBRAINS_MONO), size)
        if bold and hasattr(font, "set_variation_by_axes"):
            try:
                font.set_variation_by_axes([700])
            except (OSError, ValueError):
                pass
        return font
    except OSError:
        return _font(size, MONTSERRAT_WEIGHT_BOLD if bold else None)


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), str(text), font=font)
    return box[2] - box[0]


def _right_text(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    text: str,
    *,
    fill: str,
    font: ImageFont.ImageFont,
) -> None:
    draw.text((x - _text_width(draw, text, font), y), text, fill=fill, font=font)


def _fmt(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


@lru_cache(maxsize=64)
def _headshot(player_id: int, size: int) -> Image.Image | None:
    url = (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_{size * 3},q_auto:best/v1/people/{player_id}/headshot/67/current"
    )
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return Image.open(BytesIO(response.content)).convert("RGBA")
    except Exception:
        return None


def _portrait(player_id: int, size: int) -> Image.Image:
    raw = _headshot(player_id, size)
    if raw is None:
        raw = Image.new("RGBA", (size, size), SOFT)
    fitted = ImageOps.fit(
        raw,
        (size, size),
        method=Image.Resampling.LANCZOS,
        centering=(0.5, 0.30),
    )
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    fitted.putalpha(mask)
    framed = Image.new("RGBA", (size + 6, size + 6), (0, 0, 0, 0))
    ImageDraw.Draw(framed).ellipse((0, 0, size + 5, size + 5), fill=HAIRLINE)
    framed.alpha_composite(fitted, (3, 3))
    return framed


@lru_cache(maxsize=64)
def _team_logo(team: str, size: int) -> Image.Image | None:
    key = ESPN_LOGOS.get(str(team or "").upper(), str(team or "").lower())
    if not key:
        return None
    url = (
        "https://a.espncdn.com/combiner/i?"
        f"img=/i/teamlogos/mlb/500/scoreboard/{key}.png&h={size}&w={size}"
    )
    try:
        response = requests.get(url, timeout=12)
        response.raise_for_status()
        logo = Image.open(BytesIO(response.content)).convert("RGBA")
        return ImageOps.contain(logo, (size, size), method=Image.Resampling.LANCZOS)
    except Exception:
        return None


def _paste_team_watermark(
    canvas: Image.Image,
    *,
    team: str,
    box: tuple[int, int, int, int],
    size: int,
    opacity: float,
    x_bias: float,
) -> None:
    logo = _team_logo(team, size)
    if logo is None:
        return
    alpha = logo.getchannel("A").point(lambda value: int(value * opacity))
    logo.putalpha(alpha)

    x0, y0, x1, y1 = box
    x = round(x0 + (x1 - x0) * x_bias - logo.width / 2)
    y = round(y0 + (y1 - y0) * 0.52 - logo.height / 2)
    layer = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    layer.alpha_composite(logo, (x, y))
    mask = Image.new("L", canvas.size, 0)
    ImageDraw.Draw(mask).rectangle(box, fill=255)
    layer.putalpha(
        Image.composite(layer.getchannel("A"), Image.new("L", canvas.size, 0), mask)
    )
    canvas.alpha_composite(layer)


def _paste_logo(canvas: Image.Image, x: int, y: int, width: int) -> None:
    if not LOGO_PATH.exists():
        return
    logo = Image.open(LOGO_PATH).convert("RGBA")
    height = round(width * logo.height / logo.width)
    logo = logo.resize((width, height), Image.Resampling.LANCZOS)
    canvas.alpha_composite(logo, (x, y))


def _metric_group(
    draw: ImageDraw.ImageDraw,
    *,
    x0: int,
    x1: int,
    y: int,
    pitcher: dict,
    align_right: bool,
) -> None:
    season = pitcher["season"]
    items = (
        ("ERA", _fmt(season.get("era"))),
        ("WHIP", _fmt(season.get("whip"))),
        ("K-BB%", f"{_fmt(season.get('k_bb_pct'), 1)}%"),
    )
    width = x1 - x0
    for index, (label, value) in enumerate(items):
        center = x0 + width * (index + 0.5) / 3
        value_font = _mono(25, bold=True)
        label_font = _font(11, MONTSERRAT_WEIGHT_BOLD)
        draw.text(
            (center - _text_width(draw, value, value_font) / 2, y),
            value,
            fill=INK,
            font=value_font,
        )
        draw.text(
            (center - _text_width(draw, label, label_font) / 2, y + 36),
            label,
            fill=MUTED,
            font=label_font,
        )


def _outing_context(row: dict) -> tuple[str, str]:
    try:
        from datetime import datetime

        date_label = datetime.strptime(
            str(row.get("date") or ""), "%Y-%m-%d"
        ).strftime("%b %d").upper()
    except ValueError:
        date_label = str(row.get("date") or "").upper()
    opponent = str(row.get("opponent_team") or "???")
    location = "VS" if row.get("is_home") else "@"
    return date_label, f"{location} {opponent}"


def _recent_chart(
    draw: ImageDraw.ImageDraw,
    *,
    y: int,
    away: dict,
    home: dict,
) -> None:
    title = "RECENT START PROFILE"
    title_font = _font(14, MONTSERRAT_WEIGHT_BOLD)
    draw.text(
        (600 - _text_width(draw, title, title_font) / 2, y),
        title,
        fill=GREEN,
        font=title_font,
    )

    subtitle = "IP BARS  ·  STRIKEOUTS ABOVE  ·  EARNED RUNS BELOW"
    subtitle_font = _font(10, MONTSERRAT_WEIGHT_BOLD)
    draw.text(
        (600 - _text_width(draw, subtitle, subtitle_font) / 2, y + 21),
        subtitle,
        fill=MUTED,
        font=subtitle_font,
    )

    summary_font = _mono(16, bold=True)
    left_summary = (
        f"{away['recent'].get('ip') or '—'} IP  ·  "
        f"{int(away['recent'].get('strikeouts') or 0)} K  ·  "
        f"{_fmt(away['recent'].get('era'))} ERA"
    )
    right_summary = (
        f"{home['recent'].get('ip') or '—'} IP  ·  "
        f"{int(home['recent'].get('strikeouts') or 0)} K  ·  "
        f"{_fmt(home['recent'].get('era'))} ERA"
    )
    draw.text((48, y + 18), left_summary, fill=INK, font=summary_font)
    _right_text(draw, 1152, y + 18, right_summary, fill=INK, font=summary_font)

    baseline = y + 151
    draw.line((72, baseline, 552, baseline), fill=HAIRLINE, width=1)
    draw.line((648, baseline, 1128, baseline), fill=HAIRLINE, width=1)

    groups = (
        (away, (132, 300, 468), AWAY_BAR),
        (home, (732, 900, 1068), HOME_BAR),
    )
    bar_width = 72
    max_bar_height = 86
    for pitcher, centers, bar_fill in groups:
        outings = list(pitcher.get("recent_outings") or [])[:3]
        for center, row in zip(centers, outings):
            outs = int(row.get("outs") or 0)
            innings = outs / 3
            innings_label = f"{outs // 3}.{outs % 3}"
            bar_height = max(26, round(min(innings, 9) / 9 * max_bar_height))
            top = baseline - bar_height
            left = center - bar_width // 2
            right = center + bar_width // 2

            draw.rounded_rectangle(
                (left, top, right, baseline),
                radius=5,
                fill=bar_fill,
            )
            draw.line((left + 5, top, right - 5, top), fill=INK, width=3)

            k_label = f"{int(row.get('strikeouts') or 0)} K"
            k_font = _mono(15, bold=True)
            draw.text(
                (center - _text_width(draw, k_label, k_font) / 2, top - 23),
                k_label,
                fill=INK,
                font=k_font,
            )

            ip_label = f"{innings_label} IP"
            ip_font = _mono(12, bold=True)
            draw.text(
                (
                    center - _text_width(draw, ip_label, ip_font) / 2,
                    baseline - 24,
                ),
                ip_label,
                fill=INK,
                font=ip_font,
            )

            er_label = f"{int(row.get('earned_runs') or 0)} ER"
            er_font = _mono(12, bold=True)
            draw.text(
                (center - _text_width(draw, er_label, er_font) / 2, baseline + 7),
                er_label,
                fill=ORANGE,
                font=er_font,
            )

            date_label, opponent_label = _outing_context(row)
            context = f"{date_label}  {opponent_label}"
            context_font = _font(11, MONTSERRAT_WEIGHT_BOLD)
            draw.text(
                (
                    center - _text_width(draw, context, context_font) / 2,
                    baseline + 27,
                ),
                context,
                fill=MUTED,
                font=context_font,
            )


def render_showdown(showdown: dict, out_path: Path) -> Path:
    canvas = Image.new("RGBA", (CARD_WIDTH_X, CARD_HEIGHT_X), PAPER)
    draw = ImageDraw.Draw(canvas)
    away = showdown["away"]
    home = showdown["home"]

    draw.text(
        (48, 30),
        "SHOWDOWN OF THE DAY",
        fill=INK,
        font=_font(37, MONTSERRAT_WEIGHT_BOLD),
    )
    draw.text(
        (49, 77),
        "STARTING PITCHER FORM CHECK",
        fill=GREEN,
        font=_font(12, MONTSERRAT_WEIGHT_BOLD),
    )
    date_label = datetime_label(showdown.get("date") or "")
    meta = (
        f"{date_label}  ·  {away['team']} @ {home['team']}  ·  "
        f"{showdown.get('game_time') or 'TBD'}"
    )
    _right_text(
        draw,
        1152,
        42,
        meta,
        fill=MUTED,
        font=_font(13, MONTSERRAT_WEIGHT_BOLD),
    )
    draw.line((48, 116, 1152, 116), fill=HAIRLINE, width=1)

    player_band = (48, 132, 1152, 294)
    _paste_team_watermark(
        canvas,
        team=away["team"],
        box=(player_band[0], player_band[1], 590, player_band[3]),
        size=218,
        opacity=0.10,
        x_bias=0.75,
    )
    _paste_team_watermark(
        canvas,
        team=home["team"],
        box=(610, player_band[1], player_band[2], player_band[3]),
        size=218,
        opacity=0.10,
        x_bias=0.25,
    )

    avatar_size = 158
    canvas.alpha_composite(_portrait(int(away["id"]), avatar_size), (82, 140))
    canvas.alpha_composite(_portrait(int(home["id"]), avatar_size), (954, 140))

    name_font = _font(25, MONTSERRAT_WEIGHT_BOLD)
    draw.text((260, 151), str(away["name"]).upper(), fill=INK, font=name_font)
    _right_text(draw, 940, 151, str(home["name"]).upper(), fill=INK, font=name_font)
    draw.text(
        (260, 191),
        away["team"],
        fill=GREEN,
        font=_font(13, MONTSERRAT_WEIGHT_BOLD),
    )
    _right_text(
        draw,
        940,
        191,
        home["team"],
        fill=GREEN,
        font=_font(13, MONTSERRAT_WEIGHT_BOLD),
    )
    record_font = _mono(31, bold=True)
    draw.text((260, 218), away["season"]["record"], fill=ORANGE, font=record_font)
    _right_text(
        draw,
        940,
        218,
        home["season"]["record"],
        fill=ORANGE,
        font=record_font,
    )
    record_label_font = _font(10, MONTSERRAT_WEIGHT_BOLD)
    draw.text((260, 258), "2026 RECORD", fill=MUTED, font=record_label_font)
    _right_text(
        draw,
        940,
        258,
        "2026 RECORD",
        fill=MUTED,
        font=record_label_font,
    )
    vs_font = _font(18, MONTSERRAT_WEIGHT_BOLD)
    draw.text((600 - _text_width(draw, "VS", vs_font) / 2, 189), "VS", fill=ORANGE, font=vs_font)

    draw.text((48, 304), "2026 SEASON", fill=MUTED, font=_font(11, MONTSERRAT_WEIGHT_BOLD))
    _right_text(
        draw,
        1152,
        304,
        "2026 SEASON",
        fill=MUTED,
        font=_font(11, MONTSERRAT_WEIGHT_BOLD),
    )
    _metric_group(draw, x0=48, x1=552, y=329, pitcher=away, align_right=False)
    _metric_group(draw, x0=648, x1=1152, y=329, pitcher=home, align_right=True)
    draw.line((48, 405, 1152, 405), fill=HAIRLINE, width=1)
    draw.line((600, 142, 600, 399), fill=HAIRLINE, width=1)

    _recent_chart(draw, y=416, away=away, home=home)

    draw.line((48, 625, 1152, 625), fill=HAIRLINE, width=1)
    _paste_logo(canvas, 48, 637, 160)
    source = "DATA: MLB STATS API"
    source_font = _font(10, MONTSERRAT_WEIGHT_BOLD)
    _right_text(draw, 1152, 645, source, fill=INK, font=source_font)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(out_path, "PNG")
    return out_path


def datetime_label(date_str: str) -> str:
    try:
        from datetime import datetime

        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y").upper()
    except ValueError:
        return date_str.upper()
