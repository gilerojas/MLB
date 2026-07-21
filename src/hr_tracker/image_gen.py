"""Standardized Mallitalytics HR Tracker image renderer."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps

try:
    from ..mallitalytics_style import (
        CARD_HEIGHT_X,
        CARD_WIDTH_X,
        MONTSERRAT_WEIGHT_BOLD,
        load_montserrat,
    )
    from .history import category_lead_count
except ImportError:
    import sys

    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    from src.hr_tracker.history import category_lead_count
    from src.mallitalytics_style import (
        CARD_HEIGHT_X,
        CARD_WIDTH_X,
        MONTSERRAT_WEIGHT_BOLD,
        load_montserrat,
    )


ROOT = Path(__file__).resolve().parents[2]
LOGO_PATH = ROOT / "assets" / "brand" / "mallitalytics_horizontal_footer.png"

INK = "#2E3A43"
FOREST = "#4E7B62"
ORANGE = "#F97D34"
PAPER = "#F2EFE9"
MUTED = "#737872"
LABEL = "#59615D"
HAIRLINE = "#C8C0B3"
PORTRAIT_RING = "#D7D0C4"


def _font(size: int, *, bold: bool = False) -> ImageFont.ImageFont:
    return load_montserrat(
        size,
        weight=MONTSERRAT_WEIGHT_BOLD if bold else None,
    )


def _text_width(draw: ImageDraw.ImageDraw, text: str, face: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), str(text), font=face)
    return box[2] - box[0]


def _fit_text(draw: ImageDraw.ImageDraw, text: str, face: ImageFont.ImageFont, width: int) -> str:
    value = str(text or "")
    if _text_width(draw, value, face) <= width:
        return value
    suffix = "..."
    lo, hi = 0, len(value)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _text_width(draw, value[:mid] + suffix, face) <= width:
            lo = mid
        else:
            hi = mid - 1
    return value[:lo].rstrip() + suffix


def _ev(hr: dict) -> float:
    value = hr.get("ev_mph") or hr.get("ev")
    return float(value) if value is not None else 0.0


def _distance(hr: dict) -> int:
    value = hr.get("distance_ft") or hr.get("dist")
    return round(float(value)) if value is not None else 0


def _leaders(values: Counter) -> tuple[int, list[str]]:
    high = max(values.values())
    return high, sorted(name for name, count in values.items() if count == high)


def _headshot_url(player_id: int, size: int) -> str:
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"w_{size},q_auto:best/v1/people/{player_id}/headshot/67/current"
    )


@lru_cache(maxsize=128)
def _fetch_headshot(player_id: int, size: int) -> Image.Image | None:
    try:
        response = requests.get(_headshot_url(player_id, max(320, size * 3)), timeout=12)
        response.raise_for_status()
        return Image.open(BytesIO(response.content)).convert("RGB")
    except Exception:
        return None


def _initials(name: str) -> str:
    parts = [part for part in str(name or "").replace(",", " ").split() if part]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][0].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _round_headshot(player_id: int | None, player_name: str, size: int) -> Image.Image:
    portrait = _fetch_headshot(player_id, size) if player_id else None
    if portrait is not None:
        portrait = ImageOps.fit(
            portrait,
            (size, size),
            method=Image.Resampling.LANCZOS,
            centering=(0.5, 0.30),
        ).convert("RGBA")
    else:
        portrait = Image.new("RGBA", (size, size), PORTRAIT_RING)
        draw = ImageDraw.Draw(portrait)
        initials = _initials(player_name)
        face = _font(max(28, size // 3), bold=True)
        draw.text(
            (
                (size - _text_width(draw, initials, face)) / 2,
                size * 0.31,
            ),
            initials,
            fill=INK,
            font=face,
        )

    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    portrait.putalpha(mask)

    outer = Image.new("RGBA", (size + 6, size + 6), (0, 0, 0, 0))
    ImageDraw.Draw(outer).ellipse((0, 0, size + 5, size + 5), fill=PORTRAIT_RING)
    outer.alpha_composite(portrait, (3, 3))
    return outer


def _paste_logo(canvas: Image.Image, x: int, y: int, width: int) -> None:
    if not LOGO_PATH.exists():
        draw = ImageDraw.Draw(canvas)
        draw.text((x, y), "Mallitalytics", fill=INK, font=_font(18, bold=True))
        return
    logo = Image.open(LOGO_PATH).convert("RGBA")
    height = round(width * logo.height / logo.width)
    logo = logo.resize((width, height), Image.Resampling.LANCZOS)
    canvas.alpha_composite(logo, (x, y))


def _draw_footer(canvas: Image.Image, draw: ImageDraw.ImageDraw) -> None:
    draw.line((48, 625, 1152, 625), fill=HAIRLINE, width=1)
    _paste_logo(canvas, 48, 637, 160)
    source = "DATA: MLB / STATCAST"
    face = _font(11, bold=True)
    draw.text((1152 - _text_width(draw, source, face), 644), source, fill=INK, font=face)


def _render_empty(date_str: str, out_path: Path) -> Path:
    canvas = Image.new("RGBA", (CARD_WIDTH_X, CARD_HEIGHT_X), PAPER)
    draw = ImageDraw.Draw(canvas)
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y").upper()
    except ValueError:
        date = date_str.upper()
    draw.text((48, 42), "HOME RUN TRACKER", fill=INK, font=_font(39, bold=True))
    draw.text((49, 90), "MLB DAILY POWER SUMMARY", fill=FOREST, font=_font(13, bold=True))
    face = _font(14, bold=True)
    draw.text((1152 - _text_width(draw, date, face), 42), date, fill=MUTED, font=face)
    draw.line((48, 122, 1152, 122), fill=HAIRLINE, width=1)
    draw.text((48, 174), "No home runs recorded.", fill=LABEL, font=_font(22, bold=True))
    _draw_footer(canvas, draw)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(out_path, "PNG")
    return out_path


def render_hr_tracker_image(
    hrs: list[dict],
    date_str: str,
    out_path: Path,
    *,
    category_history: dict | None = None,
) -> Path:
    """Render a fixed-layout daily HR summary; typography never shrinks with slate size."""
    out_path = Path(out_path)
    if not hrs:
        return _render_empty(date_str, out_path)

    hardest = max((hr for hr in hrs if _ev(hr) > 0), key=_ev, default=hrs[0])
    longest = max((hr for hr in hrs if _distance(hr) > 0), key=_distance, default=hrs[0])
    player_high, player_leaders = _leaders(Counter(str(hr.get("batter") or "?") for hr in hrs))
    team_high, team_leaders = _leaders(Counter(str(hr.get("team_abbrev") or "?") for hr in hrs))
    hard_hr = sum(_ev(hr) >= 105 for hr in hrs)
    long_hr = sum(_distance(hr) >= 420 for hr in hrs)

    canvas = Image.new("RGBA", (CARD_WIDTH_X, CARD_HEIGHT_X), PAPER)
    draw = ImageDraw.Draw(canvas)

    draw.text((48, 34), "HOME RUN TRACKER", fill=INK, font=_font(39, bold=True))
    draw.text((49, 82), "MLB DAILY POWER SUMMARY", fill=FOREST, font=_font(13, bold=True))
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y").upper()
    except ValueError:
        date = date_str.upper()
    date_face = _font(14, bold=True)
    draw.text((1152 - _text_width(draw, date, date_face), 34), date, fill=MUTED, font=date_face)
    total = str(len(hrs))
    total_face = _font(55, bold=True)
    total_x = 1061 - _text_width(draw, total, total_face)
    draw.text((total_x, 57), total, fill=ORANGE, font=total_face)
    draw.text((1078, 82), "HR", fill=INK, font=_font(18, bold=True))
    draw.line((48, 122, 1152, 122), fill=HAIRLINE, width=1)

    def spotlight(
        hr: dict,
        *,
        x: int,
        label: str,
        primary: str,
        accent: str,
        category: str,
    ) -> None:
        player_id = hr.get("batter_id")
        try:
            player_id = int(player_id) if player_id else None
        except (TypeError, ValueError):
            player_id = None
        avatar = _round_headshot(player_id, str(hr.get("batter") or "?"), 154)
        canvas.alpha_composite(avatar, (x + 15, 203))

        content_x = x + 202
        draw.text((content_x, 158), label, fill=accent, font=_font(14, bold=True))
        name_face = _font(23, bold=True)
        name = _fit_text(draw, str(hr.get("batter") or "?").upper(), name_face, 318)
        draw.text((content_x, 195), name, fill=INK, font=name_face)
        draw.text(
            (content_x, 231),
            str(hr.get("team_abbrev") or hr.get("team") or ""),
            fill=LABEL,
            font=_font(13, bold=True),
        )
        draw.text((content_x - 3, 264), primary, fill=INK, font=_font(41, bold=True))
        if category == "hardest":
            detail = f'{_distance(hr)} FT  ·  {int(hr.get("launch_angle") or 0)}° LA'
        else:
            detail = f'{_ev(hr):.1f} MPH  ·  {int(hr.get("launch_angle") or 0)}° LA'
        draw.text((content_x, 329), detail, fill=FOREST, font=_font(14, bold=True))
        draw.text((content_x, 365), "PITCHER", fill=LABEL, font=_font(10, bold=True))
        pitcher_face = _font(12, bold=True)
        pitcher = _fit_text(draw, str(hr.get("pitcher") or "?"), pitcher_face, 235)
        draw.text((content_x + 65, 362), pitcher, fill=INK, font=pitcher_face)

        lead_count = (
            category_lead_count(category_history, date_str, category, player_id)
            if player_id is not None
            else None
        )
        if lead_count is not None:
            draw.text((content_x, 397), "2026 DAILY LEADS", fill=LABEL, font=_font(10, bold=True))
            draw.text((content_x + 128, 394), f"{lead_count}×", fill=accent, font=_font(13, bold=True))

    hardest_primary = f"{_ev(hardest):.1f} MPH" if _ev(hardest) else "—"
    longest_primary = f"{_distance(longest)} FT" if _distance(longest) else "—"
    spotlight(
        hardest,
        x=48,
        label="HARDEST HOME RUN",
        primary=hardest_primary,
        accent=ORANGE,
        category="hardest",
    )
    spotlight(
        longest,
        x=624,
        label="LONGEST HOME RUN",
        primary=longest_primary,
        accent=FOREST,
        category="longest",
    )
    draw.line((600, 156, 600, 420), fill=HAIRLINE, width=1)
    draw.line((48, 448, 1152, 448), fill=HAIRLINE, width=1)

    player_detail = (
        player_leaders[0].upper()
        if len(player_leaders) == 1
        else f"{len(player_leaders)} HITTERS TIED"
    )
    team_detail = team_leaders[0] if len(team_leaders) == 1 else f"{len(team_leaders)} TEAMS TIED"
    stats = (
        ("PLAYER HR HIGH", f"{player_high} HR", player_detail),
        ("TEAM HR HIGH", f"{team_high} HR", team_detail),
        ("105+ MPH HOME RUNS", str(hard_hr), "HOME RUNS"),
        ("420+ FT HOME RUNS", str(long_hr), "HOME RUNS"),
    )
    stat_w = 276
    for index, (label, value, detail) in enumerate(stats):
        x = 48 + index * stat_w
        if index:
            draw.line((x - 19, 472, x - 19, 582), fill=HAIRLINE, width=1)
        draw.text((x, 472), label, fill=LABEL, font=_font(13, bold=True))
        draw.text((x, 504), value, fill=INK, font=_font(31, bold=True))
        detail_face = _font(11, bold=True)
        detail = _fit_text(draw, detail, detail_face, stat_w - 30)
        draw.text((x, 553), detail, fill=FOREST, font=detail_face)

    _draw_footer(canvas, draw)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(out_path, "PNG")
    return out_path


render_hr_tracker_clean = render_hr_tracker_image
