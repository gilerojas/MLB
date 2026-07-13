#!/usr/bin/env python3
"""Render a Mallitalytics Home Run Derby power-radar comparison card."""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.mallitalytics_style import (
    MALLITALYTICS,
    load_jetbrains_mono,
    load_montserrat,
)
from src.mlb_headshot import neutralize_mlb_headshot_background


WIDTH = 1200
HEIGHT = 675


@dataclass(frozen=True)
class DerbyHitter:
    player_id: int
    name: str
    team: str
    hr: int
    avg_ev: float
    max_ev: float
    barrel_pct: float
    hard_hit_pct: float
    avg_hr_distance: float
    avg_bat_speed: float
    bs75_pct: float
    avg_hr_ev: float
    avg_hr_la: float
    max_hr_distance: float
    competitive_swings: int


# Snapshot through games of July 12, 2026. Values combine live MLB season totals
# with contact-quality events from the production Statcast warehouse.
FIELD = (
    DerbyHitter(656941, "Kyle Schwarber", "PHI", 32, 93.4, 113.2, 19.4, 53.4, 403.8, 77.1, 76.0, 106.6, 28.7, 460.0, 696),
    DerbyHitter(700250, "Ben Rice", "NYY", 29, 92.1, 110.9, 15.3, 47.6, 389.1, 72.6, 24.7, 103.6, 29.2, 433.0, 591),
    DerbyHitter(691406, "Junior Caminero", "TB", 28, 93.2, 116.9, 13.6, 51.6, 407.6, 79.9, 88.2, 106.5, 28.1, 463.0, 595),
    DerbyHitter(691023, "Jordan Walker", "STL", 22, 94.2, 116.6, 14.1, 51.5, 406.4, 79.2, 85.9, 107.8, 26.6, 459.0, 654),
    DerbyHitter(808959, "Munetaka Murakami", "CWS", 20, 94.1, 114.1, 20.3, 59.3, 408.5, 75.3, 56.1, 107.4, 32.5, 451.0, 417),
    DerbyHitter(575929, "Willson Contreras", "BOS", 20, 90.6, 114.4, 14.3, 47.1, 407.4, 76.9, 71.4, 106.5, 31.7, 449.0, 616),
    DerbyHitter(547180, "Bryce Harper", "PHI", 20, 90.1, 113.5, 11.3, 46.2, 399.8, 74.3, 48.3, 105.5, 28.8, 457.0, 726),
    DerbyHitter(695506, "Jac Caglianone", "KC", 15, 92.9, 116.1, 14.7, 56.0, 414.4, 77.3, 76.3, 108.0, 29.0, 444.0, 607),
)


AXES = (
    ("HR", "hr", 10.0, 35.0, "{:.0f}"),
    ("AVG EV", "avg_ev", 88.0, 96.0, "{:.1f}"),
    ("MAX EV", "max_ev", 108.0, 118.0, "{:.1f}"),
    ("BRL%", "barrel_pct", 8.0, 22.0, "{:.1f}"),
    ("HH%", "hard_hit_pct", 40.0, 60.0, "{:.1f}"),
    ("HR DIST", "avg_hr_distance", 380.0, 420.0, "{:.0f}"),
)


def rgb(key: str) -> tuple[int, int, int]:
    value = MALLITALYTICS[key].lstrip("#")
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4))


def rgba(color: tuple[int, int, int], alpha: int) -> tuple[int, int, int, int]:
    return color[0], color[1], color[2], alpha


def lerp(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))


def text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def fit_font(draw: ImageDraw.ImageDraw, text: str, max_width: int, start: int, minimum: int) -> ImageFont.ImageFont:
    for size in range(start, minimum - 1, -1):
        font = load_montserrat(size, bold=True)
        if text_width(draw, text, font) <= max_width:
            return font
    return load_montserrat(minimum, bold=True)


def fetch_headshot(player_id: int, size: int = 88) -> Image.Image | None:
    url = (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"d_people:generic:headshot:67:current.png/w_320,q_auto:best/"
        f"v1/people/{player_id}/headshot/silo/current.png"
    )
    try:
        response = requests.get(url, timeout=12)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content)).convert("RGBA")
        image = neutralize_mlb_headshot_background(image, replace_rgb=rgb("off_white"))
        return image.resize((size, size), Image.Resampling.LANCZOS)
    except (requests.RequestException, OSError, ValueError):
        return None


def circular_avatar(player: DerbyHitter, size: int) -> Image.Image:
    off = rgb("off_white")
    slate = rgb("slate")
    avatar = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    image = fetch_headshot(player.player_id, size - 8)
    mask = Image.new("L", (size - 8, size - 8), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 9, size - 9), fill=255)
    if image is not None:
        avatar.paste(image, (4, 4), mask)
    else:
        ImageDraw.Draw(avatar).ellipse((4, 4, size - 5, size - 5), fill=off)
    ImageDraw.Draw(avatar).ellipse((2, 2, size - 3, size - 3), outline=lerp(slate, off, 0.35), width=3)
    return avatar


def radar_points(player: DerbyHitter, cx: float, cy: float, radius: float) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for index, (_, field, floor, ceiling, _) in enumerate(AXES):
        value = float(getattr(player, field))
        score = max(0.08, min(1.0, (value - floor) / (ceiling - floor)))
        angle = -math.pi / 2 + index * 2 * math.pi / len(AXES)
        points.append((cx + radius * score * math.cos(angle), cy + radius * score * math.sin(angle)))
    return points


def polygon_points(cx: float, cy: float, radius: float, count: int = 6) -> list[tuple[float, float]]:
    return [
        (
            cx + radius * math.cos(-math.pi / 2 + index * 2 * math.pi / count),
            cy + radius * math.sin(-math.pi / 2 + index * 2 * math.pi / count),
        )
        for index in range(count)
    ]


def draw_radar_card(
    canvas: Image.Image,
    draw: ImageDraw.ImageDraw,
    player: DerbyHitter,
    box: tuple[int, int, int, int],
) -> None:
    x0, y0, x1, y1 = box
    off = rgb("off_white")
    ink = rgb("dark_teal")
    slate = rgb("slate")
    orange = rgb("burnt_orange")
    gold = rgb("muted_gold")
    cream = rgb("warm_cream")

    draw.rounded_rectangle((x0 + 4, y0 + 5, x1 + 4, y1 + 5), radius=12, fill=lerp(cream, slate, 0.12))
    draw.rounded_rectangle(box, radius=12, fill=off, outline=lerp(slate, cream, 0.52), width=2)

    avatar = circular_avatar(player, 66)
    canvas.paste(avatar, (x0 + 14, y0 + 12), avatar)

    name_font = fit_font(draw, player.name, x1 - x0 - 98, 18, 14)
    draw.text((x0 + 91, y0 + 15), player.name, fill=ink, font=name_font)
    meta_font = load_montserrat(13, bold=True)
    draw.text((x0 + 92, y0 + 43), player.team, fill=orange, font=meta_font)

    cx = (x0 + x1) / 2
    cy = y0 + 148
    radius = 56
    grid = lerp(slate, off, 0.68)
    axis = lerp(slate, off, 0.48)
    for level in (0.33, 0.66, 1.0):
        draw.polygon(polygon_points(cx, cy, radius * level), outline=grid, width=1)
    for px, py in polygon_points(cx, cy, radius):
        draw.line((cx, cy, px, py), fill=axis, width=1)

    polygon = radar_points(player, cx, cy, radius)
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    overlay_draw.polygon(polygon, fill=rgba(orange, 64), outline=rgba(orange, 245), width=3)
    for px, py in polygon:
        overlay_draw.ellipse((px - 2.5, py - 2.5, px + 2.5, py + 2.5), fill=rgba(gold, 255), outline=rgba(ink, 230), width=1)
    canvas.alpha_composite(overlay)

    label_font = load_jetbrains_mono(9)
    value_font = load_jetbrains_mono(9, bold=True)
    for index, (label, field, _, _, value_format) in enumerate(AXES):
        angle = -math.pi / 2 + index * 2 * math.pi / len(AXES)
        lx = cx + (radius + 16) * math.cos(angle)
        ly = cy + (radius + 16) * math.sin(angle)
        value = value_format.format(float(getattr(player, field)))
        label_text = f"{label} "
        label_width = text_width(draw, label_text, label_font)
        value_width = text_width(draw, value, value_font)
        total_width = label_width + value_width
        if math.cos(angle) > 0.25:
            tx = lx
        elif math.cos(angle) < -0.25:
            tx = lx - total_width
        else:
            tx = lx - total_width / 2
        if math.sin(angle) < -0.65:
            ty = ly - 7
        elif math.sin(angle) > 0.65:
            ty = ly - 2
        else:
            ty = ly - 5
        draw.text((tx, ty), label_text, fill=slate, font=label_font)
        draw.text((tx + label_width, ty), value, fill=orange, font=value_font)


def render(out_path: Path) -> Path:
    cream = rgb("warm_cream")
    ink = rgb("dark_teal")
    slate = rgb("slate")
    orange = rgb("burnt_orange")
    off = rgb("off_white")

    canvas = Image.new("RGBA", (WIDTH, HEIGHT), (*cream, 255))
    draw = ImageDraw.Draw(canvas)
    draw.rectangle((0, 0, WIDTH, 10), fill=orange)

    title_font = load_montserrat(39, bold=True)
    subtitle_font = load_montserrat(17)
    kicker_font = load_montserrat(13, bold=True)
    draw.text((44, 38), "2026 HOME RUN DERBY", fill=ink, font=title_font)
    draw.text((46, 88), "POWER RADAR  ·  SEASON THROUGH JUL 12", fill=slate, font=subtitle_font)
    draw.rounded_rectangle((956, 42, 1156, 100), radius=10, fill=off, outline=lerp(slate, cream, 0.5), width=2)
    draw.text((976, 57), "COMMON SCALE", fill=slate, font=kicker_font)
    draw.text((976, 77), "ALL 8 HITTERS", fill=orange, font=kicker_font)

    margin_x = 44
    gap_x = 14
    card_w = (WIDTH - margin_x * 2 - gap_x * 3) // 4
    card_h = 232
    first_y = 126
    gap_y = 14
    for index, player in enumerate(FIELD):
        col = index % 4
        row = index // 4
        x0 = margin_x + col * (card_w + gap_x)
        y0 = first_y + row * (card_h + gap_y)
        draw_radar_card(canvas, draw, player, (x0, y0, x0 + card_w, y0 + card_h))

    footer_y = HEIGHT - 42
    draw.line((44, footer_y - 13, WIDTH - 44, footer_y - 13), fill=lerp(slate, cream, 0.58), width=1)
    footer_font = load_montserrat(13)
    draw.text((44, footer_y), "Data: MLB / Statcast  ·  Fixed baseball-value ranges; farther out = more power", fill=slate, font=footer_font)
    handle = "@Mallitalytics"
    draw.text((WIDTH - 44 - text_width(draw, handle, footer_font), footer_y), handle, fill=slate, font=footer_font)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(out_path, "PNG", optimize=True)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("outputs/hr_derby_2026_power_radar.png"),
    )
    args = parser.parse_args()
    print(render(args.out))


if __name__ == "__main__":
    main()
