#!/usr/bin/env python3
"""Render eight Mallitalytics player spotlights for the 2026 HR Derby thread."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.hr_derby_power_radar import (  # noqa: E402
    FIELD,
    DerbyHitter,
    circular_avatar,
    lerp,
    rgb,
    text_width,
)
from src.mallitalytics_style import load_jetbrains_mono, load_montserrat  # noqa: E402


WIDTH = 1200
HEIGHT = 675

HIGHLIGHT_STOPS = (
    (0.00, (83, 111, 142)),   # cold slate blue
    (0.25, (103, 150, 153)),  # muted teal
    (0.50, (171, 161, 145)),  # warm neutral
    (0.75, (218, 164, 57)),   # gold
    (1.00, (178, 66, 48)),    # burnt red
)


@dataclass(frozen=True)
class Metric:
    label: str
    field: str
    floor: float
    ceiling: float
    value_format: str


@dataclass(frozen=True)
class Spotlight:
    player_id: int
    hero_label: str
    hero_field: str
    hero_format: str
    insight: str
    bars: tuple[Metric, Metric, Metric]
    supporting: tuple[Metric, Metric, Metric]


HR = Metric("HOME RUNS", "hr", 10, 35, "{:.0f}")
AVG_EV = Metric("AVG EV (MPH)", "avg_ev", 88, 96, "{:.1f}")
MAX_EV = Metric("MAX EV (MPH)", "max_ev", 108, 118, "{:.1f}")
BARREL = Metric("BARREL %", "barrel_pct", 8, 22, "{:.1f}")
HARD_HIT = Metric("HARD-HIT %", "hard_hit_pct", 40, 60, "{:.1f}")
BAT_SPEED = Metric("AVG BAT SPEED (MPH)", "avg_bat_speed", 68, 80, "{:.1f}")
BS75 = Metric("FAST SWING RATE (%)", "bs75_pct", 10, 90, "{:.1f}")
AVG_HR_EV = Metric("AVG HR EV (MPH)", "avg_hr_ev", 102, 109, "{:.1f}")
AVG_HR_LA = Metric("AVG HR LA (DEG)", "avg_hr_la", 20, 36, "{:.1f}")
AVG_HR_DIST = Metric("AVG HR DIST (FT)", "avg_hr_distance", 380, 420, "{:.0f}")
MAX_HR_DIST = Metric("LONGEST HR (FT)", "max_hr_distance", 420, 470, "{:.0f}")


SPOTLIGHTS = (
    Spotlight(
        656941,
        "HOME RUNS",
        "hr",
        "{:.0f}",
        "THE FIELD'S PROVEN\nVOLUME POWER",
        (HR, BARREL, AVG_HR_DIST),
        (AVG_HR_EV, AVG_HR_LA, BAT_SPEED),
    ),
    Spotlight(
        700250,
        "HOME RUNS",
        "hr",
        "{:.0f}",
        "29 HOMERS WITHOUT\nTOP-END BAT SPEED",
        (HR, BAT_SPEED, AVG_HR_DIST),
        (BARREL, AVG_HR_EV, AVG_HR_LA),
    ),
    Spotlight(
        691406,
        "AVG BAT SPEED (MPH)",
        "avg_bat_speed",
        "{:.1f}",
        "THE FASTEST BAT\nIN THIS FIELD",
        (BAT_SPEED, BS75, MAX_EV),
        (HR, AVG_HR_EV, MAX_HR_DIST),
    ),
    Spotlight(
        691023,
        "AVG EV (MPH)",
        "avg_ev",
        "{:.1f}",
        "THE FIELD'S HARDEST\nAVERAGE CONTACT",
        (AVG_EV, BAT_SPEED, MAX_EV),
        (HR, AVG_HR_EV, AVG_HR_DIST),
    ),
    Spotlight(
        808959,
        "HARD-HIT %",
        "hard_hit_pct",
        "{:.1f}",
        "THE BEST CONTACT-QUALITY\nBLEND IN THE FIELD",
        (HARD_HIT, BARREL, AVG_EV),
        (HR, AVG_HR_EV, AVG_HR_DIST),
    ),
    Spotlight(
        575929,
        "AVG HR DIST (FT)",
        "avg_hr_distance",
        "{:.0f}",
        "THE DISTANCE PLAYS\nABOVE THE AVG EV",
        (AVG_HR_DIST, AVG_HR_EV, BAT_SPEED),
        (HR, AVG_HR_LA, MAX_HR_DIST),
    ),
    Spotlight(
        547180,
        "LONGEST HR (FT)",
        "max_hr_distance",
        "{:.0f}",
        "HOME CROWD.\n457-FOOT CEILING.",
        (MAX_HR_DIST, HR, AVG_HR_DIST),
        (MAX_EV, AVG_HR_EV, BAT_SPEED),
    ),
    Spotlight(
        695506,
        "AVG HR DIST (FT)",
        "avg_hr_distance",
        "{:.0f}",
        "THE DEEPEST AVERAGE\nHOMER IN THE FIELD",
        (AVG_HR_DIST, AVG_HR_EV, MAX_EV),
        (HR, HARD_HIT, BAT_SPEED),
    ),
)


def metric_value(player: DerbyHitter, metric: Metric) -> float:
    return float(getattr(player, metric.field))


def score(metric: Metric, value: float) -> float:
    return max(0.0, min(1.0, (value - metric.floor) / (metric.ceiling - metric.floor)))


def gradient_color(value: float) -> tuple[int, int, int]:
    value = max(0.0, min(1.0, value))
    for (left_pos, left), (right_pos, right) in zip(HIGHLIGHT_STOPS, HIGHLIGHT_STOPS[1:]):
        if value <= right_pos:
            local = (value - left_pos) / (right_pos - left_pos)
            return lerp(left, right, local)
    return HIGHLIGHT_STOPS[-1][1]


def fit_font(
    draw: ImageDraw.ImageDraw,
    text: str,
    max_width: int,
    start: int,
    minimum: int,
) -> ImageFont.ImageFont:
    for size in range(start, minimum - 1, -1):
        font = load_montserrat(size, bold=True)
        if all(text_width(draw, line, font) <= max_width for line in text.splitlines()):
            return font
    return load_montserrat(minimum, bold=True)


def draw_bar(
    draw: ImageDraw.ImageDraw,
    player: DerbyHitter,
    metric: Metric,
    x: int,
    y: int,
    width: int,
) -> None:
    slate = rgb("slate")
    off = rgb("off_white")
    cream = rgb("warm_cream")
    value = metric_value(player, metric)
    normalized = score(metric, value)
    color = gradient_color(normalized)

    label_font = load_montserrat(13, bold=True)
    value_font = load_jetbrains_mono(18, bold=True)
    value_text = metric.value_format.format(value)
    draw.text((x, y), metric.label, fill=slate, font=label_font)
    draw.text((x + width - text_width(draw, value_text, value_font), y - 3), value_text, fill=color, font=value_font)

    track_y = y + 28
    draw.rounded_rectangle((x, track_y, x + width, track_y + 11), radius=5, fill=lerp(cream, slate, 0.16))
    fill_width = max(9, round(width * normalized))
    draw.rounded_rectangle((x, track_y, x + fill_width, track_y + 11), radius=5, fill=color)
    draw.ellipse((x + fill_width - 6, track_y - 1, x + fill_width + 6, track_y + 12), fill=off, outline=color, width=3)


def draw_supporting_stat(
    draw: ImageDraw.ImageDraw,
    player: DerbyHitter,
    metric: Metric,
    x: int,
    y: int,
    width: int,
) -> None:
    slate = rgb("slate")
    value = metric_value(player, metric)
    color = gradient_color(score(metric, value))
    label_font = load_montserrat(12, bold=True)
    value_font = load_jetbrains_mono(25, bold=True)
    value_text = metric.value_format.format(value)
    draw.text((x, y), metric.label, fill=slate, font=label_font)
    draw.text((x, y + 22), value_text, fill=color, font=value_font)
    draw.line((x + width - 1, y, x + width - 1, y + 56), fill=lerp(slate, rgb("warm_cream"), 0.60), width=1)


def render_spotlight(
    player: DerbyHitter,
    spotlight: Spotlight,
    thread_number: int,
    out_path: Path,
) -> Path:
    cream = rgb("warm_cream")
    off = rgb("off_white")
    ink = rgb("dark_teal")
    slate = rgb("slate")
    orange = rgb("burnt_orange")
    gold = rgb("muted_gold")

    image = Image.new("RGBA", (WIDTH, HEIGHT), (*cream, 255))
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, WIDTH, 10), fill=orange)

    kicker_font = load_montserrat(14, bold=True)
    title_font = load_montserrat(38, bold=True)
    sub_font = load_montserrat(15)
    draw.text((48, 42), "DERBY POWER FILE", fill=slate, font=kicker_font)
    draw.text((48, 65), player.name.upper(), fill=ink, font=fit_font(draw, player.name.upper(), 760, 38, 28))
    draw.text((50, 111), f"{player.team}  ·  2026 SEASON THROUGH JUL 12", fill=slate, font=sub_font)
    count = f"{thread_number} / 9"
    draw.text((WIDTH - 48 - text_width(draw, count, title_font), 56), count, fill=orange, font=title_font)

    left = (48, 150, 382, 588)
    draw.rounded_rectangle((left[0] + 7, left[1] + 8, left[2] + 7, left[3] + 8), radius=18, fill=lerp(cream, slate, 0.16))
    draw.rounded_rectangle(left, radius=18, fill=ink)

    avatar = circular_avatar(player, 238)
    image.paste(avatar, (96, 176), avatar)
    draw.text((82, 435), spotlight.hero_label, fill=lerp(off, slate, 0.28), font=load_montserrat(14, bold=True))
    hero = spotlight.hero_format.format(float(getattr(player, spotlight.hero_field)))
    hero_font = load_jetbrains_mono(76, bold=True)
    draw.text((80, 459), hero, fill=lerp(gold, off, 0.15), font=hero_font)

    right_x = 430
    insight_font = fit_font(draw, spotlight.insight, 710, 31, 24)
    for line_index, line in enumerate(spotlight.insight.splitlines()):
        draw.text((right_x, 160 + line_index * 38), line, fill=ink, font=insight_font)
    draw.line((right_x, 245, 1152, 245), fill=lerp(slate, cream, 0.55), width=2)

    for index, metric in enumerate(spotlight.bars):
        draw_bar(draw, player, metric, right_x, 272 + index * 72, 704)

    strip_y = 493
    strip_w = 220
    for index, metric in enumerate(spotlight.supporting):
        draw_supporting_stat(draw, player, metric, right_x + index * 236, strip_y, strip_w)

    note_font = load_montserrat(12)
    footer_y = HEIGHT - 41
    draw.line((48, footer_y - 14, WIDTH - 48, footer_y - 14), fill=lerp(slate, cream, 0.58), width=1)
    draw.text((48, footer_y), f"Data: MLB / Statcast  ·  Bat speed: {player.competitive_swings} competitive swings", fill=slate, font=note_font)
    handle = "@Mallitalytics"
    draw.text((WIDTH - 48 - text_width(draw, handle, note_font), footer_y), handle, fill=slate, font=note_font)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(out_path, "PNG", optimize=True)
    return out_path


def slugify(name: str) -> str:
    return "_".join(name.lower().replace(".", "").split())


def render_all(out_dir: Path) -> list[Path]:
    players = {player.player_id: player for player in FIELD}
    paths: list[Path] = []
    for index, spotlight in enumerate(SPOTLIGHTS, start=2):
        player = players[spotlight.player_id]
        path = out_dir / f"{index:02d}_{slugify(player.name)}.png"
        paths.append(render_spotlight(player, spotlight, index, path))
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("outputs/hr_derby_2026_thread"),
    )
    args = parser.parse_args()
    for path in render_all(args.out_dir):
        print(path)


if __name__ == "__main__":
    main()
