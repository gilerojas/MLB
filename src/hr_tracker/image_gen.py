"""
Generate Mallitalytics-styled HR Tracker card image (1200x675 for X).

Uses Pillow; colors and sizes from mallitalytics_style.
Improved: centered layout, top 6 HRs only, 🔥/💨 badges, short venue & last names, legible metadata.
"""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from ..mallitalytics_style import (
    MALLITALYTICS,
    FONT_SIZES,
    CARD_WIDTH_X,
    CARD_HEIGHT_X,
)

# Show at most this many HRs on the card; rest as "+ N more"
MAX_HR_ROWS = 6

# Subtitle and footer font sizes (slightly larger than fine_print for legibility)
SUBTITLE_SIZE = 14
META_SIZE = 15
FOOTER_SIZE = 13


def _last_name(full_name: str) -> str:
    if not full_name:
        return "?"
    parts = full_name.strip().split()
    return parts[-1] if len(parts) > 1 else full_name


def _short_venue(venue: str, max_words: int = 2) -> str:
    if not venue:
        return ""
    words = venue.strip().split()
    if len(words) <= max_words:
        return venue.strip()
    return " ".join(words[:max_words])


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Load a font; fallback to default if Inter/JetBrains not found."""
    names = []
    if bold:
        names = [
            "Inter-Bold.ttf",
            "Inter Bold.ttf",
            "Roboto-Bold.ttf",
            "Arial Bold.ttf",
            "arialbd.ttf",
        ]
    else:
        names = [
            "JetBrainsMono-Regular.ttf",
            "FiraCode-Regular.ttf",
            "Inter-Regular.ttf",
            "Arial.ttf",
            "arial.ttf",
        ]
    import sys
    search_dirs = []
    if sys.platform == "darwin":
        search_dirs = [
            Path("/System/Library/Fonts"),
            Path("/Library/Fonts"),
            Path.home() / "Library/Fonts",
        ]
    else:
        search_dirs = [
            Path("/usr/share/fonts/truetype"),
            Path("/usr/share/fonts"),
        ]
    for d in search_dirs:
        if not d.exists():
            continue
        for f in d.rglob("*.ttf"):
            if any(n.lower() in f.name.lower() for n in names):
                try:
                    return ImageFont.truetype(str(f), size)
                except (OSError, IOError):
                    pass
    return ImageFont.load_default()


def render_hr_tracker_image(
    hrs: list[dict],
    date_str: str,
    out_path: Path,
) -> Path:
    """
    Draw HR Tracker card and save to out_path.

    hrs: list of dicts with batter, pitcher, ev_mph, distance_ft, stadium, team_abbrev.
    date_str: YYYY-MM-DD.
    Shows top MAX_HR_ROWS with 🔥 longest, 💨 highest EV; centered layout; short venue & last names.
    """
    from datetime import datetime

    w, h = CARD_WIDTH_X, CARD_HEIGHT_X
    img = Image.new("RGB", (w, h), MALLITALYTICS["charcoal"])
    draw = ImageDraw.Draw(img)

    title_size = FONT_SIZES["card_title"]
    name_size = 22  # Slightly smaller than before so stats pop
    stat_size = FONT_SIZES["key_stat"]

    font_title = _load_font(title_size, bold=True)
    font_subtitle = _load_font(SUBTITLE_SIZE, bold=False)
    font_name = _load_font(name_size, bold=True)
    font_stat = _load_font(stat_size, bold=True)
    font_meta = _load_font(META_SIZE, bold=False)
    font_footer = _load_font(FOOTER_SIZE, bold=False)

    try:
        day_fmt = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        day_fmt = date_str

    # Content width and horizontal center (use full width with padding)
    padding_x = 56
    content_width = w - 2 * padding_x
    margin_x = padding_x

    # Title + subtitle
    title = f"HR Tracker — {day_fmt}"
    draw.text((margin_x, 28), title, fill=MALLITALYTICS["off_white"], font=font_title)
    subtitle = "Top HRs by exit velocity & distance"
    draw.text((margin_x, 28 + 40), subtitle, fill=MALLITALYTICS["slate"], font=font_subtitle)

    # Show top MAX_HR_ROWS, sorted by EV descending so "hardest hit" is first and badges are in view
    by_ev = sorted(hrs, key=lambda r: (r.get("ev_mph") or 0), reverse=True)
    display_hrs = by_ev[:MAX_HR_ROWS]

    # Among displayed rows: which index has longest distance, which has top EV (always 0 after sort)
    best_dist_in_view = max((r.get("distance_ft") or 0) for r in display_hrs) if display_hrs else -1
    show_longest = next((i for i, r in enumerate(display_hrs) if (r.get("distance_ft") or 0) == best_dist_in_view), None)
    show_top_ev = 0 if display_hrs else None  # First row is highest EV

    y = 28 + 40 + 24  # Below subtitle
    row_height = 98
    separator_color = MALLITALYTICS["slate"]

    if not hrs:
        draw.text(
            (margin_x, y),
            "No home runs recorded.",
            fill=MALLITALYTICS["slate"],
            font=font_meta,
        )
    else:
        for i, r in enumerate(display_hrs):
            batter = r.get("batter", "?")
            team = r.get("team_abbrev", "")
            ev = r.get("ev_mph")
            dist = r.get("distance_ft")
            stadium = _short_venue(r.get("stadium", ""))
            pitcher = _last_name(r.get("pitcher", "?"))
            hr_in_stage = r.get("hr_in_stage")
            stage = r.get("stage", "")

            stage_abbrev = {"spring_training": "ST", "regular_season": "RS"}.get(stage, "")
            if team:
                name_str = f"{_last_name(batter)} ({team})"
            else:
                name_str = _last_name(batter)
            if hr_in_stage is not None and stage_abbrev:
                name_str += f" ({hr_in_stage} {stage_abbrev})"
            elif hr_in_stage is not None:
                name_str += f" ({hr_in_stage})"

            # Badges: 🔥 longest, 💨 top EV
            badges = ""
            if i == show_longest:
                badges += "🔥 "
            if i == show_top_ev:
                badges += "💨 "
            if badges:
                draw.text((margin_x, y), badges, fill=MALLITALYTICS["burnt_orange"], font=font_meta)
                name_x = margin_x + 44  # Space for 2 emoji
            else:
                name_x = margin_x

            draw.text((name_x, y), name_str, fill=MALLITALYTICS["off_white"], font=font_name)

            stat_parts = []
            if ev is not None:
                stat_parts.append(f"{ev:.1f} mph")
            if dist is not None:
                stat_parts.append(f"{int(dist)} ft")
            stat_str = ", ".join(stat_parts) if stat_parts else "—"
            draw.text((name_x, y + 28), stat_str, fill=MALLITALYTICS["burnt_orange"], font=font_stat)

            meta_parts = []
            if stadium:
                meta_parts.append(f"@ {stadium}")
            if pitcher:
                meta_parts.append(f"vs {pitcher}")
            if meta_parts:
                meta_str = " · ".join(meta_parts)
                draw.text(
                    (name_x, y + 28 + 42),
                    meta_str,
                    fill=MALLITALYTICS["off_white"],  # Higher contrast than slate
                    font=font_meta,
                )

            y += row_height

            # Subtle separator line between rows (not after last)
            if i < len(display_hrs) - 1:
                sep_y = y - 8
                draw.line([(margin_x, sep_y), (w - margin_x, sep_y)], fill=separator_color, width=1)

        remaining = len(hrs) - len(display_hrs)
        if remaining > 0:
            draw.text(
                (margin_x, y + 8),
                f"+ {remaining} more",
                fill=MALLITALYTICS["slate"],
                font=font_meta,
            )

    # Footer: more visible
    footer_text = "@Mallitalytics"
    draw.text(
        (w - padding_x - 140, h - 36),
        footer_text,
        fill=MALLITALYTICS["slate"],
        font=font_footer,
    )

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path
