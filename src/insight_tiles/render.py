"""Mallitalytics branded image renderer for reusable Insights tiles."""

from __future__ import annotations

import math
import re
from datetime import datetime
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

try:
    import requests
except Exception:  # pragma: no cover - optional in offline render tests
    requests = None

try:
    from ..mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
    from ..mlb_headshot import neutralize_mlb_headshot_background
except ImportError:
    import sys

    _root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_root))
    from src.mallitalytics_style import CARD_HEIGHT_X, CARD_WIDTH_X, MALLITALYTICS
    from src.mlb_headshot import neutralize_mlb_headshot_background


_EXCLUDED_KEYS = {
    "player_name",
    "player_id",
    "pitcher_name",
    "pitcher_id",
    "game_pk",
    "game_date",
    "description",
    "pitch_name",
}

_PORTRAIT_LEADER_KEYS = {
    "home_run_leaders",
    "ops_leaders",
    "strikeout_leaders",
    "era_leaders",
}

_ESPN_LOGOS = {
    "ARI": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos", "CHC": "chc",
    "CWS": "chw", "CIN": "cin", "CLE": "cle", "COL": "col", "DET": "det",
    "HOU": "hou", "KC": "kc", "LAA": "laa", "LAD": "lad", "MIA": "mia",
    "MIL": "mil", "MIN": "min", "NYM": "nym", "NYY": "nyy", "OAK": "oak",
    "ATH": "oak", "PHI": "phi", "PIT": "pit", "SD": "sd", "SEA": "sea",
    "SF": "sf", "STL": "stl", "TB": "tb", "TEX": "tex", "TOR": "tor", "WSH": "wsh",
}


def _hex_to_rgb(hex_s: str) -> tuple[int, int, int]:
    h = hex_s.strip().lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _lerp(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    import sys as _sys

    if _sys.platform == "darwin":
        paths = (
            "/Library/Fonts/Montserrat-Bold.ttf",
            "/Library/Fonts/SpaceGrotesk-Bold.ttf",
            "/Library/Fonts/Arial Bold.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        ) if bold else (
            "/Library/Fonts/Montserrat-Regular.ttf",
            "/Library/Fonts/SpaceGrotesk-Regular.ttf",
            "/Library/Fonts/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        )
    else:
        paths = (
            "/usr/share/fonts/truetype/montserrat/Montserrat-Bold.ttf",
            "/usr/share/fonts/truetype/space-grotesk/SpaceGrotesk-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        ) if bold else (
            "/usr/share/fonts/truetype/montserrat/Montserrat-Regular.ttf",
            "/usr/share/fonts/truetype/space-grotesk/SpaceGrotesk-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
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


def _fit_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    s = str(text)
    if _text_w(draw, s, font) <= max_width:
        return s
    ell = "..."
    while s and _text_w(draw, s + ell, font) > max_width:
        s = s[:-1]
    return (s + ell) if s else ell


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = str(text or "").split()
    if not words:
        return []
    lines: list[str] = []
    cur = words[0]
    for word in words[1:]:
        nxt = f"{cur} {word}"
        if _text_w(draw, nxt, font) <= max_width:
            cur = nxt
        else:
            lines.append(cur)
            cur = word
    lines.append(cur)
    return lines


def _headshot_url(player_id: int, size: int) -> str:
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"d_people:generic:headshot:67:current.png/w_{size},q_auto:best/"
        f"v1/people/{player_id}/headshot/silo/current.png"
    )


@lru_cache(maxsize=512)
def _fetch_headshot(player_id: int, size: int) -> Image.Image | None:
    if requests is None:
        return None
    try:
        resp = requests.get(_headshot_url(player_id, max(160, size * 3)), timeout=8)
        resp.raise_for_status()
        return Image.open(BytesIO(resp.content)).convert("RGBA")
    except Exception:
        return None


@lru_cache(maxsize=128)
def _fetch_team_logo(team_abbrev: str, size: int) -> Image.Image | None:
    if requests is None:
        return None
    abbr = str(team_abbrev or "").strip().upper()
    if not abbr:
        return None
    key = _ESPN_LOGOS.get(abbr, abbr.lower())
    url = (
        "https://a.espncdn.com/combiner/i?"
        f"img=/i/teamlogos/mlb/500/scoreboard/{key}.png&h={size}&w={size}"
    )
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        logo = Image.open(BytesIO(resp.content)).convert("RGBA")
        if logo.width < 20 or logo.height < 20:
            return None
        return logo
    except Exception:
        return None


def _smooth_circle_mask(size: int) -> Image.Image:
    scale = 4
    mask = Image.new("L", (size * scale, size * scale), 0)
    d = ImageDraw.Draw(mask)
    d.ellipse((0, 0, size * scale - 1, size * scale - 1), fill=255)
    return mask.resize((size, size), Image.Resampling.LANCZOS)


def _circle_crop(img: Image.Image, size: int, bg: tuple[int, int, int]) -> Image.Image:
    rgba = img.convert("RGBA")
    alpha = rgba.getchannel("A")
    has_transparency = alpha.getextrema()[0] < 245
    cleaned = rgba if has_transparency else neutralize_mlb_headshot_background(img, replace_rgb=bg).convert("RGBA")

    canvas = Image.new("RGBA", (size, size), bg + (0,))
    fitted = ImageOps.contain(cleaned, (int(size * 0.92), int(size * 0.92)), method=Image.Resampling.LANCZOS)
    x = (size - fitted.width) // 2
    y = max(0, size - fitted.height + 2)
    canvas.alpha_composite(fitted, (x, y))
    mask = _smooth_circle_mask(size)
    canvas.putalpha(Image.composite(canvas.getchannel("A"), Image.new("L", (size, size), 0), mask))
    return canvas


def _initials(name: str) -> str:
    parts = [p for p in re.split(r"[\s,]+", name.strip()) if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:1].upper()
    return f"{parts[0][:1]}{parts[-1][:1]}".upper()


def _avatar_image(
    row: dict[str, Any],
    size: int,
    *,
    bg: tuple[int, int, int],
    ring: tuple[int, int, int],
    ink: tuple[int, int, int],
) -> Image.Image:
    player_id = row.get("player_id")
    try:
        pid = int(player_id)
    except (TypeError, ValueError):
        pid = None

    raw = _fetch_headshot(pid, size) if pid is not None else None
    outer = size + 10
    scale = 4
    avatar_big = Image.new("RGBA", (outer * scale, outer * scale), (0, 0, 0, 0))
    d_big = ImageDraw.Draw(avatar_big)
    d_big.ellipse((0, 0, outer * scale - 1, outer * scale - 1), fill=ring + (255,))
    inset = 5 * scale
    d_big.ellipse(
        (inset, inset, outer * scale - inset - 1, outer * scale - inset - 1),
        fill=bg + (255,),
    )
    avatar = avatar_big.resize((outer, outer), Image.Resampling.LANCZOS)

    inner = Image.new("RGBA", (size, size), bg + (255,))

    if raw is not None:
        inner.alpha_composite(_circle_crop(raw, size, bg), (0, 0))
        inner.putalpha(_smooth_circle_mask(size))
        avatar.alpha_composite(inner, (5, 5))
        return avatar

    d = ImageDraw.Draw(inner)
    font = _load_font(max(18, size // 3), bold=True)
    label = _initials(str(row.get("player_name") or ""))
    box = d.textbbox((0, 0), label, font=font)
    d.text(
        ((size + 8 - (box[2] - box[0])) / 2, (size + 8 - (box[3] - box[1])) / 2 - 3),
        label,
        fill=ink + (255,),
        font=font,
    )
    inner.putalpha(_smooth_circle_mask(size))
    avatar.alpha_composite(inner, (5, 5))
    return avatar


def _team_logo_mark(row: dict[str, Any], size: int, opacity: float) -> Image.Image | None:
    logo = _fetch_team_logo(_team_label(row), int(size * 1.5))
    if logo is None:
        return None
    logo = ImageOps.contain(logo, (size, size), method=Image.Resampling.LANCZOS)
    alpha = logo.getchannel("A").point(lambda a: int(a * max(0.0, min(1.0, opacity))))
    logo.putalpha(alpha)
    return logo


def _paste_team_logo_mark(
    img: Image.Image,
    row: dict[str, Any],
    box: tuple[int, int, int, int],
    *,
    size: int,
    opacity: float,
    x_bias: float = 0.72,
    y_bias: float = 0.50,
) -> None:
    logo = _team_logo_mark(row, size, opacity)
    if logo is None:
        return
    x0, y0, x1, y1 = box
    area_w = x1 - x0
    area_h = y1 - y0
    x = int(x0 + area_w * x_bias - logo.width / 2)
    y = int(y0 + area_h * y_bias - logo.height / 2)

    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    layer.alpha_composite(logo, (x, y))
    crop_mask = Image.new("L", img.size, 0)
    ImageDraw.Draw(crop_mask).rounded_rectangle(box, radius=22, fill=255)
    layer.putalpha(Image.composite(layer.getchannel("A"), Image.new("L", img.size, 0), crop_mask))
    img.paste(layer, (0, 0), layer)


def _team_label(row: dict[str, Any]) -> str:
    for key in ("team_abbrev", "team", "team_name"):
        value = row.get(key)
        if value:
            return str(value).strip().upper()
    return "MLB"


def _title_from_raw(title: str) -> str:
    return re.split(r"\s+·\s+", str(title or "Insight"), maxsplit=1)[0].strip() or "Insight"


def _as_of_label(game_date: str, season: int | None) -> str:
    try:
        dt = datetime.strptime(str(game_date), "%Y-%m-%d")
        day = dt.strftime("%b %-d, %Y")
    except ValueError:
        day = str(game_date or "")
    return f"{season} season · as of {day}" if season else f"As of {day}"


def _season_label(season: int | None) -> str:
    return f"{season} season" if season else "Season leaders"


def _pick_stat_key(rows: list[dict[str, Any]], preferred: str | None) -> str:
    if preferred:
        return preferred
    for row in rows:
        for key in row:
            if key not in _EXCLUDED_KEYS and row.get(key) is not None:
                return key
    return "value"


def _pretty_key(key: str) -> str:
    labels = {
        "hr": "HR",
        "ops": "OPS",
        "era": "ERA",
        "strikeouts": "K",
        "avg_ev": "Avg EV",
        "max_ev": "Max EV",
        "hit_distance": "Distance",
        "barrel_pct": "Barrel%",
        "xwoba": "xwOBA",
        "xwoba_bip": "xwOBA",
        "woba_bip": "wOBA",
        "xwoba_allowed": "xwOBA",
        "woba_allowed": "wOBA",
        "luck_delta": "Delta",
        "whiff_pct": "Whiff%",
        "chase_pct": "Chase%",
        "avg_velo": "Velo",
        "avg_spin": "Spin",
        "bs75_pct": "BS75+%",
        "rv100": "RV/100",
    }
    return labels.get(key, key.replace("_", " ").title())


def _num(v: Any) -> float | None:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if math.isfinite(n) else None


def _fmt_value(key: str, value: Any) -> str:
    n = _num(value)
    if n is None:
        return "—" if value is None else str(value)
    if key in {"ops", "era"}:
        return f"{n:.3f}" if key == "ops" else f"{n:.2f}"
    if key in {"xwoba", "xwoba_bip", "woba_bip", "xwoba_allowed", "woba_allowed"}:
        return f"{n:.3f}"
    if key in {"barrel_pct", "whiff_pct", "chase_pct", "bs75_pct"}:
        return f"{n:.1f}%"
    if key in {"avg_ev", "max_ev", "avg_velo", "launch_speed"}:
        return f"{n:.1f} mph"
    if key == "hit_distance":
        return f"{round(n)} ft"
    if key == "avg_spin":
        return f"{round(n)} rpm"
    if key == "rv100":
        return f"{n:.1f}"
    if key == "luck_delta":
        return f"{n:+.3f}"
    if abs(n - round(n)) < 0.00001:
        return str(int(round(n)))
    return f"{n:.1f}"


def _row_note(row: dict[str, Any], stat_key: str) -> str:
    bits: list[str] = []
    if row.get("pitch_type"):
        bits.append(str(row["pitch_type"]))
    if row.get("pitch_name"):
        bits.append(str(row["pitch_name"]))
    if row.get("n_pitches") is not None:
        bits.append(f"{row['n_pitches']} pitches")
    elif row.get("n_bip") is not None:
        bits.append(f"{row['n_bip']} BIP")
    elif row.get("n_tracked_swings") is not None:
        bits.append(f"{row['n_tracked_swings']} tracked swings")
    if stat_key == "luck_delta":
        xw = row.get("xwoba_bip", row.get("xwoba_allowed"))
        wo = row.get("woba_bip", row.get("woba_allowed"))
        if xw is not None and wo is not None:
            bits.append(f"xwOBA {_fmt_value('xwoba', xw)} / wOBA {_fmt_value('xwoba', wo)}")
    if row.get("game_date"):
        bits.append(str(row["game_date"]))
    return " · ".join(bits)


def _draw_pill(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    *,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int],
    outline: tuple[int, int, int] | None,
    text_fill: tuple[int, int, int],
    pad_x: int = 14,
    h: int = 28,
) -> int:
    x, y = xy
    w = _text_w(draw, text, font) + pad_x * 2
    draw.rounded_rectangle((x, y, x + w, y + h), radius=h // 2, fill=fill, outline=outline, width=1)
    box = draw.textbbox((0, 0), text, font=font)
    draw.text((x + pad_x, y + (h - (box[3] - box[1])) / 2 - 1), text, fill=text_fill, font=font)
    return w


def _date_box_label(game_date: str) -> str:
    try:
        dt = datetime.strptime(str(game_date), "%Y-%m-%d")
        return dt.strftime("%b %-d, %Y").upper()
    except ValueError:
        return str(game_date or "").upper()


def _draw_rank_badge(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    rank: int,
    *,
    size: int,
    fill: tuple[int, int, int],
    text_fill: tuple[int, int, int],
    font: ImageFont.ImageFont,
) -> None:
    draw.ellipse((x, y, x + size, y + size), fill=fill)
    label = str(rank)
    box = draw.textbbox((0, 0), label, font=font)
    draw.text(
        (x + (size - (box[2] - box[0])) / 2, y + (size - (box[3] - box[1])) / 2 - 2),
        label,
        fill=text_fill,
        font=font,
    )


def _render_portrait_leader_tile(
    *,
    title: str,
    subtitle: str,
    rows: list[dict[str, Any]],
    stat_key: str,
    game_date: str,
    season: int | None,
    out_path: Path,
    insight_key: str,
) -> Path:
    width, height = CARD_WIDTH_X, CARD_HEIGHT_X
    c = {k: _hex_to_rgb(v) for k, v in MALLITALYTICS.items()}
    bg = c["warm_cream"]
    ink = c["dark_teal"]
    slate = c["slate"]
    off = c["off_white"]
    orange = c["burnt_orange"]
    gold = c["muted_gold"]
    charcoal = c["charcoal"]

    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)

    font_title = _load_font(48, bold=True)
    font_sub = _load_font(20)
    font_kicker = _load_font(13, bold=True)
    font_small = _load_font(15)
    font_name_big = _load_font(33, bold=True)
    font_name = _load_font(20, bold=True)
    font_metric_label = _load_font(15, bold=True)
    font_hr_big = _load_font(92, bold=True)
    font_hr = _load_font(43, bold=True)
    font_rank = _load_font(17, bold=True)

    # Compact header. The footer already carries the brand handle.
    draw.rectangle((0, 0, width, 10), fill=orange)

    # Header.
    draw.text((48, 54), _fit_text(draw, _title_from_raw(title).upper(), font_title, 720), fill=ink, font=font_title)
    draw.text((50, 114), _season_label(season), fill=slate, font=font_sub)
    draw.rounded_rectangle((902, 52, 1152, 138), radius=12, fill=off, outline=_lerp(slate, bg, 0.45), width=2)
    draw.text((924, 71), "AS OF", fill=slate, font=font_metric_label)
    draw.text((924, 97), _date_box_label(game_date), fill=orange, font=_load_font(27, bold=True))

    clean = rows[:5]
    if not clean:
        draw.text((52, 256), "No leaderboard rows available.", fill=slate, font=font_sub)
    else:
        hero = clean[0]
        hx, hy, hw, hh = 48, 202, 488, 284
        draw.rounded_rectangle((hx + 8, hy + 10, hx + hw + 8, hy + hh + 10), radius=22, fill=_lerp(bg, slate, 0.18))
        draw.rounded_rectangle((hx, hy, hx + hw, hy + hh), radius=22, fill=ink)
        _paste_team_logo_mark(
            img,
            hero,
            (hx, hy, hx + hw, hy + hh),
            size=330,
            opacity=0.16,
            x_bias=0.78,
            y_bias=0.52,
        )
        _draw_rank_badge(draw, hx + 30, hy + 22, 1, size=42, fill=orange, text_fill=off, font=font_rank)

        avatar = _avatar_image(hero, 166, bg=off, ring=gold, ink=ink)
        img.paste(avatar, (hx + 36, hy + 82), avatar)

        name = str(hero.get("player_name") or f"ID {hero.get('player_id', '')}")
        name_lines = _wrap_text(draw, name, font_name_big, 245)[:2]
        if len(name_lines) == 2:
            name_lines[1] = _fit_text(draw, name_lines[1], font_name_big, 245)
        else:
            name_lines = [_fit_text(draw, name, font_name_big, 245)]
        for j, line in enumerate(name_lines):
            draw.text((hx + 230, hy + 72 + j * 38), line, fill=off, font=font_name_big)
        metric = _fmt_value(stat_key, hero.get(stat_key))
        metric = metric.replace(" mph", "").replace(" ft", "")
        draw.text((hx + 230, hy + 166), metric, fill=_lerp(off, gold, 0.35), font=font_hr_big)

        grid_x, grid_y = 566, 202
        card_w, card_h = 286, 132
        gap_x, gap_y = 18, 20
        for idx, row in enumerate(clean[1:5], start=2):
            col = (idx - 2) % 2
            rrow = (idx - 2) // 2
            x = grid_x + col * (card_w + gap_x)
            y = grid_y + rrow * (card_h + gap_y)
            draw.rounded_rectangle((x + 5, y + 7, x + card_w + 5, y + card_h + 7), radius=17, fill=_lerp(bg, slate, 0.16))
            draw.rounded_rectangle((x, y, x + card_w, y + card_h), radius=17, fill=off, outline=_lerp(slate, bg, 0.42), width=2)
            _paste_team_logo_mark(
                img,
                row,
                (x, y, x + card_w, y + card_h),
                size=176,
                opacity=0.14,
                x_bias=0.78,
                y_bias=0.50,
            )
            _draw_rank_badge(draw, x + 20, y + 14, idx, size=34, fill=ink, text_fill=off, font=font_kicker)
            avatar = _avatar_image(row, 70, bg=off, ring=_lerp(slate, bg, 0.42), ink=ink)
            img.paste(avatar, (x + 22, y + 47), avatar)

            name = str(row.get("player_name") or f"ID {row.get('player_id', '')}")
            name_lines = _wrap_text(draw, name, font_name, 128)[:2]
            if len(name_lines) == 2:
                name_lines[1] = _fit_text(draw, name_lines[1], font_name, 128)
            else:
                name_lines = [_fit_text(draw, name, font_name, 128)]
            for j, line in enumerate(name_lines):
                draw.text((x + 110, y + 20 + j * 24), line, fill=ink, font=font_name)
            value = _fmt_value(stat_key, row.get(stat_key)).replace(" mph", "").replace(" ft", "")
            vx = x + card_w - 28 - _text_w(draw, value, font_hr)
            draw.text((vx, y + 77), value, fill=ink, font=font_hr)

    footer_y = height - 36
    draw.line((48, footer_y - 17, width - 48, footer_y - 17), fill=_lerp(slate, bg, 0.55), width=1)
    draw.text((48, footer_y), "Data: MLB / Statcast", fill=slate, font=font_small)
    handle = "@Mallitalytics"
    draw.text((width - 48 - _text_w(draw, handle, font_small), footer_y), handle, fill=slate, font=font_small)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path


def render_insight_tile(
    *,
    title: str,
    subtitle: str = "",
    rows: list[dict[str, Any]],
    stat_key: str | None,
    game_date: str,
    season: int | None,
    out_path: Path,
    insight_key: str = "",
) -> Path:
    stat = _pick_stat_key(rows, stat_key)
    key = str(insight_key or "").lower()
    if key in _PORTRAIT_LEADER_KEYS or (
        stat == "hr" and "home run" in _title_from_raw(title).lower()
    ):
        return _render_portrait_leader_tile(
            title=title,
            subtitle=subtitle,
            rows=rows,
            stat_key=stat,
            game_date=game_date,
            season=season,
            out_path=out_path,
            insight_key=insight_key,
        )

    width, height = CARD_WIDTH_X, CARD_HEIGHT_X
    c = {k: _hex_to_rgb(v) for k, v in MALLITALYTICS.items()}
    bg = c["warm_cream"]
    ink = c["dark_teal"]
    slate = c["slate"]
    off = c["off_white"]
    orange = c["burnt_orange"]
    gold = c["muted_gold"]

    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)
    font_brand = _load_font(18, bold=True)
    font_title = _load_font(46, bold=True)
    font_sub = _load_font(21)
    font_small = _load_font(16)
    font_head = _load_font(17, bold=True)
    font_rank = _load_font(18, bold=True)
    font_name = _load_font(25, bold=True)
    font_metric = _load_font(27, bold=True)
    font_note = _load_font(15)

    draw.rectangle((0, 0, width, 104), fill=ink)
    draw.rectangle((0, 104, width, 113), fill=orange)
    draw.text((48, 33), "MALLITALYTICS", fill=off, font=font_brand)
    draw.text((48, 151), _fit_text(draw, _title_from_raw(title), font_title, 760), fill=ink, font=font_title)
    draw.text((48, 213), _as_of_label(game_date, season), fill=slate, font=font_sub)

    metric_label = _pretty_key(stat)
    tag = _pretty_key(str(insight_key or stat)).upper()
    draw.rounded_rectangle((900, 139, 1152, 222), radius=8, fill=off, outline=_lerp(slate, bg, 0.45), width=2)
    draw.text((922, 159), "PRIMARY METRIC", fill=slate, font=font_small)
    draw.text((922, 184), metric_label, fill=orange, font=font_metric)

    sub_lines = _wrap_text(draw, subtitle, font_sub, 820)[:2]
    y_sub = 258
    for line in sub_lines:
        draw.text((48, y_sub), line, fill=slate, font=font_sub)
        y_sub += 27

    table_x, table_y = 48, 328
    table_w = width - 96
    footer_y = height - 35
    footer_line_y = footer_y - 16
    footer_gap = 12
    max_rows = 8
    n_rows = min(len(rows), max_rows)
    row_h = min(40, max(34, (footer_line_y - footer_gap - table_y) // max(1, n_rows)))
    clean_rows = rows[:n_rows]
    notes = [_row_note(row, stat) for row in clean_rows]
    has_notes = any(notes)
    metric_w = 210
    if has_notes:
        name_w = 585
        note_w = table_w - 72 - name_w - metric_w
    else:
        note_w = 0
        name_w = table_w - 72 - metric_w
    draw.rectangle((table_x, table_y - 40, table_x + table_w, table_y), fill=off)
    draw.text((table_x + 14, table_y - 29), "RK", fill=slate, font=font_head)
    draw.text((table_x + 72, table_y - 29), "PLAYER", fill=slate, font=font_head)
    draw.text((table_x + 72 + name_w, table_y - 29), metric_label.upper(), fill=slate, font=font_head)
    if has_notes:
        draw.text((table_x + 72 + name_w + metric_w, table_y - 29), "CONTEXT", fill=slate, font=font_head)
    draw.line((table_x, table_y, table_x + table_w, table_y), fill=ink, width=3)

    vals = [_num(r.get(stat)) for r in clean_rows]
    scale_vals = [abs(v) if stat == "luck_delta" and v is not None else v for v in vals]
    finite = [v for v in vals if v is not None]
    finite_scale = [v for v in scale_vals if v is not None]
    lo, hi = (min(finite_scale), max(finite_scale)) if finite_scale else (0.0, 1.0)
    for i, row in enumerate(clean_rows):
        y = table_y + i * row_h
        base_fill = _lerp(bg, off, 0.48) if i % 2 else bg
        draw.rectangle((table_x, y, table_x + table_w, y + row_h), fill=base_fill)
        if i == 0:
            draw.rectangle((table_x, y, table_x + table_w, y + row_h), fill=_lerp(gold, bg, 0.62))
        v = _num(row.get(stat))
        scale_v = abs(v) if stat == "luck_delta" and v is not None else v
        t = 0.5 if scale_v is None or hi <= lo else (scale_v - lo) / (hi - lo)
        metric_fill = _lerp(_lerp(off, bg, 0.28), orange, t)
        mx = table_x + 72 + name_w
        draw.rectangle((mx + 2, y + 3, mx + metric_w - 8, y + row_h - 3), fill=metric_fill)
        rank = str(i + 1)
        draw.text((table_x + 17, y + max(7, (row_h - 20) // 2)), rank, fill=ink, font=font_rank)
        name = _fit_text(draw, str(row.get("player_name") or f"ID {row.get('player_id', '')}"), font_name, name_w - 26)
        draw.text((table_x + 72, y + max(5, (row_h - 27) // 2)), name, fill=ink, font=font_name)
        metric = _fmt_value(stat, row.get(stat))
        draw.text((mx + metric_w - 18 - _text_w(draw, metric, font_metric), y + max(5, (row_h - 29) // 2)), metric, fill=ink, font=font_metric)
        if has_notes:
            note = _fit_text(draw, notes[i], font_note, note_w - 20)
            draw.text((mx + metric_w + 12, y + max(10, (row_h - 17) // 2)), note, fill=slate, font=font_note)
        draw.line((table_x, y + row_h, table_x + table_w, y + row_h), fill=_lerp(slate, bg, 0.65), width=1)

    draw.line((48, footer_line_y, width - 48, footer_line_y), fill=_lerp(slate, bg, 0.55), width=1)
    draw.text((48, footer_y), "Data: MLB / Statcast warehouse", fill=slate, font=font_small)
    draw.text(((width - _text_w(draw, tag, font_small)) // 2, footer_y), tag, fill=slate, font=font_small)
    handle = "@Mallitalytics"
    draw.text((width - 48 - _text_w(draw, handle, font_small), footer_y), handle, fill=slate, font=font_small)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "PNG")
    return out_path
