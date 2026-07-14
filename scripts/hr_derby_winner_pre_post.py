#!/usr/bin/env python3
"""Compare recent Home Run Derby winners before and after the event.

The script fetches regular-season game logs from the public MLB Stats API,
builds equal 30-game pre/post summaries, and renders an event-aligned rolling
OPS chart for the last seven completed Derbies with a usable post-event sample.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from PIL import Image, ImageDraw

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.hr_derby_power_radar import fetch_headshot, lerp, rgb, text_width  # noqa: E402
from src.mallitalytics_style import load_jetbrains_mono, load_montserrat  # noqa: E402


WIDTH = 1200
HEIGHT = 675
ROLLING_GAMES = 15
WINDOW_GAMES = 30
EVENT_RANGE = 40
OPS_FLOOR = 0.450
OPS_CEILING = 1.350
REFERENCE_OPS = 0.750


@dataclass(frozen=True)
class DerbyWinner:
    season: int
    player_id: int
    player_name: str
    event_team: str
    derby_date: str


WINNERS = (
    DerbyWinner(2025, 663728, "Cal Raleigh", "SEA", "2025-07-14"),
    DerbyWinner(2024, 606192, "Teoscar Hernández", "LAD", "2024-07-15"),
    DerbyWinner(2023, 665489, "Vladimir Guerrero Jr.", "TOR", "2023-07-10"),
    DerbyWinner(2022, 665742, "Juan Soto", "WSH", "2022-07-18"),
    DerbyWinner(2021, 624413, "Pete Alonso", "NYM", "2021-07-12"),
    DerbyWinner(2019, 624413, "Pete Alonso", "NYM", "2019-07-08"),
    DerbyWinner(2018, 547180, "Bryce Harper", "WSH", "2018-07-16"),
)


@dataclass(frozen=True)
class GameLine:
    game_date: str
    at_bats: int
    hits: int
    doubles: int
    triples: int
    home_runs: int
    walks: int
    strikeouts: int
    hit_by_pitch: int
    sac_flies: int
    plate_appearances: int


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _stats_api_url(winner: DerbyWinner) -> str:
    return (
        f"https://statsapi.mlb.com/api/v1/people/{winner.player_id}/stats"
        f"?stats=gameLog&group=hitting&season={winner.season}&gameType=R"
    )


def fetch_game_log(
    winner: DerbyWinner,
    cache_dir: Path,
    *,
    refresh: bool,
) -> dict[str, Any]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{winner.season}_{winner.player_id}.json"
    if cache_path.exists() and not refresh:
        return json.loads(cache_path.read_text(encoding="utf-8"))

    response = requests.get(_stats_api_url(winner), timeout=45)
    response.raise_for_status()
    payload = response.json()
    cache_path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def parse_games(payload: dict[str, Any]) -> list[GameLine]:
    stats = payload.get("stats") or []
    splits = stats[0].get("splits", []) if stats else []
    games: list[GameLine] = []
    for split in splits:
        stat = split.get("stat") or {}
        game_date = str(split.get("date") or "")
        if not game_date:
            continue
        games.append(
            GameLine(
                game_date=game_date,
                at_bats=_safe_int(stat.get("atBats")),
                hits=_safe_int(stat.get("hits")),
                doubles=_safe_int(stat.get("doubles")),
                triples=_safe_int(stat.get("triples")),
                home_runs=_safe_int(stat.get("homeRuns")),
                walks=_safe_int(stat.get("baseOnBalls")),
                strikeouts=_safe_int(stat.get("strikeOuts")),
                hit_by_pitch=_safe_int(stat.get("hitByPitch")),
                sac_flies=_safe_int(stat.get("sacFlies")),
                plate_appearances=_safe_int(stat.get("plateAppearances")),
            )
        )
    return sorted(games, key=lambda game: game.game_date)


def summarize(games: list[GameLine]) -> dict[str, float | int | None]:
    ab = sum(game.at_bats for game in games)
    hits = sum(game.hits for game in games)
    doubles = sum(game.doubles for game in games)
    triples = sum(game.triples for game in games)
    home_runs = sum(game.home_runs for game in games)
    walks = sum(game.walks for game in games)
    strikeouts = sum(game.strikeouts for game in games)
    hbp = sum(game.hit_by_pitch for game in games)
    sf = sum(game.sac_flies for game in games)
    pa = sum(game.plate_appearances for game in games)
    singles = max(0, hits - doubles - triples - home_runs)
    total_bases = singles + doubles * 2 + triples * 3 + home_runs * 4
    obp_den = ab + walks + hbp + sf
    avg = hits / ab if ab else None
    obp = (hits + walks + hbp) / obp_den if obp_den else None
    slg = total_bases / ab if ab else None
    ops = obp + slg if obp is not None and slg is not None else None
    return {
        "games": len(games),
        "pa": pa,
        "ab": ab,
        "hits": hits,
        "home_runs": home_runs,
        "walks": walks,
        "strikeouts": strikeouts,
        "avg": round(avg, 3) if avg is not None else None,
        "obp": round(obp, 3) if obp is not None else None,
        "slg": round(slg, 3) if slg is not None else None,
        "ops": round(ops, 3) if ops is not None else None,
        "hr_per_100_pa": round(home_runs / pa * 100, 2) if pa else None,
        "k_pct": round(strikeouts / pa * 100, 1) if pa else None,
        "bb_pct": round(walks / pa * 100, 1) if pa else None,
    }


def rolling_ops(games: list[GameLine], window: int = ROLLING_GAMES) -> list[float | None]:
    values: list[float | None] = []
    for index in range(len(games)):
        if index + 1 < window:
            values.append(None)
            continue
        stats = summarize(games[index - window + 1 : index + 1])
        value = stats.get("ops")
        values.append(float(value) if value is not None else None)
    return values


def analyze_winner(winner: DerbyWinner, games: list[GameLine]) -> dict[str, Any]:
    pre_games = [game for game in games if game.game_date < winner.derby_date]
    post_games = [game for game in games if game.game_date > winner.derby_date]
    pre_window = pre_games[-WINDOW_GAMES:]
    post_window = post_games[:WINDOW_GAMES]
    rolling = rolling_ops(games)

    points = []
    for index, (game, value) in enumerate(zip(games, rolling)):
        if game.game_date < winner.derby_date:
            relative_game = index - len(pre_games)
        elif game.game_date > winner.derby_date:
            relative_game = index - len(pre_games) + 1
        else:
            relative_game = None
        if relative_game is None or not -EVENT_RANGE <= relative_game <= EVENT_RANGE:
            continue
        points.append(
            {
                "relative_game": relative_game,
                "date": game.game_date,
                "rolling_ops": value,
            }
        )

    pre_summary = summarize(pre_window)
    post_summary = summarize(post_window)
    pre_ops = pre_summary.get("ops")
    post_ops = post_summary.get("ops")
    ops_delta = (
        round(float(post_ops) - float(pre_ops), 3)
        if pre_ops is not None and post_ops is not None
        else None
    )
    return {
        **asdict(winner),
        "season_games": len(games),
        "pre_window_start": pre_window[0].game_date if pre_window else None,
        "pre_window_end": pre_window[-1].game_date if pre_window else None,
        "post_window_start": post_window[0].game_date if post_window else None,
        "post_window_end": post_window[-1].game_date if post_window else None,
        "pre_30": pre_summary,
        "post_30": post_summary,
        "ops_delta": ops_delta,
        "points": points,
    }


def _format_rate(value: Any, digits: int = 3) -> str:
    if value is None:
        return "—"
    number = float(value)
    if digits == 3:
        return f"{number:.3f}".lstrip("0")
    return f"{number:.{digits}f}"


def _short_date(value: str) -> str:
    parsed = datetime.strptime(value, "%Y-%m-%d")
    return f"{parsed.strftime('%b').upper()} {parsed.day}"


def _delta_color(delta: float) -> tuple[int, int, int]:
    cold = (83, 111, 142)
    neutral = (171, 161, 145)
    warm = (190, 73, 49)
    magnitude = min(1.0, abs(delta) / 0.180)
    return lerp(neutral, warm if delta >= 0 else cold, magnitude)


def _cached_headshot(player_id: int, size: int, cache_dir: Path) -> Image.Image | None:
    headshot_dir = cache_dir / "headshots"
    headshot_dir.mkdir(parents=True, exist_ok=True)
    headshot_path = headshot_dir / f"{player_id}.png"
    if headshot_path.exists():
        try:
            return Image.open(headshot_path).convert("RGBA").resize((size, size), Image.Resampling.LANCZOS)
        except OSError:
            pass
    source = fetch_headshot(player_id, 320)
    if source is None:
        return None
    source.save(headshot_path, "PNG")
    return source.resize((size, size), Image.Resampling.LANCZOS)


def _circular_headshot(player_id: int, size: int, cache_dir: Path) -> Image.Image:
    off = rgb("off_white")
    slate = rgb("slate")
    avatar = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    source = _cached_headshot(player_id, size - 8, cache_dir)
    mask = Image.new("L", (size - 8, size - 8), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 9, size - 9), fill=255)
    if source is not None:
        avatar.paste(source, (4, 4), mask)
    else:
        ImageDraw.Draw(avatar).ellipse((4, 4, size - 5, size - 5), fill=off)
    ImageDraw.Draw(avatar).ellipse((2, 2, size - 3, size - 3), outline=lerp(slate, off, 0.40), width=2)
    return avatar


def _plot_x(relative_game: int, x0: int, x1: int) -> float:
    return x0 + (relative_game + EVENT_RANGE) / (EVENT_RANGE * 2) * (x1 - x0)


def _plot_y(ops: float, y0: int, y1: int) -> float:
    clipped = max(OPS_FLOOR, min(OPS_CEILING, ops))
    return y1 - (clipped - OPS_FLOOR) / (OPS_CEILING - OPS_FLOOR) * (y1 - y0)


def _draw_line_segments(
    draw: ImageDraw.ImageDraw,
    points: list[dict[str, Any]],
    x0: int,
    x1: int,
    y0: int,
    y1: int,
    *,
    before_color: tuple[int, int, int],
    after_color: tuple[int, int, int],
    background: tuple[int, int, int],
) -> None:
    series: dict[str, list[list[tuple[float, float]]]] = {"before": [], "after": []}
    current_segment: list[tuple[float, float]] = []
    current_side: str | None = None
    previous_relative: int | None = None
    for point in points:
        value = point.get("rolling_ops")
        if value is None:
            if current_side and current_segment:
                series[current_side].append(current_segment)
            current_segment = []
            current_side = None
            previous_relative = None
            continue
        relative = int(point["relative_game"])
        side = "after" if relative > 0 else "before"
        coordinate = (_plot_x(relative, x0, x1), _plot_y(float(value), y0, y1))
        if current_side != side or previous_relative is None or abs(relative - previous_relative) != 1:
            if current_side and current_segment:
                series[current_side].append(current_segment)
            current_segment = [coordinate]
            current_side = side
        else:
            current_segment.append(coordinate)
        previous_relative = relative
    if current_side and current_segment:
        series[current_side].append(current_segment)

    for side, color in (("before", before_color), ("after", after_color)):
        under_stroke = lerp(color, background, 0.72)
        for segment in series[side]:
            if len(segment) < 2:
                continue
            draw.line(segment, fill=under_stroke, width=7, joint="curve")
            draw.line(segment, fill=color, width=3, joint="curve")
            for px, py in (segment[0], segment[-1]):
                draw.ellipse((px - 2, py - 2, px + 2, py + 2), fill=color)


def render(results: list[dict[str, Any]], out_path: Path, cache_dir: Path) -> Path:
    cream = rgb("warm_cream")
    off = rgb("off_white")
    ink = rgb("dark_teal")
    slate = rgb("slate")
    orange = rgb("burnt_orange")
    gold = rgb("muted_gold")
    before = (83, 111, 142)
    after = orange

    image = Image.new("RGBA", (WIDTH, HEIGHT), (*cream, 255))
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, WIDTH, 10), fill=orange)

    title_font = load_montserrat(35, bold=True)
    subtitle_font = load_montserrat(16)
    small_font = load_montserrat(12)
    small_bold = load_montserrat(12, bold=True)
    name_font = load_montserrat(16, bold=True)
    meta_font = load_montserrat(11, bold=True)
    mono_small = load_jetbrains_mono(11, bold=True)
    mono_value = load_jetbrains_mono(15, bold=True)

    draw.text((44, 37), "DOES WINNING THE DERBY CHANGE A HITTER?", fill=ink, font=title_font)
    draw.text(
        (46, 82),
        "LAST 7 COMPLETED CHAMPIONS  ·  REGULAR-SEASON PERFORMANCE AROUND THE DERBY",
        fill=slate,
        font=subtitle_font,
    )

    improved = sum(1 for result in results if float(result.get("ops_delta") or 0) > 0)
    declined = sum(1 for result in results if float(result.get("ops_delta") or 0) < 0)
    draw.rounded_rectangle((944, 37, 1156, 101), radius=10, fill=off, outline=lerp(slate, cream, 0.48), width=2)
    draw.text((963, 52), "30-GAME RESULT", fill=slate, font=small_bold)
    badge = f"{improved} UP  ·  {declined} DOWN"
    draw.text((963, 73), badge, fill=orange, font=small_bold)

    plot_x0, plot_x1 = 318, 887
    summary_x = 922
    center_x = _plot_x(0, plot_x0, plot_x1)
    header_y = 118
    tick_y = 137
    draw.text((44, header_y), "WINNER", fill=slate, font=small_bold)
    draw.text((plot_x0, header_y), "TREND: 15-GAME ROLLING OPS", fill=ink, font=small_bold)
    draw.text((plot_x0, tick_y), "-40 GAMES", fill=before, font=small_bold)
    derby_label = "DERBY"
    draw.text((center_x - text_width(draw, derby_label, small_bold) / 2, tick_y), derby_label, fill=orange, font=small_bold)
    right_label = "+40 GAMES"
    draw.text((plot_x1 - text_width(draw, right_label, small_bold), tick_y), right_label, fill=after, font=small_bold)
    draw.text((summary_x, header_y), "SUMMARY: 30-GAME OPS", fill=ink, font=small_bold)
    draw.text((summary_x, tick_y), "BEFORE → AFTER", fill=slate, font=small_bold)

    row_top = 157
    row_h = 64
    for index, result in enumerate(results):
        y = row_top + index * row_h
        if index % 2 == 0:
            draw.rounded_rectangle((44, y, 1156, y + row_h - 5), radius=7, fill=off)

        avatar = _circular_headshot(int(result["player_id"]), 48, cache_dir)
        image.paste(avatar, (51, y + 7), avatar)
        draw.text((108, y + 10), str(result["player_name"]), fill=ink, font=name_font)
        draw.text(
            (109, y + 34),
            f"{result['season']}  ·  {result['event_team']}  ·  {_short_date(str(result['derby_date']))}",
            fill=slate,
            font=meta_font,
        )

        graph_y0, graph_y1 = y + 7, y + 55
        ref_y = _plot_y(REFERENCE_OPS, graph_y0, graph_y1)
        draw.line((plot_x0, ref_y, plot_x1, ref_y), fill=lerp(slate, cream, 0.65), width=1)
        draw.line((center_x, graph_y0 - 1, center_x, graph_y1 + 1), fill=gold, width=2)
        _draw_line_segments(
            draw,
            result["points"],
            plot_x0,
            plot_x1,
            graph_y0,
            graph_y1,
            before_color=before,
            after_color=after,
            background=off if index % 2 == 0 else cream,
        )

        pre = result["pre_30"]
        post = result["post_30"]
        delta = float(result.get("ops_delta") or 0)
        delta_text = f"{delta:+.3f}".replace("+0.", "+.").replace("-0.", "-.")
        ops_text = f"OPS {_format_rate(pre.get('ops'))} → {_format_rate(post.get('ops'))}"
        draw.text((summary_x, y + 8), ops_text, fill=ink, font=mono_value)
        draw.text((1129 - text_width(draw, delta_text, mono_value), y + 8), delta_text, fill=_delta_color(delta), font=mono_value)
        hr_text = f"HR/100 PA {float(pre.get('hr_per_100_pa') or 0):.1f} → {float(post.get('hr_per_100_pa') or 0):.1f}"
        draw.text((summary_x, y + 35), hr_text, fill=slate, font=mono_small)

    footer_y = HEIGHT - 35
    draw.line((44, footer_y - 13, WIDTH - 44, footer_y - 13), fill=lerp(slate, cream, 0.58), width=1)
    draw.text(
        (44, footer_y),
        "Lines: trailing 15-game OPS · Summary: equal 30-game OPS windows · Data: MLB Stats API",
        fill=slate,
        font=small_font,
    )
    legend_x = 684
    draw.line((legend_x, footer_y + 7, legend_x + 25, footer_y + 7), fill=before, width=3)
    draw.text((legend_x + 32, footer_y), "before", fill=slate, font=small_font)
    draw.line((legend_x + 98, footer_y + 7, legend_x + 123, footer_y + 7), fill=after, width=3)
    draw.text((legend_x + 130, footer_y), "after", fill=slate, font=small_font)
    handle = "@Mallitalytics"
    draw.text((WIDTH - 44 - text_width(draw, handle, small_font), footer_y), handle, fill=slate, font=small_font)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(out_path, "PNG", optimize=True)
    return out_path


def run(
    *,
    cache_dir: Path,
    out_json: Path,
    out_image: Path,
    refresh: bool,
) -> list[dict[str, Any]]:
    results = []
    for winner in WINNERS:
        payload = fetch_game_log(winner, cache_dir, refresh=refresh)
        games = parse_games(payload)
        results.append(analyze_winner(winner, games))
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    render(results, out_image, cache_dir)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=Path("outputs/hr_derby_winner_study/cache"))
    parser.add_argument("--out-json", type=Path, default=Path("outputs/hr_derby_winner_study/results.json"))
    parser.add_argument("--out-image", type=Path, default=Path("outputs/hr_derby_winner_study/pre_post_ops.png"))
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    results = run(
        cache_dir=args.cache_dir,
        out_json=args.out_json,
        out_image=args.out_image,
        refresh=args.refresh,
    )
    for result in results:
        print(
            f"{result['season']} {result['player_name']}: "
            f"{_format_rate(result['pre_30']['ops'])} -> {_format_rate(result['post_30']['ops'])} "
            f"({result['ops_delta']:+.3f})"
        )


if __name__ == "__main__":
    main()
