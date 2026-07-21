"""Season-to-date daily HR category leaders and record detection."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from .extract import _open_raw, _raw_stem, extract_hrs_from_feed


RAW_NAME = re.compile(r"game_(\d+)_(\d{8})_feed_live")


def _daily_home_runs(warehouse: Path, season: int, through: str) -> dict[str, list[dict]]:
    raw_dir = Path(warehouse) / str(season) / "regular_season" / "raw"
    through_ymd = through.replace("-", "")
    selected: dict[tuple[str, str], Path] = {}

    if not raw_dir.exists():
        return {}

    for path in raw_dir.glob("game_*_feed_live.json*"):
        match = RAW_NAME.match(_raw_stem(path))
        if not match:
            continue
        game_pk, ymd = match.groups()
        if ymd > through_ymd:
            continue
        key = (game_pk, ymd)
        current = selected.get(key)
        if current is None or (path.suffix == ".json" and current.suffix != ".json"):
            selected[key] = path

    daily: dict[str, list[dict]] = defaultdict(list)
    for (_, ymd), path in sorted(selected.items(), key=lambda item: item[0][1]):
        try:
            with _open_raw(path) as handle:
                feed = json.load(handle)
        except Exception:
            continue
        daily[ymd].extend(extract_hrs_from_feed(feed, path))
    return dict(daily)


def build_category_history(warehouse: Path, season: int, through: str) -> dict:
    """Build daily hardest/longest leaders, cumulative lead counts, and record flags."""
    daily = _daily_home_runs(warehouse, season, through)
    hardest: Counter[int] = Counter()
    longest: Counter[int] = Counter()
    names: dict[int, str] = {}
    daily_results: dict[str, dict] = {}
    season_highs: dict[str, float | int | None] = {
        "daily_hr": None,
        "exit_velocity": None,
        "distance": None,
    }

    for ymd, hrs in sorted(daily.items()):
        ev_rows = [row for row in hrs if row.get("ev_mph") is not None and row.get("batter_id")]
        dist_rows = [
            row for row in hrs if row.get("distance_ft") is not None and row.get("batter_id")
        ]
        if not ev_rows and not dist_rows:
            continue

        result: dict = {
            "hr_count": len(hrs),
            "game_count": len({row.get("game_pk") for row in hrs if row.get("game_pk")}),
        }
        if ev_rows:
            max_ev = max(float(row["ev_mph"]) for row in ev_rows)
            result["hardest"] = []
            for row in (row for row in ev_rows if float(row["ev_mph"]) == max_ev):
                player_id = int(row["batter_id"])
                names[player_id] = row["batter"]
                hardest[player_id] += 1
                result["hardest"].append(
                    {"player_id": player_id, "player_name": row["batter"], "value": max_ev}
                )

        if dist_rows:
            max_dist = max(float(row["distance_ft"]) for row in dist_rows)
            result["longest"] = []
            for row in (row for row in dist_rows if float(row["distance_ft"]) == max_dist):
                player_id = int(row["batter_id"])
                names[player_id] = row["batter"]
                longest[player_id] += 1
                result["longest"].append(
                    {"player_id": player_id, "player_name": row["batter"], "value": max_dist}
                )

        current_values: dict[str, float | int | None] = {
            "daily_hr": len(hrs),
            "exit_velocity": max(
                (float(row["value"]) for row in result.get("hardest", [])), default=None
            ),
            "distance": max(
                (float(row["value"]) for row in result.get("longest", [])), default=None
            ),
        }
        flags: list[dict] = []
        for metric, value in current_values.items():
            previous = season_highs[metric]
            if value is None:
                continue
            if previous is not None and value >= previous:
                flags.append(
                    {
                        "metric": metric,
                        "status": "new" if value > previous else "tied",
                        "value": value,
                        "previous_high": previous,
                    }
                )
            if previous is None or value > previous:
                season_highs[metric] = value
        result["record_flags"] = flags
        daily_results[ymd] = result

    def serialize(counter: Counter[int]) -> dict[str, dict]:
        return {
            str(player_id): {"player_name": names[player_id], "daily_leads": count}
            for player_id, count in counter.items()
        }

    return {
        "season": season,
        "through": through,
        "completed_dates_with_hr": len(daily_results),
        "hardest": serialize(hardest),
        "longest": serialize(longest),
        "daily": daily_results,
    }


def category_lead_count(
    history: dict | None,
    date_str: str,
    category: str,
    player_id: int,
) -> int | None:
    """Count daily category leads through and including ``date_str``."""
    if not history:
        return None
    through_ymd = date_str.replace("-", "")
    count = 0
    for ymd, result in history.get("daily", {}).items():
        if ymd > through_ymd:
            continue
        if any(int(row["player_id"]) == player_id for row in result.get(category, [])):
            count += 1
    return count


def record_caption_lines(history: dict | None, date_str: str) -> list[str]:
    """Return all-caps season-high callouts for a daily caption."""
    if not history:
        return []
    result = history.get("daily", {}).get(date_str.replace("-", ""), {})
    lines: list[str] = []
    for flag in result.get("record_flags", []):
        status = "NEW" if flag.get("status") == "new" else "TIES"
        metric = flag.get("metric")
        value = flag.get("value")
        if metric == "daily_hr":
            lines.append(f"{status} {history['season']} MLB DAILY HR HIGH — {int(value)} HOME RUNS")
        elif metric == "exit_velocity":
            lines.append(
                f"{status} {history['season']} MLB HR EXIT-VELOCITY HIGH — {float(value):.1f} MPH"
            )
        elif metric == "distance":
            lines.append(
                f"{status} {history['season']} MLB HR DISTANCE HIGH — {int(float(value))} FT"
            )
    return lines
