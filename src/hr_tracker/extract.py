"""
Extract home runs from raw feed_live JSON for a given date.

Reads warehouse raw files, parses allPlays for eventType home_run,
and enriches with hitData (EV, distance, launch angle), stadium, pitcher, team.
"""

import gzip
import json
import re
from pathlib import Path


def _raw_stem(path: Path) -> str:
    """Base name without .json or .json.gz for feed_live files."""
    name = path.name
    if name.endswith(".json.gz"):
        return name[:-7]
    if name.endswith(".json"):
        return name[:-5]
    return path.stem


def _open_raw(path: Path):
    """Open raw file as text; supports .json and .json.gz."""
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def find_raw_paths_for_date(warehouse: Path, date_str: str, years: list[int] | None = None) -> list[Path]:
    """
    Find all raw feed_live paths whose filename date segment matches date_str.

    date_str: YYYY-MM-DD. Filename segment is YYYYMMDD.
    years: optional list of years to scan (default: year from date_str only).
    """
    target_date = date_str.replace("-", "")
    if not target_date.isdigit() or len(target_date) != 8:
        return []
    year_from_date = int(target_date[:4])
    scan_years = years if years is not None else [year_from_date]

    by_key: dict[tuple[str, str], Path] = {}
    for year in scan_years:
        base = warehouse / str(year)
        if not base.exists():
            continue
        for raw_path in base.rglob("raw/*_feed_live.json*"):
            if not (
                raw_path.name.endswith(".json")
                or raw_path.name.endswith(".json.gz")
            ):
                continue
            name = _raw_stem(raw_path)
            m = re.match(r"game_(\d+)_(\d+)_feed_live", name)
            if not m:
                continue
            game_pk, file_date = m.group(1), m.group(2)
            if file_date != target_date:
                continue
            key = (game_pk, file_date)
            if key not in by_key or (
                raw_path.name.endswith(".json")
                and not raw_path.name.endswith(".json.gz")
            ):
                by_key[key] = raw_path
    return list(by_key.values())


def _get_venue_name(feed: dict) -> str:
    """Stadium name from gameData.venue or gameData.teams.home.venue."""
    gd = feed.get("gameData", {})
    venue = gd.get("venue") or {}
    if venue.get("name"):
        return venue["name"]
    teams = gd.get("teams", {})
    home = teams.get("home", {})
    v = home.get("venue") or {}
    return v.get("name", "")


def _get_team_abbrev(feed: dict, side: str) -> str:
    """side is 'away' or 'home'."""
    teams = feed.get("gameData", {}).get("teams", {})
    t = teams.get(side, {})
    return t.get("abbreviation", "") or (t.get("name", "???")[:3].upper())


def _hit_data_from_play(play: dict) -> dict | None:
    """From playEvents get the event that has hitData (the HR swing)."""
    for ev in play.get("playEvents", []):
        if ev.get("hitData"):
            return ev["hitData"]
    return None


def _iter_stage_raw_paths_before_date(
    warehouse: Path,
    year: int,
    stage: str,
    target_ymd: str,
) -> list[Path]:
    """
    All raw feed_live paths for a given (year, stage) whose filename date < target_ymd.

    Used to compute season-to-date HR counts per batter within a stage.
    """
    base = warehouse / str(year) / stage / "raw"
    if not base.exists():
        return []
    out: list[Path] = []
    pattern = re.compile(r"game_(\d+)_(\d{8})_feed_live")
    for raw_path in base.glob("game_*_feed_live.json*"):
        name = _raw_stem(raw_path)
        m = pattern.match(name)
        if not m:
            continue
        file_date = m.group(2)
        if file_date < target_ymd:
            out.append(raw_path)
    return out


def extract_hrs_from_feed(feed: dict, raw_path: Path | None = None) -> list[dict]:
    """
    Extract all home run plays from a single feed.

    Returns list of dicts with: batter, pitcher, ev_mph, distance_ft, stadium,
    inning, rbi, description, launch_angle, trajectory, team_abbrev, game_pk.
    """
    game_pk = feed.get("gamePk")
    stadium = _get_venue_name(feed)
    away_abbrev = _get_team_abbrev(feed, "away")
    home_abbrev = _get_team_abbrev(feed, "home")

    plays = (
        feed.get("liveData", {})
        .get("plays", {})
        .get("allPlays", [])
    )
    hrs = []
    for play in plays:
        result = play.get("result") or {}
        if result.get("eventType") != "home_run":
            continue

        about = play.get("about") or {}
        matchup = play.get("matchup") or {}
        batter_info = matchup.get("batter") or {}
        pitcher_info = matchup.get("pitcher") or {}
        batter_name = batter_info.get("fullName", "")
        batter_id = batter_info.get("id")
        pitcher_name = pitcher_info.get("fullName", "")
        inning = about.get("inning")
        is_top = about.get("isTopInning", True)
        team_abbrev = away_abbrev if is_top else home_abbrev

        rbi = result.get("rbi", 0)
        description = result.get("description", "")

        hit_data = _hit_data_from_play(play)
        ev_mph = None
        distance_ft = None
        launch_angle = None
        trajectory = None
        if hit_data:
            ev_mph = hit_data.get("launchSpeed")
            distance_ft = hit_data.get("totalDistance")
            launch_angle = hit_data.get("launchAngle")
            trajectory = hit_data.get("trajectory")

        hrs.append({
            "batter": batter_name,
            "batter_id": batter_id,
            "pitcher": pitcher_name,
            "ev_mph": ev_mph,
            "distance_ft": distance_ft,
            "stadium": stadium,
            "inning": inning,
            "rbi": rbi,
            "description": description,
            "launch_angle": launch_angle,
            "trajectory": trajectory,
            "team_abbrev": team_abbrev,
            "game_pk": game_pk,
        })
    return hrs


def get_hrs_for_date(
    warehouse: Path,
    date_str: str,
    years: list[int] | None = None,
) -> list[dict]:
    """
    Load all raw feeds for the given date and return combined list of HR records.

    Each record has: batter, batter_id, pitcher, ev_mph, distance_ft, stadium, inning, rbi,
    description, launch_angle, trajectory, team_abbrev, game_pk, stage, hr_in_stage.

    hr_in_stage is **season-to-date within stage** for that batter (e.g. Spring Training HR #3),
    computed from all prior raw feeds for the same (year, stage) plus the current date.

    Sorted by game_pk then inning.
    """
    warehouse = Path(warehouse)
    target_ymd = date_str.replace("-", "")

    # Raw paths for the target date
    paths_today = find_raw_paths_for_date(warehouse, date_str, years=years)

    # Determine which (year, stage) pairs appear on this date
    stage_pairs: set[tuple[int, str]] = set()
    for raw_path in paths_today:
        try:
            rel = raw_path.relative_to(warehouse)
            parts = rel.parts
            year = int(parts[0])
            stage = parts[1] if len(parts) >= 2 else ""
            stage_pairs.add((year, stage))
        except (ValueError, IndexError):
            continue

    # Season-to-date HR counts per (batter_id, stage) from all prior dates
    prior_counts: dict[tuple[int, str], int] = {}
    for year, stage in stage_pairs:
        for raw_path in _iter_stage_raw_paths_before_date(warehouse, year, stage, target_ymd):
            try:
                with _open_raw(raw_path) as f:
                    feed = json.load(f)
            except Exception:
                continue
            hrs = extract_hrs_from_feed(feed, raw_path)
            for r in hrs:
                batter_id = r.get("batter_id")
                if batter_id is None:
                    continue
                key = (int(batter_id), stage)
                prior_counts[key] = prior_counts.get(key, 0) + 1

    # Now process the target date and assign hr_in_stage on top of prior_counts
    all_hrs: list[dict] = []
    for raw_path in paths_today:
        try:
            rel = raw_path.relative_to(warehouse)
            parts = rel.parts
            year = int(parts[0])
            stage = parts[1] if len(parts) >= 2 else ""
        except (ValueError, IndexError):
            stage = ""
        try:
            with _open_raw(raw_path) as f:
                feed = json.load(f)
        except Exception:
            continue
        hrs = extract_hrs_from_feed(feed, raw_path)
        for r in hrs:
            r["stage"] = stage
            batter_id = r.get("batter_id")
            if batter_id is None or not stage:
                r["hr_in_stage"] = None
            else:
                key = (int(batter_id), stage)
                prior_counts[key] = prior_counts.get(key, 0) + 1
                r["hr_in_stage"] = prior_counts[key]
        all_hrs.extend(hrs)

    all_hrs.sort(key=lambda r: (r.get("game_pk") or 0, r.get("inning") or 0))
    return all_hrs
