"""
Scan regular-season warehouse raw feed_live JSON for completed games in a date range.

Only ``{year}/regular_season/raw``. Extract top pitching (Game Score) and batting
(Malli line score) performances.
"""

from __future__ import annotations

import gzip
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

import requests

from api.paths import get_repo_root, get_warehouse_dir, safe_is_dir

MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
MLB_SCHEDULE_URL = f"{MLB_STATS_BASE}/schedule"
MLB_BOXSCORE_URL = f"{MLB_STATS_BASE}/game/{{game_pk}}/boxscore"
STATSAPI_HEADERS = {"User-Agent": "Mallitalytics/1.0 (mlbops intel-standouts)"}
# Cap boxscore fetches when the warehouse is empty (month window can be hundreds of games).
_MAX_API_BOXSCORES = 220

_FEED_STEM_RE = re.compile(r"^game_(\d+)_(\d{8})_feed_live$")


def _parquet_rel_for_game(
    repo_root: Path,
    warehouse: Path,
    season: int,
    game_pk: int,
    game_date: str,
) -> str | None:
    """Return repo-relative path to pitches_enriched parquet when present on disk."""
    ymd = game_date.replace("-", "")[:8]
    if len(ymd) != 8:
        return None
    p = (
        warehouse
        / str(season)
        / "regular_season"
        / "pitches_enriched"
        / f"game_{game_pk}_{ymd}_pitches_enriched.parquet"
    )
    try:
        if not p.is_file():
            return None
        return str(p.resolve().relative_to(repo_root.resolve()))
    except (OSError, ValueError):
        return None

WindowId = Literal["yesterday", "7d", "14d", "month"]


def _open_raw(path: Path):
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def _innings_to_outs(ip_val: Any) -> int:
    if ip_val is None:
        return 0
    s = str(ip_val).strip()
    if not s or s in (".--", "-.--"):
        return 0
    if "." not in s:
        try:
            whole = int(float(s))
            return whole * 3
        except ValueError:
            return 0
    a, b = s.split(".", 1)
    try:
        whole = int(a)
        partial = int(b[0]) if b else 0
    except ValueError:
        return 0
    partial = min(max(partial, 0), 2)
    return whole * 3 + partial


def _innings_float(ip_val: Any) -> float:
    outs = _innings_to_outs(ip_val)
    return outs / 3.0


def game_score_pitching(pit: dict[str, Any]) -> float:
    """Bill James-style Game Score (outs-based variant used by MLB / BR)."""
    outs = _innings_to_outs(pit.get("inningsPitched"))
    if outs <= 0:
        return float("-inf")
    full_inn = outs // 3
    k = int(pit.get("strikeOuts") or 0)
    h = int(pit.get("hits") or 0)
    er = int(pit.get("earnedRuns") or 0)
    r = int(pit.get("runs") or 0)
    ur = max(0, r - er)
    bb = int(pit.get("baseOnBalls") or 0)
    return float(
        50
        + outs
        + 2 * max(0, full_inn - 4)
        + k
        - 2 * h
        - 4 * er
        - 2 * ur
        - bb
    )


def malli_line_score_batter(bat: dict[str, Any]) -> float:
    """Same weighting as jobs/daily_card_generator.score_batter (noteworthy single-game line)."""
    hits = float(bat.get("hits") or 0)
    rbi = float(bat.get("rbi") or 0)
    hr = float(bat.get("homeRuns") or 0)
    sb = float(bat.get("stolenBases") or 0)
    bb = float(bat.get("baseOnBalls") or 0)
    return hits + rbi + hr * 3.0 + sb * 2.0 + bb * 0.5


def _is_final_feed(feed: dict[str, Any]) -> bool:
    st = (feed.get("gameData") or {}).get("status") or {}
    ag = st.get("abstractGameState")
    return isinstance(ag, str) and ag.strip().lower() == "final"


def _raw_stem(path: Path) -> str:
    name = path.name
    if name.endswith(".json.gz"):
        return name[:-7]
    if name.endswith(".json"):
        return name[:-5]
    return path.stem


# Game standouts are regular-season only (April–Oct slate). Other stages have their own UIs/scripts.
_RAW_SUBDIRS: tuple[str, ...] = ("regular_season/raw",)


def _raw_directories_for_season(warehouse: Path, year: int) -> list[Path]:
    base = warehouse / str(year)
    if not safe_is_dir(base):
        return []
    found: list[Path] = []
    for rel in _RAW_SUBDIRS:
        d = base / rel
        if safe_is_dir(d):
            found.append(d)
    return found


def _iter_feed_paths(
    warehouse: Path,
    d_start: date,
    d_end: date,
) -> tuple[list[Path], int]:
    """
    Returns (paths_in_date_range, raw_dir_count_checked).
    """
    years = range(d_start.year, d_end.year + 1)
    out: list[Path] = []
    raw_dir_count = 0
    for y in years:
        for raw in _raw_directories_for_season(warehouse, y):
            raw_dir_count += 1
            for p in raw.glob("game_*_feed_live.json*"):
                if not (
                    p.name.endswith(".json") or p.name.endswith(".json.gz")
                ):
                    continue
                stem = _raw_stem(p)
                m = _FEED_STEM_RE.match(stem)
                if not m:
                    continue
                ymd = m.group(2)
                try:
                    gd = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))
                except ValueError:
                    continue
                if gd < d_start or gd > d_end:
                    continue
                out.append(p)
    # Prefer uncompressed over .gz when both exist for same game
    by_key: dict[tuple[str, str], Path] = {}
    for p in out:
        stem = _raw_stem(p)
        m = _FEED_STEM_RE.match(stem)
        if not m:
            continue
        key = (m.group(1), m.group(2))
        cur = by_key.get(key)
        if cur is None or (
            p.name.endswith(".json") and not p.name.endswith(".json.gz")
        ):
            by_key[key] = p
    return list(by_key.values()), raw_dir_count


def _pitcher_qualifies(pit: dict[str, Any]) -> bool:
    """Starters (≥3 IP) or bulk outings (≥4 IP) so the list is start-heavy but keeps long relief gems."""
    outs = _innings_to_outs(pit.get("inningsPitched"))
    if outs < 9:
        return False
    gs = int(pit.get("gamesStarted") or 0)
    ipf = _innings_float(pit.get("inningsPitched"))
    if gs >= 1:
        return True
    return ipf >= 4.0


def _batter_qualifies(bat: dict[str, Any]) -> bool:
    pa = int(bat.get("plateAppearances") or 0)
    ab = int(bat.get("atBats") or 0)
    return pa >= 3 or ab >= 3


def _pitch_line(pit: dict[str, Any]) -> str:
    ip = pit.get("inningsPitched", "")
    k = int(pit.get("strikeOuts") or 0)
    h = int(pit.get("hits") or 0)
    er = int(pit.get("earnedRuns") or 0)
    bb = int(pit.get("baseOnBalls") or 0)
    return f"{ip} IP · {k}-{h}-{bb} · {er} ER"


def _bat_line(bat: dict[str, Any]) -> str:
    ab = int(bat.get("atBats") or 0)
    h = int(bat.get("hits") or 0)
    hr = int(bat.get("homeRuns") or 0)
    rbi = int(bat.get("rbi") or 0)
    r = int(bat.get("runs") or 0)
    bb = int(bat.get("baseOnBalls") or 0)
    return f"{ab}-{h} · {hr} HR · {rbi} RBI · {r} R · {bb} BB"


def _extract_rows(
    feed: dict[str, Any],
    feed_path: Path,
    repo_root: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rel = str(feed_path.resolve().relative_to(repo_root.resolve()))
    gd = feed.get("gameData", {})
    teams = gd.get("teams", {})
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    away_abbr = str(away.get("abbreviation") or "?")
    home_abbr = str(home.get("abbreviation") or "?")

    stem = _raw_stem(feed_path)
    m = _FEED_STEM_RE.match(stem)
    game_pk = int(m.group(1)) if m else 0
    ymd = m.group(2) if m else ""
    game_date = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}" if len(ymd) == 8 else ""
    wh = get_warehouse_dir()
    season_guess = int(ymd[:4]) if len(ymd) == 8 else int(game_date[:4]) if len(game_date) >= 4 else 0
    pq_rel = (
        _parquet_rel_for_game(repo_root, wh, season_guess, game_pk, game_date)
        if game_pk and game_date
        else None
    )

    pitchers: list[dict[str, Any]] = []
    batters: list[dict[str, Any]] = []

    box = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for side, opp in (("away", home_abbr), ("home", away_abbr)):
        team = box.get(side) or {}
        abbrev = away_abbr if side == "away" else home_abbr
        players = team.get("players") or {}
        for _pid_key, player in players.items():
            person = player.get("person") or {}
            pid = person.get("id")
            pname = person.get("fullName") or ""
            if not pid:
                continue
            stats = player.get("stats") or {}
            pit = stats.get("pitching") or {}
            if pit and _pitcher_qualifies(pit):
                gs = game_score_pitching(pit)
                if not (gs == float("-inf")):
                    pitchers.append({
                        "player_id": int(pid),
                        "player_name": pname,
                        "team": abbrev,
                        "opponent": opp,
                        "game_date": game_date,
                        "game_pk": game_pk,
                        "game_score": round(gs, 1),
                        "line": _pitch_line(pit),
                        "feed_path": rel,
                        "data_source": "warehouse",
                    })
            bat = stats.get("batting") or {}
            if bat and _batter_qualifies(bat):
                ms = malli_line_score_batter(bat)
                batters.append({
                    "player_id": int(pid),
                    "player_name": pname,
                    "team": abbrev,
                    "opponent": opp,
                    "game_date": game_date,
                    "game_pk": game_pk,
                    "malli_score": round(ms, 2),
                    "line": _bat_line(bat),
                    "feed_path": rel,
                    "parquet_path": pq_rel,
                    "data_source": "warehouse",
                })
    return pitchers, batters


def _extract_rows_from_boxscore_rest(
    box_data: dict[str, Any],
    game_pk: int,
    game_date: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Same metrics as feed_live, from GET /game/{{pk}}/boxscore (regular-season shape)."""
    repo_root = get_repo_root()
    teams_root = box_data.get("teams") or {}
    ta = (teams_root.get("away") or {}).get("team") or {}
    th = (teams_root.get("home") or {}).get("team") or {}
    away_abbr = str(ta.get("abbreviation") or ta.get("fileCode") or "?").upper()
    home_abbr = str(th.get("abbreviation") or th.get("fileCode") or "?").upper()
    wh = get_warehouse_dir()
    season_guess = int(game_date[:4]) if len(game_date) >= 4 else 0
    pq_rel = (
        _parquet_rel_for_game(repo_root, wh, season_guess, game_pk, game_date)
        if season_guess and game_pk and game_date
        else None
    )

    box = teams_root
    pitchers: list[dict[str, Any]] = []
    batters: list[dict[str, Any]] = []

    for side, opp in (("away", home_abbr), ("home", away_abbr)):
        team = box.get(side) or {}
        abbrev = away_abbr if side == "away" else home_abbr
        players = team.get("players") or {}
        for _pid_key, player in players.items():
            if not isinstance(player, dict):
                continue
            person = player.get("person") or {}
            pid = person.get("id")
            pname = person.get("fullName") or ""
            if not pid:
                continue
            stats = player.get("stats") or {}
            pit = stats.get("pitching") or {}
            if pit and _pitcher_qualifies(pit):
                gs = game_score_pitching(pit)
                if gs != float("-inf"):
                    pitchers.append({
                        "player_id": int(pid),
                        "player_name": pname,
                        "team": abbrev,
                        "opponent": opp,
                        "game_date": game_date,
                        "game_pk": game_pk,
                        "game_score": round(gs, 1),
                        "line": _pitch_line(pit),
                        "feed_path": None,
                        "data_source": "api",
                    })
            bat = stats.get("batting") or {}
            if bat and _batter_qualifies(bat):
                ms = malli_line_score_batter(bat)
                batters.append({
                    "player_id": int(pid),
                    "player_name": pname,
                    "team": abbrev,
                    "opponent": opp,
                    "game_date": game_date,
                    "game_pk": game_pk,
                    "malli_score": round(ms, 2),
                    "line": _bat_line(bat),
                    "feed_path": None,
                    "parquet_path": pq_rel,
                    "data_source": "api",
                })
    return pitchers, batters


def _standouts_from_mlb_api(
    d_start: date,
    d_end: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Regular-season Final games only; used when the warehouse has no feed_live in range."""
    all_p: list[dict[str, Any]] = []
    all_b: list[dict[str, Any]] = []
    meta: dict[str, Any] = {
        "schedule_days": 0,
        "boxscores_fetched": 0,
        "schedule_errors": 0,
        "boxscore_errors": 0,
        "capped": False,
    }
    capped = False
    day = d_start
    while day <= d_end:
        if meta["boxscores_fetched"] >= _MAX_API_BOXSCORES:
            meta["capped"] = True
            break
        ds = day.isoformat()
        raw = None
        try:
            r = requests.get(
                MLB_SCHEDULE_URL,
                params={"sportId": 1, "date": ds},
                headers=STATSAPI_HEADERS,
                timeout=20,
            )
            r.raise_for_status()
            raw = r.json()
        except Exception:
            meta["schedule_errors"] += 1
            day += timedelta(days=1)
            continue
        meta["schedule_days"] += 1

        for gd in raw.get("dates", []) or []:
            for g in gd.get("games", []) or []:
                if meta["boxscores_fetched"] >= _MAX_API_BOXSCORES:
                    capped = True
                    break
                if (g.get("gameType") or "").strip().upper() != "R":
                    continue
                st = (g.get("status") or {}).get("abstractGameState") or ""
                if not (isinstance(st, str) and st.strip().lower() == "final"):
                    continue
                pk = g.get("gamePk")
                if pk is None:
                    continue
                official = g.get("officialDate")
                gdate = official if isinstance(official, str) and len(official) >= 10 else ds
                gdate = gdate[:10]

                try:
                    br = requests.get(
                        MLB_BOXSCORE_URL.format(game_pk=int(pk)),
                        headers=STATSAPI_HEADERS,
                        timeout=25,
                    )
                    br.raise_for_status()
                    box = br.json()
                except Exception:
                    meta["boxscore_errors"] += 1
                    continue
                meta["boxscores_fetched"] += 1
                try:
                    pr, batr = _extract_rows_from_boxscore_rest(box, int(pk), gdate)
                    all_p.extend(pr)
                    all_b.extend(batr)
                except (KeyError, TypeError, ValueError, AttributeError):
                    meta["boxscore_errors"] += 1
            if capped:
                break
        if capped:
            meta["capped"] = True
            break
        day += timedelta(days=1)

    return all_p, all_b, meta


def window_bounds(window: WindowId, today: date) -> tuple[date, date]:
    if window == "yesterday":
        d = today - timedelta(days=1)
        return d, d
    if window == "7d":
        return today - timedelta(days=6), today
    if window == "14d":
        return today - timedelta(days=13), today
    # month = last 30 calendar days inclusive
    return today - timedelta(days=29), today


def compute_daily_standouts(
    *,
    window: WindowId,
    limit: int,
    today: date | None = None,
) -> dict[str, Any]:
    today = today or date.today()
    d_start, d_end = window_bounds(window, today)
    repo = get_repo_root()
    warehouse = get_warehouse_dir().resolve()
    paths, raw_dir_count = _iter_feed_paths(warehouse, d_start, d_end)

    all_p: list[dict[str, Any]] = []
    all_b: list[dict[str, Any]] = []
    errors = 0
    feeds_not_final = 0
    feeds_empty_boxscore = 0
    for p in paths:
        try:
            with _open_raw(p) as f:
                feed = json.load(f)
        except (OSError, json.JSONDecodeError):
            errors += 1
            continue
        if not _is_final_feed(feed):
            feeds_not_final += 1
            continue
        try:
            pr, br = _extract_rows(feed, p, repo)
            if not pr and not br:
                feeds_empty_boxscore += 1
            all_p.extend(pr)
            all_b.extend(br)
        except (KeyError, TypeError, ValueError, AttributeError):
            errors += 1
            continue

    data_source: Literal["warehouse", "api"] = "warehouse"
    source_note: str | None = None
    api_meta: dict[str, Any] = {}

    if len(paths) == 0:
        api_p, api_b, api_meta = _standouts_from_mlb_api(d_start, d_end)
        if api_p or api_b:
            all_p.extend(api_p)
            all_b.extend(api_b)
            data_source = "api"
            source_note = (
                "No local feed_live files for this range — filled from MLB Stats API (regular, Final only). "
                "Pitcher cards: use Queue. Batter cards need a synced warehouse feed for that game."
            )
            if api_meta.get("capped"):
                source_note += (
                    f" API boxscore cap ({_MAX_API_BOXSCORES}) reached — narrow the window or sync the warehouse."
                )

    all_p.sort(key=lambda r: (r["game_score"], r.get("game_date", "")), reverse=True)
    all_b.sort(key=lambda r: (r["malli_score"], r.get("game_date", "")), reverse=True)

    hint: str | None = None
    if len(paths) == 0 and not all_p and not all_b:
        if not safe_is_dir(warehouse):
            hint = (
                "Warehouse root does not exist. Set MLB_WAREHOUSE_DIR or sync from Drive "
                "(./scripts/pull_mlbops_from_drive.sh or Hub Settings)."
            )
        elif raw_dir_count == 0:
            hint = (
                "No season raw folders found (expected e.g. "
                f"{warehouse}/{{year}}/regular_season/raw/). Sync the warehouse mirror or run ingest."
            )
        else:
            hint = (
                f"No feed_live files for {d_start.isoformat()}–{d_end.isoformat()} under "
                f"{raw_dir_count} raw folder(s), and MLB Stats API returned no standouts "
                f"(schedule errors {api_meta.get('schedule_errors', 0)}, "
                f"boxscore errors {api_meta.get('boxscore_errors', 0)}). "
                "Check network or try again after games go Final."
            )
    elif len(paths) > 0 and not all_p and not all_b:
        hint = (
            f"Read {len(paths)} feed file(s) in range: {feeds_not_final} not Final, "
            f"{feeds_empty_boxscore} Final with no qualifying lines, {errors} parse errors. "
            "Pitching: ≥3 IP as starter or ≥4 IP relief. Batting: ≥3 PA or ≥3 AB."
        )

    return {
        "window": window,
        "date_start": d_start.isoformat(),
        "date_end": d_end.isoformat(),
        "as_of": today.isoformat(),
        "data_source": data_source,
        "source_note": source_note,
        "api_boxscores_fetched": api_meta.get("boxscores_fetched", 0),
        "api_schedule_days": api_meta.get("schedule_days", 0),
        "api_capped": bool(api_meta.get("capped")),
        "warehouse_dir": str(warehouse),
        "raw_dirs_touched": raw_dir_count,
        "feeds_scanned": len(paths),
        "feeds_not_final": feeds_not_final,
        "feeds_no_qualifying_lines": feeds_empty_boxscore,
        "parse_errors": errors,
        "hint": hint,
        "pitchers": all_p[:limit],
        "batters": all_b[:limit],
    }
