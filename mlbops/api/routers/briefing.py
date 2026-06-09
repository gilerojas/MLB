"""
Morning briefing aggregate for the hub home page.

GET /briefing — latest intel snapshot slice + schedule counts + queue draft count + warehouse freshness
"""
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from api.db.database import get_queue_status_counts
from api.intel_standouts import mlb_today
from api.paths import get_repo_root, get_warehouse_dir, safe_is_dir
from api.routers.intel import INTEL_SNAPSHOT_DIR, _list_snapshot_files
from api.routers.schedule import _fetch_schedule, _parse_games

REPO_ROOT = get_repo_root()
WAREHOUSE_ROOT = get_warehouse_dir()

router = APIRouter(prefix="/briefing", tags=["briefing"])


def _latest_snapshot_path() -> Optional[Path]:
    files = _list_snapshot_files()
    return files[0][1] if files else None


def _load_snapshot_at(anchor: str) -> dict[str, Any]:
    path = INTEL_SNAPSHOT_DIR / f"intel_{anchor}.json"
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"No intel snapshot for {anchor}")
    return json.loads(path.read_text(encoding="utf-8"))


def _last_drive_sync() -> Optional[str]:
    sentinel = REPO_ROOT / "data" / ".last_drive_sync"
    try:
        return sentinel.read_text().strip() or None
    except OSError:
        return None


_PARQUET_DATE_RE = __import__("re").compile(r"game_\d+_(\d{8})_pitches_enriched")
_NUMERIC_ONLY_RE = re.compile(r"^\d+$")
_STATSAPI_PEOPLE_URL = "https://statsapi.mlb.com/api/v1/people/{player_id}"
_MLB_REGULAR_SEASON_GAMES = 2430
_HTTP = requests.Session()
_HTTP.trust_env = False


def _game_date_from_parquet(p: Path) -> Optional[str]:
    """Extract YYYY-MM-DD game date from filename, e.g. game_822834_20260330_pitches_enriched.parquet."""
    m = _PARQUET_DATE_RE.search(p.stem)
    if not m:
        return None
    raw = m.group(1)  # YYYYMMDD
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"


def _display_path(path: Path) -> str:
    path = path.resolve()
    for base, label in ((REPO_ROOT, "app"), (WAREHOUSE_ROOT, "warehouse")):
        try:
            return f"{label}/{path.relative_to(base.resolve()).as_posix()}"
        except ValueError:
            continue
    return str(path)


def _warehouse_freshness() -> dict[str, Any]:
    """Latest game date among pitches_enriched parquets — bounded scan, no full-tree rglob."""
    if not safe_is_dir(WAREHOUSE_ROOT):
        return {"source": "warehouse", "latest_game_date": None, "latest_parquet_path": None}
    latest_game_date: Optional[str] = None
    latest_path: Optional[Path] = None
    today_y = mlb_today().year
    stages = ("regular_season", "postseason")
    try:
        for year in range(today_y, today_y - 10, -1):
            for stage in stages:
                enriched = WAREHOUSE_ROOT / str(year) / stage / "pitches_enriched"
                if not safe_is_dir(enriched):
                    continue
                for p in enriched.glob("*.parquet"):
                    gd = _game_date_from_parquet(p)
                    if gd and (latest_game_date is None or gd > latest_game_date):
                        latest_game_date = gd
                        latest_path = p
    except (OSError, TimeoutError):
        pass
    if latest_path is None:
        return {"source": "warehouse", "latest_game_date": None, "latest_parquet_path": None}
    return {
        "source": "warehouse",
        "latest_game_date": latest_game_date,
        "latest_parquet_path": _display_path(latest_path),
    }


def _season_progress() -> dict[str, Any]:
    """Regular-season game completion based on VPS warehouse game parquets."""
    today = mlb_today()
    season = today.year
    enriched = WAREHOUSE_ROOT / str(season) / "regular_season" / "pitches_enriched"
    game_pks: set[str] = set()
    latest_game_date: Optional[str] = None

    if safe_is_dir(enriched):
        try:
            for p in enriched.glob("game_*_*_pitches_enriched.parquet"):
                parts = p.stem.split("_")
                if len(parts) < 4:
                    continue
                game_pk = parts[1]
                gd = _game_date_from_parquet(p)
                game_pks.add(game_pk)
                if gd and (latest_game_date is None or gd > latest_game_date):
                    latest_game_date = gd
        except (OSError, TimeoutError):
            pass

    games_played = len(game_pks)
    pct = round((games_played / _MLB_REGULAR_SEASON_GAMES) * 100, 2)
    return {
        "season": season,
        "stage": "regular_season",
        "games_played": games_played,
        "total_games": _MLB_REGULAR_SEASON_GAMES,
        "percent": min(pct, 100.0),
        "latest_game_date": latest_game_date,
        "source": "warehouse_pitches_enriched",
    }


def _top_anomalies(snapshot: dict[str, Any], n: int = 3) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    for a in snapshot.get("anomalies_pitchers") or []:
        if isinstance(a, dict):
            combined.append({**a, "_kind": "pitcher"})
    for a in snapshot.get("anomalies_batters") or []:
        if isinstance(a, dict):
            combined.append({**a, "_kind": "batter"})
    # Prefer larger absolute delta when sortable
    def score(x: dict[str, Any]) -> float:
        d = x.get("delta")
        if isinstance(d, (int, float)):
            return abs(float(d))
        return 0.0

    combined.sort(key=score, reverse=True)
    top = combined[:n]

    # Some snapshots contain player_name as raw MLBAM ID strings; resolve those
    # to full names for dashboard readability.
    name_cache: dict[int, Optional[str]] = {}

    def _resolve_name(player_id: int) -> Optional[str]:
        if player_id in name_cache:
            return name_cache[player_id]
        try:
            r = _HTTP.get(
                _STATSAPI_PEOPLE_URL.format(player_id=player_id),
                timeout=4,
            )
            r.raise_for_status()
            people = (r.json() or {}).get("people") or []
            if people:
                name = str((people[0] or {}).get("fullName") or "").strip()
                if name:
                    name_cache[player_id] = name
                    return name
        except Exception:
            pass
        name_cache[player_id] = None
        return None

    out: list[dict[str, Any]] = []
    for item in top:
        row = dict(item)
        pid_raw = row.get("player_id") or row.get("mlbam_id") or row.get("id")
        try:
            player_id = int(pid_raw) if pid_raw is not None else None
        except (TypeError, ValueError):
            player_id = None

        raw_name = str(row.get("player_name") or "").strip()
        needs_resolution = not raw_name or bool(_NUMERIC_ONLY_RE.fullmatch(raw_name))
        if needs_resolution and player_id:
            resolved = _resolve_name(player_id)
            if resolved:
                row["player_name"] = resolved
            else:
                row["player_name"] = f"Player {player_id}"

        out.append(row)

    return out


def _get_briefing_sync(anchor: Optional[str]) -> dict[str, Any]:
    today = mlb_today()
    yesterday = today - timedelta(days=1)
    yday_str = yesterday.isoformat()
    today_str = today.isoformat()

    if anchor:
        try:
            date.fromisoformat(anchor)
        except ValueError:
            raise HTTPException(status_code=400, detail="anchor must be YYYY-MM-DD")
        snapshot = _load_snapshot_at(anchor)
        snap_anchor = anchor
    else:
        path = _latest_snapshot_path()
        if not path:
            snapshot = {}
            snap_anchor = None
        else:
            snap_anchor = path.stem.replace("intel_", "")
            snapshot = json.loads(path.read_text(encoding="utf-8"))

    try:
        raw_today = _fetch_schedule(today_str)
        games_today = _parse_games(raw_today)
    except HTTPException:
        games_today = []

    try:
        raw_yesterday = _fetch_schedule(yday_str)
        games_yesterday = _parse_games(raw_yesterday)
    except HTTPException:
        games_yesterday = []

    counts = get_queue_status_counts()
    freshness = _warehouse_freshness()
    freshness["last_drive_sync_utc"] = _last_drive_sync()
    freshness["season_progress"] = _season_progress()

    # Compact game list for dashboard — only fields the UI needs
    _COMPACT_KEYS = (
        "game_pk", "game_date", "status",
        "away_team", "away_team_id", "away_score", "away_wins", "away_losses", "away_probable",
        "home_team", "home_team_id", "home_score", "home_wins", "home_losses", "home_probable",
    )
    games_today_compact = [
        {k: g.get(k) for k in _COMPACT_KEYS}
        for g in games_today
    ]

    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "intel_anchor": snap_anchor,
        "schedule": {
            "today_date": today_str,
            "yesterday_date": yday_str,
            "games_today_count": len(games_today),
            "games_yesterday_count": len(games_yesterday),
            "games_today": games_today_compact,
        },
        "snapshot": snapshot if snapshot else None,
        "top_anomalies": _top_anomalies(snapshot, 5) if snapshot else [],
        "queue": {
            "draft_count": counts.get("draft", 0),
            "by_status": counts,
        },
        "data_freshness": freshness,
    }


@router.get("")
async def get_briefing(
    anchor: Optional[str] = Query(
        None,
        description="Intel anchor date YYYY-MM-DD (default: latest snapshot on disk)",
    ),
):
    return await run_in_threadpool(_get_briefing_sync, anchor)
