"""
Card generation endpoints — wrap existing Python scripts via subprocess.

POST /cards/batter   → runs batter_card_daily.py
POST /cards/pitcher  → runs mallitalytics_daily_card.py
POST /cards/hr-tracker → runs hr_tracker_daily.py
POST /cards/probables-board → runs probables_board_daily.py
POST /cards/games-of-day → runs games_of_day_board.py
"""
import json
import re
import secrets
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests as _requests
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from api.db.database import insert_queue_item
from api.paths import (
    get_repo_root,
    get_tweet_max_chars,
    get_warehouse_dir,
    safe_is_dir,
    truncate_tweet_text_to_cap,
)

STATS_BASE = "https://statsapi.mlb.com/api/v1"

REPO_ROOT = get_repo_root()
OUTPUTS_ROOT = REPO_ROOT / "outputs"
MLB_PYTHON = str(REPO_ROOT / "mlb_env" / "bin" / "python")
import os as _os
_API_HOST = _os.getenv("FASTAPI_STATIC_BASE", "http://127.0.0.1:8000")
STATIC_BASE = f"{_API_HOST.rstrip('/')}/static"

router = APIRouter(prefix="/cards", tags=["cards"])


def _card_json_from_stdout(stdout: str) -> Optional[dict]:
    """Parse pitcher/batter card snapshot printed between --- Card JSON --- markers."""
    start, end = "--- Card JSON ---", "--- End Card JSON ---"
    if start not in stdout or end not in stdout:
        return None
    try:
        i = stdout.index(start) + len(start)
        j = stdout.index(end, i)
        raw = stdout[i:j].strip()
        out = json.loads(raw)
        return out if isinstance(out, dict) else None
    except (ValueError, json.JSONDecodeError):
        return None


def _player_name_from_card_meta(meta: Optional[dict]) -> Optional[str]:
    if not meta:
        return None
    name = meta.get("player_name")
    return str(name).strip() if name else None


def _game_pk_from_card_meta(meta: Optional[dict]) -> Optional[int]:
    if not meta:
        return None
    gk = meta.get("game_pk")
    if gk is None:
        return None
    try:
        return int(gk)
    except (TypeError, ValueError):
        return None


def _default_batter_tweet_from_meta(
    meta: Optional[dict], pname: Optional[str], pid: int, game_date: str,
) -> str:
    label = (pname or str(pid)).strip()
    if isinstance(meta, dict):
        btc = meta.get("batter_tweet_context") or {}
        hero = btc.get("hero_headline")
        if hero:
            return f"{label} — {hero} | {game_date} #Mallitalytics"
        evs = meta.get("notable_game_events")
        if isinstance(evs, list) and evs:
            try:
                top = min(evs, key=lambda x: int((x or {}).get("priority", 99)))
            except (TypeError, ValueError):
                top = evs[0]
            if isinstance(top, dict) and top.get("label"):
                return f"{label} — {top['label']} | {game_date} #Mallitalytics"
    return f"{label} | {game_date} | #Mallitalytics"


def _default_pitcher_tweet_from_meta(
    meta: Optional[dict], pname: Optional[str], pid: int, game_date: str,
) -> str:
    label = (pname or str(pid)).strip()
    if isinstance(meta, dict):
        evs = meta.get("notable_game_events")
        if isinstance(evs, list) and evs:
            try:
                top = min(evs, key=lambda x: int((x or {}).get("priority", 99)))
            except (TypeError, ValueError):
                top = evs[0]
            if isinstance(top, dict) and top.get("label"):
                return f"{label} — {top['label']} | {game_date} #Mallitalytics"
        src = meta.get("source_metadata")
        if isinstance(src, dict) and src.get("is_mlb_debut_game"):
            box = meta.get("box") or {}
            ip, k, er, bb = box.get("ip"), box.get("k"), box.get("er"), box.get("bb")
            if ip is not None and k is not None and er is not None:
                bb_s = f", {bb} BB" if bb is not None else ""
                return (
                    f"{label} — MLB debut: {ip} IP, {er} ER, {k} K{bb_s}. "
                    f"Welcome to The Show. | {game_date} #Mallitalytics"
                )
            return f"{label} — MLB debut | {game_date} #Mallitalytics"
    return f"{label} | {game_date} | #Mallitalytics"


def _tweet_body_after_marker(stdout: str, marker: str = "--- Tweet ---") -> str:
    """Extract multiline tweet body after a marker until a '(N chars)' summary line."""
    lines = stdout.splitlines()
    for i, line in enumerate(lines):
        if line.strip() == marker:
            body: list[str] = []
            for j in range(i + 1, len(lines)):
                ln = lines[j]
                stripped = ln.strip()
                if stripped.startswith("(") and "char" in stripped.lower():
                    break
                body.append(ln)
            return "\n".join(body).strip()
    return ""


def _hr_tracker_tweet_from_stdout(stdout: str) -> str:
    """Extract tweet body printed after '--- Tweet ---' by hr_tracker_daily --format all."""
    return _tweet_body_after_marker(stdout, "--- Tweet ---")


def _run_script(cmd: list[str]) -> tuple[str, str]:
    """Run a card script and return (stdout, stderr). Raises HTTPException on failure."""
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Script failed:\n{result.stderr[-2000:]}",
        )
    return result.stdout, result.stderr


def _extract_saved_path(stdout: str) -> Optional[Path]:
    """Parse saved PNG path from script stdout.

    Handles multiple formats:
      → Saved: /abs/path.png          (batter/pitcher cards)
      ->  Saved: /abs/path.png
      Image: outputs/relative.png     (hr_tracker_daily.py)

    Uses the *last* arrow match so stray earlier lines (warnings, embedded text) cannot
    steal the path; pitcher scripts print Card JSON before the final Saved line.
    """
    last_arrow: Optional[Path] = None
    last_image: Optional[Path] = None
    for line in stdout.splitlines():
        m = re.search(r"(?:→|->)\s*Saved:\s*(.+\.png)", line)
        if m:
            p = Path(m.group(1).strip())
            last_arrow = p if p.is_absolute() else REPO_ROOT / p
        m2 = re.search(r"Image:\s*(.+\.png)", line)
        if m2:
            p = Path(m2.group(1).strip())
            last_image = p if p.is_absolute() else REPO_ROOT / p
    return last_arrow or last_image


def _image_url(abs_path: Path) -> str:
    """Build /static/... URL; resolves paths so symlink / realpath mismatches do not 500."""
    out_root = OUTPUTS_ROOT.resolve()
    candidates = (abs_path, abs_path.expanduser(), abs_path.resolve())
    rel: Optional[Path] = None
    for c in candidates:
        try:
            rel = c.resolve().relative_to(out_root)
            break
        except ValueError:
            continue
    if rel is None:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Card PNG is not under the API outputs directory ({out_root}). "
                f"Script reported: {abs_path}"
            ),
        )
    return f"{STATIC_BASE}/{rel.as_posix()}"


def _game_date_from_feed(feed_path: str) -> str:
    """Extract YYYYMMDD from feed filename and convert to YYYY-MM-DD."""
    m = re.search(r"_(\d{8})_feed", feed_path)
    if m:
        d = m.group(1)
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return str(date.today())


def _season_from_feed(feed_path: str) -> int:
    m = re.search(r"/(\d{4})/", feed_path)
    return int(m.group(1)) if m else date.today().year


def _stage_from_feed(feed_path: str) -> str:
    for stage in ("regular_season", "spring_training", "playoffs", "all_star"):
        if stage in feed_path:
            return stage
    return "regular_season"


def _game_pk_from_feed(feed_path: str) -> Optional[int]:
    m = re.search(r"game_(\d+)_", feed_path)
    return int(m.group(1)) if m else None


_RAW_STAGES = (
    "regular_season",
    "postseason",
    "spring_training",
    "playoffs",
    "all_star",
)
_FEED_LIVE_RE = re.compile(r"game_(\d+)_(\d{8})_feed_live")


def _feed_live_index(warehouse: Path, seasons: tuple[int, ...]) -> dict[tuple[int, str], str]:
    """Map (game_pk, YYYYMMDD) -> path relative to repo root.

    Scans only ``{season}/{stage}/raw/`` — no full-tree ``rglob`` (that can take minutes).
    Prefers ``.json`` over ``.json.gz`` when both exist for the same game.
    """
    available: dict[tuple[int, str], str] = {}
    for year in seasons:
        for stage in _RAW_STAGES:
            raw_dir = warehouse / str(year) / stage / "raw"
            if not safe_is_dir(raw_dir):
                continue
            for path in sorted(raw_dir.glob("game_*_feed_live.json")):
                m = _FEED_LIVE_RE.search(path.name)
                if not m:
                    continue
                key = (int(m.group(1)), m.group(2))
                try:
                    available[key] = str(path.relative_to(REPO_ROOT))
                except ValueError:
                    continue
            for path in sorted(raw_dir.glob("game_*_feed_live.json.gz")):
                m = _FEED_LIVE_RE.search(path.name)
                if not m:
                    continue
                key = (int(m.group(1)), m.group(2))
                if key in available:
                    continue
                try:
                    available[key] = str(path.relative_to(REPO_ROOT))
                except ValueError:
                    continue
    return available


# ──────────────────────────────────────────────────────────
# Request models
# ──────────────────────────────────────────────────────────

class BatterCardRequest(BaseModel):
    player_id: int
    feed_path: Optional[str] = None
    parquet_path: Optional[str] = None
    game_date: Optional[str] = None
    dark: bool = False
    tweet_text: Optional[str] = None


class PitcherCardRequest(BaseModel):
    player_id: int
    game_date: str          # YYYY-MM-DD  (or "yesterday")
    dark: bool = False
    tweet_text: Optional[str] = None
    parquet_path: Optional[str] = None  # explicit parquet override


class HRTrackerRequest(BaseModel):
    game_date: Optional[str] = None    # YYYY-MM-DD, defaults to yesterday
    tweet_text: Optional[str] = None


class GamesOfDayRequest(BaseModel):
    game_date: Optional[str] = None    # YYYY-MM-DD, defaults to today
    tweet_text: Optional[str] = None


class ProbablesBoardRequest(BaseModel):
    game_date: Optional[str] = None    # YYYY-MM-DD, defaults to today
    tweet_text: Optional[str] = None


# ──────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────


def _generate_batter_card_sync(req: BatterCardRequest) -> dict:
    if not req.feed_path and not req.parquet_path and not req.game_date:
        raise HTTPException(
            status_code=400,
            detail="Provide feed_path, parquet_path, or game_date for batter card generation.",
        )

    suffix = secrets.token_hex(4)
    cmd = [
        MLB_PYTHON,
        "scripts/batter_card_daily.py",
        "--output-suffix",
        suffix,
    ]
    if req.dark:
        cmd.append("--dark")

    if req.feed_path:
        fp = Path(req.feed_path)
        if not fp.is_absolute():
            fp = REPO_ROOT / fp
        cmd += ["--batter", str(req.player_id), "--feed", str(fp)]
    elif req.parquet_path:
        pp = Path(req.parquet_path)
        if not pp.is_absolute():
            pp = REPO_ROOT / pp
        cmd += ["--batter", str(req.player_id), "--parquet", str(pp)]
    else:
        cmd += ["--batters", str(req.player_id), "--date", (req.game_date or "yesterday").strip()]

    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="Card PNG not found after generation.")

    meta = _card_json_from_stdout(stdout)
    gk_meta = _game_pk_from_card_meta(meta)
    game_pk = gk_meta
    pname = _player_name_from_card_meta(meta)

    game_date = str(meta.get("game_date") or "")[:10] if meta else ""
    season = int(str(game_date)[:4]) if len(game_date) >= 4 else date.today().year
    stage = "regular_season"

    if req.feed_path:
        game_date = game_date or _game_date_from_feed(req.feed_path)
        game_pk = game_pk if game_pk is not None else _game_pk_from_feed(req.feed_path)
        season = _season_from_feed(req.feed_path)
        stage = _stage_from_feed(req.feed_path)
    elif req.parquet_path:
        pp = Path(req.parquet_path)
        if not pp.is_absolute():
            pp = REPO_ROOT / pp
        m = re.search(r"/(\d{4})/([^/]+)/", str(pp))
        if m:
            season = int(m.group(1))
            stage = m.group(2)
        m2 = re.search(r"game_(\d+)_(\d{8})_", pp.name)
        if m2:
            game_pk = int(m2.group(1))
            d = m2.group(2)
            game_date = game_date or f"{d[:4]}-{d[4:6]}-{d[6:]}"
    else:
        gd = (req.game_date or "yesterday").strip()
        if gd == "yesterday":
            game_date = game_date or str(date.today() - timedelta(days=1))
        else:
            game_date = game_date or gd[:10]

    tweet = req.tweet_text or _default_batter_tweet_from_meta(meta, pname, req.player_id, game_date)

    item_id = insert_queue_item(
        content_type="batter_card",
        title=out_path.stem,
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=season,
        stage=stage,
        game_pk=game_pk,
        player_id=req.player_id,
        player_name=pname,
        meta=meta,
    )

    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet, "image_path": str(out_path)}


@router.post("/batter")
async def generate_batter_card(req: BatterCardRequest):
    return await run_in_threadpool(_generate_batter_card_sync, req)


def _generate_pitcher_card_sync(req: PitcherCardRequest) -> dict:
    cmd = [
        MLB_PYTHON, "scripts/mallitalytics_daily_card.py",
        "--pitchers", str(req.player_id),
        "--date", req.game_date,
        # Unique filename avoids two hub requests overwriting the same PNG (race → flaky 500s).
        "--output-suffix", secrets.token_hex(4),
    ]
    if req.dark:
        cmd.append("--dark")
    if req.parquet_path:
        cmd += ["--parquet", req.parquet_path, "--pitcher", str(req.player_id)]

    stdout, stderr = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        tail_out = (stdout or "")[-3500:].strip()
        tail_err = (stderr or "")[-2000:].strip()
        parts = ["Pitcher card PNG not found after the script ran."]
        if tail_err:
            parts.append(f"--- stderr (tail) ---\n{tail_err}")
        if tail_out:
            parts.append(f"--- stdout (tail) ---\n{tail_out}")
        raise HTTPException(status_code=500, detail="\n\n".join(parts))

    game_date = req.game_date if req.game_date != "yesterday" else str(date.today() - timedelta(days=1))
    meta = _card_json_from_stdout(stdout)
    pname = _player_name_from_card_meta(meta)
    game_pk = _game_pk_from_card_meta(meta)

    tweet = req.tweet_text or _default_pitcher_tweet_from_meta(meta, pname, req.player_id, game_date)

    item_id = insert_queue_item(
        content_type="pitcher_card",
        title=out_path.stem,
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=int(game_date[:4]),
        stage="regular_season",
        game_pk=game_pk,
        player_id=req.player_id,
        player_name=pname,
        meta=meta,
    )

    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet, "image_path": str(out_path)}


@router.post("/pitcher")
async def generate_pitcher_card(req: PitcherCardRequest):
    return await run_in_threadpool(_generate_pitcher_card_sync, req)


def _generate_hr_tracker_sync(req: HRTrackerRequest) -> dict:
    game_date = req.game_date or str(date.today() - timedelta(days=1))
    cmd = [
        MLB_PYTHON, "scripts/hr_tracker_daily.py",
        "--date", game_date,
        "--format", "all",
        "--output-dir", str(OUTPUTS_ROOT),
    ]

    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="HR tracker PNG not found after generation.")

    cap = get_tweet_max_chars()
    tweet = truncate_tweet_text_to_cap(
        req.tweet_text
        or _hr_tracker_tweet_from_stdout(stdout)
        or f"💥 Home Runs — {game_date} #Mallitalytics",
        cap,
    )

    item_id = insert_queue_item(
        content_type="hr_tracker",
        title=f"HR Tracker {game_date}",
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=int(game_date[:4]),
        stage="regular_season",
    )

    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet, "image_path": str(out_path)}


@router.post("/hr-tracker")
async def generate_hr_tracker(req: HRTrackerRequest):
    return await run_in_threadpool(_generate_hr_tracker_sync, req)


def _generate_games_of_day_sync(req: GamesOfDayRequest) -> dict:
    """Run games_of_day_board.py — slate PNG + story-style tweet (probables on card)."""
    game_date = req.game_date or str(date.today())
    cmd = [
        MLB_PYTHON,
        "scripts/games_of_day_board.py",
        "--date",
        game_date,
        "--format",
        "all",
        "--output-dir",
        str(OUTPUTS_ROOT),
        "--output-suffix",
        secrets.token_hex(4),
    ]
    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="Games of Day PNG not found after generation.")

    cap = get_tweet_max_chars()
    tweet = truncate_tweet_text_to_cap(
        req.tweet_text
        or _tweet_body_after_marker(stdout)
        or f"Games of the day — {game_date} #Mallitalytics",
        cap,
    )

    item_id = insert_queue_item(
        content_type="games_of_day",
        title=f"Games of Day {game_date}",
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=int(game_date[:4]),
        stage="regular_season",
    )
    return {"id": item_id, "tweet_text": tweet, "game_date": game_date, "image_url": _image_url(out_path), "image_path": str(out_path)}


@router.post("/games-of-day")
async def generate_games_of_day(req: GamesOfDayRequest):
    """Generate Games of Day slate (PNG) + tweet draft."""
    return await run_in_threadpool(_generate_games_of_day_sync, req)


def _generate_probables_board_sync(req: ProbablesBoardRequest) -> dict:
    game_date = req.game_date or str(date.today())
    cmd = [
        MLB_PYTHON,
        "scripts/probables_board_daily.py",
        "--date",
        game_date,
        "--format",
        "all",
        "--output-dir",
        str(OUTPUTS_ROOT),
        "--output-suffix",
        secrets.token_hex(4),
    ]
    stdout, _ = _run_script(cmd)
    out_path = _extract_saved_path(stdout)
    if not out_path or not out_path.exists():
        raise HTTPException(status_code=500, detail="Probables board PNG not found after generation.")

    cap = get_tweet_max_chars()
    tweet = truncate_tweet_text_to_cap(
        req.tweet_text
        or _tweet_body_after_marker(stdout)
        or f"Probable starters — {game_date} #Mallitalytics",
        cap,
    )

    item_id = insert_queue_item(
        content_type="probables_board",
        title=f"Probables board {game_date}",
        tweet_text=tweet,
        image_path=str(out_path),
        image_url=_image_url(out_path),
        game_date=game_date,
        season=int(game_date[:4]),
        stage="regular_season",
    )
    return {"id": item_id, "image_url": _image_url(out_path), "tweet_text": tweet, "image_path": str(out_path)}


@router.post("/probables-board")
async def generate_probables_board(req: ProbablesBoardRequest):
    """Probable starters for the slate: PNG + tweet (W-L and ERA on image)."""
    return await run_in_threadpool(_generate_probables_board_sync, req)


def _search_players_sync(q: str, limit: int) -> dict:
    """MLB Stats API — run off the event loop."""
    try:
        r = _requests.get(
            f"{STATS_BASE}/people/search",
            params={"names": q, "sportId": 1},
            timeout=10,
        )
        r.raise_for_status()
        people = r.json().get("people", []) or []
        return {
            "players": [
                {
                    "id": p["id"],
                    "fullName": p.get("fullName", ""),
                    "primaryPosition": (p.get("primaryPosition") or {}).get("abbreviation", ""),
                    "currentTeam": (p.get("currentTeam") or {}).get("name", ""),
                }
                for p in people[:limit]
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLB people search error: {e}")


@router.get("/players/search")
async def search_players(
    q: str = Query(..., min_length=2, max_length=60),
    limit: int = Query(10, ge=1, le=25),
):
    """Search MLB players by name (MLB Stats API people/search)."""
    return await run_in_threadpool(_search_players_sync, q, limit)


def _player_games_sync(
    player_id: int,
    season: Optional[int],
    position: str,
) -> dict:
    if position not in ("pitcher", "batter"):
        raise HTTPException(status_code=400, detail="position must be pitcher or batter")
    season = season or date.today().year
    group = "pitching" if position == "pitcher" else "hitting"
    try:
        r = _requests.get(
            f"{STATS_BASE}/people/{player_id}/stats",
            params={"stats": "gameLog", "season": season, "group": group},
            timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLB stats error: {e}")

    splits: list[dict] = []
    for stat_group in r.json().get("stats", []):
        splits.extend(stat_group.get("splits", []))

    warehouse = get_warehouse_dir()
    try:
        seasons = (season, season - 1)
        available = _feed_live_index(warehouse, seasons)
    except OSError:
        available = {}

    games = []
    for sp in splits:
        game = sp.get("game") or {}
        team = sp.get("team") or {}
        opponent = sp.get("opponent") or {}
        stat = sp.get("stat") or {}
        gd_raw = sp.get("date", "")
        gd_compact = gd_raw.replace("-", "")
        gp = game.get("gamePk")
        key = (int(gp), gd_compact) if gp is not None else None
        feed_path = available.get(key) if key else None
        games.append({
            "game_pk": gp,
            "game_date": gd_raw,
            "home_away": "home" if sp.get("isHome") else "away",
            "team": team.get("name", ""),
            "opponent": opponent.get("name", ""),
            "stat": stat,
            "has_local_data": feed_path is not None,
            "feed_path": feed_path,
        })

    games.sort(key=lambda x: x["game_date"] or "", reverse=True)
    return {"games": games[:30], "season": season, "player_id": player_id}


@router.get("/players/{player_id}/games")
async def player_games(
    player_id: int,
    season: Optional[int] = Query(None),
    position: str = Query("pitcher"),
):
    """Game log for a player with local warehouse availability flag."""
    return await run_in_threadpool(_player_games_sync, player_id, season, position)


@router.get("/{item_id}/preview")
def get_card_preview(item_id: int):
    from api.db.database import get_queue_item
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    return {
        "id": item["id"],
        "image_url": item["image_url"],
        "tweet_text": item["tweet_text"],
        "status": item["status"],
        "title": item["title"],
        "player_name": item["player_name"],
        "game_date": item["game_date"],
    }
