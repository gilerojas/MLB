"""/live/* — in-game event scanner and per-event Queue promotion.

Flow:
  1. Hub calls POST /live/scan?date=YYYY-MM-DD.
  2. Router lists today's MLB games, skips Preview games, pulls /feed/live
     for each Live or Final game, runs detectors, and INSERT OR IGNOREs each
     event into the `live_events` table (unique dedupe_key).
  3. Hub renders the events table with per-row Queue + Dismiss buttons.
  4. POST /live/events/{id}/queue inserts a content_queue draft and links it
     back via live_events.queue_id + status='queued'.
  5. POST /live/events/{id}/dismiss flips status to 'dismissed'.

All heavy work (HTTP to MLB + SQLite writes) runs in a thread pool so the
FastAPI event loop stays responsive.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from api.db.database import get_db, insert_queue_item
from api.live import detect, fetch, text

router = APIRouter(prefix="/live", tags=["live"])


# ---------------------------------------------------------------------------
# schemas

class LiveGameSummary(BaseModel):
    game_pk: int
    game_date: Optional[str] = None
    status_abstract: str
    status_detailed: str
    away_team_abbr: Optional[str] = None
    home_team_abbr: Optional[str] = None
    away_score: Optional[int] = None
    home_score: Optional[int] = None
    inning: Optional[int] = None
    inning_state: Optional[str] = None


class LiveEvent(BaseModel):
    id: int
    dedupe_key: str
    game_pk: int
    game_date: str
    event_type: str
    player_id: Optional[int] = None
    player_name: Optional[str] = None
    headline: str
    tweet_text: str
    payload: Optional[dict[str, Any]] = None
    status: str
    queue_id: Optional[int] = None
    detected_at: str


class ScanResponse(BaseModel):
    date: str
    games_scanned: int
    games_total: int
    new_events: int
    events: list[LiveEvent]
    errors: list[str] = []


# ---------------------------------------------------------------------------
# helpers

def _summarize_game(g: dict[str, Any]) -> LiveGameSummary:
    status = g.get("status") or {}
    teams = g.get("teams") or {}
    line = g.get("linescore") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    return LiveGameSummary(
        game_pk=int(g.get("gamePk") or 0),
        game_date=(g.get("officialDate") or g.get("gameDate") or "")[:10] or None,
        status_abstract=str(status.get("abstractGameState") or ""),
        status_detailed=str(status.get("detailedState") or ""),
        away_team_abbr=((away.get("team") or {}).get("abbreviation")) or None,
        home_team_abbr=((home.get("team") or {}).get("abbreviation")) or None,
        away_score=away.get("score"),
        home_score=home.get("score"),
        inning=line.get("currentInning"),
        inning_state=line.get("inningState"),
    )


def _row_to_event(row: dict[str, Any]) -> LiveEvent:
    payload_raw = row.get("payload_json")
    payload: Optional[dict[str, Any]]
    if payload_raw:
        try:
            payload = json.loads(payload_raw)
        except (TypeError, ValueError):
            payload = None
    else:
        payload = None
    return LiveEvent(
        id=int(row["id"]),
        dedupe_key=row["dedupe_key"],
        game_pk=int(row["game_pk"]),
        game_date=row["game_date"],
        event_type=row["event_type"],
        player_id=row.get("player_id"),
        player_name=row.get("player_name"),
        headline=row["headline"],
        tweet_text=row["tweet_text"],
        payload=payload,
        status=row["status"],
        queue_id=row.get("queue_id"),
        detected_at=row["detected_at"],
    )


def _insert_events(events: list[dict[str, Any]]) -> int:
    """Insert detected events (INSERT OR IGNORE). Returns # newly inserted."""
    if not events:
        return 0
    new_count = 0
    with get_db() as conn:
        for ev in events:
            tweet = text.build_tweet(ev)
            try:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO live_events
                        (dedupe_key, game_pk, game_date, event_type,
                         player_id, player_name, headline, tweet_text,
                         payload_json, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')
                    """,
                    (
                        ev["dedupe_key"],
                        int(ev.get("game_pk") or 0),
                        str(ev.get("game_date") or ""),
                        ev["event_type"],
                        ev.get("player_id"),
                        ev.get("player_name"),
                        ev["headline"],
                        tweet,
                        json.dumps(ev.get("payload") or {}, default=str),
                    ),
                )
                if cur.rowcount:
                    new_count += 1
            except Exception as e:  # pragma: no cover - defensive
                # Do not let a single bad event abort the scan.
                conn.rollback()
                raise RuntimeError(f"insert failed for {ev.get('dedupe_key')}: {e}")
    return new_count


def _select_events_for_date(game_date: str) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, dedupe_key, game_pk, game_date, event_type,
                   player_id, player_name, headline, tweet_text,
                   payload_json, status, queue_id, detected_at
            FROM live_events
            WHERE game_date = ?
            ORDER BY detected_at DESC, id DESC
            """,
            (game_date,),
        ).fetchall()
        return [dict(r) for r in rows]


def _scan_sync(date: Optional[str]) -> ScanResponse:
    d = date or fetch.today_et()
    errors: list[str] = []
    try:
        games = fetch.list_games(d)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLB schedule fetch failed: {e}")

    detected_all: list[dict[str, Any]] = []
    scanned = 0
    for g in games:
        status = g.get("status") or {}
        abstract = str(status.get("abstractGameState") or "")
        detailed = str(status.get("detailedState") or "")
        if not (fetch.is_live_state(abstract, detailed) or fetch.is_final_state(abstract)):
            continue
        game_pk = int(g.get("gamePk") or 0)
        if not game_pk:
            continue
        try:
            feed = fetch.get_live_feed(game_pk)
        except Exception as e:
            errors.append(f"game {game_pk}: feed fetch failed ({e})")
            continue
        try:
            ev_list = detect.run_all(feed)
        except Exception as e:
            errors.append(f"game {game_pk}: detect failed ({e})")
            continue
        detected_all.extend(ev_list)
        scanned += 1

    try:
        new_count = _insert_events(detected_all)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB insert failed: {e}")

    rows = _select_events_for_date(d)
    events = [_row_to_event(r) for r in rows]
    return ScanResponse(
        date=d,
        games_scanned=scanned,
        games_total=len(games),
        new_events=new_count,
        events=events,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# endpoints

@router.get("/games", response_model=list[LiveGameSummary])
async def live_games(
    date: Optional[str] = Query(default=None, description="YYYY-MM-DD (ET). Defaults to today."),
):
    """Lightweight today's-slate summary: live + final games."""
    d = date or fetch.today_et()
    try:
        games = await run_in_threadpool(fetch.list_games, d)
    except Exception as e:
        # Non-fatal for the hub UI: if MLB schedule is temporarily unavailable,
        # keep the page functional and let events still load/scan independently.
        return []
    return [_summarize_game(g) for g in games]


@router.get("/events", response_model=list[LiveEvent])
async def live_events_for_date(
    date: Optional[str] = Query(default=None),
):
    """All detected events for a date (no rescan)."""
    d = date or fetch.today_et()
    rows = await run_in_threadpool(_select_events_for_date, d)
    return [_row_to_event(r) for r in rows]


@router.post("/scan", response_model=ScanResponse)
async def scan(
    date: Optional[str] = Query(default=None, description="YYYY-MM-DD (ET). Defaults to today."),
):
    """Fetch live feeds for all live/final games and refresh event detection.

    Idempotent: UNIQUE(dedupe_key) means repeat scans do not duplicate events.
    """
    return await run_in_threadpool(_scan_sync, date)


@router.post("/events/{event_id}/queue", response_model=LiveEvent)
async def queue_event(event_id: int):
    """Insert the event as a content_queue draft and link it back."""
    def _do() -> dict[str, Any]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM live_events WHERE id = ?", (event_id,)
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="event not found")
            r = dict(row)
            if r["status"] == "queued" and r["queue_id"]:
                return r  # already queued — return current row
        # Insert queue item outside the outer connection: insert_queue_item
        # opens its own connection with retry. meta holds the raw payload.
        meta: dict[str, Any] = {
            "source": "live_event",
            "event_type": r["event_type"],
            "dedupe_key": r["dedupe_key"],
            "headline": r["headline"],
        }
        if r.get("payload_json"):
            try:
                meta["payload"] = json.loads(r["payload_json"])
            except (TypeError, ValueError):
                pass
        queue_id = insert_queue_item(
            content_type="live_event",
            title=r["headline"],
            tweet_text=r["tweet_text"],
            game_pk=r.get("game_pk"),
            player_id=r.get("player_id"),
            player_name=r.get("player_name"),
            game_date=r.get("game_date") or "",
            meta=meta,
        )
        with get_db() as conn:
            conn.execute(
                "UPDATE live_events SET status='queued', queue_id=? WHERE id=?",
                (queue_id, event_id),
            )
            row2 = conn.execute(
                "SELECT * FROM live_events WHERE id = ?", (event_id,)
            ).fetchone()
        return dict(row2)

    row = await run_in_threadpool(_do)
    return _row_to_event(row)


@router.post("/events/{event_id}/dismiss", response_model=LiveEvent)
async def dismiss_event(event_id: int):
    def _do() -> dict[str, Any]:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM live_events WHERE id = ?", (event_id,)
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="event not found")
            conn.execute(
                "UPDATE live_events SET status='dismissed' WHERE id = ?",
                (event_id,),
            )
            row2 = conn.execute(
                "SELECT * FROM live_events WHERE id = ?", (event_id,)
            ).fetchone()
        return dict(row2)

    row = await run_in_threadpool(_do)
    return _row_to_event(row)
