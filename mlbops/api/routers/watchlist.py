"""
Watchlist — jobs/player_watchlist.json + player_watchlist table.

GET  /watchlist      — all players (active + inactive) for management UI
PUT  /watchlist      — replace JSON file and sync DB
"""
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from api.db.database import get_watchlist, write_watchlist_json_and_sync_db

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


class WatchlistPlayerIn(BaseModel):
    player_id: int
    player_name: str = ""
    position: Optional[str] = None
    team_abbrev: Optional[str] = None
    active: bool = True
    priority: int = Field(default=5, ge=1, le=10)
    notes: Optional[str] = ""


class WatchlistPutBody(BaseModel):
    players: list[WatchlistPlayerIn]


def _list_watchlist_sync() -> dict:
    players = get_watchlist(active_only=False)
    return {"players": players}


@router.get("")
async def list_watchlist():
    return await run_in_threadpool(_list_watchlist_sync)


def _put_watchlist_sync(body: WatchlistPutBody) -> dict:
    rows = [p.model_dump() for p in body.players]
    by_id: dict[int, dict[str, Any]] = {}
    for r in rows:
        by_id[int(r["player_id"])] = r
    ordered = list(by_id.values())
    ordered.sort(key=lambda x: (x.get("priority", 5), x["player_id"]))
    updated = write_watchlist_json_and_sync_db(ordered)
    return {"players": updated, "count": len(updated)}


@router.put("")
async def put_watchlist(body: WatchlistPutBody):
    return await run_in_threadpool(_put_watchlist_sync, body)
