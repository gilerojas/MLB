from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from api.services.fantasy_service import get_streamer_matrix

router = APIRouter(prefix="/fantasy", tags=["fantasy"])


@router.get("/streamers")
async def streamers(
    game_date: str | None = Query(default=None, description="Target date in YYYY-MM-DD format."),
    season: int | None = Query(default=None, ge=2021, le=2030),
    limit: int = Query(default=30, ge=1, le=100),
    include_live_probables: bool = Query(
        default=True,
        description="Try MLB Stats API probables first, then fall back to local warehouse projection.",
    ),
):
    try:
        return await run_in_threadpool(
            get_streamer_matrix,
            game_date,
            season,
            limit,
            include_live_probables,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
