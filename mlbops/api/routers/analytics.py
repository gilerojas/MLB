"""Manual post-performance analytics endpoints."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from api.db.database import get_db, get_queue_item
from api.services.analytics_service import (
    build_performance_payload,
    get_growth_summary,
    upsert_post_performance,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _growth_summary_sync(days: int) -> dict:
    with get_db() as conn:
        return get_growth_summary(conn, days)


@router.get("/growth-summary")
async def growth_summary(days: int = Query(30, ge=1, le=365)):
    return await run_in_threadpool(_growth_summary_sync, days)


class PostPerformanceInput(BaseModel):
    x_post_id: Optional[str] = None
    posted_at: Optional[str] = None
    impressions: int = Field(default=0, ge=0)
    likes: int = Field(default=0, ge=0)
    replies: int = Field(default=0, ge=0)
    reposts: int = Field(default=0, ge=0)
    quote_tweets: int = Field(default=0, ge=0)
    bookmarks: int = Field(default=0, ge=0)
    profile_visits: int = Field(default=0, ge=0)
    follows: int = Field(default=0, ge=0)
    notes: str = ""


def _list_performance_sync(limit: int, offset: int) -> dict:
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT pp.*, cq.title, cq.player_name, cq.tweet_text
            FROM post_performance pp
            LEFT JOIN content_queue cq ON cq.id = pp.queue_item_id
            ORDER BY COALESCE(pp.posted_at, pp.updated_at, pp.created_at) DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS n FROM post_performance").fetchone()
    return {"items": [dict(row) for row in rows], "total": int(total["n"] if total else 0)}


@router.get("/performance")
async def list_performance(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return await run_in_threadpool(_list_performance_sync, limit, offset)


def _get_performance_sync(queue_item_id: int) -> dict:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM post_performance WHERE queue_item_id = ?",
            (queue_item_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Performance metrics not found.")
    return dict(row)


@router.get("/performance/{queue_item_id}")
async def get_performance(queue_item_id: int):
    return await run_in_threadpool(_get_performance_sync, queue_item_id)


def _upsert_performance_sync(queue_item_id: int, body: PostPerformanceInput) -> dict:
    item = get_queue_item(queue_item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    payload = build_performance_payload(item, body.model_dump())
    with get_db() as conn:
        return upsert_post_performance(conn, payload)


@router.put("/performance/{queue_item_id}")
async def upsert_performance(queue_item_id: int, body: PostPerformanceInput):
    return await run_in_threadpool(_upsert_performance_sync, queue_item_id, body)
