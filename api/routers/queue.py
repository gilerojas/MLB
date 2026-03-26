"""
Queue management endpoints.

GET    /queue           — list items (filterable by status/date)
GET    /queue/{id}      — single item detail
PATCH  /queue/{id}      — update tweet_text or status
DELETE /queue/{id}      — delete a draft item
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.db.database import (
    delete_queue_item,
    get_queue_item,
    list_queue,
    update_queue_item,
)

router = APIRouter(prefix="/queue", tags=["queue"])


class QueueItemPatch(BaseModel):
    tweet_text: Optional[str] = None
    status: Optional[str] = None


@router.get("")
def get_queue(
    status: Optional[str] = Query(None, description="Filter by status"),
    game_date: Optional[str] = Query(None, description="Filter by game date YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    items = list_queue(status=status, game_date=game_date, limit=limit, offset=offset)
    return {"items": items, "count": len(items)}


@router.get("/{item_id}")
def get_item(item_id: int):
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    return item


@router.patch("/{item_id}")
def patch_item(item_id: int, body: QueueItemPatch):
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")

    updates = {}
    if body.tweet_text is not None:
        updates["tweet_text"] = body.tweet_text[:280]
    if body.status is not None:
        allowed_statuses = {"draft", "approved", "rejected", "posted", "failed"}
        if body.status not in allowed_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {allowed_statuses}")
        updates["status"] = body.status
        if body.status in ("approved", "rejected"):
            updates["reviewed_at"] = datetime.utcnow().isoformat()

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update.")

    update_queue_item(item_id, **updates)
    return get_queue_item(item_id)


@router.delete("/{item_id}")
def delete_item(item_id: int):
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    if item["status"] != "draft":
        raise HTTPException(status_code=400, detail="Only draft items can be deleted.")
    deleted = delete_queue_item(item_id)
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete item.")
    return {"deleted": True, "id": item_id}
