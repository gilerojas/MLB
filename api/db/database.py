"""SQLite helpers shared across FastAPI routers and job scripts."""
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent.parent.parent / "data" / "hub.db"


@contextmanager
def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_queue_item(
    content_type: str,
    title: str,
    tweet_text: str,
    image_path: str,
    image_url: str,
    game_date: str,
    season: int,
    stage: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    player_name: Optional[str] = None,
    meta: Optional[dict] = None,
) -> int:
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO content_queue
                (content_type, title, tweet_text, image_path, image_url,
                 game_pk, player_id, player_name, game_date, season, stage, meta_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                content_type, title, tweet_text, image_path, image_url,
                game_pk, player_id, player_name, game_date, season, stage,
                json.dumps(meta) if meta else None,
            ),
        )
        return cur.lastrowid


def get_queue_item(item_id: int) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM content_queue WHERE id = ?", (item_id,)
        ).fetchone()
        return dict(row) if row else None


def list_queue(
    status: Optional[str] = None,
    game_date: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    clauses = []
    params: list = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if game_date:
        clauses.append("game_date = ?")
        params.append(game_date)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM content_queue {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        return [dict(r) for r in rows]


def update_queue_item(item_id: int, **fields) -> bool:
    allowed = {
        "status", "tweet_text", "twitter_post_id", "twitter_likes",
        "twitter_retweets", "twitter_replies", "twitter_impressions",
        "reviewed_at", "posted_at", "error_message",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    with get_db() as conn:
        conn.execute(
            f"UPDATE content_queue SET {set_clause} WHERE id = ?",
            list(updates.values()) + [item_id],
        )
        return True


def delete_queue_item(item_id: int) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM content_queue WHERE id = ? AND status = 'draft'",
            (item_id,),
        )
        return cur.rowcount > 0


def get_watchlist(active_only: bool = True) -> list[dict]:
    with get_db() as conn:
        where = "WHERE active = 1" if active_only else ""
        rows = conn.execute(
            f"SELECT * FROM player_watchlist {where} ORDER BY priority ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def log_notification(
    notification_type: str,
    channel: str,
    recipient: str,
    subject: Optional[str],
    body_preview: Optional[str],
    status: str,
    external_id: Optional[str] = None,
) -> int:
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO notification_log
                (notification_type, channel, recipient, subject, body_preview, status, external_id)
            VALUES (?,?,?,?,?,?,?)
            """,
            (notification_type, channel, recipient, subject, body_preview, status, external_id),
        )
        return cur.lastrowid
