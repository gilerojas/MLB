"""Database helpers shared across FastAPI routers and job scripts.

Local development defaults to SQLite. Production can use Postgres by setting:

  MLBOPS_DB_BACKEND=postgres
  DATABASE_URL=postgresql://...
"""
import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from api.paths import get_repo_root
from api.services.content_taxonomy import normalize_queue_metadata

REPO_ROOT = get_repo_root()
DB_PATH = REPO_ROOT / "data" / "hub.db"
WATCHLIST_JSON_PATH = REPO_ROOT / "jobs" / "player_watchlist.json"


def _db_backend() -> str:
    return os.environ.get("MLBOPS_DB_BACKEND", "sqlite").strip().lower() or "sqlite"


def using_postgres() -> bool:
    return _db_backend() in {"postgres", "postgresql", "pg"}


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("MLBOPS_DB_BACKEND=postgres requires DATABASE_URL.")
    return url


def _sqlite_row_to_dict(row) -> dict:
    return dict(row) if row is not None else {}


class _PgCursorAdapter:
    def __init__(self, cursor):
        self._cursor = cursor
        self.rowcount = cursor.rowcount
        self.lastrowid: Optional[int] = None

    def fetchone(self):
        row = self._cursor.fetchone()
        return row

    def fetchall(self):
        return self._cursor.fetchall()


class _PgConnectionAdapter:
    """Small DB-API compatibility layer for existing SQLite-style router SQL.

    This is intentionally narrow: it supports the query patterns currently used
    by mlbops routes while allowing Phase 1 to keep local SQLite unchanged.
    """

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None):
        cur = self._conn.cursor()
        q = _translate_sqlite_sql(sql)
        p = _translate_params(params)
        cur.execute(q, p)
        adapter = _PgCursorAdapter(cur)
        if q.lstrip().upper().startswith("INSERT") and "RETURNING id" in q:
            row = cur.fetchone()
            if row:
                adapter.lastrowid = int(row["id"])
        adapter.rowcount = cur.rowcount
        return adapter

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def _translate_params(params):
    if params is None:
        return None
    if isinstance(params, dict):
        return params
    return tuple(params)


_NAMED_PARAM_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


def _translate_named_params(sql: str) -> str:
    return _NAMED_PARAM_RE.sub(r"%(\1)s", sql)


def _translate_qmark_params(sql: str) -> str:
    return sql.replace("?", "%s")


def _translate_sqlite_sql(sql: str) -> str:
    s = sql
    upper = s.lstrip().upper()
    insert_or_ignore = upper.startswith("INSERT OR IGNORE")
    if insert_or_ignore:
        s = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", s, count=1, flags=re.I)

    s = s.replace("datetime('now')", "CURRENT_TIMESTAMP")
    s = s.replace("datetime('now', ?)", "CURRENT_TIMESTAMP + (%s)::interval")
    s = s.replace("date('now', ?)", "CURRENT_DATE + (%s)::interval")

    if ":" in s:
        s = _translate_named_params(s)
    s = _translate_qmark_params(s)

    if insert_or_ignore and "ON CONFLICT" not in s.upper():
        s = s.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # Existing insert_queue_item expects lastrowid. Add RETURNING only where safe.
    if upper.startswith("INSERT INTO CONTENT_QUEUE") and "RETURNING" not in s.upper():
        s = s.rstrip().rstrip(";") + " RETURNING id"
    elif upper.startswith("INSERT INTO NOTIFICATION_LOG") and "RETURNING" not in s.upper():
        s = s.rstrip().rstrip(";") + " RETURNING id"

    return s


@contextmanager
def get_db():
    if using_postgres():
        import psycopg
        from psycopg.rows import dict_row

        raw_conn = psycopg.connect(_database_url(), row_factory=dict_row)
        conn = _PgConnectionAdapter(raw_conn)
    else:
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
    image_path: Optional[str] = None,
    image_url: Optional[str] = None,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    player_name: Optional[str] = None,
    game_date: str = "",
    season: int = 0,
    stage: str = "regular_season",
    meta: Optional[dict] = None,
) -> int:
    ip = image_path if image_path is not None else ""
    iu = image_url if image_url is not None else ""
    normalized = normalize_queue_metadata(content_type, meta)
    merged_meta = {**(meta or {}), **normalized}
    payload = (
        content_type, title, tweet_text, ip, iu,
        game_pk, player_id, player_name, game_date, season, stage,
        json.dumps(merged_meta) if merged_meta else None,
        normalized["content_pillar"],
        normalized["hook_type"],
        normalized["intended_kpi"],
        normalized["priority_score"],
        normalized["campaign"],
        normalized["source_module"],
        normalized["manual_or_ai"],
        normalized["experiment_tag"],
    )
    sql = """
            INSERT INTO content_queue
                (content_type, title, tweet_text, image_path, image_url,
                 game_pk, player_id, player_name, game_date, season, stage, meta_json,
                 content_pillar, hook_type, intended_kpi, priority_score,
                 campaign, source_module, manual_or_ai, experiment_tag)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
    last_err: Optional[sqlite3.OperationalError] = None
    for attempt in range(8):
        try:
            with get_db() as conn:
                cur = conn.execute(sql, payload)
                return cur.lastrowid
        except sqlite3.OperationalError as e:
            last_err = e
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            time.sleep(0.04 * (attempt + 1))
    assert last_err is not None
    raise last_err


def get_queue_item(item_id: int) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM content_queue WHERE id = ?", (item_id,)
        ).fetchone()
        return dict(row) if row else None


def count_queue(
    status: Optional[str] = None,
    game_date: Optional[str] = None,
    content_pillar: Optional[str] = None,
    intended_kpi: Optional[str] = None,
) -> int:
    clauses = []
    params: list = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if game_date:
        clauses.append("game_date = ?")
        params.append(game_date)
    if content_pillar:
        clauses.append("content_pillar = ?")
        params.append(content_pillar)
    if intended_kpi:
        clauses.append("intended_kpi = ?")
        params.append(intended_kpi)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_db() as conn:
        row = conn.execute(
            f"SELECT COUNT(*) AS n FROM content_queue {where}", params
        ).fetchone()
        return int(row["n"]) if row else 0


def list_queue(
    status: Optional[str] = None,
    game_date: Optional[str] = None,
    content_pillar: Optional[str] = None,
    intended_kpi: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "created_at",
    order: str = "desc",
) -> list[dict]:
    clauses = []
    params: list = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if game_date:
        clauses.append("game_date = ?")
        params.append(game_date)
    if content_pillar:
        clauses.append("content_pillar = ?")
        params.append(content_pillar)
    if intended_kpi:
        clauses.append("intended_kpi = ?")
        params.append(intended_kpi)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sort_cols = {"created_at", "game_date", "content_type", "priority_score", "content_pillar"}
    col = sort_by if sort_by in sort_cols else "created_at"
    ord_sql = "ASC" if order.lower() == "asc" else "DESC"
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM content_queue {where} ORDER BY {col} {ord_sql} LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        return [dict(r) for r in rows]


def update_queue_item(item_id: int, **fields) -> bool:
    allowed = {
        "status", "tweet_text", "twitter_post_id", "twitter_likes",
        "twitter_retweets", "twitter_replies", "twitter_impressions",
        "reviewed_at", "posted_at", "error_message", "meta_json",
        "content_pillar", "hook_type", "intended_kpi", "priority_score",
        "campaign", "source_module", "manual_or_ai", "experiment_tag",
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


def delete_draft_queue_items() -> int:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM content_queue WHERE status = 'draft'")
        return int(cur.rowcount or 0)


def get_queue_status_counts() -> dict[str, int]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM content_queue GROUP BY status"
        ).fetchall()
        return {str(r["status"]): int(r["n"]) for r in rows}


def get_watchlist(active_only: bool = True) -> list[dict]:
    with get_db() as conn:
        where = "WHERE active = 1" if active_only else ""
        rows = conn.execute(
            f"SELECT * FROM player_watchlist {where} ORDER BY priority ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def write_watchlist_json_and_sync_db(players: list[dict]) -> list[dict]:
    """Persist jobs/player_watchlist.json and replace player_watchlist table to match."""
    if not using_postgres():
        WATCHLIST_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        WATCHLIST_JSON_PATH.write_text(
            json.dumps(players, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    with get_db() as conn:
        conn.execute("DELETE FROM player_watchlist")
        for p in players:
            conn.execute(
                """
                INSERT INTO player_watchlist
                    (player_id, player_name, position, team_id, team_abbrev, active, priority, notes)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    int(p["player_id"]),
                    p.get("player_name") or "",
                    p.get("position"),
                    p.get("team_id"),
                    p.get("team_abbrev"),
                    1 if p.get("active", True) else 0,
                    int(p.get("priority", 5)),
                    p.get("notes") or "",
                ),
            )
    return get_watchlist(active_only=False)


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
