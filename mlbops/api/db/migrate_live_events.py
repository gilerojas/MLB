"""Migration: live-events feature.

1) Create `live_events` table for idempotent in-game event detection.
2) Expand `content_queue.content_type` CHECK to include 'live_event'.

Safe to re-run — each step is guarded.

Run:
  cd mlbops && ../mlb_env/bin/python -m api.db.migrate_live_events
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
DB_PATH = _REPO / "data" / "hub.db"

CREATE_LIVE_EVENTS = """
CREATE TABLE IF NOT EXISTS live_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key     TEXT NOT NULL UNIQUE,
    game_pk        INTEGER NOT NULL,
    game_date      TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    player_id      INTEGER,
    player_name    TEXT,
    headline       TEXT NOT NULL,
    tweet_text     TEXT NOT NULL,
    payload_json   TEXT,
    status         TEXT NOT NULL DEFAULT 'new'
                   CHECK(status IN ('new','queued','dismissed')),
    queue_id       INTEGER,
    detected_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_live_events_date ON live_events(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_live_events_status ON live_events(status)",
    "CREATE INDEX IF NOT EXISTS idx_live_events_game ON live_events(game_pk)",
]

# Full CHECK list must match migrate_queue_content_types plus 'live_event'.
CONTENT_QUEUE_NEW = """
CREATE TABLE content_queue_new (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content_type        TEXT NOT NULL CHECK(content_type IN (
                            'batter_card','pitcher_card','hr_tracker',
                            'game_recap','leaderboard','preview','games_of_day',
                            'probables_board','insight_tile','text_only',
                            'live_event'
                        )),
    status              TEXT NOT NULL DEFAULT 'draft' CHECK(status IN (
                            'draft','approved','rejected','posted','failed'
                        )),
    title               TEXT,
    tweet_text          TEXT,
    image_path          TEXT,
    image_url           TEXT,
    game_pk             INTEGER,
    player_id           INTEGER,
    player_name         TEXT,
    game_date           TEXT,
    season              INTEGER,
    stage               TEXT,
    meta_json           TEXT,
    error_message       TEXT,
    twitter_post_id     TEXT,
    twitter_likes       INTEGER DEFAULT 0,
    twitter_retweets    INTEGER DEFAULT 0,
    twitter_replies     INTEGER DEFAULT 0,
    twitter_impressions INTEGER DEFAULT 0,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    reviewed_at         TEXT,
    posted_at           TEXT
);
"""


def _queue_already_supports_live(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='content_queue'"
    ).fetchone()
    return bool(row and row[0] and "live_event" in row[0])


def _migrate_content_queue(conn: sqlite3.Connection) -> None:
    if _queue_already_supports_live(conn):
        print("content_queue already allows live_event — skipping queue migration.")
        return
    print("Migrating content_queue to add 'live_event' content type…")
    conn.execute("BEGIN IMMEDIATE")
    conn.executescript(CONTENT_QUEUE_NEW.strip())
    conn.execute("INSERT INTO content_queue_new SELECT * FROM content_queue")
    conn.execute("DROP TABLE content_queue")
    conn.execute("ALTER TABLE content_queue_new RENAME TO content_queue")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON content_queue(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_date ON content_queue(game_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_player ON content_queue(player_id)")
    conn.commit()


def migrate() -> None:
    if not DB_PATH.is_file():
        print(f"No database at {DB_PATH}; nothing to do.", file=sys.stderr)
        return

    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.executescript(CREATE_LIVE_EVENTS.strip())
        for sql in CREATE_INDEXES:
            conn.execute(sql)
        conn.commit()
        print("live_events table ready.")

        _migrate_content_queue(conn)
        print(f"OK: migration complete on {DB_PATH}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
