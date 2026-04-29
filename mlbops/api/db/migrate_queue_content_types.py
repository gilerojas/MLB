"""
One-time migration: expand content_queue.content_type CHECK for probables_board,
insight_tile, text_only.

Run:
  cd mlbops && ../mlb_env/bin/python -m api.db.migrate_queue_content_types
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
DB_PATH = _REPO / "data" / "hub.db"

CREATE_NEW = """
CREATE TABLE content_queue_new (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content_type        TEXT NOT NULL CHECK(content_type IN (
                            'batter_card','pitcher_card','hr_tracker',
                            'game_recap','leaderboard','preview','games_of_day',
                            'probables_board','insight_tile','text_only'
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


def migrate() -> None:
    if not DB_PATH.is_file():
        print(f"No database at {DB_PATH}; nothing to do.", file=sys.stderr)
        return

    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='content_queue'"
        )
        row = cur.fetchone()
        if not row or not row[0]:
            print("content_queue missing; nothing to do.", file=sys.stderr)
            return
        if "insight_tile" in row[0]:
            print("content_queue already allows insight_tile.")
            return

        conn.execute("BEGIN IMMEDIATE")
        conn.executescript(CREATE_NEW.strip())
        conn.execute("INSERT INTO content_queue_new SELECT * FROM content_queue")
        conn.execute("DROP TABLE content_queue")
        conn.execute("ALTER TABLE content_queue_new RENAME TO content_queue")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON content_queue(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_date ON content_queue(game_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_player ON content_queue(player_id)")
        conn.commit()
        print(f"OK: migrated {DB_PATH}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
