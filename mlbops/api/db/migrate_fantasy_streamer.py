"""Migration: allow fantasy streamer queue items.

Expands `content_queue.content_type` CHECK to include `fantasy_streamer`.

Run:
  cd mlbops && ../mlb_env/bin/python -m api.db.migrate_fantasy_streamer
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
DB_PATH = _REPO / "data" / "hub.db"

CONTENT_QUEUE_NEW = """
CREATE TABLE content_queue_new (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content_type        TEXT NOT NULL CHECK(content_type IN (
                            'batter_card','pitcher_card','hr_tracker',
                            'game_recap','leaderboard','preview','games_of_day',
                            'probables_board','insight_tile','text_only',
                            'live_event','fantasy_streamer'
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
    posted_at           TEXT,
    content_pillar      TEXT,
    hook_type           TEXT,
    intended_kpi        TEXT,
    priority_score      INTEGER DEFAULT 0,
    campaign            TEXT,
    source_module       TEXT,
    manual_or_ai        TEXT,
    experiment_tag      TEXT
);
"""

COLUMNS = [
    "id",
    "content_type",
    "status",
    "title",
    "tweet_text",
    "image_path",
    "image_url",
    "game_pk",
    "player_id",
    "player_name",
    "game_date",
    "season",
    "stage",
    "meta_json",
    "error_message",
    "twitter_post_id",
    "twitter_likes",
    "twitter_retweets",
    "twitter_replies",
    "twitter_impressions",
    "created_at",
    "reviewed_at",
    "posted_at",
    "content_pillar",
    "hook_type",
    "intended_kpi",
    "priority_score",
    "campaign",
    "source_module",
    "manual_or_ai",
    "experiment_tag",
]

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_queue_status ON content_queue(status)",
    "CREATE INDEX IF NOT EXISTS idx_queue_date ON content_queue(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_queue_player ON content_queue(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_queue_content_pillar ON content_queue(content_pillar)",
    "CREATE INDEX IF NOT EXISTS idx_queue_intended_kpi ON content_queue(intended_kpi)",
    "CREATE INDEX IF NOT EXISTS idx_queue_priority ON content_queue(priority_score)",
]


def _already_supports_fantasy(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='content_queue'"
    ).fetchone()
    return bool(row and row[0] and "fantasy_streamer" in row[0])


def migrate() -> None:
    if not DB_PATH.is_file():
        print(f"No database at {DB_PATH}; nothing to do.", file=sys.stderr)
        return

    conn = sqlite3.connect(str(DB_PATH))
    try:
        if _already_supports_fantasy(conn):
            print("content_queue already allows fantasy_streamer.")
            return

        conn.execute("BEGIN IMMEDIATE")
        conn.executescript(CONTENT_QUEUE_NEW.strip())
        cols = ", ".join(COLUMNS)
        conn.execute(f"INSERT INTO content_queue_new ({cols}) SELECT {cols} FROM content_queue")
        conn.execute("DROP TABLE content_queue")
        conn.execute("ALTER TABLE content_queue_new RENAME TO content_queue")
        for sql in INDEXES:
            conn.execute(sql)
        conn.commit()
        print(f"OK: migrated {DB_PATH}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
