"""Migration: Session 1 upscale foundation.

Adds first-class queue metadata columns and a manual post_performance table.

Run:
  cd mlbops && ../mlb_env/bin/python -m api.db.migrate_upscale_session1
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
DB_PATH = _REPO / "data" / "hub.db"

QUEUE_COLUMNS = {
    "content_pillar": "TEXT",
    "hook_type": "TEXT",
    "intended_kpi": "TEXT",
    "priority_score": "INTEGER DEFAULT 0",
    "campaign": "TEXT",
    "source_module": "TEXT",
    "manual_or_ai": "TEXT",
    "experiment_tag": "TEXT",
}

CREATE_POST_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS post_performance (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_item_id                   INTEGER NOT NULL UNIQUE REFERENCES content_queue(id) ON DELETE CASCADE,
    x_post_id                       TEXT,
    posted_at                       TEXT,
    content_type                    TEXT,
    content_pillar                  TEXT,
    hook_type                       TEXT,
    intended_kpi                    TEXT,
    impressions                     INTEGER DEFAULT 0,
    likes                           INTEGER DEFAULT 0,
    replies                         INTEGER DEFAULT 0,
    reposts                         INTEGER DEFAULT 0,
    quote_tweets                    INTEGER DEFAULT 0,
    bookmarks                       INTEGER DEFAULT 0,
    profile_visits                  INTEGER DEFAULT 0,
    follows                         INTEGER DEFAULT 0,
    engagement_rate                 REAL DEFAULT 0,
    bookmark_rate                   REAL DEFAULT 0,
    reply_rate                      REAL DEFAULT 0,
    repost_rate                     REAL DEFAULT 0,
    follows_per_1000_impressions    REAL DEFAULT 0,
    notes                           TEXT,
    created_at                      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at                      TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_queue_content_pillar ON content_queue(content_pillar)",
    "CREATE INDEX IF NOT EXISTS idx_queue_intended_kpi ON content_queue(intended_kpi)",
    "CREATE INDEX IF NOT EXISTS idx_queue_priority ON content_queue(priority_score)",
    "CREATE INDEX IF NOT EXISTS idx_post_perf_pillar ON post_performance(content_pillar)",
    "CREATE INDEX IF NOT EXISTS idx_post_perf_posted ON post_performance(posted_at)",
]

BACKFILL_QUEUE_METADATA = """
UPDATE content_queue
SET
    content_pillar = COALESCE(content_pillar, CASE content_type
        WHEN 'probables_board' THEN 'probables'
        WHEN 'pitcher_card' THEN 'pitcher_to_watch'
        WHEN 'batter_card' THEN 'player_card'
        WHEN 'leaderboard' THEN 'leaderboard_watch'
        WHEN 'insight_tile' THEN 'statcast_signal'
        WHEN 'hr_tracker' THEN 'hr_tracker'
        WHEN 'games_of_day' THEN 'matchup_edge'
        WHEN 'live_event' THEN 'live_event'
        ELSE 'text_only'
    END),
    hook_type = COALESCE(hook_type, CASE content_type
        WHEN 'probables_board' THEN 'hidden_edge'
        WHEN 'hr_tracker' THEN 'rare_air'
        WHEN 'live_event' THEN 'live_reaction'
        WHEN 'leaderboard' THEN 'bookmark_utility'
        WHEN 'insight_tile' THEN 'signal_vs_noise'
        ELSE 'what_changed'
    END),
    intended_kpi = COALESCE(intended_kpi, CASE content_type
        WHEN 'hr_tracker' THEN 'reposts'
        WHEN 'text_only' THEN 'replies'
        ELSE 'bookmarks'
    END),
    priority_score = CASE
        WHEN priority_score IS NULL OR priority_score = 0 THEN CASE content_type
            WHEN 'probables_board' THEN 80
            WHEN 'live_event' THEN 76
            WHEN 'hr_tracker' THEN 74
            WHEN 'pitcher_card' THEN 72
            WHEN 'batter_card' THEN 70
            WHEN 'leaderboard' THEN 68
            WHEN 'games_of_day' THEN 66
            WHEN 'insight_tile' THEN 64
            ELSE 50
        END
        ELSE priority_score
    END,
    campaign = COALESCE(campaign, 'daily_mlb'),
    source_module = COALESCE(source_module, content_type),
    manual_or_ai = COALESCE(manual_or_ai, 'manual'),
    experiment_tag = COALESCE(experiment_tag, '');
"""


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def migrate() -> None:
    if not DB_PATH.is_file():
        print(f"No database at {DB_PATH}; nothing to do.", file=sys.stderr)
        return

    conn = sqlite3.connect(str(DB_PATH))
    try:
        existing = _existing_columns(conn, "content_queue")
        for name, ddl in QUEUE_COLUMNS.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE content_queue ADD COLUMN {name} {ddl}")
                print(f"Added content_queue.{name}")

        conn.executescript(CREATE_POST_PERFORMANCE.strip())
        conn.execute(BACKFILL_QUEUE_METADATA)
        for sql in CREATE_INDEXES:
            conn.execute(sql)
        conn.commit()
        print(f"OK: Session 1 upscale migration complete on {DB_PATH}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
