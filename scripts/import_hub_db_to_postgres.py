#!/usr/bin/env python3
"""Import local data/hub.db control-plane tables into Postgres.

Usage:
  DATABASE_URL=postgresql://... python scripts/import_hub_db_to_postgres.py

The script is idempotent for existing IDs: it creates schema, upserts rows by
primary key, and advances Postgres sequences after import.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "hub.db"
SCHEMA = ROOT / "mlbops" / "api" / "db" / "schema_postgres.sql"
TABLES = (
    "content_queue",
    "player_watchlist",
    "notification_log",
    "twitter_metrics_snapshots",
    "live_events",
    "security_audit_log",
    "post_performance",
)


def _database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "").strip()
    if not raw:
        raise SystemExit("DATABASE_URL is required.")
    return raw


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "select name from sqlite_master where type='table' and name=?",
        (table,),
    ).fetchone()
    return row is not None


def _sqlite_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, table):
        return []
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute(f"select * from {table}")]


def _pg_columns(conn, table: str) -> list[str]:
    rows = conn.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public' and table_name = %s
        order by ordinal_position
        """,
        (table,),
    ).fetchall()
    return [str(row["column_name"]) for row in rows]


def _upsert_table(pg, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    columns = _pg_columns(pg, table)
    common = [c for c in columns if c in rows[0]]
    if not common:
        return 0
    pk = "player_id" if table == "player_watchlist" else "id"
    placeholders = ", ".join(f"%({c})s" for c in common)
    quoted_cols = ", ".join(common)
    updates = [c for c in common if c != pk]
    update_sql = ", ".join(f"{c}=excluded.{c}" for c in updates)
    sql = (
        f"insert into {table} ({quoted_cols}) values ({placeholders}) "
        f"on conflict ({pk}) do update set {update_sql}"
        if updates
        else f"insert into {table} ({quoted_cols}) values ({placeholders}) on conflict ({pk}) do nothing"
    )
    with pg.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def _reset_sequence(pg, table: str) -> None:
    if table == "player_watchlist":
        return
    seq_row = pg.execute("select pg_get_serial_sequence(%s, 'id') as seq", (table,)).fetchone()
    seq = seq_row["seq"] if seq_row else None
    if not seq:
        return
    pg.execute(
        "select setval(%s, coalesce((select max(id) from " + table + "), 0) + 1, false)",
        (seq,),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Import hub.db into Postgres")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--schema", type=Path, default=SCHEMA)
    args = parser.parse_args()

    sqlite_path = args.db.expanduser().resolve()
    if not sqlite_path.is_file():
        raise SystemExit(f"SQLite DB not found: {sqlite_path}")
    schema_sql = args.schema.read_text(encoding="utf-8")

    with psycopg.connect(_database_url(), row_factory=dict_row) as pg:
        pg.execute(schema_sql)
        with sqlite3.connect(str(sqlite_path)) as sq:
            sq.row_factory = sqlite3.Row
            for table in TABLES:
                rows = _sqlite_rows(sq, table)
                n = _upsert_table(pg, table, rows)
                _reset_sequence(pg, table)
                print(f"{table}: imported {n} row(s)")
        pg.commit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
