#!/usr/bin/env python3
"""Export local hub.db tables to JSONL files for Supabase import.

This does not contact Supabase. It creates portable JSONL snapshots under
supabase/seed/ so the migration can be reviewed before loading into Postgres.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "hub.db"
DEFAULT_OUT = ROOT / "supabase" / "seed"
TABLES = (
    "content_queue",
    "player_watchlist",
    "live_events",
    "post_performance",
    "twitter_metrics_snapshots",
    "notification_log",
    "security_audit_log",
)
JSON_COLUMNS = {
    "content_queue": ("meta_json",),
    "live_events": ("payload_json",),
    "security_audit_log": ("details_json",),
}


def _parse_jsonish(value: Any) -> Any:
    if value is None or not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    found = conn.execute(
        "select name from sqlite_master where type='table' and name = ?",
        (table,),
    ).fetchone()
    if not found:
        return []
    rows = [dict(row) for row in conn.execute(f"select * from {table}")]
    for row in rows:
        for col in JSON_COLUMNS.get(table, ()):
            if col in row:
                row[col] = _parse_jsonish(row[col])
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Export hub.db tables to Supabase JSONL seed files")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    db_path = args.db.expanduser().resolve()
    out_dir = args.out_dir.expanduser().resolve()
    if not db_path.is_file():
        raise SystemExit(f"hub.db not found: {db_path}")

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"source_db": str(db_path), "tables": {}}
    with sqlite3.connect(str(db_path)) as conn:
        for table in TABLES:
            rows = _rows(conn, table)
            out_path = out_dir / f"{table}.jsonl"
            with out_path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            manifest["tables"][table] = {"rows": len(rows), "path": str(out_path)}
            print(f"{table}: {len(rows)} row(s) -> {out_path}")

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
