"""
Season-level player bios from feed_live ``gameData.players``.

Each raw game repeats the full roster (~7% of feed JSON). Merging into one file
per season deduplicates bios for digest/cards/API without losing fields.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def season_registry_path(warehouse: Path, season: int | str) -> Path:
    """``{warehouse}/{season}/players_registry.json`` — all stages merged."""
    return warehouse / str(season) / "players_registry.json"


def merge_game_data_players_from_feed(
    feed: dict,
    registry_path: Path,
) -> dict[str, Any]:
    """
    Upsert ``gameData.players`` into the season registry (key = str(mlbam_id)).

    Returns stats: ``n_seen_in_game``, ``n_new_ids``, ``registry_total``.
    """
    players = (feed.get("gameData") or {}).get("players") or {}
    existing: dict[str, dict] = {}
    if registry_path.exists():
        try:
            with open(registry_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                existing = {str(k): v for k, v in data.items() if isinstance(v, dict)}
        except (json.JSONDecodeError, OSError):
            existing = {}

    n_new = 0
    n_seen = 0
    for _key, pdata in players.items():
        if not isinstance(pdata, dict):
            continue
        pid = pdata.get("id")
        if pid is None:
            continue
        try:
            sk = str(int(pid))
        except (TypeError, ValueError):
            continue
        n_seen += 1
        if sk not in existing:
            n_new += 1
        existing[sk] = pdata

    registry_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = registry_path.with_suffix(registry_path.suffix + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False)
        tmp_path.replace(registry_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return {
        "n_seen_in_game": n_seen,
        "n_new_ids": n_new,
        "registry_total": len(existing),
    }
