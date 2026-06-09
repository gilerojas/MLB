"""Heuristic content scoring for Mallitalytics queue decisions."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from api.services.content_taxonomy import CONTENT_TYPE_DEFAULTS, normalize_queue_metadata

WEIGHTS = {
    "timeliness": 0.25,
    "statistical_strength": 0.20,
    "audience_relevance": 0.20,
    "visual_clarity": 0.15,
    "reply_bookmark_potential": 0.10,
    "fantasy_relevance": 0.10,
}


def _parse_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _clamp(score: int) -> int:
    return max(0, min(100, int(score)))


def _content_type(item: dict[str, Any]) -> str:
    return str(item.get("content_type") or "text_only")


def _has_image(item: dict[str, Any]) -> bool:
    return bool(item.get("image_path") or item.get("image_url"))


def _has_player(item: dict[str, Any]) -> bool:
    return bool(item.get("player_id") or item.get("player_name"))


def _timeliness(item: dict[str, Any]) -> int:
    content_type = _content_type(item)
    if content_type == "live_event":
        return 95
    game_date = _parse_date(item.get("game_date"))
    if not game_date:
        return 50
    delta = (game_date - date.today()).days
    if delta == 0:
        return 90
    if delta == 1:
        return 75
    if delta == -1:
        return 70
    if -3 <= delta <= 2:
        return 60
    return 35


def _statistical_strength(item: dict[str, Any], meta: dict[str, Any]) -> int:
    content_type = _content_type(item)
    if content_type in {"pitcher_card", "batter_card", "leaderboard", "insight_tile", "pitching_index"}:
        base = 78
    elif content_type in {"hr_tracker", "live_event", "probables_board"}:
        base = 68
    else:
        base = 45
    if meta:
        base += 8
    if any(key in meta for key in ("statcast", "xwoba", "barrel", "whiff", "exit_velocity", "distance_ft")):
        base += 10
    return _clamp(base)


def _audience_relevance(item: dict[str, Any]) -> int:
    content_type = _content_type(item)
    base = {
        "probables_board": 82,
        "live_event": 84,
        "hr_tracker": 80,
        "pitching_index": 78,
        "pitcher_card": 74,
        "batter_card": 72,
        "leaderboard": 70,
        "games_of_day": 68,
        "insight_tile": 62,
        "text_only": 48,
    }.get(content_type, 50)
    if _has_player(item):
        base += 6
    return _clamp(base)


def _visual_clarity(item: dict[str, Any]) -> int:
    content_type = _content_type(item)
    if _has_image(item):
        return 82 if content_type in {"pitcher_card", "batter_card", "probables_board", "pitching_index"} else 76
    if content_type in {"leaderboard", "insight_tile"}:
        return 58
    return 42


def _reply_bookmark_potential(item: dict[str, Any], normalized: dict[str, Any]) -> int:
    content_type = _content_type(item)
    kpi = normalized.get("intended_kpi")
    base = {
        "probables_board": 88,
        "leaderboard": 82,
        "pitching_index": 82,
        "pitcher_card": 76,
        "games_of_day": 72,
        "live_event": 70,
        "hr_tracker": 68,
        "batter_card": 66,
        "text_only": 60,
        "insight_tile": 64,
    }.get(content_type, 55)
    if kpi in {"bookmarks", "replies"}:
        base += 4
    return _clamp(base)


def _fantasy_relevance(item: dict[str, Any], normalized: dict[str, Any]) -> int:
    pillar = normalized.get("content_pillar")
    content_type = _content_type(item)
    if pillar in {"matchup_edge", "pitcher_to_watch", "probables", "buy_sell"}:
        return 78
    if content_type in {"pitcher_card", "probables_board", "games_of_day", "pitching_index"}:
        return 72
    if content_type == "live_event":
        return 44
    return 28


def score_queue_item(item: dict[str, Any]) -> dict[str, Any]:
    meta = _parse_meta(item.get("meta_json"))
    normalized = normalize_queue_metadata(_content_type(item), {**meta, **item})
    factors = {
        "timeliness": _timeliness(item),
        "statistical_strength": _statistical_strength(item, meta),
        "audience_relevance": _audience_relevance(item),
        "visual_clarity": _visual_clarity(item),
        "reply_bookmark_potential": _reply_bookmark_potential(item, normalized),
        "fantasy_relevance": _fantasy_relevance(item, normalized),
    }
    weighted = sum(factors[key] * WEIGHTS[key] for key in WEIGHTS)
    priority_score = _clamp(round(weighted))
    defaults = CONTENT_TYPE_DEFAULTS.get(_content_type(item), CONTENT_TYPE_DEFAULTS["text_only"])
    recommended_pillar = str(normalized.get("content_pillar") or defaults["content_pillar"])
    primary_kpi = str(normalized.get("intended_kpi") or defaults["intended_kpi"])
    strongest = max(factors, key=factors.get)
    weakest = min(factors, key=factors.get)
    reason = (
        f"{priority_score}/100: strongest on {strongest.replace('_', ' ')} "
        f"({factors[strongest]}), weakest on {weakest.replace('_', ' ')} ({factors[weakest]})."
    )
    return {
        "priority_score": priority_score,
        "primary_kpi": primary_kpi,
        "recommended_pillar": recommended_pillar,
        "reason": reason,
        "factors": factors,
        "weights": WEIGHTS,
        "scored_at": datetime.utcnow().isoformat(),
        "model": "heuristic_v1",
    }
