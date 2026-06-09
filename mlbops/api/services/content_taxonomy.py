"""Mallitalytics content taxonomy and queue metadata defaults."""
from __future__ import annotations

from typing import Any

CONTENT_PILLARS = {
    "probables",
    "pitcher_to_watch",
    "player_card",
    "leaderboard_watch",
    "statcast_signal",
    "pitching_index",
    "hr_tracker",
    "buy_sell",
    "matchup_edge",
    "fantasy_streamer",
    "live_event",
    "text_only",
}

HOOK_TYPES = {
    "hidden_edge",
    "what_changed",
    "one_chart_one_takeaway",
    "signal_vs_noise",
    "box_score_missed",
    "bookmark_utility",
    "debate_prompt",
    "rare_air",
    "live_reaction",
}

INTENDED_KPIS = {
    "bookmarks",
    "replies",
    "reposts",
    "profile_visits",
    "follows",
    "impressions",
}

CONTENT_TYPE_DEFAULTS: dict[str, dict[str, Any]] = {
    "probables_board": {
        "content_pillar": "probables",
        "hook_type": "hidden_edge",
        "intended_kpi": "bookmarks",
        "priority_score": 80,
    },
    "pitcher_card": {
        "content_pillar": "pitcher_to_watch",
        "hook_type": "what_changed",
        "intended_kpi": "bookmarks",
        "priority_score": 72,
    },
    "batter_card": {
        "content_pillar": "player_card",
        "hook_type": "what_changed",
        "intended_kpi": "profile_visits",
        "priority_score": 70,
    },
    "leaderboard": {
        "content_pillar": "leaderboard_watch",
        "hook_type": "bookmark_utility",
        "intended_kpi": "bookmarks",
        "priority_score": 68,
    },
    "insight_tile": {
        "content_pillar": "statcast_signal",
        "hook_type": "signal_vs_noise",
        "intended_kpi": "profile_visits",
        "priority_score": 64,
    },
    "hr_tracker": {
        "content_pillar": "hr_tracker",
        "hook_type": "rare_air",
        "intended_kpi": "reposts",
        "priority_score": 74,
    },
    "pitching_index": {
        "content_pillar": "pitching_index",
        "hook_type": "one_chart_one_takeaway",
        "intended_kpi": "bookmarks",
        "priority_score": 76,
    },
    "games_of_day": {
        "content_pillar": "matchup_edge",
        "hook_type": "hidden_edge",
        "intended_kpi": "bookmarks",
        "priority_score": 66,
    },
    "fantasy_streamer": {
        "content_pillar": "fantasy_streamer",
        "hook_type": "bookmark_utility",
        "intended_kpi": "bookmarks",
        "priority_score": 70,
    },
    "live_event": {
        "content_pillar": "live_event",
        "hook_type": "live_reaction",
        "intended_kpi": "reposts",
        "priority_score": 76,
    },
    "text_only": {
        "content_pillar": "text_only",
        "hook_type": "debate_prompt",
        "intended_kpi": "replies",
        "priority_score": 50,
    },
}


def _clean_choice(value: Any, allowed: set[str], fallback: str) -> str:
    if isinstance(value, str):
        cleaned = value.strip().lower().replace("-", "_").replace(" ", "_")
        if cleaned in allowed:
            return cleaned
    return fallback


def _clean_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    cleaned = str(value).strip()
    return cleaned or fallback


def _clean_score(value: Any, fallback: int) -> int:
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError):
        score = fallback
    return max(0, min(100, score))


def normalize_queue_metadata(content_type: str, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return stable metadata columns for a queue item.

    The queue still accepts partial generator metadata. This helper applies
    Mallitalytics defaults so every new item is groupable in analytics.
    """
    meta = meta if isinstance(meta, dict) else {}
    defaults = CONTENT_TYPE_DEFAULTS.get(content_type, CONTENT_TYPE_DEFAULTS["text_only"])
    creation_mode = meta.get("creation_mode")
    ai_assisted = meta.get("ai_assisted")
    if creation_mode == "ai_assisted" or ai_assisted is True:
        manual_or_ai = "ai"
    else:
        manual_or_ai = "manual"

    return {
        "content_pillar": _clean_choice(
            meta.get("content_pillar") or meta.get("pillar"),
            CONTENT_PILLARS,
            str(defaults["content_pillar"]),
        ),
        "hook_type": _clean_choice(
            meta.get("hook_type"),
            HOOK_TYPES,
            str(defaults["hook_type"]),
        ),
        "intended_kpi": _clean_choice(
            meta.get("intended_kpi") or meta.get("primary_kpi"),
            INTENDED_KPIS,
            str(defaults["intended_kpi"]),
        ),
        "priority_score": _clean_score(meta.get("priority_score"), int(defaults["priority_score"])),
        "campaign": _clean_text(meta.get("campaign"), "daily_mlb"),
        "source_module": _clean_text(meta.get("source_module") or meta.get("source"), content_type),
        "manual_or_ai": manual_or_ai,
        "experiment_tag": _clean_text(meta.get("experiment_tag"), ""),
    }
