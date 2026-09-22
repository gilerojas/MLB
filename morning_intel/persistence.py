"""
Cross-snapshot persistence for morning intel.

Every run writes ``snapshots/intel_YYYY-MM-DD.json`` and nothing has ever read
them back, so each morning re-derived its noise from scratch. A false positive
from binomial noise almost never repeats three days running; a real velocity loss
repeats every day. Counting repeats is the cheapest multiplicity control available
and needs no distributional assumptions at all.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

# Anomalies retained in each snapshot purely so the next run can measure repeats.
# The newsletter still shows far fewer; this pool is the memory, not the output.
SIGNAL_POOL_SIZE = 200
LOOKBACK_SNAPSHOTS = 7


def _key(item: dict) -> str:
    return f"{item.get('player_id')}:{item.get('role')}:{item.get('metric')}"


def load_recent(snapshot_dir: Path, anchor: date, limit: int = LOOKBACK_SNAPSHOTS) -> list[tuple[date, dict]]:
    """Most recent snapshots strictly before ``anchor``, newest first."""
    out: list[tuple[date, dict]] = []
    if not snapshot_dir.exists():
        return out
    for path in sorted(snapshot_dir.glob("intel_*.json"), reverse=True):
        stem = path.stem.replace("intel_", "")
        try:
            d = date.fromisoformat(stem)
        except ValueError:
            continue
        if d >= anchor:
            continue
        try:
            with path.open(encoding="utf-8") as fh:
                out.append((d, json.load(fh)))
        except (OSError, json.JSONDecodeError):
            continue
        if len(out) >= limit:
            break
    return out


def build_history(snapshots: list[tuple[date, dict]]) -> dict[str, dict]:
    """
    Map anomaly key -> repeat history across prior snapshots.

    ``seen_in`` counts how many of the loaded snapshots carried the signal and is
    robust to gap days; ``streak`` counts consecutive snapshots back from the most
    recent, which is the stronger claim when snapshots are contiguous.
    """
    per_key_days: dict[str, list[date]] = {}
    per_key_windows: dict[str, set[str]] = {}
    ordered_days = [d for d, _ in snapshots]
    for d, snap in snapshots:
        pool = snap.get("signal_pool")
        if not isinstance(pool, list):
            pool = list(snap.get("anomalies_pitchers") or []) + list(snap.get("anomalies_batters") or [])
        for item in pool:
            if not isinstance(item, dict):
                continue
            key = _key(item)
            per_key_days.setdefault(key, []).append(d)
            per_key_windows.setdefault(key, set()).add(str(item.get("window_end") or ""))

    history: dict[str, dict] = {}
    for key, days in per_key_days.items():
        present = set(days)
        streak = 0
        for d in ordered_days:
            if d in present:
                streak += 1
            else:
                break
        # A window that never advanced is the same evidence re-printed, not a
        # repeat. Counting it as one is how a player who stopped playing would
        # collect the largest persistence bonus in the system.
        windows = {w for w in per_key_windows.get(key, set()) if w}
        history[key] = {
            "seen_in": len(present),
            "of_snapshots": len(ordered_days),
            "streak": streak,
            "distinct_windows": len(windows),
            "first_seen": min(present).isoformat(),
        }
    return history


def confirmed_streak(info: dict | None) -> int:
    """
    Repeats backed by genuinely new evidence.

    Zero when the window never moved, however many mornings the line reappeared.
    """
    if not info:
        return 0
    if int(info.get("distinct_windows") or 0) < 2:
        return 0
    return int(info.get("streak") or 0)


def streak_label(info: dict | None) -> str:
    """Short human tag for the newsletter."""
    if not info or not info.get("seen_in"):
        return "new today"
    streak = confirmed_streak(info)
    if streak >= 2:
        return f"{streak + 1} days running"
    distinct = int(info.get("distinct_windows") or 0)
    # Zero means the prior snapshots predate window dates, which is not the same
    # as knowing the window sat still.
    if distinct == 1 and int(info.get("seen_in") or 0) >= 2:
        return "repeat of unchanged window"
    return f"seen {info['seen_in']} of last {info['of_snapshots']}"


def apply(items: list[dict], history: dict[str, dict]) -> list[dict]:
    """Tag each anomaly with its repeat history. Mutates and returns ``items``."""
    for item in items:
        info = history.get(_key(item))
        item["persistence"] = info or {"seen_in": 0, "of_snapshots": 0, "streak": 0}
        item["persistence_label"] = streak_label(info)
    return items
