"""
Scouting ledger and news entity resolution.

News used to reach the editorial model as six bare headline strings, so nothing
could join a story to a signal. The value was never the headline list — it is
"sinker usage down 11 points over three starts AND he left Aug 30 with forearm
tightness". Stats give the what, reporting gives the why, and neither is worth
publishing alone.

The ledger is the decoupling point: continuous collectors (Grok, RSS, the
transactions API) append here on their own cadence, and the morning run only ever
reads it. A flaky collector at 6am can no longer break the newsletter, and the
file accumulates into a corpus you can ask "what did we know before the velo
drop?" later.
"""
from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Source reliability. Tier 1 is official record, tier 2 is established reporting,
# tier 3 is aggregation or rumor. Tier 3 never drives a published claim.
TIER_OFFICIAL = 1
TIER_REPORTING = 2
TIER_RUMOR = 3

EVENT_TYPES = {"injury", "role_change", "transaction", "performance", "quote", "other"}

_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _PUNCT_RE.sub(" ", text.lower())
    return _WS_RE.sub(" ", text).strip()


def build_name_index(registry_path: Path) -> dict[str, int]:
    """
    Normalized-name -> player_id from the warehouse registry.

    Full names are always indexed. Bare surnames are indexed only when unique in
    the registry, so a "Ramos" headline never silently attaches to the wrong
    player.
    """
    try:
        with registry_path.open(encoding="utf-8") as fh:
            registry = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}

    index: dict[str, int] = {}
    surname_hits: dict[str, set[int]] = {}
    for raw_id, rec in (registry or {}).items():
        if not isinstance(rec, dict):
            continue
        try:
            pid = int(rec.get("id") or raw_id)
        except (TypeError, ValueError):
            continue
        full = _normalize(rec.get("fullName"))
        if full:
            index[full] = pid
        first = _normalize(rec.get("firstName"))
        last = _normalize(rec.get("lastName"))
        if first and last:
            index.setdefault(f"{first[0]} {last}", pid)
        if last:
            surname_hits.setdefault(last, set()).add(pid)

    for last, pids in surname_hits.items():
        if len(pids) == 1 and last not in index:
            index[last] = next(iter(pids))
    return index


def resolve_players(text: str, index: dict[str, int], max_hits: int = 6) -> list[int]:
    """
    Player ids mentioned in free text. Longest names match first.

    A matched span is consumed so a shorter overlapping entry cannot also fire —
    without that, "Troy Melton" matches the full name and then a bare surname
    belonging to a different player, attaching one claim to two people.
    """
    if not text or not index:
        return []
    haystack = f" {_normalize(text)} "
    found: list[int] = []
    for name in sorted(index, key=len, reverse=True):
        if len(name) < 4:
            continue
        token = f" {name} "
        if token in haystack:
            # Leave the boundary spaces so adjacent names still match.
            haystack = haystack.replace(name, "\x00" * len(name))
            pid = index[name]
            if pid not in found:
                found.append(pid)
            if len(found) >= max_hits:
                break
    return found


def append_entries(ledger_path: Path, entries: list[dict]) -> int:
    """Append claims to the JSONL ledger. Returns the count written."""
    if not entries:
        return 0
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with ledger_path.open("a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
            written += 1
    return written


def read_since(ledger_path: Path, since: datetime) -> list[dict]:
    """Ledger entries at or after ``since``. Malformed lines are skipped."""
    if not ledger_path.exists():
        return []
    out: list[dict] = []
    with ledger_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts_raw = entry.get("ts")
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except ValueError:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= since:
                out.append(entry)
    return out


def make_entry(
    source: str,
    tier: int,
    claim: str,
    url: str,
    player_ids: list[int],
    event_type: str = "other",
    team_id: int | None = None,
    ts: datetime | None = None,
) -> dict:
    """One ledger row. A claim with no source url is not a claim we keep."""
    return {
        "ts": (ts or datetime.now(timezone.utc)).isoformat(),
        "source": source,
        "tier": int(tier),
        "event_type": event_type if event_type in EVENT_TYPES else "other",
        "claim": str(claim).strip(),
        "url": str(url).strip(),
        "player_ids": [int(p) for p in player_ids],
        "team_id": team_id,
    }


def corroborate(
    anomalies: list[dict],
    entries: list[dict],
    window_days: int = 14,
    now: datetime | None = None,
) -> list[dict]:
    """
    Attach matching reporting to each anomaly.

    An anomaly with a mechanism behind it — an injury exit, a role change — is
    worth more than a larger one with no explanation, so this is what lets the
    ranking prefer stories over outliers.
    """
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=window_days)
    by_player: dict[int, list[dict]] = {}
    for entry in entries:
        try:
            ts = datetime.fromisoformat(str(entry.get("ts")))
        except (ValueError, TypeError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            continue
        for pid in entry.get("player_ids") or []:
            by_player.setdefault(int(pid), []).append(entry)

    for item in anomalies:
        pid = int(item.get("player_id") or 0)
        hits = by_player.get(pid) or []
        hits = sorted(hits, key=lambda e: (e.get("tier", 9), e.get("ts")), reverse=False)[:3]
        item["corroboration"] = [
            {
                "event_type": h.get("event_type"),
                "claim": h.get("claim"),
                "url": h.get("url"),
                "tier": h.get("tier"),
                "source": h.get("source"),
                "ts": h.get("ts"),
            }
            for h in hits
        ]
    return anomalies
