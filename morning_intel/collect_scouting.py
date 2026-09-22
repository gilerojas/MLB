"""
Scouting collector — appends dated, sourced claims to the scouting ledger.

Runs on its own cadence, independent of the newsletter. The morning job only ever
reads the ledger, so a slow or failing collector at 6am can no longer take the
briefing down with it, and the file accumulates into a history you can query later.

Three sources, by reliability tier:
  1  MLB transactions API and MLB.com news  (official record)
  2  Grok over recent reporting             (established reporting, url required)
  3  reserved for aggregators and rumor     (never drives a published claim)

Grok's job here is extraction, not judgment. It is asked for structured claims
with source links and anything returning without a url is dropped, because a
summary with no source is exactly the failure mode worth designing against.

Usage:
    python morning_intel/collect_scouting.py --hours 24
    python morning_intel/collect_scouting.py --hours 24 --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

_INTEL_DIR = Path(__file__).resolve().parent
REPO_ROOT = _INTEL_DIR.parent
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(_INTEL_DIR / ".env")
load_dotenv(REPO_ROOT / "jobs" / ".env")
load_dotenv(REPO_ROOT / "mlbops" / ".env")

from morning_intel import scouting as _scout  # noqa: E402

WAREHOUSE_ROOT = Path(os.getenv("MLB_WAREHOUSE_ROOT") or (REPO_ROOT / "data" / "warehouse" / "mlb"))
LEDGER = Path(os.getenv("MLB_SCOUTING_LEDGER") or (_INTEL_DIR / "scouting" / "ledger.jsonl"))
STATS_BASE = "https://statsapi.mlb.com/api/v1"
MLB_NEWS_RSS = "https://www.mlb.com/feeds/news/rss.xml"
# Agent Tools API. The older live-search parameters on chat/completions now
# return HTTP 410, and without a search tool Grok correctly returns nothing at all.
GROK_URL = "https://api.x.ai/v1/responses"

# Grok labels roster moves with its own vocabulary; map onto the ledger's.
_EVENT_ALIASES = {
    "roster_move": "role_change",
    "lineup_change": "role_change",
    "rotation_change": "role_change",
    "signing": "transaction",
    "trade": "transaction",
    "callup": "role_change",
}

# Transaction codes worth carrying as scouting context.
_TX_EVENTS = {
    "SC": "role_change",
    "OPT": "role_change",
    "RCL": "role_change",
    "TR": "transaction",
    "SFA": "transaction",
    "REL": "transaction",
    "CLW": "transaction",
    "SE": "transaction",
    "DES": "transaction",
}


def collect_transactions(name_index: dict[str, int], day: date) -> list[dict]:
    """Official transactions for a day. Tier 1: this is the record, not a report."""
    try:
        response = requests.get(
            f"{STATS_BASE}/transactions",
            params={"startDate": day.isoformat(), "endDate": day.isoformat()},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return []

    entries = []
    for tx in payload.get("transactions", []) or []:
        description = (tx.get("description") or "").strip()
        if not description:
            continue
        code = (tx.get("typeCode") or "").upper()
        event_type = _TX_EVENTS.get(code, "transaction")
        if re.search(r"injured list|IL\b|injury", description, re.I):
            event_type = "injury"
        person = tx.get("person") or {}
        pids = [int(person["id"])] if person.get("id") else _scout.resolve_players(description, name_index)
        team = tx.get("toTeam") or tx.get("team") or {}
        entries.append(_scout.make_entry(
            source="mlb_transactions",
            tier=_scout.TIER_OFFICIAL,
            claim=description,
            url=f"https://www.mlb.com/transactions/{day.isoformat()}",
            player_ids=pids,
            event_type=event_type,
            team_id=team.get("id"),
            ts=datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc),
        ))
    return entries


def collect_mlb_news(name_index: dict[str, int]) -> list[dict]:
    """MLB.com headlines. Tier 1 as a source, but a headline is only ever context."""
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    try:
        response = requests.get(MLB_NEWS_RSS, timeout=20)
        response.raise_for_status()
        root = ET.fromstring(response.text)
    except (requests.RequestException, ET.ParseError):
        return []

    entries = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        if not title or not url.startswith("https://www.mlb.com/"):
            continue
        ts = None
        raw = (item.findtext("pubDate") or "").strip()
        if raw:
            try:
                ts = parsedate_to_datetime(raw)
            except (TypeError, ValueError, OverflowError):
                ts = None
        pids = _scout.resolve_players(title, name_index)
        if not pids:
            continue
        entries.append(_scout.make_entry(
            source="mlb_news",
            tier=_scout.TIER_OFFICIAL,
            claim=title,
            url=url,
            player_ids=pids,
            event_type="other",
            ts=ts,
        ))
    return entries


_GROK_PROMPT = """You are a data extractor for a baseball analytics desk. Report only what \
established reporting has actually published in the last {hours} hours about MLB players.

Return STRICT JSON, an object with one key "claims", whose value is a list. Each item:
  "player": full name as written
  "event_type": one of injury, role_change, transaction, performance, quote
  "claim": one factual sentence, no speculation and no analysis
  "url": the direct source url
  "outlet": the publication name

Hard rules:
- Every claim MUST carry a real source url. Omit any claim you cannot attribute.
- Do not infer, predict, aggregate or editorialize. Extraction only.
- Prefer beat reporters and team announcements over aggregators.
- If you have nothing attributable, return {{"claims": []}}.
"""


def collect_grok(name_index: dict[str, int], hours: int) -> tuple[list[dict], list[str]]:
    """Recent reporting via Grok. Returns (entries, warnings)."""
    key = os.getenv("X_API_KEY", "").strip()
    if not key:
        return [], ["X_API_KEY not set — skipped Grok collection."]
    model = os.getenv("GROK_MODEL", "grok-3-latest")
    try:
        response = requests.post(
            GROK_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "input": _GROK_PROMPT.format(hours=hours),
                "tools": [{"type": "web_search"}, {"type": "x_search"}],
            },
            timeout=240,
        )
        response.raise_for_status()
        payload_json = response.json()
    except (requests.RequestException, ValueError) as exc:
        return [], [f"Grok collection failed: {type(exc).__name__}"]

    content = ""
    for item in payload_json.get("output", []) or []:
        for chunk in item.get("content", []) or []:
            if chunk.get("type") in ("output_text", "text"):
                content += chunk.get("text", "")
    content = content.strip()
    if not content:
        return [], ["Grok returned no text output."]

    match = re.search(r"\{.*\}", content, re.S)
    if not match:
        return [], ["Grok returned no parsable JSON."]
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError:
        return [], ["Grok returned malformed JSON."]

    entries, dropped = [], 0
    for claim in payload.get("claims", []) or []:
        if not isinstance(claim, dict):
            continue
        url = str(claim.get("url") or "").strip()
        text = str(claim.get("claim") or "").strip()
        # An unsourced claim is not a claim we keep.
        if not url.startswith("http") or not text:
            dropped += 1
            continue
        pids = _scout.resolve_players(str(claim.get("player") or ""), name_index)
        if not pids:
            pids = _scout.resolve_players(text, name_index)
        if not pids:
            dropped += 1
            continue
        event_type = str(claim.get("event_type") or "other")
        entries.append(_scout.make_entry(
            source=f"grok:{claim.get('outlet') or 'unknown'}",
            tier=_scout.TIER_REPORTING,
            claim=text,
            url=url,
            player_ids=pids,
            event_type=_EVENT_ALIASES.get(event_type, event_type),
        ))
    warnings = [f"Dropped {dropped} unsourced or unresolved Grok claims."] if dropped else []
    return entries, warnings


def dedupe_against_ledger(entries: list[dict], lookback_days: int = 5) -> list[dict]:
    """Drop claims already recorded, so repeated runs do not inflate corroboration."""
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    existing = {(e.get("claim"), e.get("url")) for e in _scout.read_since(LEDGER, since)}
    return [e for e in entries if (e["claim"], e["url"]) not in existing]


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect scouting claims into the ledger")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window for reporting")
    parser.add_argument("--season", type=int, default=None, help="Season for the player registry")
    parser.add_argument("--dry-run", action="store_true", help="Print entries without writing")
    parser.add_argument("--skip-grok", action="store_true", help="Official sources only")
    args = parser.parse_args()

    season = args.season or date.today().year
    name_index = _scout.build_name_index(WAREHOUSE_ROOT / str(season) / "players_registry.json")
    if not name_index:
        print(f"WARNING: no players_registry.json for {season}; claims cannot be linked.")

    today = datetime.now(timezone.utc).date()
    entries: list[dict] = []
    warnings: list[str] = []
    entries += collect_transactions(name_index, today)
    entries += collect_transactions(name_index, today - timedelta(days=1))
    entries += collect_mlb_news(name_index)
    if not args.skip_grok:
        grok_entries, grok_warnings = collect_grok(name_index, args.hours)
        entries += grok_entries
        warnings += grok_warnings

    entries = dedupe_against_ledger(entries)
    by_tier: dict[int, int] = {}
    for entry in entries:
        by_tier[entry["tier"]] = by_tier.get(entry["tier"], 0) + 1

    for warning in warnings:
        print(f"  note: {warning}")
    if args.dry_run:
        for entry in entries:
            print(json.dumps(entry, ensure_ascii=False))
        print(f"[dry-run] {len(entries)} new entries, by tier: {by_tier}")
        return

    written = _scout.append_entries(LEDGER, entries)
    print(f"Wrote {written} entries to {LEDGER} (by tier: {by_tier})")


if __name__ == "__main__":
    main()
