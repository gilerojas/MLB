"""
Queue management endpoints.

GET    /queue              — list items (filterable by status/date, sortable)
GET    /queue/summary      — counts by status
GET    /queue/{id}         — single item detail
POST   /queue/{id}/redraft — Claude rewrite (draft text + meta)
PATCH  /queue/{id}         — update tweet_text or status
DELETE /queue/{id}         — delete a draft item
DELETE /queue/drafts       — delete all draft items
"""
import json
import math
import os
import random
import re
import secrets
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from api.paths import (
    get_outputs_dir,
    get_redraft_batter_tweet_target_range,
    get_redraft_max_tokens,
    get_redraft_meta_max_chars,
    get_redraft_pitcher_tweet_target_range,
    get_repo_root,
    get_tweet_max_chars,
    truncate_tweet_text_to_cap,
)

_REPO_ROOT = get_repo_root()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.fantasy_streamer import render_streamer_projection
from src.insight_tiles import render_insight_tile

from api.db.database import (
    count_queue,
    delete_draft_queue_items,
    delete_queue_item,
    get_db,
    get_queue_item,
    get_queue_status_counts,
    insert_queue_item,
    list_queue,
    update_queue_item,
)
from api.services.content_scoring import score_queue_item
from api.services.content_taxonomy import CONTENT_PILLARS, HOOK_TYPES, INTENDED_KPIS

router = APIRouter(prefix="/queue", tags=["queue"])
_API_HOST = os.getenv("FASTAPI_STATIC_BASE", "http://127.0.0.1:8000")
_STATIC_BASE = f"{_API_HOST.rstrip('/')}/static"

# Four analyst personas — rotated randomly each redraft call.
# Each drives a different instinct, hook style, and tone so cards
# written from the same raw data read differently.
_PITCHER_PERSONAS: tuple[dict, ...] = (
    {
        "name": "The Quant",
        "instinct": "Lead with the single most extreme number in the data. The pitcher's name comes after the stat, not before it.",
        "hook_style": "Stat-lead: open with the outlier number, one sentence on what it means, then anchor it to the pitcher.",
        "tone": "Precise, dry, lets the number do the talking. No adjectives that aren't backed by the JSON.",
        "banned_openers": "Never open with a name + verb construction (e.g. 'Dustin May delivered...').",
    },
    {
        "name": "The Contrarian",
        "instinct": "Find the gap between the headline result and what the data actually reveals. What did the broadcast miss?",
        "hook_style": "Contrast-open: season form vs tonight's line (only using explicit JSON fields), OR the pitch everyone ignored that quietly controlled the outing.",
        "tone": "Sharp, slightly skeptical. Rewards the reader who gets past the box score.",
        "banned_openers": "Never open with a straightforward compliment or 'strong outing' framing.",
    },
    {
        "name": "The Technician",
        "instinct": "One count-state split or sequencing detail tells the whole story. Zoom in on a mechanical truth most fans skip.",
        "hook_style": "Count-state or sequence-open: lead with a split percentage or situational pitch before the box line.",
        "tone": "Nerdy but not dry — you are sharing a secret the broadcast did not show.",
        "banned_openers": "Never open with a generic result summary.",
    },
    {
        "name": "The Beat Writer",
        "instinct": "Make the reader feel the outing. Pick one moment or pattern that defines the night and build outward from it.",
        "hook_style": "Momentum-open: start with a verb or a tension beat (a streak broken, a pattern held, a number that changed). Not a stat recitation.",
        "tone": "Cinematic but compact. Every word earns its place. No filler.",
        "banned_openers": "Never open with a raw stat as the first word.",
    },
)

# Three legal post structures — model picks the one that serves the angle best.
_POST_FORMATS: tuple[str, ...] = (
    "DENSE_SINGLE: One tight paragraph. Everything in one breath. Best when the story is a single clean thing — no sub-plots needed.",
    "TWO_PUNCHES: Two short blocks separated by one blank line. Block 1 = the fact or angle. Block 2 = what it means or a forward-looking beat. No connective tissue between them. Best for stark contrasts or bounce-back stories.",
    "TRADITIONAL_2P: Two paragraphs. First = hook + box line. Second = story. Clean, readable. Use when the data has one primary thread and one supporting detail.",
    "TRADITIONAL_3P: Three paragraphs. First = hook + box line. Second = story. Third = one question or forward-looking beat — only if clearly under the character budget and the third beat adds genuine new information.",
)

_BATTER_PERSONAS: tuple[dict, ...] = (
    {
        "name": "The Barrel Broker",
        "instinct": "Damage first — EV, hard-hit share, and xwOBA on contact tell the truth before the counting stats pile on.",
        "hook_style": "Contact-open: lead with the hardest moment or the quality-of-contact line, then tie it to the box.",
        "tone": "Confident, kinetic, never corny. No fake drama.",
        "banned_openers": "Never open with 'A night at the plate for…' or a generic label like 'Batter card'.",
    },
    {
        "name": "The Sequencing Skeptic",
        "instinct": "Which at-bats moved the game? PA log + pitch mix — who got punished and how.",
        "hook_style": "Process-open: one swing-decision or matchup beat before the slash line.",
        "tone": "Sharp, slightly clinical, rewards readers who watch beyond the HR clip.",
        "banned_openers": "Never open with only slash stats (X-for-Y) with no story hook.",
    },
    {
        "name": "The Chaos Agent",
        "instinct": "If the JSON flags a wild event (grand slam, multi-HR, crooked number), that chaos is the headline — not a tertiary stat.",
        "hook_style": "Event-first: name the biggest moment in plain English, then stack proof from the JSON.",
        "tone": "High energy, tight clauses, zero throat-clearing.",
        "banned_openers": "Never bury a grand slam or no-hit bid breaker under a soft lead-in.",
    },
    {
        "name": "The Scout's Notebook",
        "instinct": "Athleticism and swing metrics (bat speed, swing length) plus game leverage from RE24 when present.",
        "hook_style": "Tool-open: one measurable swing trait, then outcomes.",
        "tone": "Grounded, authoritative, no prospect clichés.",
        "banned_openers": "Never open with hashtags or card type labels.",
    },
)


class QueueItemPatch(BaseModel):
    tweet_text: Optional[str] = None
    status: Optional[str] = None
    content_pillar: Optional[str] = None
    hook_type: Optional[str] = None
    intended_kpi: Optional[str] = None
    priority_score: Optional[int] = Field(default=None, ge=0, le=100)
    campaign: Optional[str] = None
    experiment_tag: Optional[str] = None


class InsightDraftRequest(BaseModel):
    """Enqueue an Insights tile as a text-only queue draft (as-of date + season in meta)."""

    title: str
    tweet_text: str
    game_date: str
    season: int
    meta: dict = Field(default_factory=dict)


class FantasyStreamerDraftRequest(BaseModel):
    pitcher: str
    player_id: Optional[int] = None
    team: str
    opponent: str
    game_date: str
    season: int
    game_pk: Optional[int] = None
    venue: Optional[str] = None
    home_away: Optional[str] = None
    probable_status: str = "projected_rotation"
    stream_score: int = Field(ge=0, le=100)
    pitcher_hand: Optional[str] = None
    projected_malli_score: Optional[float] = None
    projected: dict = Field(default_factory=dict)
    k_upside: int = Field(ge=0, le=100)
    ratio_risk: int = Field(ge=0, le=100)
    opponent_k_profile: int = Field(ge=0, le=100)
    opponent_power_risk: int = Field(ge=0, le=100)
    confidence: int = Field(ge=0, le=100)
    league_fit: str
    note: str = ""
    factor_scores: dict = Field(default_factory=dict)


def _output_image_url(abs_path: Path) -> str:
    out_root = get_outputs_dir().resolve()
    try:
        rel = abs_path.resolve().relative_to(out_root)
    except ValueError:
        return ""
    return f"{_STATIC_BASE}/{rel.as_posix()}"


def _insight_rows(meta: dict) -> list[dict]:
    rows = meta.get("rows")
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


def _get_queue_sync(
    status: Optional[str],
    game_date: Optional[str],
    limit: int,
    offset: int,
    sort_by: str,
    order: str,
    content_pillar: Optional[str],
    intended_kpi: Optional[str],
) -> dict:
    items = list_queue(
        status=status,
        game_date=game_date,
        content_pillar=content_pillar,
        intended_kpi=intended_kpi,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        order=order,
    )
    total = count_queue(status=status, game_date=game_date, content_pillar=content_pillar, intended_kpi=intended_kpi)
    return {"items": items, "count": len(items), "total": total}


@router.get("")
async def get_queue(
    status: Optional[str] = Query(None, description="Filter by status"),
    game_date: Optional[str] = Query(None, description="Filter by game date YYYY-MM-DD"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("created_at", description="created_at | game_date | content_type"),
    order: str = Query("desc", description="asc | desc"),
    content_pillar: Optional[str] = Query(None, description="Filter by content pillar"),
    intended_kpi: Optional[str] = Query(None, description="Filter by primary KPI"),
):
    return await run_in_threadpool(
        _get_queue_sync, status, game_date, limit, offset, sort_by, order, content_pillar, intended_kpi
    )


def _queue_summary_sync() -> dict:
    counts = get_queue_status_counts()
    total = sum(counts.values())
    return {"by_status": counts, "total": total}


@router.get("/summary")
async def queue_summary():
    """Counts by status for briefing badge and dashboards."""
    return await run_in_threadpool(_queue_summary_sync)


@router.get("/taxonomy")
async def queue_taxonomy():
    return {
        "content_pillars": sorted(CONTENT_PILLARS),
        "hook_types": sorted(HOOK_TYPES),
        "intended_kpis": sorted(INTENDED_KPIS),
    }


def _insight_draft_sync(body: InsightDraftRequest) -> dict:
    cap = get_tweet_max_chars()
    text = truncate_tweet_text_to_cap(body.tweet_text or "", cap)
    if not text.strip():
        raise HTTPException(status_code=400, detail="tweet_text is required.")
    title = (body.title or "Insight")[:220]
    try:
        datetime.strptime(body.game_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="game_date must be YYYY-MM-DD.")
    meta = body.meta if isinstance(body.meta, dict) else {}
    meta = {**meta, "source": "insights", "as_of_date": body.game_date, "season": body.season}
    rows = _insight_rows(meta)
    image_path = ""
    image_url = ""
    if rows:
        safe_key = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(meta.get("insight_key") or title)).strip("_")[:80]
        suffix = secrets.token_hex(4)
        out_path = get_outputs_dir() / "insights" / f"insight_{body.game_date.replace('-', '')}_{safe_key}_{suffix}.png"
        render_insight_tile(
            title=title,
            subtitle=str(meta.get("sublabel") or ""),
            rows=rows,
            stat_key=str(meta.get("stat_key") or "") or None,
            game_date=body.game_date,
            season=int(body.season),
            out_path=out_path,
            insight_key=str(meta.get("insight_key") or ""),
        )
        image_path = str(out_path)
        image_url = _output_image_url(out_path)
        meta = {**meta, "image_renderer": "insight_tiles", "image_path": image_path}
    item_id = insert_queue_item(
        content_type="insight_tile",
        title=title,
        tweet_text=text,
        image_path=image_path,
        image_url=image_url,
        game_date=body.game_date,
        season=int(body.season),
        stage="regular_season",
        meta=meta,
    )
    return {"id": item_id, "title": title, "image_url": image_url, "image_path": image_path}


@router.post("/insight-draft")
async def create_insight_draft(body: InsightDraftRequest):
    """From Insights hub: send tile copy + structured meta to Launch station (no image)."""
    return await run_in_threadpool(_insight_draft_sync, body)


def _fantasy_streamer_draft_sync(body: FantasyStreamerDraftRequest) -> dict:
    try:
        datetime.strptime(body.game_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="game_date must be YYYY-MM-DD.")

    pitcher = (body.pitcher or "").strip()
    if not pitcher:
        raise HTTPException(status_code=400, detail="pitcher is required.")

    title = f"Streamer Matrix: {pitcher}"
    projected = body.projected if isinstance(body.projected, dict) else {}
    malli = body.projected_malli_score
    projected_line = ""
    if projected:
        projected_line = (
            f"Projected: {projected.get('ip', '—')} IP, {projected.get('k', '—')} K, "
            f"{projected.get('er', '—')} ER, {projected.get('whip', '—')} WHIP."
        )
    headline = (
        f"Projected MalliScore {malli:.1f}"
        if isinstance(malli, (int, float))
        else f"Stream score {body.stream_score}/100"
    )
    tweet = (
        f"{pitcher} {f'({body.pitcher_hand}) ' if body.pitcher_hand else ''}vs {body.opponent}: {headline}.\n\n"
        f"{projected_line}\n\n"
        f"K upside {body.k_upside}/100, ratio risk {body.ratio_risk}/100, "
        f"opponent K profile {body.opponent_k_profile}/100.\n\n"
        f"League fit: {body.league_fit}. {body.note}"
    ).strip()
    tweet = truncate_tweet_text_to_cap(tweet, get_tweet_max_chars())

    image_path = ""
    image_url = ""
    ymd = body.game_date.replace("-", "")
    safe_pitcher = re.sub(r"[^a-zA-Z0-9_-]+", "_", pitcher).strip("_")[:48]
    suffix = secrets.token_hex(4)
    out_path = get_outputs_dir() / "fantasy_streamer" / f"projection_{ymd}_{safe_pitcher}_{suffix}.png"
    try:
        render_streamer_projection(
            pitcher=pitcher,
            opponent=body.opponent,
            game_date=body.game_date,
            projected=projected,
            projected_malli_score=malli if isinstance(malli, (int, float)) else None,
            player_id=body.player_id,
            team=body.team,
            pitcher_hand=body.pitcher_hand,
            probable_status=body.probable_status,
            home_away=body.home_away,
            venue=body.venue,
            out_path=out_path,
        )
        image_path = str(out_path)
        image_url = _output_image_url(out_path)
    except Exception:
        image_path = ""
        image_url = ""

    meta = {
        "source": "fantasy_service",
        "source_module": "fantasy_service",
        "content_pillar": "fantasy_streamer",
        "hook_type": "bookmark_utility",
        "intended_kpi": "bookmarks",
        "primary_kpi": "bookmarks",
        "priority_score": int(round(malli)) if isinstance(malli, (int, float)) else body.stream_score,
        "campaign": "daily_mlb",
        "image_renderer": "fantasy_streamer_projection",
        "image_path": image_path,
        "streamer": body.model_dump(),
    }
    item_id = insert_queue_item(
        content_type="fantasy_streamer",
        title=title,
        tweet_text=tweet,
        image_path=image_path,
        image_url=image_url,
        game_pk=body.game_pk,
        player_id=body.player_id,
        player_name=pitcher,
        game_date=body.game_date,
        season=int(body.season),
        stage="regular_season",
        meta=meta,
    )
    return {"id": item_id, "title": title, "tweet_text": tweet, "image_url": image_url, "image_path": image_path}


@router.post("/fantasy-streamer-draft")
async def create_fantasy_streamer_draft(body: FantasyStreamerDraftRequest):
    """From Fantasy hub: send a streamer candidate to Launch station for manual review."""
    return await run_in_threadpool(_fantasy_streamer_draft_sync, body)


def _style_stats_sync(days: int) -> dict:
    cutoff = f"-{int(days)} days"
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, tweet_text, status, posted_at, reviewed_at, created_at, meta_json "
            "FROM content_queue "
            "WHERE content_type = 'pitcher_card' "
            "AND status IN ('posted', 'approved') "
            "AND COALESCE(posted_at, reviewed_at, created_at) >= datetime('now', ?) "
            "ORDER BY COALESCE(posted_at, reviewed_at, created_at) DESC",
            (cutoff,),
        ).fetchall()
    by_angle: dict[str, int] = {}
    by_persona: dict[str, int] = {}
    by_format: dict[str, int] = {}
    by_provider: dict[str, int] = {}
    items: list[dict] = []
    for row in rows:
        meta = {}
        raw = row["meta_json"]
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    meta = parsed
            except json.JSONDecodeError:
                meta = {}
        style = meta.get("redraft_style") if isinstance(meta, dict) else None
        if not isinstance(style, dict):
            continue
        angle = str(style.get("angle_id") or "unknown")
        persona = str(style.get("persona") or "unknown")
        fmt = str(style.get("format") or "unknown")
        provider = str(style.get("provider") or "unknown")
        by_angle[angle] = by_angle.get(angle, 0) + 1
        by_persona[persona] = by_persona.get(persona, 0) + 1
        by_format[fmt] = by_format.get(fmt, 0) + 1
        by_provider[provider] = by_provider.get(provider, 0) + 1
        items.append({
            "id": row["id"],
            "status": row["status"],
            "angle_id": angle,
            "persona": persona,
            "format": fmt,
            "provider": provider,
            "tweet_text": (row["tweet_text"] or "")[:220],
            "timestamp": row["posted_at"] or row["reviewed_at"] or row["created_at"],
        })
    return {
        "days": days,
        "total_with_style": len(items),
        "by_angle": by_angle,
        "by_persona": by_persona,
        "by_format": by_format,
        "by_provider": by_provider,
        "items": items,
    }


@router.get("/style-stats")
async def style_stats(days: int = Query(14, ge=1, le=90)):
    return await run_in_threadpool(_style_stats_sync, days)


_ORPHAN_END_WORD = re.compile(
    r"(?i)(?<=[.!?])\s+(how|what|why|when|where|who|whom|which)\s*$"
)
_SINGLE_FRAGMENT_PARA = re.compile(
    r"(?i)^(how|what|why|when|where|who|whom|which)\.?$"
)
_TAIL_STARTS_LIKE_QUESTION = re.compile(
    r"(?i)^(how|what|why|when|where|who|whom|which|does|did|is|are|can|could|would|should)\b"
)


def _trim_redraft_cutoff_tail(s: str) -> str:
    """Drop trailing incomplete fragments from token/char cutoff."""
    t = s.strip()
    if not t:
        return t
    # Same-line orphan: "... game. How" -> "... game."
    m = _ORPHAN_END_WORD.search(t)
    if m:
        t = t[: m.start()].rstrip()
    parts = [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]
    while parts and _SINGLE_FRAGMENT_PARA.fullmatch(parts[-1].strip()):
        parts.pop()
    t = "\n\n".join(parts).strip()
    if not t or t.endswith((".", "!", "?")):
        return t

    last_punct = max(t.rfind("."), t.rfind("!"), t.rfind("?"))
    if last_punct < 0:
        return t

    tail = t[last_punct + 1 :].strip()
    if not tail:
        return t[: last_punct + 1].rstrip()

    # Remove a likely cutoff final sentence/paragraph such as
    # "... HR.\n\nHow does his two-strike Splitter usage (35%"
    should_drop_tail = (
        len(tail) <= 160
        or tail.count("(") > tail.count(")")
        or tail.endswith(("%", ",", ";", ":", "-", "/"))
        or _TAIL_STARTS_LIKE_QUESTION.match(tail) is not None
    )
    if should_drop_tail:
        return t[: last_punct + 1].rstrip()
    return t


def _sanitize_redraft_output(raw: str, meta: Optional[dict] = None) -> str:
    """Fix common model artifacts: literal backslash-n, stray /n, Unicode dashes that read as AI slop."""
    s = (raw or "").strip()
    if not s:
        return s
    s = s.replace("\\n", "\n")
    s = re.sub(r"(^|\s)/n", r"\1\n", s)
    s = s.replace("\u2014", " - ").replace("\u2013", "-")
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = s.strip()
    s = _trim_redraft_cutoff_tail(s)
    # Safety trim if a model ignores the short-first band for pitcher / batter cards
    if meta and meta.get("card_type") == "pitcher_card":
        _pmin, pmax = get_redraft_pitcher_tweet_target_range()
        if len(s) > pmax + 120:
            chunk = s[: pmax + 100]
            if " " in chunk:
                s = chunk.rsplit(" ", 1)[0].rstrip(",;:")
            else:
                s = chunk
            s = s.strip()
            s = _trim_redraft_cutoff_tail(s)
    if meta and meta.get("card_type") == "batter_card":
        _bmin, bmax = get_redraft_batter_tweet_target_range()
        if len(s) > bmax + 120:
            chunk = s[: bmax + 100]
            if " " in chunk:
                s = chunk.rsplit(" ", 1)[0].rstrip(",;:")
            else:
                s = chunk
            s = s.strip()
            s = _trim_redraft_cutoff_tail(s)
    return s


_PITCHER_BANNED_FIRST_SENTENCE = re.compile(
    r"(?i)\b(two-strike|2-strike|count-state|count state|put-away|put away|lean(?:ed)? on|go-to weapon)\b"
)
_PITCHER_BOX_LINE_OPEN = re.compile(
    r"(?i)^\s*(?:[A-Z][A-Za-z'.-]+\s+)?\d+(?:\.\d)?\s*IP\b.*\b(?:ER|K|BB|H)\b"
)
_PITCHER_BANNED_CLOSER = re.compile(
    r"(?i)(will .{0,40}(?:hold|keep working|scale)|is this the new normal|peak form or fluke|real or just a hot night|is he back)\??\s*$"
)
_PITCHER_WHIFF_CSW_TEMPLATE_OPEN = re.compile(
    r"(?i)^\s*(?:\d+\s+whiffs?\s+(?:across|on|over)\s+\d+\s+pitches?|"
    r"(?:a\s+)?\d+(?:\.\d+)?%\s+CSW\b|CSW\b|whiffs?\b)"
)
_PITCHER_CSW_WHIFF_FUELED = re.compile(
    r"(?i)\b(?:csw|whiffs?)\b[^.!?]{0,90}\bfueled\b|\bfueled\b[^.!?]{0,90}\b(?:csw|whiffs?)\b"
)


def _first_sentence(text: str) -> str:
    parts = re.split(r"(?<=[.!?])\s+", (text or "").strip(), maxsplit=1)
    return parts[0].strip() if parts else ""


def _char_jaccard(a: str, b: str) -> float:
    sa = set((a or "").lower()[:60])
    sb = set((b or "").lower()[:60])
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _pitcher_redraft_violations(text: str, meta: dict, recent: list[str]) -> list[str]:
    if not text.strip() or meta.get("card_type") != "pitcher_card":
        return []
    style = meta.get("_redraft_style_pending") or meta.get("redraft_style") or {}
    angle_id = style.get("angle_id") if isinstance(style, dict) else None
    first = _first_sentence(text)
    violations: list[str] = []
    if angle_id != "count_state" and _PITCHER_BANNED_FIRST_SENTENCE.search(first):
        violations.append(f"Opening used banned count-state framing: {first}")
    if _PITCHER_WHIFF_CSW_TEMPLATE_OPEN.search(first):
        violations.append(
            "Opening used the repetitive whiff/CSW template. Do not open with 'N whiffs across Y pitches' "
            "or a CSW-rate sentence; move whiffs/CSW to support."
        )
    if _PITCHER_BOX_LINE_OPEN.search(first):
        violations.append(f"Opening is just the box line: {first}")
    if _PITCHER_CSW_WHIFF_FUELED.search(text):
        violations.append(
            "Avoid template/casual causation around CSW or whiffs with 'fueled'. "
            "State game-wide CSW/whiffs as outing support instead."
        )
    if _PITCHER_BANNED_CLOSER.search(text.strip()):
        violations.append("Closer used banned rhetorical-doubt/template phrasing.")
    for old in recent:
        if _char_jaccard(text, old) > 0.55:
            violations.append("Opening is too similar to one of the last three pitcher posts.")
            break
    return violations


def _build_retry_prompt(original_prompt: str, bad_output: str, violations: list[str]) -> str:
    return (
        f"{original_prompt}\n\n"
        "--- RETRY REQUIRED ---\n"
        "Your previous draft violated these deterministic checks:\n"
        + "\n".join(f"- {v}" for v in violations)
        + "\n\nPrevious draft:\n"
        + bad_output
        + "\n\nRewrite once. Keep the selected angle, avoid the violations, and reply with ONLY the final tweet text."
    )


def _pitcher_redraft_beat_sheet(meta: dict) -> str:
    """Data-driven menu for paragraph 2 so redrafts do not all repeat mix + BS75 + whiffs + CSW."""
    lines: list[str] = [
        "Beat sheet — paragraph 2: pick ONE primary story from the bullets that apply, plus at most ONE supporting thread.",
        "Anti-template: cite at most TWO of (primary pitch mix / top-2 usage, BS75+ game rate, outing whiffs together with CSW%) "
        "unless the JSON truly lacks other angles (no season_pitching_stats, empty recent_outings, no outing_context deltas, "
        "no pitch_tendencies row with n_pitches >= 4, no xwOBA best/worst in pitcher_tweet_context).",
    ]

    evn = meta.get("notable_game_events")
    if isinstance(evn, list) and evn:
        lines.append(
            "- Game narrative (notable_game_events): MUST reflect the top item (e.g. no-hitter through 8 / first hit late) "
            "before small-sample pitch-type xwOBA beats."
        )

    srcm = meta.get("source_metadata")
    is_debut_game = bool(isinstance(srcm, dict) and srcm.get("is_mlb_debut_game"))
    if is_debut_game:
        lines.append(
            "- MLB debut (source_metadata.is_mlb_debut_game): lead or follow the opening with a welcome-to-the-big-leagues "
            "beat (player_name from JSON). Fans care when a young arm debuts and throws well — tie the welcome to real box stats only."
        )

    sps = meta.get("season_pitching_stats")
    if isinstance(sps, dict) and (
        sps.get("era") is not None or sps.get("whip") is not None or sps.get("innings_pitched")
    ):
        lines.append(
            "- Season line (season_pitching_stats): you may cite cumulative ERA, WHIP, IP, games only as given; "
            "respect the note field — never imply same-night box is already baked in if unsure."
        )
    if isinstance(sps, dict) and not is_debut_game:
        try:
            gs = int(sps.get("games_started") or 0)
        except (TypeError, ValueError):
            gs = 0
        ip_s = str(sps.get("innings_pitched") or "").strip()
        if gs >= 2 or (ip_s and ip_s not in ("0", "0.0", "")):
            lines.append(
                "- Established season arm (not a debut; season_pitching_stats has volume): this is a veteran line — "
                "do NOT use rookie/DFS tone ('hot night', 'heater', 'one-hit gem' as sizzle with no insight, or "
                "'delivered' + generic praise). Do NOT end with a faux-profound rhetorical doubt question "
                "(e.g. 'Was this peak form or just a hot night?' / 'Is this real?'). "
                "That reads as AI and undersells a known starter; close with a concrete baseball beat "
                "(command, a pitch plan, two-strike behavior, what repeated from recent starts) or a tight period."
            )

    ro = meta.get("recent_outings")
    if isinstance(ro, list) and len(ro) > 0:
        lines.append(
            "- Prior starts: compare to recent_outings[0] (date + line) and/or recent_prior_summary means when present."
        )

    oc = meta.get("outing_context")
    vs = oc.get("vs_last_start") if isinstance(oc, dict) else None
    if isinstance(vs, dict) and any(
        vs.get(k) is not None for k in ("er_delta", "csw_pct_delta", "zone_pct_delta", "avg_velo_delta_mph")
    ):
        lines.append("- Momentum: outing_context.vs_last_start — one delta (ER, CSW, zone, velo) can anchor the story.")

    ptc = meta.get("pitcher_tweet_context")
    if isinstance(ptc, dict):
        fh = ptc.get("form_hints")
        if isinstance(fh, list) and len(fh) > 0:
            lines.append("- Form hints (pitcher_tweet_context.form_hints): at most one nod; do not invent a narrative.")

    tendencies = meta.get("pitch_tendencies_by_situation")
    if isinstance(tendencies, list) and any(
        isinstance(t, dict) and int(t.get("n_pitches") or 0) >= 4 for t in tendencies
    ):
        lines.append(
            "- Count-state: at most ONE pitch_tendencies_by_situation row with n_pitches >= 4 "
            "(prefer pitcher_tweet_context.tendency_highlight when present)."
        )

    if isinstance(ptc, dict) and (ptc.get("best_xwoba_pitch") or ptc.get("worst_xwoba_pitch")):
        lines.append(
            "- Contact quality: optional one beat from best_xwoba_pitch vs worst_xwoba_pitch — no HR/run causation."
        )

    box = meta.get("box")
    if isinstance(box, dict) and box.get("gb_pct") is not None:
        lines.append("- Ground-ball share (box.gb_pct): optional if it supports the line score.")

    lines.append(
        "- BS75+ game rate (box.fast_swing_pct / game_fast_swing_pct): swing bat-speed mix only — whiffs count in the "
        "denominator; never read it as loud contact or 'they were on him' without BIP-quality stats."
    )
    lines.append(
        "- Process stats (header_summary whiffs/CSW/zone, pitcher_tweet_context, arsenal): use only what serves the story; "
        "mention at most TWO pitch types by name unless a third is essential."
    )
    return "\n".join(lines) + "\n\n"


def _pitch_row_xwoba_supported(d: dict) -> bool:
    """Aligns with card JSON: pitch-type xwOBA beats need real BIP support or a large pitch sample."""
    if not isinstance(d, dict):
        return False
    try:
        np = int(d.get("n_pitches") or 0)
    except (TypeError, ValueError):
        return False
    nb = d.get("n_bip")
    if nb is not None and str(nb).strip() != "":
        try:
            nb_i = int(float(nb))
        except (TypeError, ValueError):
            nb_i = 0
        return np >= 15 and nb_i >= 6
        return np >= 25


def _ip_to_float(ip) -> float:
    s = str(ip or "0").strip()
    if not s:
        return 0.0
    whole, _, frac = s.partition(".")
    try:
        innings = int(whole)
    except ValueError:
        return 0.0
    try:
        outs = int((frac or "0")[:1])
    except ValueError:
        outs = 0
    return innings + min(max(outs, 0), 2) / 3.0


def _weighted_arsenal_velo(meta: dict) -> float | None:
    rows = meta.get("arsenal")
    if not isinstance(rows, list):
        return None
    num = 0.0
    den = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            count = int(row.get("count") or 0)
            velo = float(row.get("velo"))
        except (TypeError, ValueError):
            continue
        if count <= 0:
            continue
        num += velo * count
        den += count
    return round(num / den, 2) if den else None


def _signal_angle_id(signal: dict) -> str:
    stat = str(signal.get("stat") or "")
    if stat in ("game_shape", "mlb_debut"):
        return "game_event"
    if stat in ("command_event",):
        return "command"
    if stat in ("velo_spike", "velo_drop"):
        return "velo"
    if stat in ("efficiency",):
        return "efficiency"
    if stat in ("arsenal_split", "elite_pitch_xwoba", "xwoba_gap", "chase_share_high", "zone_shape", "gb_pct"):
        return "arsenal_split" if stat == "arsenal_split" else stat
    if stat in ("two_strike_dominance",):
        return "count_state"
    if stat in ("bs75_rate",):
        return "bs75"
    if stat in ("whiffs", "csw_pct"):
        return "whiff_csw"
    if stat in ("bounce_back",):
        return "bounce_back"
    return stat or "box_line"


def _signal_base_weight(signal: dict) -> float:
    flag = str(signal.get("flag") or "").lower()
    if flag == "must_mention":
        return 100.0
    if flag == "elite":
        return 10.0
    if flag == "strong":
        return 8.0
    if flag in ("high", "low", "wide"):
        return 5.0
    if flag == "medium":
        return 3.0
    return 2.0


_ANGLE_PERSONAS: dict[str, tuple[str, ...]] = {
    "game_event": ("The Beat Writer", "The Quant"),
    "command": ("The Quant", "The Beat Writer"),
    "velo": ("The Quant", "The Contrarian"),
    "efficiency": ("The Quant", "The Beat Writer"),
    "arsenal_split": ("The Technician", "The Quant"),
    "chase_share_high": ("The Technician", "The Contrarian"),
    "zone_shape": ("The Quant", "The Technician"),
    "count_state": ("The Technician",),
    "bs75": ("The Contrarian", "The Quant"),
    "whiff_csw": ("The Quant", "The Technician"),
    "bounce_back": ("The Contrarian", "The Beat Writer"),
    "season_context": ("The Beat Writer", "The Contrarian"),
}


def _recent_pitcher_style_context(n: int = 7) -> list[dict]:
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT tweet_text, meta_json FROM content_queue "
                "WHERE content_type = 'pitcher_card' AND status IN ('draft', 'approved', 'posted') "
                "ORDER BY COALESCE(posted_at, reviewed_at, created_at) DESC LIMIT ?",
                (n,),
            ).fetchall()
        out: list[dict] = []
        for row in rows:
            meta = {}
            raw = row["meta_json"]
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        meta = parsed
                except json.JSONDecodeError:
                    meta = {}
            style = meta.get("redraft_style") if isinstance(meta, dict) else None
            out.append({
                "tweet_text": row["tweet_text"] or "",
                "angle_id": (style or {}).get("angle_id") if isinstance(style, dict) else None,
            })
        return out
    except Exception:
        return []


def _select_pitcher_style(meta: dict, signals: list[dict], recent_ctx: list[dict]) -> dict:
    candidates: dict[str, dict] = {}
    for sig in signals:
        angle_id = _signal_angle_id(sig)
        weight = _signal_base_weight(sig)
        cur = candidates.get(angle_id)
        if cur is None or weight > cur["weight"]:
            candidates[angle_id] = {"angle_id": angle_id, "weight": weight, "signal": sig}

    if not candidates:
        season = meta.get("season_pitching_stats")
        angle_id = "season_context" if isinstance(season, dict) and season else "box_line"
        candidates[angle_id] = {
            "angle_id": angle_id,
            "weight": 1.5,
            "signal": {"stat": angle_id, "flag": "fallback", "note": "No single outlier dominates; lead with the cleanest box/process story."},
        }

    last3 = [r.get("angle_id") for r in recent_ctx[:3] if r.get("angle_id")]
    last7 = [r.get("angle_id") for r in recent_ctx[:7] if r.get("angle_id")]
    scored: list[dict] = []
    for cand in candidates.values():
        angle_id = cand["angle_id"]
        weight = float(cand["weight"])
        flag = str((cand.get("signal") or {}).get("flag") or "")
        if flag != "must_mention":
            if angle_id in last3:
                weight *= 0.25
            elif angle_id in last7:
                weight *= 0.50
        scored.append({**cand, "effective_weight": weight})
    scored.sort(key=lambda c: c["effective_weight"], reverse=True)
    top_weight = scored[0]["effective_weight"]
    pool = [c for c in scored if c["effective_weight"] >= top_weight * 0.85]
    selected = random.choice(pool)

    allowed_personas = _ANGLE_PERSONAS.get(selected["angle_id"]) or tuple(p["name"] for p in _PITCHER_PERSONAS)
    persona_pool = [p for p in _PITCHER_PERSONAS if p["name"] in allowed_personas]
    persona = random.choice(persona_pool or list(_PITCHER_PERSONAS))
    post_format = random.choice(_POST_FORMATS)
    return {
        "angle_id": selected["angle_id"],
        "angle_signal": selected.get("signal") or {},
        "candidate_angles": [
            {
                "angle_id": c["angle_id"],
                "weight": round(float(c["effective_weight"]), 3),
                "stat": (c.get("signal") or {}).get("stat"),
                "flag": (c.get("signal") or {}).get("flag"),
            }
            for c in scored[:8]
        ],
        "persona": persona,
        "format": post_format,
    }


def _batter_redraft_beat_sheet(meta: dict) -> str:
    lines: list[str] = [
        "Beat sheet — pick ONE primary story plus at most ONE supporting thread.",
        "Priority: notable_game_events (if any) > standout_signals > line/box > sabermetrics > PA log color.",
        "Anti-template: do not stack more than TWO of (max EV, hard-hit%, xwOBA on BBE, pitch mix seen) unless the JSON lacks a headline event.",
    ]
    evs = meta.get("notable_game_events")
    if isinstance(evs, list) and evs:
        lines.append(
            f"- Headline events (notable_game_events): {len(evs)} entr(y/ies) — the lowest priority number is the lead story."
        )
    btc = meta.get("batter_tweet_context")
    if isinstance(btc, dict) and btc.get("hero_headline"):
        lines.append(f"- Hero hook (batter_tweet_context.hero_headline): {btc['hero_headline']}")
    line = meta.get("line") or meta.get("box") or {}
    if isinstance(line, dict) and any(line.get(k) is not None for k in ("ab", "h", "hr", "rbi")):
        lines.append("- Box: line or box — AB / H / HR / RBI / BB / K must appear once.")
    sab = meta.get("sabermetrics")
    if isinstance(sab, dict) and any(
        sab.get(k) is not None for k in ("max_ev", "xwoba", "hard_hit_pct", "bat_speed", "re24")
    ):
        lines.append("- Contact / swing: sabermetrics — cite only keys present; no invented barrels.")
    pa = meta.get("pa_log")
    if isinstance(pa, list) and pa:
        lines.append("- PA log: at most one matchup or inning callout; never narrate every PA.")
    lines.append(
        "- xwOBA on BBE: optional supporting detail only when batted_balls sample is non-trivial; "
        "never let a micro-sample pitch-type story steal the lead from a headline event."
    )
    return "\n".join(lines) + "\n\n"


def _compute_batter_standout_signals(meta: dict) -> list[dict]:
    signals: list[dict] = []
    line = meta.get("line") or meta.get("box") or {}
    sab = meta.get("sabermetrics") or {}
    evs = [e for e in (meta.get("notable_game_events") or []) if isinstance(e, dict)]
    if evs:
        try:
            top = min(evs, key=lambda x: int(x.get("priority", 99)))
        except (TypeError, ValueError):
            top = evs[0]
        lab = top.get("label")
        if lab:
            signals.append({
                "stat": "notable_game",
                "flag": "must_mention",
                "note": f"MUST mention early in post: {lab}",
            })
    try:
        mx = sab.get("max_ev")
        if mx is not None and float(mx) >= 110:
            signals.append({
                "stat": "max_ev", "value": float(mx), "flag": "elite",
                "note": f"Elite max EV ({mx} mph) — strong supporting hook if no headline event.",
            })
    except (TypeError, ValueError):
        pass
    try:
        hh = sab.get("hard_hit_pct")
        if hh is not None and float(hh) >= 55:
            signals.append({
                "stat": "hard_hit_pct", "value": float(hh), "flag": "high",
                "note": "Very high hard-hit share for the game — contact authority story.",
            })
    except (TypeError, ValueError):
        pass
    try:
        hr_ct = int(line.get("hr") or 0)
        if hr_ct >= 2:
            signals.append({
                "stat": "multi_hr", "value": hr_ct, "flag": "strong",
                "note": "Multi-homer game — do not bury under generic praise.",
            })
    except (TypeError, ValueError):
        pass
    try:
        r24 = sab.get("re24")
        if r24 is not None and float(r24) >= 1.25:
            signals.append({
                "stat": "re24", "value": float(r24), "flag": "strong",
                "note": "Large positive RE24 sum — leverage / sequencing win.",
            })
    except (TypeError, ValueError):
        pass
    return signals


def _get_recent_batter_tweets(n: int = 3) -> list[str]:
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT tweet_text FROM content_queue "
                "WHERE content_type = 'batter_card' AND status IN ('posted', 'approved') "
                "ORDER BY created_at DESC LIMIT ?",
                (n,),
            ).fetchall()
        return [r["tweet_text"] for r in rows if r["tweet_text"]]
    except Exception:
        return []


def _compute_standout_signals(meta: dict) -> list[dict]:
    """Pre-compute statistically unusual signals so the prompt can force the model to reckon with them."""
    signals: list[dict] = []
    box = meta.get("box") or {}
    ctx = meta.get("pitcher_tweet_context") or {}
    season = meta.get("season_pitching_stats") or {}
    arsenal = [r for r in (meta.get("arsenal") or []) if isinstance(r, dict)]

    evs = meta.get("notable_game_events")
    if isinstance(evs, list) and evs:
        try:
            top = min(evs, key=lambda x: int((x or {}).get("priority", 99)))
        except (TypeError, ValueError):
            top = evs[0] if evs else {}
        if isinstance(top, dict) and top.get("label"):
            signals.append({
                "stat": "game_shape",
                "flag": "must_mention",
                "note": (
                    f"MUST mention prominently (first two sentences): {top['label']}; "
                    "do not bury under pitch-mix or xwOBA trivia."
                ),
            })

    src_meta = meta.get("source_metadata")
    if isinstance(src_meta, dict) and src_meta.get("is_mlb_debut_game"):
        pname = (meta.get("player_name") or "this pitcher").strip()
        signals.append({
            "stat": "mlb_debut",
            "flag": "must_mention",
            "note": (
                f"MLB debut game for {pname} (Stats API mlbDebutDate matches this game). "
                "Include a sharp welcome-to-the-big-leagues / first MLB start hook in the first or second sentence "
                "(vary phrasing — e.g. welcome to The Show, big league debut, first trip to a big-league mound). "
                "Then connect to how the outing looked using only box and process stats in the JSON. "
                "Do not invent prospect rank, draft slot, or scouting grades unless present in JSON."
            ),
        })

    # Whiff extremes
    whiffs = int(box.get("whiffs") or 0)
    total_pitches = int(box.get("total_pitches") or 0)
    er = int(box.get("er") or 0)
    if whiffs >= 12:
        signals.append({"stat": "whiffs", "value": whiffs, "flag": "elite",
                        "note": f"{whiffs} whiffs is a standout single-outing total — lead with or prominently feature this."})
    elif whiffs <= 1 and total_pitches >= 80:
        signals.append({"stat": "whiffs", "value": whiffs, "flag": "low",
                        "note": "Almost no whiffs on 80+ pitches — survival was location and contact management, not swing-and-miss."})

    # CSW extremes
    csw = float(box.get("csw_pct") or 0)
    if csw >= 35:
        signals.append({"stat": "csw_pct", "value": round(csw, 1), "flag": "elite",
                        "note": "Elite called+swinging strike rate for the outing."})
    elif csw <= 18 and total_pitches >= 80:
        signals.append({"stat": "csw_pct", "value": round(csw, 1), "flag": "low",
                        "note": "Unusually low CSW — got through the lineup without swing-and-miss as a weapon."})

    # BS75+ rate extremes
    bs75 = box.get("fast_swing_pct")
    if bs75 is not None:
        bs75 = float(bs75)
        if bs75 <= 0.06:
            signals.append({"stat": "bs75_rate", "value": round(bs75 * 100, 1), "flag": "low",
                            "note": "Very few swings cleared the 75 mph bat-speed band (BS75+ is per swing, not per BIP) — "
                            "quiet barrel speed on swings taken; do not equate to soft contact unless BIP/EV/xwOBA says so."})
        elif bs75 >= 0.25:
            signals.append({"stat": "bs75_rate", "value": round(bs75 * 100, 1), "flag": "high",
                            "note": "High BS75+ — many swings reached the hard bat-speed band; whiffs and fouls count. "
                            "Frame as aggressive swings, not 'squaring him up' or automatic hard contact unless "
                            "hard-hit / EV / xwOBA on BIP in the JSON backs it."})

    # Elite xwOBA on best pitch (requires real sample sizes from card JSON)
    best = ctx.get("best_xwoba_pitch") or {}
    worst = ctx.get("worst_xwoba_pitch") or {}
    if _pitch_row_xwoba_supported(best) and best.get("xwoba") is not None and float(best["xwoba"]) <= 0.150:
        signals.append({"stat": "elite_pitch_xwoba", "pitch": best.get("pitch_type"), "value": best["xwoba"],
                        "flag": "elite", "note": f"{best.get('pitch_type')} was nearly untouchable by contact quality — consider leading with this."})
    if (
        _pitch_row_xwoba_supported(best)
        and _pitch_row_xwoba_supported(worst)
        and best.get("xwoba") is not None
        and worst.get("xwoba") is not None
    ):
        gap = float(worst["xwoba"]) - float(best["xwoba"])
        if gap >= 0.200:
            signals.append({"stat": "xwoba_gap", "best_pitch": best.get("pitch_type"), "best_val": best["xwoba"],
                            "worst_pitch": worst.get("pitch_type"), "worst_val": worst["xwoba"],
                            "gap": round(gap, 3), "flag": "wide",
                            "note": "Large contact-quality gap between best and worst pitch — the arsenal split is the story."})

    # GB% extreme
    gb = box.get("gb_pct")
    if gb is not None and float(gb) >= 0.60:
        signals.append({"stat": "gb_pct", "value": round(float(gb) * 100, 1), "flag": "high",
                        "note": "Heavy ground-ball outing — kept the ball on the ground consistently."})

    # Velo shape vs recent prior mean
    prior = meta.get("recent_prior_summary") or {}
    cur_velo = _weighted_arsenal_velo(meta)
    prior_velo = prior.get("avg_velo_mph_mean") if isinstance(prior, dict) else None
    if cur_velo is not None and prior_velo is not None:
        try:
            velo_delta = round(float(cur_velo) - float(prior_velo), 2)
            if velo_delta >= 1.0:
                signals.append({"stat": "velo_spike", "value": velo_delta, "current_velo": cur_velo,
                                "prior_mean_velo": prior_velo, "flag": "strong",
                                "note": f"Average velo was up {velo_delta} mph vs recent prior mean — velo-led story."})
            elif velo_delta <= -1.0:
                signals.append({"stat": "velo_drop", "value": velo_delta, "current_velo": cur_velo,
                                "prior_mean_velo": prior_velo, "flag": "strong",
                                "note": f"Average velo was down {abs(velo_delta)} mph vs recent prior mean — shape/command had to carry more weight."})
        except (TypeError, ValueError):
            pass

    # Zone shape extremes
    zone = box.get("zone_pct")
    if zone is not None and total_pitches >= 80:
        try:
            zone_f = float(zone)
            if zone_f >= 55:
                signals.append({"stat": "zone_shape", "value": round(zone_f, 1), "flag": "high",
                                "note": "Very high zone rate on starter volume — strike-zone pressure / attack story."})
            elif zone_f <= 38:
                signals.append({"stat": "zone_shape", "value": round(zone_f, 1), "flag": "low",
                                "note": "Very low zone rate on starter volume — chase/edge command or traffic-management story, not pure zone pounding."})
        except (TypeError, ValueError):
            pass

    # Single-pitch chase outlier
    for row in arsenal:
        try:
            chase = row.get("chase_pct")
            count = int(row.get("count") or 0)
            if chase is not None and float(chase) >= 0.45 and count >= 15:
                signals.append({"stat": "chase_share_high", "pitch": row.get("pitch_type"), "value": round(float(chase) * 100, 1),
                                "n_pitches": count, "flag": "high",
                                "note": f"{row.get('pitch_type')} drew a high chase share on real volume — expand the story beyond count-state."})
                break
        except (TypeError, ValueError):
            continue

    # Arsenal split: one pitch clearly carried the plan
    top_pitch = None
    for row in arsenal:
        try:
            usage = float(row.get("usage_pct") or 0)
            count = int(row.get("count") or 0)
        except (TypeError, ValueError):
            continue
        if usage >= 0.50 and count >= 25:
            top_pitch = (row, usage, count)
            break
    if top_pitch:
        row, usage, count = top_pitch
        signals.append({"stat": "arsenal_split", "pitch": row.get("pitch_type"), "usage_pct": round(usage * 100, 1),
                        "n_pitches": count, "flag": "strong",
                        "note": f"{row.get('pitch_type')} took at least half the arsenal — pure attack-mode pitch-plan story."})

    # Command / traffic event
    ip_val = _ip_to_float(box.get("ip"))
    bb = int(box.get("bb") or 0)
    if ip_val >= 6.0 and bb <= 0:
        signals.append({"stat": "command_event", "bb": bb, "ip": box.get("ip"), "flag": "strong",
                        "note": "No walks over 6+ IP — command/strike-throwing can lead the post."})
    elif bb >= 3 and er <= 2 and ip_val >= 5.0:
        signals.append({"stat": "command_event", "bb": bb, "er": er, "ip": box.get("ip"), "flag": "medium",
                        "note": "Worked around traffic with limited damage — volatility/control story, not dominance framing."})

    # Two-strike dominance
    tendency = ctx.get("tendency_highlight") or {}
    try:
        tendency_share = float(tendency.get("dominant_share") or 0)
        tendency_np = int(tendency.get("n_pitches") or 0)
    except (TypeError, ValueError):
        tendency_share, tendency_np = 0.0, 0
    if (
        tendency.get("situation_key") == "two_strike"
        and tendency_share >= 0.55
        and tendency_np >= 12
    ):
        signals.append({"stat": "two_strike_dominance", "pitch": tendency.get("dominant_pitch_type"),
                        "share_pct": round(tendency_share * 100, 1), "flag": "medium",
                        "note": f"True two-strike pitch-plan outlier: {tendency.get('dominant_pitch_type')} at {round(tendency_share*100,1)}%."})

    # Bounce-back: season ERA >= 5.0 but gave up <=1 ER over >=5 IP
    era_season = season.get("era")
    ip_str = str(box.get("ip") or "0")
    try:
        ip_parts = ip_str.split(".")
        ip_val = int(ip_parts[0]) + (int(ip_parts[1]) / 3 if len(ip_parts) > 1 and ip_parts[1] else 0)
        if era_season and float(era_season) >= 5.0 and er <= 1 and ip_val >= 5.0:
            signals.append({"stat": "bounce_back", "season_era": era_season, "game_er": er,
                            "flag": "strong", "note": f"Strong bounce-back: {era_season} ERA entry, only {er} ER tonight."})
    except (ValueError, IndexError):
        pass

    # Pitch efficiency
    pa = int(box.get("pa") or 0)
    if pa > 0 and total_pitches > 0:
        p_per_pa = total_pitches / pa
        if p_per_pa <= 3.45 and total_pitches >= 80:
            signals.append({"stat": "efficiency", "pitches_per_pa": round(p_per_pa, 2), "flag": "strong",
                            "note": f"{round(p_per_pa, 2)} pitches/PA — exceptional efficiency, attacked the zone."})

    return signals


def _get_recent_pitcher_tweets(n: int = 3) -> list[str]:
    """Pull last N posted/approved pitcher_card tweet texts to inform angle rotation."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT tweet_text FROM content_queue "
                "WHERE content_type = 'pitcher_card' AND status IN ('draft', 'approved', 'posted') "
                "ORDER BY created_at DESC LIMIT ?",
                (n,),
            ).fetchall()
        return [r["tweet_text"] for r in rows if r["tweet_text"]]
    except Exception:
        return []


def _pitcher_angle_rotation_guardrail(recent: list[str]) -> str:
    """
    Build hard anti-repeat guidance from the latest posted pitcher tweets.
    Keeps persona style variation, but prevents repeating the same lead angle.
    """
    if not recent:
        return "No recent rotation risk detected."
    texts = [t.lower() for t in recent if t]
    if not texts:
        return "No recent rotation risk detected."

    def _count_hits(terms: tuple[str, ...]) -> int:
        return sum(1 for t in texts if any(term in t for term in terms))

    two_strike_hits = _count_hits(
        (
            "two-strike",
            "2-strike",
            "two strike",
            "count-state",
            "count state",
            "when he needed outs",
            "put-away",
            "put away",
        )
    )
    csw_whiff_hits = _count_hits(("csw", "whiff", "whiffs"))

    lines: list[str] = []
    if two_strike_hits >= 2:
        lines.append(
            "- Rotation hard-stop: recent posts overused two-strike/count-state framing. "
            "Do NOT lead with two-strike share, put-away pitch talk, or count-state conviction unless "
            "notable_game_events or standout_signals mark that as must_mention."
        )
    if csw_whiff_hits >= 3:
        lines.append(
            "- Rotation hard-stop: recent posts overused CSW/whiff-first openings. "
            "Use a different primary lead (line score shape, command/efficiency, season context, or game event) "
            "and keep CSW/whiffs as support."
        )
    if csw_whiff_hits >= 1:
        lines.append(
            "- Hard opener ban: do not start with 'N whiffs across/on/over Y pitches', a raw CSW% sentence, "
            "or any whiff/CSW-first construction. Find a different doorway into the outing."
        )
    if not lines:
        return "No recent rotation risk detected."
    return "\n".join(lines)


def _prompt_pitcher_card(
    *,
    global_cap: int,
    pmin: int,
    pmax: int,
    pitcher_redraft_block: str,
    persona_block: str,
    format_block: str,
    selected_angle_block: str,
    recent_tweets_block: str,
    angle_guardrail_block: str,
    meta: str,
    text: str,
) -> str:
    return (
        "You write posts for @Mallitalytics (MLB + LIDOM): pitching strategist brain, sharp X delivery, "
        "no invented numbers.\n"
        "Platform hard cap: {global_cap} characters.\n\n"
        "=== pitcher_card redraft ===\n\n"
        "--- STEP 1: FIND THE ANGLE (internal — do not print this step) ---\n"
        "Check standout_signals in the JSON first — these are pre-computed outliers. "
        "If any signal is flagged 'elite' or 'strong', it MUST be your primary angle unless a different signal is more extreme.\n"
        "Then scan for these yourself: whiffs >=12, csw_pct >=35 or <=18 with 80+ pitches, "
        "bs75_rate <=6% or >=25%, best_xwoba_pitch <=0.150 (only if that row includes n_pitches/n_bip with real samples), "
        "xwOBA gap >=0.200 between best and worst pitch (same sample rule), "
        "gb_pct >=60%, two-strike dominant_share >=55% with 12+ pitches, season ERA >=5.0 with <=1 ER tonight, "
        "pitches_per_pa <=3.45 with 80+ pitches, average velo +/-1 mph vs recent prior mean, "
        "zone rate >=55% or <=38% on 80+ pitches, single-pitch chase >=45% on 15+ pitches, "
        "top pitch usage >=50%, or command event (0 BB over 6+ IP / traffic with limited damage).\n"
        "If standout_signals includes game_shape with flag must_mention, that game event beats every other angle "
        "unless another signal is even more extreme.\n"
        "If JSON notable_game_events is non-empty, the TOP item's label must appear as a plain-English phrase in the "
        "first two sentences (you may add inning detail). Do not paraphrase it into a different story.\n"
        "If source_metadata.is_mlb_debut_game is true, you MUST work in an MLB debut welcome (player_name) in the first "
        "or second sentence — energetic but not corny; tie to a strong line score when the box supports it. "
        "If both a notable_game_events must-mention and debut apply, put the game event in sentence one and the debut "
        "welcome in sentence two (or fuse cleanly in one line if it fits).\n"
        "Pick ONE primary angle — the single most statistically unusual or narratively compelling thing in this card.\n"
        "Check recent_tweets_context: if the last 3 posts all used the same angle type, deprioritize it unless "
        "the data is extreme enough to justify repeating.\n\n"
        "--- SELECTED ANGLE (non-negotiable) ---\n"
        "{selected_angle_block}\n"
        "You MUST lead with this selected angle. Other angles may appear only as the supporting beat.\n"
        "If the selected angle is not count_state, do not open with two-strike, count-state, put-away, "
        "leaned on, or go-to weapon framing.\n\n"
        "--- STEP 2: YOUR PERSONA ---\n"
        "{persona_block}\n"
        "Your persona shapes your instinct, hook style, and tone. Do NOT state the persona name in the post.\n\n"
        "--- STEP 3: YOUR FORMAT ---\n"
        "{format_block}\n"
        "Choose the format that best serves your angle and persona. "
        "Do not default to the same format every time — the format should follow the story.\n\n"
        "--- STEP 4: WRITE THE POST ---\n"
        "Non-negotiable constraints:\n"
        "- Target {pmin}-{pmax} characters total.\n"
        "- Voice: sound like a sharp analyst, not a highlight caption. Banned as opening energy or empty lead-ins: "
        "stifling, electric, filthy, dominant (as a standalone vibe word before any fact), "
        "gem, masterpiece, 'went off', or name + delivered + generic compliment. If you do not have a number or "
        "a concrete process angle in the first 8 words, rewrite the lede. Prefer: stat first, or matchup + one cold fact.\n"
        "- Never open with the template 'N whiffs across/on/over Y pitches' or 'A X% CSW rate...'. "
        "Even when selected_angle is whiff_csw, lead through the game shape, opponent problem, pitch-plan tension, "
        "or box-line contrast, then cite whiffs/CSW as the support beat.\n"
        "- Banned enders (read as template AI, especially for established starters in season): rhetorical 'Was this X or Y?', "
        "'peak form or fluke', 'is he back', 'real or just a hot night', 'Will the X hold as the go-to weapon?', "
        "'Will the X keep working?', 'Is this the new normal?', 'Will it scale?' — unless the JSON explicitly encodes a comeback, "
        "first start off IL, or true rookie debut (is_mlb_debut_game). For veterans, never frame the game as a lottery ticket.\n"
        "- First-sentence test: if your first sentence contains two-strike, 2-strike, count-state, put-away, lean on, "
        "leaned on, or go-to weapon, rewrite it unless selected_angle is count_state. If your first sentence is only "
        "the box line (IP, ER, K, BB, H), rewrite it.\n"
        "- All numbers from the JSON only — zero invented stats.\n"
        "- The box line (IP, ER, K, BB, H, pitch count) must appear somewhere, but does NOT have to be the opening.\n"
        "- Say the result once. No tautology (e.g. do not pair 'scoreless' with '0 ER' as two separate reveals).\n"
        "- xwOBA is contact-quality signal, not causation. Safe phrasing: 'showed the weakest contact-quality signal'. "
        "Never say a named pitch 'allowed' a specific HR/hit/run unless pitch-to-event attribution is in the JSON.\n"
        "- Do not lead with or over-weight a per-pitch-type xwOBA beat unless pitcher_tweet_context includes "
        "n_pitches >= 15 and n_bip >= 6 on that row (or n_pitches >= 25 when n_bip is absent). "
        "Tiny samples are not the story.\n"
        "- Game-wide whiff/CSW totals belong to the OUTING, not to any single pitch. "
        "Never write a sentence where a pitch type is the subject and whiffs/CSW is the predicate "
        "(e.g. 'The curveball fueled 20 whiffs' or 'It generated a 37% CSW' are BANNED — "
        "those totals are across all pitches). "
        "Avoid 'fueled' around CSW/whiffs. Safe support framing: 'He racked up 20 whiffs on the night' "
        "or '37% CSW across 100 pitches'.\n"
        "- When citing game_fast_swing_pct, call it the card BS75+ rate (swings >=75 mph bat speed / all swings).\n"
        "- BS75+ is a swing-level bat-speed share (denominator = swings). It includes whiffs and fouls — a high rate is NOT "
        "the same as hard contact or 'squaring him up.' Reserve damage/solid-contact language for when JSON supports it "
        "(e.g. hard-hit%, max EV, pitch-type or game xwOBA on BIP). Safe words: aggressive swings, hard swing-speed band, "
        "athletic swings.\n"
        "- Banned phrasing: do not write 'ERA coming in' / 'ERA going out' unless BOTH sides are literally ERA from the "
        "JSON (e.g. season_pitching_stats.era vs a clearly labeled post-start ERA field). Never attach 'ERA going out' "
        "to a pitch type, slider, or contact-quality stat — that is a category error.\n"
        "- Opponent: only reference if JSON opponent field is present, non-empty, and differs from team.\n"
        "- Do not describe opponent lineup quality unless an explicit opponent-strength field is in the JSON.\n"
        "- RV/100: optional, at most six words, never the lead.\n"
        "- Hashtags: #Mallitalytics always; at most one of #MLB #LIDOM #FantasyMLB or a team tag.\n"
        "- Banned: markdown (**bold**), Unicode em dash, 'drop your take below', "
        "invented barrels/EV/damage, more than 2 emojis, literal backslash-n or /n as text.\n\n"
        "Available data angles (menu — pick what serves the story, not all of it):\n"
        "{pitcher_redraft_block}\n"
        "Recent posts for angle rotation (avoid leading with the same angle type back-to-back):\n"
        "{recent_tweets_block}\n\n"
        "Rotation guardrails (derived from recent posts):\n"
        "{angle_guardrail_block}\n\n"
        "Context JSON:\n{meta}\n\n"
        "Original draft:\n{text}\n\n"
        "Reply with ONLY the final tweet text. No preamble."
    ).format(
        global_cap=global_cap,
        pmin=pmin,
        pmax=pmax,
        pitcher_redraft_block=pitcher_redraft_block,
        persona_block=persona_block,
        format_block=format_block,
        selected_angle_block=selected_angle_block,
        recent_tweets_block=recent_tweets_block,
        angle_guardrail_block=angle_guardrail_block,
        meta=meta,
        text=text,
    )


def _prompt_batter_card(
    *,
    global_cap: int,
    bmin: int,
    bmax: int,
    batter_redraft_block: str,
    batter_persona_block: str,
    batter_format_block: str,
    batter_recent_block: str,
    meta: str,
    text: str,
) -> str:
    return (
        "You write posts for @Mallitalytics (MLB + LIDOM): hitting operator brain, sharp X delivery, "
        "no invented numbers.\n"
        "Platform hard cap: {global_cap} characters.\n\n"
        "=== batter_card redraft ===\n\n"
        "--- STEP 1: FIND THE ANGLE ---\n"
        "Check standout_signals first. If any signal has flag 'must_mention', that MUST be honored in the first two sentences.\n"
        "Then scan: max_ev >= 110, hard_hit_pct >= 55, multi-HR night (line.hr >= 2), re24 >= 1.25 when present.\n"
        "Pick ONE primary angle — headline events (notable_game_events) beat micro-stats.\n"
        "Check Recent batter posts: if the last 3 batter posts all opened the same way, rotate your hook.\n\n"
        "--- STEP 2: YOUR PERSONA ---\n"
        "{batter_persona_block}\n"
        "Do NOT state the persona name in the post.\n\n"
        "--- STEP 3: YOUR FORMAT ---\n"
        "{batter_format_block}\n\n"
        "--- STEP 4: WRITE THE POST ---\n"
        "- Target {bmin}-{bmax} characters total.\n"
        "- All numbers from the JSON only — zero invented stats.\n"
        "- Include the batting line (AB, H, HR, RBI, BB, K) once; it does not have to be the opening.\n"
        "- Never bury a grand slam, multi-HR game, or other notable_game_events item under generic praise or tiny-sample xwOBA.\n"
        "- If notable_game_events includes a grand slam (or standout_signals says to mention it), include the exact "
        "two-word phrase grand slam in the FIRST sentence.\n"
        "- xwOBA / contact-quality: supporting detail only; skip micro-sample pitch-type fairy tales.\n"
        "- Hashtags: #Mallitalytics always; at most one of #MLB #LIDOM #FantasyMLB or a team tag.\n"
        "- Banned: markdown (**bold**), Unicode em dash, 'drop your take below', literal backslash-n or /n as text.\n\n"
        "Beat sheet:\n{batter_redraft_block}\n"
        "Recent batter posts (angle rotation):\n{batter_recent_block}\n\n"
        "Context JSON:\n{meta}\n\n"
        "Original draft:\n{text}\n\n"
        "Reply with ONLY the final tweet text. No preamble."
    ).format(
        global_cap=global_cap,
        bmin=bmin,
        bmax=bmax,
        batter_redraft_block=batter_redraft_block,
        batter_persona_block=batter_persona_block,
        batter_format_block=batter_format_block,
        batter_recent_block=batter_recent_block,
        meta=meta,
        text=text,
    )


def _prompt_generic(*, global_cap: int, meta: str, text: str) -> str:
    return (
        "You write posts for @Mallitalytics (MLB + LIDOM): punchy rewrite, no invented numbers.\n"
        "Platform hard cap: {global_cap} characters.\n\n"
        "Punchy rewrite under {global_cap} chars, plain text, keep existing hashtags if any, no markdown.\n\n"
        "Context JSON:\n{meta}\n\n"
        "Original draft:\n{text}\n\n"
        "Reply with ONLY the final tweet text. No preamble."
    ).format(global_cap=global_cap, meta=meta, text=text)


def _prompt_slate_card(*, global_cap: int, meta: str, text: str, content_type: str) -> str:
    """
    Full-day slate posts (probables board, games of day) — not player cards.
    Stops the model from inventing fake head-to-head 'collision' when teams are in different games.
    """
    label = (
        "Probable starters"
        if content_type == "probables_board"
        else "Pitching Index"
        if content_type == "pitching_index"
        else "Games of the day / slate"
    )
    return (
        "You lightly edit a multi-game MLB slate post for @Mallitalytics (plain text, one post).\n"
        f"Post kind: {label}.\n"
        f"Platform hard cap: {global_cap} characters.\n\n"
        "You MUST follow all of the following. If the draft already violates a rule, fix it; do not repeat the mistake.\n"
        "- The text may name several teams. Those teams are not necessarily playing each other. "
        "Never imply one shared matchup, 'collision', or that exactly one of two named streaks 'must' end for both sides "
        "unless the draft already ties them to the same game in one line (e.g. same Away @ Home pairing). "
        "Different games on the same day can all be wins or all losses; do not add forced drama across unrelated games.\n"
        "- Banned (unless the original draft is explicitly about a single A vs B game and you are not inventing that game): "
        "lines like \"someone's streak ends today\", \"something's gotta give\", \"when they meet\", or any thesis that fuses "
        "two teams who are not established as opponents in the draft.\n"
        "- Do not invent games, first pitch, streak lengths, or standings. Tighten wording and length only.\n"
        "- Prefer short parallel facts; dry is better than a clever but wrong narrative.\n\n"
        f"Context JSON:\n{meta}\n\n"
        f"Original draft:\n{text}\n\n"
        "Reply with ONLY the final tweet text. No preamble."
    )


def _build_prompt(text: str, meta: dict) -> str:
    cap = get_redraft_meta_max_chars()
    pmin, pmax = get_redraft_pitcher_tweet_target_range()
    bmin, bmax = get_redraft_batter_tweet_target_range()

    pitcher_block = ""
    batter_block = ""
    persona_block = ""
    format_block = ""
    selected_angle_block = "Selected angle: box_line"
    recent_tweets_block = "None available."
    batter_persona_block = ""
    batter_format_block = ""
    batter_recent_block = "None available."

    work = dict(meta) if isinstance(meta, dict) else {}

    if work.get("card_type") == "pitcher_card":
        signals = _compute_standout_signals(work)
        if signals:
            work = {**work, "standout_signals": signals}

        pitcher_block = _pitcher_redraft_beat_sheet(work)

        recent_ctx = _recent_pitcher_style_context(7)
        selected_style = _select_pitcher_style(work, signals, recent_ctx)
        persona = selected_style["persona"]
        persona_block = (
            f"Persona: {persona['name']}\n"
            f"Instinct: {persona['instinct']}\n"
            f"Hook style: {persona['hook_style']}\n"
            f"Tone: {persona['tone']}\n"
            f"Constraint: {persona['banned_openers']}"
        )

        post_format = selected_style["format"]
        format_block = f"Selected format: {post_format}"
        angle_signal = selected_style.get("angle_signal") or {}
        selected_angle_block = (
            f"Selected angle: {selected_style['angle_id']}\n"
            f"Selected signal: {json.dumps(angle_signal, default=str)}"
        )
        work = {
            **work,
            "redraft_style": {
                "angle_id": selected_style["angle_id"],
                "angle_signal": angle_signal,
                "candidate_angles": selected_style.get("candidate_angles") or [],
                "persona": persona["name"],
                "format": post_format.split(":", 1)[0],
            },
        }
        if isinstance(meta, dict):
            meta["_redraft_style_pending"] = work["redraft_style"]

        recent = [r.get("tweet_text", "") for r in recent_ctx[:3] if r.get("tweet_text")]
        if recent:
            recent_tweets_block = "\n".join(
                f"  [{i+1}] {t[:200]}" for i, t in enumerate(recent)
            )
        pitcher_angle_guardrail_block = _pitcher_angle_rotation_guardrail(recent)
        recent_angle_ids = [r.get("angle_id") for r in recent_ctx[:7] if r.get("angle_id")]
        if recent_angle_ids[:2].count("count_state") > 0 and selected_style["angle_id"] != "count_state":
            pitcher_angle_guardrail_block += (
                "\n- BANNED THIS RUN: count-state framing, two-strike share leads, put-away pitch openers, "
                "'leaned on the X' + count-state."
            )
        if recent_angle_ids[:2].count("bs75") > 0 and selected_style["angle_id"] != "bs75":
            pitcher_angle_guardrail_block += "\n- BANNED THIS RUN: BS75+ lede; keep bat-speed share as support only."
        if recent_angle_ids[:2].count("whiff_csw") > 0 and selected_style["angle_id"] != "whiff_csw":
            pitcher_angle_guardrail_block += "\n- BANNED THIS RUN: CSW/whiff lede; mention only as support."

    elif work.get("card_type") == "batter_card":
        bsig = _compute_batter_standout_signals(work)
        if bsig:
            work = {**work, "standout_signals": bsig}
        batter_block = _batter_redraft_beat_sheet(work)
        bpersona = random.choice(_BATTER_PERSONAS)
        batter_persona_block = (
            f"Persona: {bpersona['name']}\n"
            f"Instinct: {bpersona['instinct']}\n"
            f"Hook style: {bpersona['hook_style']}\n"
            f"Tone: {bpersona['tone']}\n"
            f"Constraint: {bpersona['banned_openers']}"
        )
        batter_fmt = random.choice(_POST_FORMATS)
        batter_format_block = f"Selected format: {batter_fmt}"
        recent_b = _get_recent_batter_tweets(3)
        if recent_b:
            batter_recent_block = "\n".join(
                f"  [{i+1}] {t[:200]}" for i, t in enumerate(recent_b)
            )

    meta_json = json.dumps(work, default=str)[:cap]
    gcap = get_tweet_max_chars()
    content_type = work.get("content_type")
    if content_type in ("probables_board", "games_of_day", "pitching_index"):
        return _prompt_slate_card(
            global_cap=gcap, meta=meta_json, text=text, content_type=content_type
        )

    if work.get("card_type") == "pitcher_card":
        return _prompt_pitcher_card(
            global_cap=gcap,
            pmin=pmin,
            pmax=pmax,
            pitcher_redraft_block=pitcher_block,
            persona_block=persona_block,
            format_block=format_block,
            selected_angle_block=selected_angle_block,
            recent_tweets_block=recent_tweets_block,
            angle_guardrail_block=pitcher_angle_guardrail_block,
            meta=meta_json,
            text=text,
        )
    if work.get("card_type") == "batter_card":
        return _prompt_batter_card(
            global_cap=gcap,
            bmin=bmin,
            bmax=bmax,
            batter_redraft_block=batter_block,
            batter_persona_block=batter_persona_block,
            batter_format_block=batter_format_block,
            batter_recent_block=batter_recent_block,
            meta=meta_json,
            text=text,
        )
    return _prompt_generic(global_cap=gcap, meta=meta_json, text=text)


def _redraft_claude(text: str, meta: dict) -> str:
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured.")
    from anthropic import Anthropic
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
    client = Anthropic(api_key=key)
    prompt = _build_prompt(text, meta)

    def _call(p: str) -> str:
        msg = client.messages.create(
            model=model,
            max_tokens=get_redraft_max_tokens(),
            messages=[{"role": "user", "content": p}],
        )
        parts = [getattr(b, "text", None) for b in msg.content if getattr(b, "text", None)]
        return "".join(parts).strip() or str(msg.content[0])

    out = truncate_tweet_text_to_cap(_sanitize_redraft_output(_call(prompt), meta))
    recent = _get_recent_pitcher_tweets(3) if meta.get("card_type") == "pitcher_card" else []
    violations = _pitcher_redraft_violations(out, meta, recent)
    if violations:
        retry_prompt = _build_retry_prompt(prompt, out, violations)
        out = truncate_tweet_text_to_cap(_sanitize_redraft_output(_call(retry_prompt), meta))
    return out


def _redraft_grok(text: str, meta: dict) -> str:
    key = os.getenv("X_API_KEY")
    if not key:
        raise HTTPException(status_code=503, detail="X_API_KEY not configured.")
    import requests as _req
    model = os.getenv("GROK_MODEL", "grok-3-latest")
    prompt = _build_prompt(text, meta)

    def _call(p: str) -> str:
        resp = _req.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "max_tokens": get_redraft_max_tokens(),
                "messages": [{"role": "user", "content": p}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

    try:
        out = truncate_tweet_text_to_cap(_sanitize_redraft_output(_call(prompt), meta))
        recent = _get_recent_pitcher_tweets(3) if meta.get("card_type") == "pitcher_card" else []
        violations = _pitcher_redraft_violations(out, meta, recent)
        if violations:
            retry_prompt = _build_retry_prompt(prompt, out, violations)
            out = truncate_tweet_text_to_cap(_sanitize_redraft_output(_call(retry_prompt), meta))
        return out
    except _req.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Grok error {e.response.status_code}: {e.response.text[:400]}") from e


def _redraft_item_sync(item_id: int, provider: str) -> dict:
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    text = item.get("tweet_text") or ""
    meta_raw = item.get("meta_json")
    parsed: dict = {}
    if meta_raw:
        try:
            parsed = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
            if not isinstance(parsed, dict):
                parsed = {}
        except json.JSONDecodeError:
            parsed = {}
    base = {
        "queue_id": item_id,
        "content_type": item.get("content_type"),
        "title": item.get("title"),
        "player_id": item.get("player_id"),
        "player_name": item.get("player_name"),
        "game_date": item.get("game_date"),
        "season": item.get("season"),
        "stage": item.get("stage"),
        "game_pk": item.get("game_pk"),
    }
    base = {k: v for k, v in base.items() if v is not None}
    meta: dict = {**base, **parsed}
    try:
        if provider == "grok":
            out = _redraft_grok(text, meta)
            model = os.getenv("GROK_MODEL", "grok-3-latest")
        else:
            out = _redraft_claude(text, meta)
            model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        style = meta.pop("_redraft_style_pending", None)
        if isinstance(style, dict):
            style = {
                **style,
                "provider": provider,
                "model": model,
                "generated_at": datetime.utcnow().isoformat(),
            }
            updated_meta = {
                **parsed,
                "redraft_style": style,
                "ai_assisted": True,
                "creation_mode": "ai_assisted",
            }
            update_queue_item(
                item_id,
                meta_json=json.dumps(updated_meta, default=str),
                manual_or_ai="ai",
            )
        return {"tweet_text": out, "model": model, "provider": provider}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{provider} error: {e}") from e


@router.post("/{item_id}/redraft")
async def redraft_item(item_id: int, provider: str = "claude"):
    """provider: 'claude' (default) or 'grok'"""
    return await run_in_threadpool(_redraft_item_sync, item_id, provider)


def _get_item_sync(item_id: int) -> dict:
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    return item


@router.get("/{item_id}")
async def get_item(item_id: int):
    return await run_in_threadpool(_get_item_sync, item_id)


def _merge_meta(raw: Optional[str], patch: dict) -> str:
    meta = {}
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                meta = parsed
        except json.JSONDecodeError:
            meta = {}
    return json.dumps({**meta, **patch}, default=str)


def _score_item_sync(item_id: int) -> dict:
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    score = score_queue_item(item)
    meta_json = _merge_meta(item.get("meta_json"), {"content_score": score})
    update_queue_item(
        item_id,
        priority_score=int(score["priority_score"]),
        content_pillar=str(score["recommended_pillar"]),
        intended_kpi=str(score["primary_kpi"]),
        meta_json=meta_json,
    )
    updated = get_queue_item(item_id)
    return {"score": score, "item": updated}


@router.post("/{item_id}/score")
async def score_item(item_id: int):
    return await run_in_threadpool(_score_item_sync, item_id)


def _patch_item_sync(item_id: int, body: QueueItemPatch) -> dict:
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")

    updates = {}
    if body.tweet_text is not None:
        updates["tweet_text"] = truncate_tweet_text_to_cap(body.tweet_text)
    if body.status is not None:
        allowed_statuses = {"draft", "approved", "rejected", "posted", "failed"}
        if body.status not in allowed_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {allowed_statuses}")
        updates["status"] = body.status
        if body.status in ("approved", "rejected"):
            updates["reviewed_at"] = datetime.utcnow().isoformat()
    if body.content_pillar is not None:
        if body.content_pillar not in CONTENT_PILLARS:
            raise HTTPException(status_code=400, detail="Invalid content_pillar.")
        updates["content_pillar"] = body.content_pillar
    if body.hook_type is not None:
        if body.hook_type not in HOOK_TYPES:
            raise HTTPException(status_code=400, detail="Invalid hook_type.")
        updates["hook_type"] = body.hook_type
    if body.intended_kpi is not None:
        if body.intended_kpi not in INTENDED_KPIS:
            raise HTTPException(status_code=400, detail="Invalid intended_kpi.")
        updates["intended_kpi"] = body.intended_kpi
    if body.priority_score is not None:
        updates["priority_score"] = body.priority_score
    if body.campaign is not None:
        updates["campaign"] = body.campaign.strip()[:80]
    if body.experiment_tag is not None:
        updates["experiment_tag"] = body.experiment_tag.strip()[:120]

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update.")

    update_queue_item(item_id, **updates)
    meta_updates = {k: updates[k] for k in ("content_pillar", "hook_type", "intended_kpi", "priority_score", "campaign", "experiment_tag") if k in updates}
    if meta_updates:
        fresh = get_queue_item(item_id)
        update_queue_item(item_id, meta_json=_merge_meta(fresh.get("meta_json") if fresh else None, meta_updates))
    return get_queue_item(item_id)


@router.patch("/{item_id}")
async def patch_item(item_id: int, body: QueueItemPatch):
    return await run_in_threadpool(_patch_item_sync, item_id, body)


def _delete_all_drafts_sync() -> dict:
    deleted = delete_draft_queue_items()
    return {"deleted": deleted, "status": "draft"}


@router.delete("/drafts")
async def delete_all_drafts():
    return await run_in_threadpool(_delete_all_drafts_sync)


def _delete_item_sync(item_id: int) -> dict:
    item = get_queue_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found.")
    if item["status"] != "draft":
        raise HTTPException(status_code=400, detail="Only draft items can be deleted.")
    deleted = delete_queue_item(item_id)
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete item.")
    return {"deleted": True, "id": item_id}


@router.delete("/{item_id}")
async def delete_item(item_id: int):
    return await run_in_threadpool(_delete_item_sync, item_id)
