"""Default tweet text for a detected live event.

Kept deliberately short (every draft is editable in the Queue UI). No emojis
per the repo UI rule; `#Mallitalytics` suffix is dropped here so redraft has
room — add it in the Queue if you want.
"""
from __future__ import annotations

from typing import Any


def _fmt_ev(v: Any) -> str:
    try:
        return f"{float(v):.1f} mph"
    except (TypeError, ValueError):
        return ""


def _fmt_dist(v: Any) -> str:
    try:
        return f"{int(round(float(v)))} ft"
    except (TypeError, ValueError):
        return ""


def _inning_word(half: str, inning: int) -> str:
    suf = {1: "st", 2: "nd", 3: "rd"}.get(inning if inning < 20 else inning % 10, "th")
    if 11 <= (inning % 100) <= 13:
        suf = "th"
    h = (half or "").lower()
    if h.startswith("top"):
        return f"top of the {inning}{suf}"
    if h.startswith("bot"):
        return f"bottom of the {inning}{suf}"
    return f"{inning}{suf}"


def _hr(payload: dict[str, Any]) -> str:
    name = payload.get("batter_name") or "Batter"
    bat = payload.get("bat_team_abbr") or ""
    pit = payload.get("pit_team_abbr") or ""
    inning = int(payload.get("inning") or 0)
    half = payload.get("half") or ""
    pitcher = payload.get("pitcher_name") or "the pitcher"
    ev = _fmt_ev(payload.get("launch_speed_mph"))
    dist = _fmt_dist(payload.get("total_distance_ft"))
    rbi = payload.get("rbi")
    bits: list[str] = []
    if ev:
        bits.append(ev)
    if dist:
        bits.append(dist)
    stat_tail = " · ".join(bits)
    rbi_tail = ""
    if isinstance(rbi, int) and rbi > 1:
        rbi_tail = f" ({rbi} RBI)"
    head = f"{name} — HR"
    if bat or pit:
        head += f" ({bat or '?'} vs {pit or '?'})"
    body = f"{_inning_word(half, inning)} off {pitcher}{rbi_tail}."
    stat_line = f"\n{stat_tail}" if stat_tail else ""
    return f"{head}\n{body}{stat_line}"


def _multi_hr(payload: dict[str, Any]) -> str:
    name = payload.get("batter_name") or "Batter"
    n = int(payload.get("hr_count") or 2)
    bat = payload.get("bat_team_abbr") or ""
    inning = payload.get("latest_inning")
    ev = _fmt_ev(payload.get("latest_ev_mph"))
    dist = _fmt_dist(payload.get("latest_distance_ft"))
    tail_bits = [x for x in (ev, dist) if x]
    tail = f" · {' · '.join(tail_bits)}" if tail_bits else ""
    suffix = "nd" if n == 2 else "rd" if n == 3 else "th"
    return (
        f"{name} — HR #{n} of the night{tail}.\n"
        f"{bat} is riding the {n}{suffix} long ball"
        f"{f' (thru {inning})' if inning else ''}."
    )


def _no_hit_bid(payload: dict[str, Any]) -> str:
    inning = int(payload.get("through_inning") or 0)
    pit = payload.get("pitching_team_abbr") or ""
    bat = payload.get("batting_team_abbr") or ""
    return (
        f"{pit} — combined no-hit bid through {inning} vs {bat}.\n"
        f"Still hitless. Watching."
    )


def _k_milestone(payload: dict[str, Any]) -> str:
    name = payload.get("pitcher_name") or "Pitcher"
    k = int(payload.get("k_count") or 0)
    inning = payload.get("through_inning")
    abbr = payload.get("pit_team_abbr") or ""
    return (
        f"{name} — {k} K through {inning}.\n"
        f"{abbr} starter carving."
    )


def _cycle_watch(payload: dict[str, Any]) -> str:
    name = payload.get("batter_name") or "Batter"
    missing = payload.get("missing") or []
    abbr = payload.get("bat_team_abbr") or ""
    need = ", ".join(missing) or "one more hit"
    return (
        f"{name} — cycle watch.\n"
        f"Needs: {need} ({abbr})."
    )


def _final(payload: dict[str, Any]) -> str:
    win = payload.get("winner_abbr") or ""
    lose = payload.get("loser_abbr") or ""
    wr = payload.get("winner_runs")
    lr = payload.get("loser_runs")
    innings = payload.get("total_innings")
    extra = ""
    try:
        if innings and int(innings) > 9:
            extra = f" (F/{innings})"
    except (TypeError, ValueError):
        pass
    return f"FINAL{extra} — {win} {wr}, {lose} {lr}."


def _debut(payload: dict[str, Any]) -> str:
    name = payload.get("player_name") or "Player"
    team = payload.get("team_abbr") or payload.get("team_name") or ""
    pos = payload.get("position") or ""
    pos_tail = f" ({pos})" if pos else ""
    return (
        f"{name}{pos_tail} — MLB debut for {team}.\n"
        f"Welcome to The Show."
    )


_BUILDERS = {
    "hr": _hr,
    "multi_hr": _multi_hr,
    "no_hit_bid": _no_hit_bid,
    "k_milestone": _k_milestone,
    "cycle_watch": _cycle_watch,
    "final": _final,
    "debut": _debut,
}


def build_tweet(event: dict[str, Any]) -> str:
    etype = event.get("event_type") or ""
    fn = _BUILDERS.get(etype)
    if fn is None:
        return event.get("headline") or ""
    return fn(event.get("payload") or {}).strip()
