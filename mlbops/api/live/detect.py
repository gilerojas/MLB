"""Live event detectors.

Each detector is a pure function: `(feed_json) -> list[dict]`. The returned
dicts share a common shape:

    {
        "event_type": "hr" | "multi_hr" | "no_hit_bid" | "k_milestone"
                      | "cycle_watch" | "final" | "debut",
        "dedupe_key": str,              # globally unique across re-scans
        "game_pk":    int,
        "game_date":  "YYYY-MM-DD",
        "player_id":  Optional[int],
        "player_name": Optional[str],
        "headline":   str,              # one-line summary for the hub UI
        "payload":    dict,             # free-form context (scored into tweet)
    }

`text.py` turns the payload into the default `tweet_text`. This module never
talks to the DB or the Stats API directly.
"""
from __future__ import annotations

from typing import Any, Optional

# Thresholds / knobs
K_MILESTONE_STEPS: tuple[int, ...] = (10, 12, 14, 16, 18)
NO_HIT_BID_MIN_INNING: int = 5  # emit at end of 5th+ when opposing hits == 0
CYCLE_WATCH_NEEDED: int = 3     # emit when batter has this many distinct hit types


# ---------------------------------------------------------------------------
# small helpers

def _game_pk(feed: dict[str, Any]) -> int:
    return int(((feed.get("gameData") or {}).get("game") or {}).get("pk") or 0)


def _game_date(feed: dict[str, Any]) -> str:
    # `officialDate` is the date MLB books the game under (handles TZ/suspensions).
    return str(((feed.get("gameData") or {}).get("datetime") or {}).get("officialDate") or "")


def _teams_meta(feed: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return {'home': {id, name, abbr}, 'away': {id, name, abbr}}."""
    teams = (feed.get("gameData") or {}).get("teams") or {}
    out: dict[str, dict[str, Any]] = {}
    for side in ("home", "away"):
        t = teams.get(side) or {}
        out[side] = {
            "id": t.get("id"),
            "name": t.get("name"),
            "abbr": t.get("abbreviation") or t.get("teamCode") or "",
        }
    return out


def _all_plays(feed: dict[str, Any]) -> list[dict[str, Any]]:
    plays = (feed.get("liveData") or {}).get("plays") or {}
    return list(plays.get("allPlays") or [])


def _current_play(feed: dict[str, Any]) -> Optional[dict[str, Any]]:
    return ((feed.get("liveData") or {}).get("plays") or {}).get("currentPlay") or None


def _abstract_state(feed: dict[str, Any]) -> str:
    st = (feed.get("gameData") or {}).get("status") or {}
    return str(st.get("abstractGameState") or "")


def _linescore(feed: dict[str, Any]) -> dict[str, Any]:
    return (feed.get("liveData") or {}).get("linescore") or {}


def _batting_side_for_play(play: dict[str, Any]) -> str:
    """Return 'home' or 'away' for the batting team in this play."""
    # about.halfInning: 'top' => away bats / home pitches; 'bottom' => home bats.
    half = (play.get("about") or {}).get("halfInning") or ""
    return "away" if half.lower() == "top" else "home"


def _pitching_side_for_play(play: dict[str, Any]) -> str:
    return "home" if _batting_side_for_play(play) == "away" else "away"


def _batter(play: dict[str, Any]) -> tuple[Optional[int], Optional[str]]:
    m = play.get("matchup") or {}
    b = m.get("batter") or {}
    return (b.get("id"), b.get("fullName"))


def _pitcher(play: dict[str, Any]) -> tuple[Optional[int], Optional[str]]:
    m = play.get("matchup") or {}
    p = m.get("pitcher") or {}
    return (p.get("id"), p.get("fullName"))


def _inning(play: dict[str, Any]) -> int:
    return int((play.get("about") or {}).get("inning") or 0)


def _play_end_index(play: dict[str, Any]) -> int:
    # atBatIndex is unique per PA per game; endTime is not monotonic for diffs.
    return int(play.get("atBatIndex") or 0)


# ---------------------------------------------------------------------------
# detectors

def detect_home_runs(feed: dict[str, Any]) -> list[dict[str, Any]]:
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    teams = _teams_meta(feed)
    out: list[dict[str, Any]] = []
    for p in _all_plays(feed):
        result = p.get("result") or {}
        if (result.get("eventType") or "").lower() != "home_run":
            continue
        idx = _play_end_index(p)
        b_id, b_name = _batter(p)
        p_id, p_name = _pitcher(p)
        inning = _inning(p)
        bat_side = _batting_side_for_play(p)
        pit_side = _pitching_side_for_play(p)
        # hit data from the final pitch of the PA
        hit = {}
        for ev in p.get("playEvents") or []:
            hd = ev.get("hitData") or {}
            if hd:
                hit = hd
        description = (result.get("description") or "").strip()
        rbi = result.get("rbi")
        out.append({
            "event_type": "hr",
            "dedupe_key": f"hr:{game_pk}:{idx}",
            "game_pk": game_pk,
            "game_date": game_date,
            "player_id": b_id,
            "player_name": b_name,
            "headline": (
                f"{b_name} HR — inning {inning} "
                f"({teams['away']['abbr']} @ {teams['home']['abbr']})"
            ),
            "payload": {
                "batter_id": b_id,
                "batter_name": b_name,
                "pitcher_id": p_id,
                "pitcher_name": p_name,
                "inning": inning,
                "half": (p.get("about") or {}).get("halfInning"),
                "bat_team_abbr": teams[bat_side]["abbr"],
                "pit_team_abbr": teams[pit_side]["abbr"],
                "rbi": rbi,
                "launch_speed_mph": hit.get("launchSpeed"),
                "launch_angle_deg": hit.get("launchAngle"),
                "total_distance_ft": hit.get("totalDistance"),
                "description": description,
                "away_score": result.get("awayScore"),
                "home_score": result.get("homeScore"),
            },
        })
    return out


def detect_multi_hr(feed: dict[str, Any], hrs: Optional[list[dict[str, Any]]] = None) -> list[dict[str, Any]]:
    """2nd, 3rd, 4th+ HR of the game for the same batter. Emits one event per
    HR beyond the first, keyed by (batter, count), so re-scans are idempotent.
    """
    if hrs is None:
        hrs = detect_home_runs(feed)
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    counter: dict[int, int] = {}
    out: list[dict[str, Any]] = []
    for ev in hrs:
        pid = ev["payload"].get("batter_id")
        if pid is None:
            continue
        counter[pid] = counter.get(pid, 0) + 1
        n = counter[pid]
        if n < 2:
            continue
        name = ev["payload"].get("batter_name")
        out.append({
            "event_type": "multi_hr",
            "dedupe_key": f"multi_hr:{game_pk}:{pid}:{n}",
            "game_pk": game_pk,
            "game_date": game_date,
            "player_id": pid,
            "player_name": name,
            "headline": f"{name} multi-HR — #{n} of the night",
            "payload": {
                "batter_id": pid,
                "batter_name": name,
                "hr_count": n,
                "latest_inning": ev["payload"].get("inning"),
                "latest_distance_ft": ev["payload"].get("total_distance_ft"),
                "latest_ev_mph": ev["payload"].get("launch_speed_mph"),
                "bat_team_abbr": ev["payload"].get("bat_team_abbr"),
            },
        })
    return out


def detect_no_hit_bid(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """Combined no-hit bid: opposing team has 0 hits through inning ≥5.

    Emits one event per side at the first qualifying completed inning (and at
    every later completed inning where the bid survives), keyed by inning so
    scans remain idempotent.
    """
    abstract = _abstract_state(feed)
    if abstract.lower() not in {"live"}:
        return []
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    teams = _teams_meta(feed)
    line = _linescore(feed)
    current_inning = int(line.get("currentInning") or 0)
    out: list[dict[str, Any]] = []
    if current_inning < NO_HIT_BID_MIN_INNING:
        return out
    innings = line.get("innings") or []
    # sum hits per side through the last *completed* inning on each side.
    # An inning dict has 'home' and 'away' sub-objects with 'hits'.
    for side, opp in (("home", "away"), ("away", "home")):
        # "hits against the pitchers of `side`" = hits by `opp`
        hits = 0
        last_completed = 0
        for inn in innings:
            inn_num = int(inn.get("num") or 0)
            opp_hits = ((inn.get(opp) or {}).get("hits") or 0)
            # Only count completed innings for the opposing side.
            if opp == "away":
                # away bats in the top; always completed if any pitches recorded
                completed = inn_num < current_inning or (
                    inn_num == current_inning
                    and (line.get("inningHalf") or "").lower() != "top"
                )
            else:
                completed = inn_num < current_inning
            if not completed:
                continue
            hits += int(opp_hits or 0)
            last_completed = max(last_completed, inn_num)
        if last_completed < NO_HIT_BID_MIN_INNING:
            continue
        if hits != 0:
            continue
        pit_abbr = teams[side]["abbr"]
        bat_abbr = teams[opp]["abbr"]
        out.append({
            "event_type": "no_hit_bid",
            "dedupe_key": f"no_hit_bid:{game_pk}:{side}:{last_completed}",
            "game_pk": game_pk,
            "game_date": game_date,
            "player_id": None,
            "player_name": None,
            "headline": f"{pit_abbr} combined no-hit bid through {last_completed} vs {bat_abbr}",
            "payload": {
                "through_inning": last_completed,
                "pitching_team_abbr": pit_abbr,
                "batting_team_abbr": bat_abbr,
                "pitching_side": side,
            },
        })
    return out


def detect_k_milestones(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """Pitchers reaching 10/12/14/16/18+ strikeouts in the game."""
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    teams = _teams_meta(feed)
    # Count Ks by pitcher in play order so we know the inning each threshold
    # was reached.
    counter: dict[int, int] = {}
    names: dict[int, str] = {}
    team_side: dict[int, str] = {}
    threshold_hits: dict[tuple[int, int], dict[str, Any]] = {}
    for p in _all_plays(feed):
        result = p.get("result") or {}
        if (result.get("eventType") or "").lower() not in {"strikeout", "strikeout_double_play"}:
            continue
        pit_id, pit_name = _pitcher(p)
        if pit_id is None:
            continue
        counter[pit_id] = counter.get(pit_id, 0) + 1
        names[pit_id] = pit_name or names.get(pit_id, "")
        team_side[pit_id] = _pitching_side_for_play(p)
        k = counter[pit_id]
        if k in K_MILESTONE_STEPS and (pit_id, k) not in threshold_hits:
            threshold_hits[(pit_id, k)] = {
                "inning": _inning(p),
                "half": (p.get("about") or {}).get("halfInning"),
            }
    out: list[dict[str, Any]] = []
    for (pid, k), info in threshold_hits.items():
        side = team_side.get(pid, "")
        abbr = teams.get(side, {}).get("abbr", "")
        out.append({
            "event_type": "k_milestone",
            "dedupe_key": f"k_milestone:{game_pk}:{pid}:{k}",
            "game_pk": game_pk,
            "game_date": game_date,
            "player_id": pid,
            "player_name": names.get(pid),
            "headline": f"{names.get(pid)} hits {k} K — {abbr} (thru {info['inning']})",
            "payload": {
                "pitcher_id": pid,
                "pitcher_name": names.get(pid),
                "k_count": k,
                "through_inning": info["inning"],
                "pit_team_abbr": abbr,
                "game_k_total": counter[pid],
            },
        })
    return out


def detect_cycle_watch(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """Batters with 3 of the 4 hit types (single/double/triple/HR).

    Suppressed once they actually cycle (all 4 present). Key includes which
    three types are present so a double-then-triple-then-HR does not collide
    with single-then-double-then-HR and the hub always shows the latest state.
    """
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    teams = _teams_meta(feed)
    hits_by_batter: dict[int, dict[str, Any]] = {}
    _types_by_event = {"single": "1B", "double": "2B", "triple": "3B", "home_run": "HR"}
    for p in _all_plays(feed):
        result = p.get("result") or {}
        ev = (result.get("eventType") or "").lower()
        t = _types_by_event.get(ev)
        if not t:
            continue
        bid, bname = _batter(p)
        if bid is None:
            continue
        side = _batting_side_for_play(p)
        rec = hits_by_batter.setdefault(bid, {"name": bname, "types": set(), "side": side})
        rec["types"].add(t)
    out: list[dict[str, Any]] = []
    for bid, rec in hits_by_batter.items():
        types: set[str] = rec["types"]
        if len(types) < CYCLE_WATCH_NEEDED:
            continue
        if len(types) == 4:
            # Already cycled — skip the watch (a full cycle is a different
            # event; call it out separately if desired later).
            continue
        sig = "-".join(sorted(types))  # e.g. '1B-3B-HR'
        missing = sorted({"1B", "2B", "3B", "HR"} - types)
        abbr = teams.get(rec["side"], {}).get("abbr", "")
        out.append({
            "event_type": "cycle_watch",
            "dedupe_key": f"cycle_watch:{game_pk}:{bid}:{sig}",
            "game_pk": game_pk,
            "game_date": game_date,
            "player_id": bid,
            "player_name": rec["name"],
            "headline": f"{rec['name']} one {', '.join(missing)} from the cycle ({abbr})",
            "payload": {
                "batter_id": bid,
                "batter_name": rec["name"],
                "types_present": sorted(list(types)),
                "missing": missing,
                "bat_team_abbr": abbr,
            },
        })
    return out


def detect_final(feed: dict[str, Any]) -> list[dict[str, Any]]:
    abstract = _abstract_state(feed)
    if abstract.lower() != "final":
        return []
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    teams = _teams_meta(feed)
    line = _linescore(feed)
    home_runs = ((line.get("teams") or {}).get("home") or {}).get("runs")
    away_runs = ((line.get("teams") or {}).get("away") or {}).get("runs")
    # Winner/loser
    if home_runs is None or away_runs is None:
        return []
    if home_runs > away_runs:
        win_abbr, win_runs = teams["home"]["abbr"], home_runs
        lose_abbr, lose_runs = teams["away"]["abbr"], away_runs
    elif away_runs > home_runs:
        win_abbr, win_runs = teams["away"]["abbr"], away_runs
        lose_abbr, lose_runs = teams["home"]["abbr"], home_runs
    else:
        # tie (unusual — spring training, suspended, etc.)
        win_abbr = lose_abbr = "TIE"
        win_runs, lose_runs = home_runs, away_runs
    return [{
        "event_type": "final",
        "dedupe_key": f"final:{game_pk}",
        "game_pk": game_pk,
        "game_date": game_date,
        "player_id": None,
        "player_name": None,
        "headline": f"FINAL — {win_abbr} {win_runs}, {lose_abbr} {lose_runs}",
        "payload": {
            "home_team_abbr": teams["home"]["abbr"],
            "away_team_abbr": teams["away"]["abbr"],
            "home_runs": home_runs,
            "away_runs": away_runs,
            "winner_abbr": win_abbr,
            "loser_abbr": lose_abbr,
            "winner_runs": win_runs,
            "loser_runs": lose_runs,
            "total_innings": line.get("currentInning"),
        },
    }]


def detect_debut(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """MLB debut detection via `mlbDebutDate` in the boxscore player block.

    This is best-effort: the field only exists when the Stats API tags the
    player, and it should equal today's game date for a debut. We compare to
    `gameData.datetime.officialDate`.
    """
    game_pk = _game_pk(feed)
    game_date = _game_date(feed)
    teams = _teams_meta(feed)
    box = (feed.get("liveData") or {}).get("boxscore") or {}
    out: list[dict[str, Any]] = []
    for side in ("home", "away"):
        side_meta = teams.get(side, {})
        players = ((box.get("teams") or {}).get(side) or {}).get("players") or {}
        for _, pdata in players.items():
            person = pdata.get("person") or {}
            debut = person.get("mlbDebutDate")
            if not debut or debut != game_date:
                continue
            pos = ((pdata.get("position") or {}).get("abbreviation")) or ""
            pid = person.get("id")
            pname = person.get("fullName")
            out.append({
                "event_type": "debut",
                "dedupe_key": f"debut:{game_pk}:{pid}",
                "game_pk": game_pk,
                "game_date": game_date,
                "player_id": pid,
                "player_name": pname,
                "headline": f"{pname} — MLB debut ({side_meta.get('abbr')})",
                "payload": {
                    "player_id": pid,
                    "player_name": pname,
                    "team_abbr": side_meta.get("abbr"),
                    "team_name": side_meta.get("name"),
                    "position": pos,
                },
            })
    return out


# ---------------------------------------------------------------------------
# dispatcher

def run_all(feed: dict[str, Any]) -> list[dict[str, Any]]:
    """Run every detector on a feed payload, returning the combined list.

    Order matters only for human readability (HR listed before multi-HR etc.).
    Dedupe keys guarantee the DB layer can safely insert with UNIQUE constraint.
    """
    hrs = detect_home_runs(feed)
    events: list[dict[str, Any]] = []
    events.extend(hrs)
    events.extend(detect_multi_hr(feed, hrs))
    events.extend(detect_no_hit_bid(feed))
    events.extend(detect_k_milestones(feed))
    events.extend(detect_cycle_watch(feed))
    events.extend(detect_final(feed))
    events.extend(detect_debut(feed))
    return events
