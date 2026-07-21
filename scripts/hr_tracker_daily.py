#!/usr/bin/env python3
"""
HR Tracker daily: text, tweet, and optional image from raw feed_live.

Extracts home runs for a date from warehouse raw files (EV, distance, stadium,
pitcher). Outputs plain text, tweet-ready copy (top 5 HR lines + “more on card”; length cap via MLBOPS_TWEET_MAX_CHARS), and/or
Mallitalytics-styled PNG.

Usage:
  python scripts/hr_tracker_daily.py                        # today: warehouse regular_season, else API
  python scripts/hr_tracker_daily.py --date 2026-03-26
  python scripts/hr_tracker_daily.py --yesterday            # yesterday (RS warehouse + API fallback)
  python scripts/hr_tracker_daily.py --live --date 2026-03-26   # HRs from API only (regular season)
  python scripts/hr_tracker_daily.py --warehouse-all-stages --date 2026-02-21  # include ST raw
  python scripts/hr_tracker_daily.py --spring-training --date 2026-03-01
  python scripts/hr_tracker_daily.py --warehouse-only --date 2026-03-26  # no API fallback
  python scripts/hr_tracker_daily.py --wbc                  # WBC (live API)
  python scripts/hr_tracker_daily.py --wbc --date 2026-03-11
  python scripts/hr_tracker_daily.py --format tweet
  python scripts/hr_tracker_daily.py --format image --output-dir outputs
  python scripts/hr_tracker_daily.py --format all --output-dir outputs
"""
import argparse
import sys
from collections import Counter
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Project root for src imports when run as script
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.hr_tracker import (
    build_category_history,
    category_lead_count,
    get_hrs_for_date,
    record_caption_lines,
    render_hr_tracker_image,
)
from src.hr_tracker.extract import extract_hrs_from_feed
from src.hr_tracker.name_display import last_name_compact as _last_name

# MLB / WBC Stats API
MLB_SPORT_ID = 1
SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
# WBC sport ID in the MLB Stats API
WBC_SPORT_ID = 51

# WBC team abbreviation → country flag emoji
COUNTRY_FLAGS: dict[str, str] = {
    "USA": "🇺🇸",
    "DOM": "🇩🇴",
    "DR":  "🇩🇴",
    "JPN": "🇯🇵",
    "MEX": "🇲🇽",
    "VEN": "🇻🇪",
    "PRI": "🇵🇷",
    "PR":  "🇵🇷",
    "CUB": "🇨🇺",
    "PAN": "🇵🇦",
    "COL": "🇨🇴",
    "NED": "🇳🇱",
    "ITA": "🇮🇹",
    "KOR": "🇰🇷",
    "TPE": "🇹🇼",
    "AUS": "🇦🇺",
    "ISR": "🇮🇱",
    "GBR": "🇬🇧",
    "NIC": "🇳🇮",
    "CAN": "🇨🇦",
    "CHN": "🇨🇳",
    "ARG": "🇦🇷",
    "BRA": "🇧🇷",
    "CZE": "🇨🇿",
    "BAH": "🇧🇸",
    "SAF": "🇿🇦",
}


def fetch_mlb_regular_season_hrs_for_date(date_str: str) -> list[dict]:
    """
    Fetch regular-season (gameType R) home runs for a date from the MLB Stats API.
    No local warehouse required — uses schedule + feed/live for each Final game.
    """
    params = {"sportId": MLB_SPORT_ID, "date": date_str}
    try:
        r = requests.get(SCHEDULE_URL, params=params, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        print(f"MLB schedule fetch failed: {exc}", file=sys.stderr)
        return []

    games: list[dict] = []
    for d in r.json().get("dates", []):
        for g in d.get("games", []):
            games.append(g)

    if not games:
        print(f"No MLB games in schedule for {date_str}.")
        return []

    rs_games = [g for g in games if (g.get("gameType") or "").strip().upper() == "R"]
    if not rs_games:
        print(
            f"No regular-season (gameType R) games on {date_str} "
            f"({len(games)} other game(s) — use --spring-training or --warehouse-all-stages for ST warehouse)."
        )
        return []

    print(f"MLB regular season: {len(rs_games)} game(s) on {date_str} (fetching feeds …)")
    all_hrs: list[dict] = []
    for g in rs_games:
        game_pk = g.get("gamePk")
        if not game_pk:
            continue
        if (g.get("status") or {}).get("abstractGameState") != "Final":
            continue
        feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        try:
            rf = requests.get(feed_url, timeout=60)
            rf.raise_for_status()
            feed = rf.json()
        except Exception as exc:
            print(f"  Feed fetch failed for game {game_pk}: {exc}", file=sys.stderr)
            continue
        hrs = extract_hrs_from_feed(feed)
        for hr in hrs:
            hr["stage"] = "regular_season"
            hr["hr_in_stage"] = None
        all_hrs.extend(hrs)

    all_hrs.sort(key=lambda r: (r.get("game_pk") or 0, r.get("inning") or 0))
    return all_hrs


def fetch_wbc_hrs_for_date(date_str: str) -> list[dict]:
    """
    Fetch WBC home runs for a date directly from the MLB Stats API.
    No local warehouse needed — fetches feed/live for each Final game.
    """
    params = {"sportId": WBC_SPORT_ID, "date": date_str}
    try:
        r = requests.get(SCHEDULE_URL, params=params, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        print(f"WBC schedule fetch failed: {exc}", file=sys.stderr)
        return []

    games: list[dict] = []
    for d in r.json().get("dates", []):
        for g in d.get("games", []):
            games.append(g)

    if not games:
        print(f"No WBC games found for {date_str}.")
        return []

    print(f"WBC: {len(games)} game(s) found for {date_str}")
    all_hrs: list[dict] = []
    for g in games:
        game_pk = g.get("gamePk")
        if not game_pk:
            continue
        if (g.get("status") or {}).get("abstractGameState") != "Final":
            continue
        feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        try:
            rf = requests.get(feed_url, timeout=60)
            rf.raise_for_status()
            feed = rf.json()
        except Exception as exc:
            print(f"  Feed fetch failed for game {game_pk}: {exc}", file=sys.stderr)
            continue
        hrs = extract_hrs_from_feed(feed)
        for hr in hrs:
            hr["stage"] = "wbc"
            hr["hr_in_stage"] = None
        all_hrs.extend(hrs)

    all_hrs.sort(key=lambda r: (r.get("game_pk") or 0, r.get("inning") or 0))
    return all_hrs


def _short_venue(venue: str, max_words: int = 2) -> str:
    """First N words of venue name for tweet-friendly length."""
    if not venue:
        return ""
    words = venue.strip().split()
    if len(words) <= max_words:
        return venue.strip()
    return " ".join(words[:max_words])


def _hr_line(r: dict, compact: bool = False, show_flags: bool = False) -> str:
    """One bullet line. If compact: last names, short venue, 'vs' for tweet fit."""
    batter = r.get("batter", "?")
    team = r.get("team_abbrev", "")
    ev = r.get("ev_mph")
    dist = r.get("distance_ft")
    stadium = r.get("stadium", "")
    pitcher = r.get("pitcher", "?")
    hr_in_stage = r.get("hr_in_stage")
    stage = r.get("stage", "")

    stage_abbrev = {"spring_training": "ST", "regular_season": "", "wbc": "WBC"}.get(
        stage, stage[:2].upper() if stage else ""
    )

    if compact:
        batter = _last_name(batter)
        pitcher = _last_name(pitcher)
        stadium = _short_venue(stadium)
        if hr_in_stage is not None:
            if stage_abbrev:
                batter = f"{batter} ({hr_in_stage} {stage_abbrev})"
            else:
                batter = f"{batter} ({hr_in_stage})"

    flag = COUNTRY_FLAGS.get((team or "").upper(), "") if show_flags else ""
    prefix = f"{flag} " if flag else ""

    parts = []
    if team:
        parts.append(f"{prefix}{batter} ({team})")
    else:
        parts.append(f"{prefix}{batter}")
    # hr_in_stage is folded into batter name in compact mode above
    if hr_in_stage is not None and not compact:
        if stage_abbrev:
            parts[-1] = parts[-1] + f" ({hr_in_stage} {stage_abbrev})"
        else:
            parts[-1] = parts[-1] + f" ({hr_in_stage})"

    stat_parts = []
    if ev is not None:
        stat_parts.append(f"{ev:.1f} mph")
    if dist is not None:
        stat_parts.append(f"{int(dist)} ft")
    if stat_parts:
        parts.append(" — " + ", ".join(stat_parts))
    if stadium:
        parts.append(f" @ {stadium}")
    if pitcher:
        parts.append(f" — vs {pitcher}" if compact else f" — off {pitcher}")

    return "• " + "".join(parts)


def _longest_and_top_ev_indexes(hrs: list[dict]) -> tuple[int | None, int | None]:
    """Return (index of longest by distance, index of highest EV). None if no data."""
    idx_longest = None
    idx_top_ev = None
    best_dist = -1
    best_ev = -1.0
    for i, r in enumerate(hrs):
        d = r.get("distance_ft")
        if d is not None and d > best_dist:
            best_dist = d
            idx_longest = i
        ev = r.get("ev_mph")
        if ev is not None and ev > best_ev:
            best_ev = ev
            idx_top_ev = i
    return idx_longest, idx_top_ev


def build_text_block(hrs: list[dict], date_str: str, day_fmt: str, *, compact: bool = True, show_flags: bool = False) -> str:
    """Full text block: header + one line per HR. compact=True for tweet-friendly lines."""
    header = f"HR Tracker — {day_fmt}"
    if not hrs:
        return f"{header}\n\nNo home runs on this date."
    lines = [_hr_line(r, compact=compact, show_flags=show_flags) for r in hrs]
    idx_top_ev = _longest_and_top_ev_indexes(hrs)[1]
    for i in range(len(lines)):
        if i == idx_top_ev:
            lines[i] = "💨 " + lines[i]
    return f"{header}\n\n" + "\n".join(lines)


def build_tweet(
    hrs: list[dict],
    date_str: str,
    day_fmt: str,
    *,
    intro: str | None = None,
    hashtag: str | None = None,
    compact: bool = True,
    show_flags: bool = False,
    max_len: int = 10_000,
    tweet_hr_line_limit: int | None = 5,
) -> str:
    """Tweet-ready text. Default: top ``tweet_hr_line_limit`` HR lines + '(+N more on card)'.
    Image shows the full list. Truncates to ``max_len`` if needed.
    """
    intro_line = intro or f"HR Tracker — {day_fmt}"
    if not hrs:
        body = "No home runs on this date."
        tweet = f"{intro_line}\n\n{body}"
        if hashtag:
            tweet += f"\n{hashtag}"
        return tweet

    intro_line = f"{intro_line} · {len(hrs)} HR"
    lines_raw = [_hr_line(r, compact=compact, show_flags=show_flags) for r in hrs]
    idx_top_ev = _longest_and_top_ev_indexes(hrs)[1]
    limit = len(lines_raw) if tweet_hr_line_limit is None else min(tweet_hr_line_limit, len(lines_raw))
    lines_out: list[str] = []
    for i in range(limit):
        ln = lines_raw[i]
        if i == idx_top_ev:
            ln = "💨 " + ln
        lines_out.append(ln)
    remaining = len(lines_raw) - limit
    if remaining > 0:
        lines_out.append(f"(+{remaining} more on card)")
    block = "\n".join(lines_out)
    tweet = f"{intro_line}\n\n{block}"
    if hashtag:
        tweet += f"\n{hashtag}"

    if len(tweet) <= max_len:
        return tweet

    # Truncate to fit
    reserve = len(intro_line) + 4
    if hashtag:
        reserve += len(hashtag) + 2
    body_max = max_len - reserve - 15  # space for "+N more"

    parts = []
    for ln in lines_out:
        candidate = "\n".join(parts + [ln]) if parts else ln
        if len(candidate) <= body_max:
            parts.append(ln)
        else:
            break
    if not parts:
        parts = [lines_out[0][: body_max - 3] + "…"]
    body = "\n".join(parts)
    rem2 = len(lines_out) - len(parts)
    if rem2 > 0:
        body += f"\n(+{rem2} more)"
    tweet = f"{intro_line}\n\n{body}"
    if hashtag:
        tweet += f"\n{hashtag}"
    return tweet


def _ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _story_caption_lines(
    hrs: list[dict],
    date_str: str,
    day_fmt: str,
    category_history: dict,
) -> list[str]:
    """Build a compact daily storyline for the standardized HR Tracker card."""
    hardest = max(
        (row for row in hrs if row.get("ev_mph") is not None),
        key=lambda row: float(row["ev_mph"]),
        default=hrs[0],
    )
    longest = max(
        (row for row in hrs if row.get("distance_ft") is not None),
        key=lambda row: float(row["distance_ft"]),
        default=hrs[0],
    )
    player_counts = Counter(str(row.get("batter") or "?") for row in hrs)
    team_counts = Counter(str(row.get("team_abbrev") or "?") for row in hrs)
    player_high = max(player_counts.values())
    player_leaders = sorted(name for name, count in player_counts.items() if count == player_high)
    team_high = max(team_counts.values())
    team_leaders = sorted(team for team, count in team_counts.items() if count == team_high)
    weekday = datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
    year = date_str[:4]

    hardest_name = str(hardest.get("batter") or "?")
    hardest_team = str(hardest.get("team_abbrev") or "?")
    hardest_ev = float(hardest.get("ev_mph") or 0)
    longest_name = str(longest.get("batter") or "?")
    longest_team = str(longest.get("team_abbrev") or "?")
    longest_distance = int(float(longest.get("distance_ft") or 0))

    record_lines = record_caption_lines(category_history, date_str)
    has_daily_record = any("DAILY HR HIGH" in line for line in record_lines)
    if has_daily_record:
        hero = player_leaders[0] if len(player_leaders) == 1 else hardest_name
        hook = f"{hero} powered the loudest MLB home run day of {year}."
    elif player_high >= 3 and len(player_leaders) == 1:
        hook = f"{player_leaders[0]} owned {weekday}'s power slate with {player_high} HR."
    elif player_counts[hardest_name] >= 2:
        hook = f"{hardest_name} supplied the loudest swing in a {len(hrs)}-HR {weekday}."
    else:
        hook = f"{len(hrs)} home runs left the yard across MLB on {weekday}."

    hardest_leads = category_lead_count(
        category_history,
        date_str,
        "hardest",
        int(hardest.get("batter_id") or 0),
    )
    longest_leads = category_lead_count(
        category_history,
        date_str,
        "longest",
        int(longest.get("batter_id") or 0),
    )
    hardest_suffix = (
        f"; {_ordinal(hardest_leads)} daily EV lead" if hardest_leads and hardest_leads > 1 else ""
    )
    longest_suffix = (
        f"; {_ordinal(longest_leads)} daily distance lead"
        if longest_leads and longest_leads > 1
        else ""
    )

    if len(team_leaders) == 1:
        club_line = f"-> {team_leaders[0]} led MLB clubs with {team_high} HR"
    else:
        club_line = f"-> {len(team_leaders)} clubs tied for the team high at {team_high} HR"
    hard_count = sum(float(row.get("ev_mph") or 0) >= 105 for row in hrs)
    club_line += f"; {hard_count} HR reached 105+ mph"

    body = [
        hook,
        "",
        f"-> Hardest: {hardest_name} ({hardest_team}), {hardest_ev:.1f} mph{hardest_suffix}",
        f"-> Longest: {longest_name} ({longest_team}), {longest_distance} ft{longest_suffix}",
        club_line,
    ]
    return [*record_lines, "", *body] if record_lines else body


def build_story_tweet(
    hrs: list[dict],
    date_str: str,
    day_fmt: str,
    category_history: dict,
    *,
    intro: str | None = None,
    hashtag: str | None = None,
    max_len: int = 280,
) -> str:
    """Render the daily storyline without cutting a sentence or metric line in half."""
    if not hrs:
        return build_tweet(
            hrs,
            date_str,
            day_fmt,
            intro=intro,
            hashtag=hashtag,
            max_len=max_len,
        )

    lines = _story_caption_lines(hrs, date_str, day_fmt, category_history)
    if intro:
        lines.insert(0, intro)
        lines.insert(1, "")
    if hashtag:
        lines.append(hashtag)

    tweet = "\n".join(lines)
    while len(tweet) > max_len and len(lines) > 1:
        removable = next(
            (
                index
                for index in range(len(lines) - 1, -1, -1)
                if lines[index].startswith("->")
            ),
            None,
        )
        if removable is None:
            break
        del lines[removable]
        while lines and not lines[-1]:
            lines.pop()
        tweet = "\n".join(lines)
    if len(tweet) > max_len:
        tweet = tweet[: max(0, max_len - 1)].rstrip() + "…"
    return tweet


def main() -> None:
    ap = argparse.ArgumentParser(
        description="HR Tracker: daily home runs from warehouse raw → text, tweet, image"
    )
    ap.add_argument(
        "--date",
        default=None,
        help="Date YYYY-MM-DD (default: today; use --yesterday for yesterday)",
    )
    ap.add_argument(
        "--yesterday",
        action="store_true",
        help="Use yesterday's date (shortcut for --date $(date -v-1d +%%Y-%%m-%%d))",
    )
    ap.add_argument(
        "--wbc",
        action="store_true",
        help="Fetch WBC home runs live from MLB Stats API instead of local warehouse",
    )
    ap.add_argument(
        "--live",
        action="store_true",
        help="Fetch MLB regular-season HRs from the API only (ignore warehouse)",
    )
    ap.add_argument(
        "--warehouse-only",
        action="store_true",
        help="Never call MLB API for HRs; warehouse files only (no fallback if empty)",
    )
    ap.add_argument(
        "--no-season-counts",
        action="store_true",
        help="Skip season-to-date HR number lookup; much faster for quick daily queue generation.",
    )
    ap.add_argument(
        "--warehouse-all-stages",
        action="store_true",
        help="Read raw from all stages under each season (spring + regular + playoffs). "
        "Default is regular_season/raw only.",
    )
    ap.add_argument(
        "--spring-training",
        action="store_true",
        help="Only read spring_training/raw in the warehouse (mutually exclusive focus)",
    )
    ap.add_argument(
        "--warehouse",
        type=Path,
        default=Path("data/warehouse/mlb"),
        help="Warehouse root (default: data/warehouse/mlb)",
    )
    ap.add_argument(
        "--format",
        choices=("text", "tweet", "image", "all"),
        default="text",
        help="Output: text, tweet, image, or all",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs"),
        help="Directory for image file (default: outputs)",
    )
    ap.add_argument("--intro", help="Override tweet intro line")
    ap.add_argument("--hashtag", help="Append hashtag to tweet (e.g. #MLB)")
    ap.add_argument(
        "--tweet-all-hr-lines",
        action="store_true",
        help="Include every HR line in tweet text (default: top 5 + '+N more on card')",
    )
    ap.add_argument(
        "--full-names",
        action="store_true",
        help="Use full batter/pitcher names and full venue (longer lines, less tweet-friendly)",
    )
    args = ap.parse_args()

    # Resolve date: --yesterday > --date > today
    if args.yesterday:
        args.date = (_date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif args.date is None:
        args.date = datetime.now().strftime("%Y-%m-%d")

    try:
        day_fmt = datetime.strptime(args.date, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        day_fmt = args.date

    show_flags = args.wbc
    if args.spring_training and args.warehouse_all_stages:
        print("Use only one of --spring-training or --warehouse-all-stages.", file=sys.stderr)
        sys.exit(2)

    if args.wbc:
        print(f"Fetching WBC home runs for {args.date} ...")
        hrs = fetch_wbc_hrs_for_date(args.date)
    elif args.live:
        print(f"Fetching MLB regular-season home runs (API) for {args.date} ...")
        hrs = fetch_mlb_regular_season_hrs_for_date(args.date)
    else:
        if args.spring_training:
            stages = ["spring_training"]
        elif args.warehouse_all_stages:
            stages = None
        else:
            stages = ["regular_season"]
        hrs = get_hrs_for_date(
            args.warehouse,
            args.date,
            stages=stages,
            include_prior_counts=not args.no_season_counts,
        )
        if (
            not hrs
            and not args.warehouse_only
            and not args.spring_training
        ):
            print(
                f"No HRs in warehouse for {args.date}; trying MLB API (regular season Final games) …"
            )
            hrs = fetch_mlb_regular_season_hrs_for_date(args.date)
    # Sort by distance descending (furthest first); no distance → end
    hrs = sorted(hrs, key=lambda r: -(r.get("distance_ft") or 0))

    fmt = args.format
    category_history = None
    if (
        fmt in ("tweet", "image", "all")
        and not args.wbc
        and not args.spring_training
        and args.warehouse.exists()
    ):
        try:
            category_history = build_category_history(
                args.warehouse,
                int(args.date[:4]),
                args.date,
            )
        except Exception as exc:
            print(f"HR category history unavailable: {exc}", file=sys.stderr)

    compact = not args.full_names
    if fmt in ("text", "all"):
        text = build_text_block(hrs, args.date, day_fmt, compact=compact, show_flags=show_flags)
        print(text)
        if fmt == "all":
            print()

    if fmt in ("tweet", "all"):
        import os as _os

        try:
            _cap = int((_os.environ.get("MLBOPS_TWEET_MAX_CHARS") or "10000").strip() or "10000")
        except ValueError:
            _cap = 10_000
        _cap = max(1, min(250_000, _cap))
        if category_history and not args.tweet_all_hr_lines:
            tweet = build_story_tweet(
                hrs,
                args.date,
                day_fmt,
                category_history,
                intro=args.intro,
                hashtag=args.hashtag,
                max_len=min(_cap, 280),
            )
        else:
            tweet = build_tweet(
                hrs,
                args.date,
                day_fmt,
                intro=args.intro,
                hashtag=args.hashtag,
                compact=compact,
                show_flags=show_flags,
                max_len=_cap,
                tweet_hr_line_limit=None if args.tweet_all_hr_lines else 5,
            )
        if fmt == "all":
            print("--- Tweet ---")
        print(tweet)
        print(f"\n({len(tweet)} chars)")

    if fmt in ("image", "all"):
        out_path = args.output_dir / f"hr_tracker_{args.date.replace('-', '')}.png"
        render_hr_tracker_image(
            hrs,
            args.date,
            out_path,
            category_history=category_history,
        )
        print(f"\nImage: {out_path}")

    if fmt == "text" and not hrs:
        pass  # already printed "No home runs..."
    if fmt == "tweet" and not hrs:
        pass


if __name__ == "__main__":
    main()
