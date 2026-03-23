#!/usr/bin/env python3
"""
HR Tracker daily: text, tweet, and optional image from raw feed_live.

Extracts home runs for a date from warehouse raw files (EV, distance, stadium,
pitcher). Outputs plain text, tweet-ready copy (280-char aware), and/or
Mallitalytics-styled PNG.

Usage:
  python scripts/hr_tracker_daily.py                        # text for today
  python scripts/hr_tracker_daily.py --date 2026-02-21
  python scripts/hr_tracker_daily.py --yesterday            # text for yesterday (MLB)
  python scripts/hr_tracker_daily.py --wbc                  # WBC HRs yesterday (live from API)
  python scripts/hr_tracker_daily.py --wbc --date 2026-03-11
  python scripts/hr_tracker_daily.py --format tweet
  python scripts/hr_tracker_daily.py --format image --output-dir outputs
  python scripts/hr_tracker_daily.py --format all --output-dir outputs
"""
import argparse
import sys
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Project root for src imports when run as script
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.hr_tracker import get_hrs_for_date, render_hr_tracker_image
from src.hr_tracker.extract import extract_hrs_from_feed

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


def fetch_wbc_hrs_for_date(date_str: str) -> list[dict]:
    """
    Fetch WBC home runs for a date directly from the MLB Stats API.
    No local warehouse needed — fetches feed/live for each Final game.
    """
    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {"sportId": WBC_SPORT_ID, "date": date_str}
    try:
        r = requests.get(url, params=params, timeout=30)
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


def _last_name(full_name: str) -> str:
    """Last word of full name, or full if single word."""
    if not full_name:
        return "?"
    parts = full_name.strip().split()
    return parts[-1] if len(parts) > 1 else full_name


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

    if compact:
        batter = _last_name(batter)
        pitcher = _last_name(pitcher)
        stadium = _short_venue(stadium)

    # Stage abbrev for display (e.g. ST = Spring Training)
    stage_abbrev = {"spring_training": "ST", "regular_season": "RS", "wbc": "WBC"}.get(
        stage, stage[:2].upper() if stage else ""
    )

    flag = COUNTRY_FLAGS.get((team or "").upper(), "") if show_flags else ""
    prefix = f"{flag} " if flag else ""

    parts = []
    if team:
        parts.append(f"{prefix}{batter} ({team})")
    else:
        parts.append(f"{prefix}{batter}")
    if hr_in_stage is not None and stage_abbrev:
        parts.append(f" ({hr_in_stage} {stage_abbrev})")
    elif hr_in_stage is not None:
        parts.append(f" ({hr_in_stage})")

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
    max_len: int = 280,
) -> str:
    """Tweet-ready text; truncate with '+N more' if over max_len."""
    intro_line = intro or f"HR Tracker — {day_fmt}"
    if not hrs:
        body = "No home runs on this date."
        tweet = f"{intro_line}\n\n{body}"
        if hashtag:
            tweet += f"\n{hashtag}"
        return tweet

    lines = [_hr_line(r, compact=compact, show_flags=show_flags) for r in hrs]
    idx_top_ev = _longest_and_top_ev_indexes(hrs)[1]
    for i in range(len(lines)):
        if i == idx_top_ev:
            lines[i] = "💨 " + lines[i]
    block = "\n".join(lines)
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
    for ln in lines:
        candidate = "\n".join(parts + [ln]) if parts else ln
        if len(candidate) <= body_max:
            parts.append(ln)
        else:
            break
    if not parts:
        parts = [lines[0][: body_max - 3] + "…"]
    body = "\n".join(parts)
    remaining = len(lines) - len(parts)
    if remaining > 0:
        body += f"\n(+{remaining} more)"
    tweet = f"{intro_line}\n\n{body}"
    if hashtag:
        tweet += f"\n{hashtag}"
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
    if args.wbc:
        print(f"Fetching WBC home runs for {args.date} ...")
        hrs = fetch_wbc_hrs_for_date(args.date)
    else:
        hrs = get_hrs_for_date(args.warehouse, args.date)
    # Sort by distance descending (furthest first); no distance → end
    hrs = sorted(hrs, key=lambda r: -(r.get("distance_ft") or 0))

    fmt = args.format
    compact = not args.full_names
    if fmt in ("text", "all"):
        text = build_text_block(hrs, args.date, day_fmt, compact=compact, show_flags=show_flags)
        print(text)
        if fmt == "all":
            print()

    if fmt in ("tweet", "all"):
        tweet = build_tweet(
            hrs,
            args.date,
            day_fmt,
            intro=args.intro,
            hashtag=args.hashtag,
            compact=compact,
            show_flags=show_flags,
        )
        if fmt == "all":
            print("--- Tweet ---")
        print(tweet)
        print(f"\n({len(tweet)} chars)")

    if fmt in ("image", "all"):
        out_path = args.output_dir / f"hr_tracker_{args.date.replace('-', '')}.png"
        render_hr_tracker_image(hrs, args.date, out_path)
        print(f"\nImage: {out_path}")

    if fmt == "text" and not hrs:
        pass  # already printed "No home runs..."
    if fmt == "tweet" and not hrs:
        pass


if __name__ == "__main__":
    main()
