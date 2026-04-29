#!/usr/bin/env python3
"""
Games of the day: branded PNG + tweet text for Launch station (POST /cards/games-of-day).

Usage:
  python scripts/games_of_day_board.py --date 2026-04-08
  python scripts/games_of_day_board.py --format all --output-dir outputs
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.probables_board import build_probable_rows_for_date, build_story_tweet
from src.probables_board.render import render_probables_board


def _tweet_max_chars() -> int:
    try:
        cap = int((os.environ.get("MLBOPS_TWEET_MAX_CHARS") or "10000").strip() or "10000")
    except ValueError:
        cap = 10_000
    return max(1, min(250_000, cap))


def main() -> None:
    ap = argparse.ArgumentParser(description="Games of the day board (schedule slate image)")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--format", choices=("tweet", "image", "all"), default="all")
    ap.add_argument("--output-dir", type=Path, default=Path("outputs"))
    ap.add_argument("--output-suffix", default="", help="Unique filename token")
    ap.add_argument("--hashtag", default="#Mallitalytics")
    args = ap.parse_args()

    date_str = args.date or date.today().strftime("%Y-%m-%d")

    rows = build_probable_rows_for_date(date_str)
    stem = f"games_of_day_{date_str.replace('-', '')}"
    if args.output_suffix:
        stem = f"{stem}_{args.output_suffix}"
    out_path = args.output_dir / f"{stem}.png"

    if args.format in ("tweet", "all"):
        cap = _tweet_max_chars()
        tweet = build_story_tweet(
            date_str, rows, cap, hashtag=args.hashtag, headline="slate"
        )
        if args.format == "all":
            print("--- Tweet ---")
        print(tweet)
        print(f"\n({len(tweet)} chars)")

    if args.format in ("image", "all"):
        render_probables_board(
            rows,
            date_str,
            out_path,
            heading_title="GAMES OF DAY",
            heading_subtitle="Probable starters · season W-L and ERA · first pitch Eastern",
        )
        print(f"\nImage: {out_path}")


if __name__ == "__main__":
    main()
