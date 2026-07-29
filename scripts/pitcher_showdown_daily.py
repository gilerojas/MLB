#!/usr/bin/env python3
"""Generate the daily Mallitalytics probable-starter showdown."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pitcher_showdown import build_showdown, build_showdown_tweet, render_showdown


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily starting pitcher showdown")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--away-pitcher-id", type=int)
    parser.add_argument("--home-pitcher-id", type=int)
    parser.add_argument("--format", choices=("tweet", "image", "all", "json"), default="all")
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    parser.add_argument("--output-suffix", default="")
    args = parser.parse_args()

    showdown = build_showdown(
        args.date,
        away_pitcher_id=args.away_pitcher_id,
        home_pitcher_id=args.home_pitcher_id,
    )
    try:
        cap = int(os.getenv("MLBOPS_TWEET_MAX_CHARS", "280"))
    except ValueError:
        cap = 280
    cap = max(1, min(280, cap))
    tweet = build_showdown_tweet(showdown, max_len=cap)

    if args.format == "json":
        print(json.dumps(showdown, indent=2, ensure_ascii=False))
        return
    if args.format in ("tweet", "all"):
        if args.format == "all":
            print("--- Tweet ---")
        print(tweet)
        print(f"\n({len(tweet)} chars)")
    if args.format in ("image", "all"):
        safe_suffix = re.sub(r"[^a-zA-Z0-9_-]+", "_", args.output_suffix).strip("_")
        suffix = f"_{safe_suffix}" if safe_suffix else ""
        out_path = (
            args.output_dir
            / "pitcher_showdown"
            / f"pitcher_showdown_{args.date.replace('-', '')}{suffix}.png"
        )
        render_showdown(showdown, out_path)
        print(f"\nImage: {out_path}")


if __name__ == "__main__":
    main()
