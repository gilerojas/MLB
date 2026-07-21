#!/usr/bin/env python3
"""Build season-to-date daily HR exit-velocity and distance leader counts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.hr_tracker.history import build_category_history


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", type=Path, required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--through", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    result = build_category_history(args.warehouse, args.season, args.through)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"Wrote {args.output} ({result['completed_dates_with_hr']} dates)")


if __name__ == "__main__":
    main()
