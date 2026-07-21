from __future__ import annotations

import importlib
import sys
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.hr_tracker_daily import build_story_tweet
from src.hr_tracker.history import category_lead_count, record_caption_lines


DATE = "2026-07-20"


def _history() -> dict:
    return {
        "season": 2026,
        "daily": {
            "20260718": {
                "hardest": [{"player_id": 1}],
                "longest": [{"player_id": 2}],
                "record_flags": [],
            },
            "20260719": {
                "hardest": [{"player_id": 1}],
                "longest": [{"player_id": 3}],
                "record_flags": [],
            },
            "20260720": {
                "hardest": [{"player_id": 1}],
                "longest": [{"player_id": 2}],
                "record_flags": [
                    {
                        "metric": "daily_hr",
                        "status": "new",
                        "value": 53,
                        "previous_high": 52,
                    },
                    {
                        "metric": "distance",
                        "status": "tied",
                        "value": 470,
                        "previous_high": 470,
                    },
                ],
            },
        },
    }


def _home_runs() -> list[dict]:
    return [
        {
            "batter": "Yordan Alvarez",
            "batter_id": 1,
            "team_abbrev": "HOU",
            "pitcher": "Joe Pitcher",
            "ev_mph": 113.8,
            "distance_ft": 419,
            "launch_angle": 27,
        },
        {
            "batter": "Yordan Alvarez",
            "batter_id": 1,
            "team_abbrev": "HOU",
            "pitcher": "Sam Starter",
            "ev_mph": 108.3,
            "distance_ft": 428,
            "launch_angle": 31,
        },
        {
            "batter": "Salvador Perez",
            "batter_id": 2,
            "team_abbrev": "KC",
            "pitcher": "Max Thrower",
            "ev_mph": 105.4,
            "distance_ft": 470,
            "launch_angle": 29,
        },
    ]


def test_record_caption_lines_and_category_counts() -> None:
    history = _history()

    assert category_lead_count(history, DATE, "hardest", 1) == 3
    assert category_lead_count(history, DATE, "longest", 2) == 2
    assert record_caption_lines(history, DATE) == [
        "NEW 2026 MLB DAILY HR HIGH — 53 HOME RUNS",
        "TIES 2026 MLB HR DISTANCE HIGH — 470 FT",
    ]


def test_story_caption_prioritizes_records_and_stays_tweet_sized() -> None:
    caption = build_story_tweet(
        _home_runs(),
        DATE,
        "20 Jul 2026",
        _history(),
        max_len=280,
    )

    assert caption.startswith("NEW 2026 MLB DAILY HR HIGH")
    assert "Yordan Alvarez powered the loudest MLB home run day of 2026." in caption
    assert "Hardest: Yordan Alvarez (HOU), 113.8 mph; 3rd daily EV lead" in caption
    assert len(caption) <= 280


def test_renderer_has_fixed_social_dimensions(tmp_path: Path, monkeypatch) -> None:
    image_gen = importlib.import_module("src.hr_tracker.image_gen")
    monkeypatch.setattr(image_gen, "_fetch_headshot", lambda *_args, **_kwargs: None)
    output = tmp_path / "hr_tracker.png"

    image_gen.render_hr_tracker_image(
        _home_runs(),
        DATE,
        output,
        category_history=_history(),
    )

    with Image.open(output) as image:
        assert image.size == (1200, 675)
        assert image.mode == "RGB"
