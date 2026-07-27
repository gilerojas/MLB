from __future__ import annotations

import importlib
import sys
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pitcher_showdown.fetch import _aggregate, _ip_from_outs
from src.pitcher_showdown.story import build_showdown_tweet


def _pitcher(
    name: str,
    player_id: int,
    team: str,
    season_era: float,
    recent_era: float,
    recent_kbb: float,
) -> dict:
    return {
        "id": player_id,
        "name": name,
        "team": team,
        "season": {
            "record": "7-4",
            "era": season_era,
            "whip": 1.08,
            "k_bb_pct": 20.4,
        },
        "recent": {
            "era": recent_era,
            "whip": 1.02,
            "k_bb_pct": recent_kbb,
            "ip": "18.0",
            "strikeouts": 17,
        },
        "recent_outings": [
            {"outs": 18, "earned_runs": 2},
            {"outs": 15, "earned_runs": 1},
            {"outs": 21, "earned_runs": 0},
        ],
        "rolling_era": [3.4, 2.9, 2.2, recent_era],
    }


def _showdown() -> dict:
    return {
        "date": "2026-07-25",
        "game_time": "6:40 PM ET",
        "away": _pitcher("Shota Imanaga", 684007, "CHC", 3.91, 1.62, 17.6),
        "home": _pitcher("Paul Skenes", 694973, "PIT", 3.43, 2.45, 21.4),
    }


def test_recent_form_uses_outs_for_baseball_innings() -> None:
    rows = [
        {
            "outs": 14,
            "hits": 4,
            "earned_runs": 2,
            "walks": 3,
            "strikeouts": 8,
            "batters_faced": 19,
            "games_started": 1,
            "wins": 0,
            "losses": 1,
        },
        {
            "outs": 15,
            "hits": 7,
            "earned_runs": 1,
            "walks": 1,
            "strikeouts": 5,
            "batters_faced": 23,
            "games_started": 1,
            "wins": 1,
            "losses": 0,
        },
        {
            "outs": 21,
            "hits": 6,
            "earned_runs": 0,
            "walks": 1,
            "strikeouts": 4,
            "batters_faced": 26,
            "games_started": 1,
            "wins": 1,
            "losses": 0,
        },
    ]

    result = _aggregate(rows)

    assert _ip_from_outs(50) == "16.2"
    assert result["ip"] == "16.2"
    assert round(result["era"], 2) == 1.62
    assert round(result["whip"], 2) == 1.32
    assert round(result["k_bb_pct"], 1) == 17.6
    assert result["wins"] == 2
    assert result["losses"] == 1


def test_tweet_is_grounded_and_under_limit() -> None:
    tweet = build_showdown_tweet(_showdown())

    assert "Shota Imanaga vs Paul Skenes" in tweet
    assert "18.0 IP, 17 K, 1.62 ERA" in tweet
    assert "CHC @ PIT · 6:40 PM ET" in tweet
    assert len(tweet) <= 280


def test_profile_uses_only_pregame_logs_for_season_and_recent(monkeypatch) -> None:
    fetcher = importlib.import_module("src.pitcher_showdown.fetch")
    rows = [
        {
            "date": "2026-06-01",
            "games_started": 1,
            "wins": 1,
            "losses": 0,
            "outs": 18,
            "hits": 4,
            "earned_runs": 1,
            "walks": 1,
            "strikeouts": 7,
            "batters_faced": 23,
        },
        {
            "date": "2026-06-05",
            "games_started": 0,
            "wins": 0,
            "losses": 0,
            "outs": 3,
            "hits": 0,
            "earned_runs": 0,
            "walks": 0,
            "strikeouts": 2,
            "batters_faced": 3,
        },
        {
            "date": "2026-06-10",
            "games_started": 1,
            "wins": 0,
            "losses": 1,
            "outs": 15,
            "hits": 6,
            "earned_runs": 3,
            "walks": 2,
            "strikeouts": 5,
            "batters_faced": 22,
        },
        {
            "date": "2026-06-16",
            "games_started": 1,
            "wins": 1,
            "losses": 0,
            "outs": 21,
            "hits": 3,
            "earned_runs": 0,
            "walks": 1,
            "strikeouts": 9,
            "batters_faced": 25,
        },
    ]
    monkeypatch.setattr(fetcher, "_game_log", lambda *_args, **_kwargs: rows)

    profile = fetcher.fetch_pitcher_profile(
        {"id": 1, "name": "Test Pitcher", "team": "TST"},
        season=2026,
        before_date="2026-06-20",
    )

    assert profile["season"]["record"] == "2-1"
    assert profile["season"]["starts"] == 3
    assert profile["season"]["ip"] == "19.0"
    assert len(profile["recent_outings"]) == 3
    assert all(row["games_started"] == 1 for row in profile["recent_outings"])


def test_renderer_keeps_fixed_social_dimensions(tmp_path: Path, monkeypatch) -> None:
    renderer = importlib.import_module("src.pitcher_showdown.render")
    monkeypatch.setattr(renderer, "_headshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(renderer, "_team_logo", lambda *_args, **_kwargs: None)
    output = tmp_path / "showdown.png"

    renderer.render_showdown(_showdown(), output)

    with Image.open(output) as image:
        assert image.size == (1200, 675)
        assert image.mode == "RGB"
