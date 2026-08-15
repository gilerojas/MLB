"""Verify the Game Score benchmarks against published reference lines.

Game Score v2 is newly implemented for the MalliScore validation study, so it is
checked against outings whose published scores are widely cited before it is used
to judge MalliScore.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.study.malliscore_validation.game_score import (  # noqa: E402
    add_game_scores,
    game_score_v1,
    game_score_v2,
)


def test_kerry_wood_20k_matches_published_scores() -> None:
    """1998-05-06: 9 IP, 20 K, 1 H, 0 BB, 0 R. GSv1 = 105, GSv2 = 112."""
    assert game_score_v1(outs=27, strikeouts=20, hits=1, earned_runs=0,
                         unearned_runs=0, walks=0) == 105.0
    assert game_score_v2(outs=27, strikeouts=20, walks=0, hits=1,
                         runs=0, home_runs=0) == 112.0


def test_v2_prices_a_home_run_three_times() -> None:
    """A solo HR costs 2 (hit) + 3 (run) + 6 (HR) = 11 points."""
    clean = game_score_v2(outs=18, strikeouts=5, walks=0, hits=0, runs=0, home_runs=0)
    solo_hr = game_score_v2(outs=18, strikeouts=5, walks=0, hits=1, runs=1, home_runs=1)
    assert clean - solo_hr == 11.0


def test_v2_values_every_out_equally() -> None:
    """Unlike v1, v2 has no bonus for innings past the fourth -- every out is 2."""
    per_out = [
        game_score_v2(outs=o, strikeouts=0, walks=0, hits=0, runs=0, home_runs=0)
        for o in range(3, 28, 3)
    ]
    assert all(b - a == 6.0 for a, b in zip(per_out, per_out[1:]))


def test_zero_out_outings_receive_a_game_score() -> None:
    # A starter can record 0.0 IP. Both formulas still have a valid score.
    assert game_score_v1(0, 0, 0, 0, 0, 0) == 50.0
    assert game_score_v2(0, 0, 0, 0, 0, 0) == 40.0


def test_add_game_scores_matches_scalar_functions() -> None:
    df = pd.DataFrame(
        {
            "outs": [27, 18, 9],
            "strikeouts": [20, 6, 2],
            "walks": [0, 2, 3],
            "hits": [1, 5, 8],
            "home_runs": [0, 1, 2],
            "earned_runs": [0, 2, 6],
            "runs": [0, 3, 7],
        }
    )
    out = add_game_scores(df)
    assert out["game_score_v1"].iloc[0] == 105.0
    assert out["game_score_v2"].iloc[0] == 112.0
    for i in range(len(df)):
        r = df.iloc[i]
        assert out["game_score_v2"].iloc[i] == game_score_v2(
            r.outs, r.strikeouts, r.walks, r.hits, r.runs, r.home_runs
        )
    # Unearned runs are derived, and penalized at half the earned-run rate in v1.
    assert list(out["unearned_runs"]) == [0, 1, 1]
    assert not out["gs_v2_runs_imputed"].any()


def test_missing_runs_falls_back_to_earned_and_is_flagged() -> None:
    df = pd.DataFrame(
        {
            "outs": [18],
            "strikeouts": [6],
            "walks": [2],
            "hits": [5],
            "home_runs": [1],
            "earned_runs": [2],
            "runs": [pd.NA],
        }
    )
    out = add_game_scores(df)
    assert bool(out["gs_v2_runs_imputed"].iloc[0])
    assert out["runs_allowed"].iloc[0] == 2
    assert out["unearned_runs"].iloc[0] == 0
