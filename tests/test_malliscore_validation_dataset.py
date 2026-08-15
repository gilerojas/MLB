"""Invariants for the MalliScore validation dataset.

These guard the properties the study's conclusions depend on -- above all that the
rolling baselines and next-start links contain no leakage, since either would
manufacture predictive signal in Phase 5.

Skipped when the dataset has not been built; rebuild with
`research/study/malliscore_validation/build_dataset.py`.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "research/study/malliscore_validation/outputs/outings_2024_2026.parquet"
GOLDEN = ROOT / "tests/fixtures/malliscore_v3_golden.json"

pytestmark = pytest.mark.skipif(
    not DATASET.exists(), reason="validation dataset not built"
)


@pytest.fixture(scope="module")
def data() -> pd.DataFrame:
    return pd.read_parquet(DATASET)


def test_seasons_and_counts(data: pd.DataFrame) -> None:
    counts = data.groupby("season").size().to_dict()
    assert counts == {2024: 4749, 2025: 4764, 2026: 3515}


def test_every_row_has_true_earned_runs(data: pd.DataFrame) -> None:
    """Rows are dropped rather than defaulted to 2 ER, unlike malli_score_from_actual."""
    assert data["earned_runs"].notna().all()
    assert data["runs_allowed"].notna().all()
    assert not data["gs_v2_runs_imputed"].any()


def test_scores_match_frozen_v3(data: pd.DataFrame) -> None:
    golden = json.loads(GOLDEN.read_text())
    for case in golden["cases"]:
        row = data[(data["game_pk"] == case["game_pk"]) & (data["pitcher"] == case["pitcher"])]
        assert len(row) == 1, f"expected exactly one row for {case['game_pk']}"
        assert row["malli_score"].iloc[0] == pytest.approx(
            case["expected"]["malli_score"], abs=1e-9
        )


def test_rolling_baselines_exclude_the_current_start(data: pd.DataFrame) -> None:
    """The roll must be over prior starts only -- shift(1) before rolling."""
    df = data.sort_values(["pitcher", "game_date", "game_pk"])
    expected = (
        df.groupby("pitcher", sort=False)["malli_score"]
        .shift(1)
        .groupby(df["pitcher"], sort=False)
        .transform(lambda s: s.rolling(5, min_periods=2).mean())
    )
    assert np.allclose(
        df["malli_score_roll5"].fillna(-999), expected.fillna(-999), equal_nan=True
    )


def test_next_start_link_is_within_season_and_ordered(data: pd.DataFrame) -> None:
    df = data.sort_values(["pitcher", "season", "game_date", "game_pk"])
    expected = df.groupby(["pitcher", "season"], sort=False)["malli_score"].shift(-1)
    assert np.allclose(df["next_malli_score"].fillna(-999), expected.fillna(-999))
    # Exactly one dangling row per pitcher-season: its final start.
    assert int(data["next_malli_score"].isna().sum()) == data.groupby(
        ["pitcher", "season"]
    ).ngroups
    linked = data[data["has_next_start"]]
    assert (linked["next_game_date"] > linked["game_date"]).all()


def test_unearned_run_share_is_plausible(data: pd.DataFrame) -> None:
    """League-wide roughly 7% of runs are unearned; a wild value means bad parsing."""
    share = data["unearned_runs"].sum() / data["runs_allowed"].sum()
    assert 0.03 < share < 0.12


def test_game_score_v2_centers_near_fifty(data: pd.DataFrame) -> None:
    """Tango calibrated v2 so an average start scores about 50."""
    assert 48.0 < data["game_score_v2"].mean() < 54.0


def test_both_normalizations_present_and_close_but_not_identical(data: pd.DataFrame) -> None:
    assert data["malli_score"].corr(data["malli_score_slate"]) > 0.99
    assert data["malli_slate_delta"].abs().max() > 1.0
