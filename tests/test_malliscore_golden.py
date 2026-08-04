"""Freeze MalliScore V3.

These tests pin the shipped V3 formula against 50 real starter outings so that
V4 development cannot silently change historical scores. A failure here means
V3 output moved -- either revert the change, or regenerate the fixture
deliberately via `research/study/malliscore_validation/make_golden.py`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.pitching_performances.malli_score import (
    MALLISCORE_VERSION,
    OutingRawMetrics,
    default_league_norms,
    malliscore_v2,
)

GOLDEN = Path(__file__).parent / "fixtures" / "malliscore_v3_golden.json"


@pytest.fixture(scope="module")
def golden() -> dict:
    return json.loads(GOLDEN.read_text())


def test_version_matches_golden_fixture(golden: dict) -> None:
    assert MALLISCORE_VERSION == golden["malliscore_version"]


def test_v3_scores_are_frozen(golden: dict) -> None:
    norms = default_league_norms()
    drifted = []
    for case in golden["cases"]:
        actual = malliscore_v2(OutingRawMetrics(**case["input"]), norms)
        for key, want in case["expected"].items():
            if actual[key] != pytest.approx(want, abs=1e-9):
                drifted.append(
                    f"{case['game_date']} {case['pitcher_name']} ({case['game_pk']}) "
                    f"{key}: expected {want} got {actual[key]}"
                )
    assert not drifted, "V3 output changed:\n" + "\n".join(drifted)


def test_golden_covers_the_score_range(golden: dict) -> None:
    """The fixture is only a useful guard if it spans the distribution."""
    scores = [c["expected"]["malli_score"] for c in golden["cases"]]
    assert len(scores) == golden["n_cases"] == 50
    assert min(scores) <= 0.0, "fixture must retain the pathological exact-zero cases"
    assert max(scores) > 65.0, "fixture must retain elite outings"


def test_zero_score_cases_are_run_prevention_collapse(golden: dict) -> None:
    """Documents the known V3 defect the study is investigating.

    A blow-up start drives WHIP past the z-clamp, pinning run prevention to 0,
    and `harmonic_mean` then forces the final score to exactly 0 regardless of
    how the pitcher performed on every other axis.
    """
    zeros = [c for c in golden["cases"] if c["expected"]["malli_score"] <= 0.0]
    assert zeros, "expected at least one collapsed outing in the fixture"
    for case in zeros:
        assert case["expected"]["run_prevention_score"] == 0.0
        assert case["expected"]["dominance_score"] > 0.0
        assert case["expected"]["core_score"] == 0.0
