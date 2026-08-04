"""MalliScore V4 behavior.

V4 exists to fix one specific, demonstrated defect in V3: a blow-up start drove WHIP
past the z-clamp, pinned run prevention to 0, and the harmonic mean then forced the
final score to exactly 0 -- collapsing a 40-point Game Score range onto one number.
These tests pin that fix and the guardrails around it.

See docs/MALLISCORE_V4_DECISION.md and research/study/malliscore_validation/.
"""

from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from src.pitching_performances.malli_score import (
    MALLISCORE_V4_VERSION,
    OutingRawMetrics,
    default_league_norms,
    malliscore_v2,
    malliscore_v4,
    reach_rate_allowed,
    score_outing_dict,
    v4_league_norms,
)


def outing(**overrides) -> OutingRawMetrics:
    base = dict(
        swstr_pct=11.8,
        called_strike_pct=16.2,
        chase_pct=28.5,
        xwoba_allowed=0.321,
        game_whip=1.2,
        earned_runs=2,
        home_runs=1,
        pitches=90,
        outs=18,
        batters_faced=24,
        hits=5,
        walks=2,
        hit_by_pitch=0,
    )
    base.update(overrides)
    return OutingRawMetrics(**base)


def test_version_is_distinct_from_v3() -> None:
    assert MALLISCORE_V4_VERSION == "4.0.0"


def test_requires_batters_faced_rather_than_guessing() -> None:
    """Silently falling back to V3 would return a number on a different scale."""
    with pytest.raises(ValueError, match="batters_faced"):
        malliscore_v4(outing(batters_faced=0))


def test_requires_complete_official_reach_inputs() -> None:
    """A missing official reach component must not masquerade as a zero."""
    with pytest.raises(ValueError, match="official H, BB and HBP"):
        malliscore_v4(outing(hits=None))


def test_blowup_start_no_longer_collapses_to_zero() -> None:
    """The defect V4 exists to fix: 1 out, 4 hits, 3 walks, 5 earned runs."""
    disaster = outing(outs=1, game_whip=21.0, earned_runs=5, home_runs=1,
                      pitches=38, batters_faced=9, hits=4, walks=3)
    assert malliscore_v2(disaster, default_league_norms())["malli_score"] == 0.0
    v4 = malliscore_v4(disaster)
    assert v4["malli_score"] > 0.0
    assert v4["run_prevention_score"] > 0.0


def test_worse_disasters_still_rank_below_less_bad_ones() -> None:
    """V3 rated these identically at 0; V4 must keep them ordered."""
    bad = outing(outs=6, game_whip=6.0, earned_runs=5, home_runs=1,
                 pitches=55, batters_faced=14, hits=7, walks=5)
    worse = outing(outs=6, game_whip=9.0, earned_runs=9, home_runs=3,
                   pitches=60, batters_faced=19, hits=12, walks=6)
    assert malliscore_v2(bad, default_league_norms())["malli_score"] == 0.0
    assert malliscore_v2(worse, default_league_norms())["malli_score"] == 0.0
    assert malliscore_v4(bad)["malli_score"] > malliscore_v4(worse)["malli_score"]


def test_reach_rate_is_bounded_where_whip_is_not() -> None:
    """One out with seven baserunners: WHIP explodes, the per-BF rate does not."""
    assert reach_rate_allowed(hits=4, walks=3, hit_by_pitch=0, batters_faced=9) == pytest.approx(7 / 9)
    whip = (4 + 3) / (1 / 3.0)
    assert whip == pytest.approx(21.0)
    assert math.isnan(reach_rate_allowed(1, 1, 0, 0))


def test_hit_by_pitch_counts_as_a_reach() -> None:
    without_hbp = outing(hits=4, walks=1, hit_by_pitch=0)
    with_hbp = outing(hits=4, walks=1, hit_by_pitch=2)
    assert malliscore_v4(with_hbp)["reach_rate_allowed"] == pytest.approx(7 / 24)
    assert malliscore_v4(with_hbp)["malli_score"] < malliscore_v4(without_hbp)["malli_score"]


def test_actual_outing_adapter_ships_v4_and_retains_v3_comparison() -> None:
    metric = {
        "whiffs": 12,
        "called_strikes": 14,
        "csw": 26,
        "chase_pct": 31.0,
        "xwoba_allowed": 0.285,
    }
    box = SimpleNamespace(
        ip="6.0",
        hits=5,
        walks=2,
        hit_by_pitch=1,
        batters_faced=25,
        er=2,
        home_runs=1,
        strikeouts=7,
    )
    scored = score_outing_dict(metric, box, 92, default_league_norms())
    assert scored["malli_score_version"] == MALLISCORE_V4_VERSION
    assert scored["reach_rate_allowed"] == pytest.approx(8 / 25)
    assert scored["malli_score"] == pytest.approx(
        malliscore_v4(
            outing(
                swstr_pct=12 / 92 * 100,
                called_strike_pct=14 / 92 * 100,
                chase_pct=31.0,
                xwoba_allowed=0.285,
                game_whip=7 / 6,
                earned_runs=2,
                home_runs=1,
                pitches=92,
                outs=18,
                batters_faced=25,
                hits=5,
                walks=2,
                hit_by_pitch=1,
            )
        )["malli_score"]
    )
    assert scored["malli_score_v3"] != scored["malli_score"]


def test_ordinary_outings_stay_close_to_v3() -> None:
    """V4 fixes the tail; it must not relocate the middle of the distribution."""
    for outs, whip, er, bf, hits, walks in [
        (18, 1.0, 2, 24, 5, 1),
        (21, 0.86, 1, 27, 5, 1),
        (15, 1.4, 3, 22, 5, 2),
    ]:
        raw = outing(
            outs=outs,
            game_whip=whip,
            earned_runs=er,
            batters_faced=bf,
            hits=hits,
            walks=walks,
        )
        v3 = malliscore_v2(raw, default_league_norms())["malli_score"]
        v4 = malliscore_v4(raw)["malli_score"]
        assert abs(v4 - v3) < 8.0, f"{outs} outs drifted {v4 - v3:+.1f}"


def test_workload_and_fusion_are_unchanged_from_v3() -> None:
    """The study found weights and the workload curve weakly identified, so V4 keeps them."""
    raw = outing()
    assert malliscore_v4(raw)["workload"] == malliscore_v2(raw, default_league_norms())["workload"]
    v4 = malliscore_v4(raw)
    expected = 2 * v4["dominance_score"] * v4["run_prevention_score"] / (
        v4["dominance_score"] + v4["run_prevention_score"]
    )
    assert v4["core_score"] == pytest.approx(expected)


def test_score_stays_in_range() -> None:
    for whip, er, hr, outs, bf, hits, walks in [
        (0.0, 0, 0, 27, 27, 0, 0),
        (21.0, 9, 4, 1, 12, 7, 0),
        (1.2, 2, 1, 18, 24, 5, 2),
    ]:
        score = malliscore_v4(
            outing(
                game_whip=whip,
                earned_runs=er,
                home_runs=hr,
                outs=outs,
                batters_faced=bf,
                hits=hits,
                walks=walks,
            )
        )["malli_score"]
        assert 0.0 <= score <= 100.0
