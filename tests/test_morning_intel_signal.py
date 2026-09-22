"""
Regression tests for the morning intel signal layer.

The window-staleness cases exist because a real failure shipped: opportunity-based
windows have no clock, so a hitter who stopped playing kept his "last 10 BBE"
indefinitely and the identical line published every morning. Giancarlo Stanton's
window sat on 2026-04-21..24 and was still being reported in mid-May, and would
have reported today at 150 days stale. The persistence bonus made it worse, since
a frozen signal repeats daily and collected the largest streak in the system.
"""
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from morning_intel import persistence as _persist  # noqa: E402
from morning_intel import signal_stats as _sig  # noqa: E402


def _frame(dates: list[date]) -> pd.DataFrame:
    return pd.DataFrame({"gd": dates})


class WindowStalenessTests(unittest.TestCase):
    """A window must describe recent form, not a frozen snapshot."""

    def setUp(self):
        from morning_intel import morning_intel as mi
        self.mi = mi
        self.anchor = date(2026, 9, 21)

    def test_recent_window_is_current(self):
        window = _frame([self.anchor - timedelta(days=d) for d in (1, 3, 5)])
        self.assertTrue(self.mi._window_is_current(window, self.anchor, 45))

    def test_stale_window_is_rejected(self):
        # Stanton's actual window: ten batted balls from late April.
        window = _frame([date(2026, 4, 21), date(2026, 4, 23), date(2026, 4, 24)])
        self.assertFalse(self.mi._window_is_current(window, self.anchor, 45))

    def test_window_stretched_across_months_is_rejected(self):
        # Recent enough at the end, but "last 10" spanning half a season is not form.
        window = _frame([date(2026, 5, 1), date(2026, 9, 20)])
        self.assertFalse(self.mi._window_is_current(window, self.anchor, 45))

    def test_empty_window_is_rejected(self):
        self.assertFalse(self.mi._window_is_current(_frame([]), self.anchor, 45))


class PersistenceTests(unittest.TestCase):
    """Repeats only count when the underlying evidence actually moved."""

    def test_unchanged_window_earns_no_streak(self):
        info = {"seen_in": 4, "of_snapshots": 4, "streak": 4, "distinct_windows": 1}
        self.assertEqual(_persist.confirmed_streak(info), 0)
        self.assertEqual(_persist.streak_label(info), "repeat of unchanged window")

    def test_advancing_window_earns_its_streak(self):
        info = {"seen_in": 4, "of_snapshots": 4, "streak": 3, "distinct_windows": 4}
        self.assertEqual(_persist.confirmed_streak(info), 3)
        self.assertEqual(_persist.streak_label(info), "4 days running")

    def test_missing_window_dates_do_not_claim_staleness(self):
        # Snapshots written before window dates existed: unknown, not unchanged.
        info = {"seen_in": 2, "of_snapshots": 7, "streak": 1, "distinct_windows": 0}
        self.assertEqual(_persist.streak_label(info), "seen 2 of last 7")

    def test_history_tracks_distinct_windows(self):
        snaps = [
            (date(2026, 9, 20), {"signal_pool": [
                {"player_id": 1, "role": "batter", "metric": "barrel_pct", "window_end": "2026-04-24"},
            ]}),
            (date(2026, 9, 19), {"signal_pool": [
                {"player_id": 1, "role": "batter", "metric": "barrel_pct", "window_end": "2026-04-24"},
            ]}),
        ]
        history = _persist.build_history(snaps)
        info = history["1:batter:barrel_pct"]
        self.assertEqual(info["streak"], 2)
        self.assertEqual(info["distinct_windows"], 1)
        self.assertEqual(_persist.confirmed_streak(info), 0)


class SignalMathTests(unittest.TestCase):
    """Ranking by surprise, not by raw size."""

    def test_small_sample_rate_swing_is_not_significant(self):
        # Three barrels in ten batted balls against a 5% baseline.
        se = _sig.rate_se_from_pcts(30.0, 10, 5.0, 40)
        self.assertLess(abs(_sig.zed(25.0, se)), 3.0)

    def test_large_sample_velo_drop_dominates(self):
        se = _sig.mean_diff_se(2.1, 280, 2.0, 950, metric="avg_velo_mph")
        self.assertGreater(abs(_sig.zed(-1.4, se)), 5.0)

    def test_one_outing_cannot_carry_a_mix_shift(self):
        """A shift driven by a single start must never be publishable."""
        # 90% in one outing, near zero in the other two, against a 5% baseline.
        one_start = abs(_sig.zed(31.7 - 5.0, _sig.cluster_se([90.0, 2.0, 3.0], 5.0)))
        consistent = abs(_sig.zed(33.3 - 5.0, _sig.cluster_se([33.0, 36.0, 31.0], 5.0)))
        # It may still be worth tracking, but it cannot clear the publishable bar,
        # while the same shift held across every outing clearly does.
        self.assertLess(one_start, 2.5)
        self.assertGreater(consistent, 5.0)

    def test_consistent_mix_shift_survives(self):
        se = _sig.cluster_se([33.0, 36.0, 31.0], 5.0)
        self.assertGreater(abs(_sig.zed(33.3 - 5.0, se)), 3.0)

    def test_fdr_rejects_a_pure_noise_slate(self):
        # 200 tests drawn just under a nominal 2-sigma cutoff.
        keep = _sig.benjamini_hochberg([_sig.two_sided_p(1.9)] * 200, q=0.05)
        self.assertEqual(sum(keep), 0)

    def test_fdr_keeps_a_clear_effect(self):
        p_values = [_sig.two_sided_p(1.0)] * 199 + [_sig.two_sided_p(6.0)]
        self.assertTrue(_sig.benjamini_hochberg(p_values, q=0.05)[-1])

    def test_prior_blend_favors_prior_when_sample_is_thin(self):
        blended = _sig.blend_prior(45.0, 5, 8.0, "barrel_pct")
        self.assertLess(blended, 15.0)

    def test_prior_blend_favors_observation_when_sample_is_large(self):
        blended = _sig.blend_prior(45.0, 800, 8.0, "barrel_pct")
        self.assertGreater(blended, 40.0)


if __name__ == "__main__":
    unittest.main()
