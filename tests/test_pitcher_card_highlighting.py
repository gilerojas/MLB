import unittest

import numpy as np
import pandas as pd

from scripts.mallitalytics_daily_card import (
    _count_from_rate,
    _quality_percentile,
    _stabilized_rate,
    group_arsenal,
    process_pitches,
)


BENCHMARK = {
    "p5": 0.05,
    "p20": 0.15,
    "p40": 0.22,
    "p60": 0.30,
    "p80": 0.40,
    "p95": 0.55,
    "mean": 0.26,
}


class PitcherCardHighlightingTests(unittest.TestCase):
    def test_small_sample_rate_is_shrunk_toward_league_average(self):
        graded = _stabilized_rate(
            2,
            4,
            BENCHMARK,
            prior_strength=8,
            fallback_mean=0.25,
        )
        self.assertGreater(graded, 0.26)
        self.assertLess(graded, 0.50)

    def test_lower_allowed_metric_maps_to_higher_pitcher_quality(self):
        low_xwoba = _quality_percentile(0.15, BENCHMARK, higher_is_better=False)
        high_xwoba = _quality_percentile(0.45, BENCHMARK, higher_is_better=False)
        self.assertGreater(low_xwoba, high_xwoba)

    def test_chase_and_bs75_use_opportunity_denominators(self):
        rows = []
        descriptions = [
            "swinging_strike",
            "foul",
            "ball",
            "ball",
            "foul",
            "called_strike",
        ]
        zones = [11, 12, 13, 14, 5, 6]
        bat_speeds = [76.0, 72.0, np.nan, np.nan, np.nan, np.nan]
        pitch_types = ["FC"] * len(descriptions)
        for index, (description, zone, bat_speed, pitch_type) in enumerate(
            zip(descriptions, zones, bat_speeds, pitch_types)
        ):
            rows.append(
                {
                    "pitch_type": pitch_type,
                    "stand": "R",
                    "description": description,
                    "zone": zone,
                    "type": "S" if description in {"swinging_strike", "called_strike"} else "B",
                    "pfx_z": 0.5,
                    "pfx_x": 0.1,
                    "bb_type": None,
                    "launch_speed": np.nan,
                    "estimated_woba_using_speedangle": np.nan,
                    "delta_run_exp": 0.0,
                    "release_speed": 90.0,
                    "release_spin_rate": 2500.0,
                    "release_extension": 6.5,
                    "release_pos_x": -2.0,
                    "release_pos_z": 6.0,
                    "bat_speed": bat_speed,
                    "events": None,
                    "inning": 1,
                    "at_bat_number": index + 1,
                    "pitch_number": 1,
                }
            )

        arsenal = group_arsenal(process_pitches(pd.DataFrame(rows)))
        cutter = arsenal.iloc[0]
        self.assertEqual(int(cutter["out_zone"]), 4)
        self.assertEqual(int(cutter["chase"]), 2)
        self.assertAlmostEqual(float(cutter["chase_pct"]), 0.50)
        self.assertEqual(int(cutter["tracked_swing"]), 2)
        self.assertAlmostEqual(float(cutter["fast_swing_pct"]), 0.50)

    def test_bs75_display_count_ignores_missing_rate(self):
        self.assertEqual(_count_from_rate(np.nan, 3), 0)
        self.assertEqual(_count_from_rate(0.50, np.nan), 0)
        self.assertEqual(_count_from_rate(0.50, 3), 2)


if __name__ == "__main__":
    unittest.main()
