import unittest

from scripts.hr_derby_winner_pre_post import (
    DerbyWinner,
    GameLine,
    analyze_winner,
    summarize,
)


def game(game_date: str) -> GameLine:
    return GameLine(
        game_date=game_date,
        at_bats=4,
        hits=2,
        doubles=1,
        triples=0,
        home_runs=1,
        walks=1,
        strikeouts=1,
        hit_by_pitch=0,
        sac_flies=0,
        plate_appearances=5,
    )


class DerbyWinnerPrePostTests(unittest.TestCase):
    def test_summarize_recomputes_rate_stats_from_counting_data(self):
        result = summarize([game("2024-07-01")])

        self.assertEqual(result["ops"], 2.1)
        self.assertEqual(result["hr_per_100_pa"], 20.0)
        self.assertEqual(result["k_pct"], 20.0)
        self.assertEqual(result["bb_pct"], 20.0)

    def test_event_alignment_keeps_doubleheader_games_distinct(self):
        winner = DerbyWinner(2024, 1, "Test Hitter", "TST", "2024-07-15")
        games = [game("2024-07-14") for _ in range(40)]
        games.extend(game("2024-07-16") for _ in range(40))

        result = analyze_winner(winner, games)
        positions = [point["relative_game"] for point in result["points"]]

        self.assertEqual(positions, list(range(-40, 0)) + list(range(1, 41)))


if __name__ == "__main__":
    unittest.main()
