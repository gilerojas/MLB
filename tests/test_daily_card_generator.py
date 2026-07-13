import unittest
from unittest.mock import patch

from jobs.daily_card_generator import api_service_headers, find_player_performances


class DailyCardGeneratorTests(unittest.TestCase):
    def test_selects_requested_player_in_both_roles(self):
        performers = {
            "batters": [(10, "Target", 4.0, False), (20, "Other", 8.0, True)],
            "pitchers": [(10, "Target", 6.0, False)],
        }

        selected = find_player_performances(performers, 10)

        self.assertEqual([row[0] for row in selected["batters"]], [10])
        self.assertEqual([row[0] for row in selected["pitchers"]], [10])

    def test_returns_empty_roles_when_player_did_not_appear(self):
        selected = find_player_performances({"batters": [], "pitchers": []}, 99)
        self.assertEqual(selected, {"batters": [], "pitchers": []})

    def test_service_header_is_forwarded_when_configured(self):
        with patch.dict("os.environ", {"MLBOPS_API_SERVICE_TOKEN": "token-value"}):
            self.assertEqual(
                api_service_headers(), {"x-mlbops-service-token": "token-value"}
            )


if __name__ == "__main__":
    unittest.main()
