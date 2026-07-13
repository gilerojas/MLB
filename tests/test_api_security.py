import unittest

from mlbops.api.security import service_request_status


class ApiServiceSecurityTests(unittest.TestCase):
    def test_health_and_static_assets_are_public(self):
        self.assertEqual(service_request_status("/health", "", "", production=True), "public")
        self.assertEqual(
            service_request_status("/static/card.png", "", "", production=True),
            "public",
        )

    def test_production_fails_closed_without_strong_token(self):
        self.assertEqual(
            service_request_status("/queue", "", "", production=True),
            "misconfigured",
        )
        self.assertEqual(
            service_request_status("/queue", "short", "short", production=True),
            "misconfigured",
        )

    def test_protected_route_requires_matching_token(self):
        token = "a" * 32
        self.assertEqual(
            service_request_status("/queue", "wrong", token, production=True),
            "unauthorized",
        )
        self.assertEqual(
            service_request_status("/queue", token, token, production=True),
            "authorized",
        )

    def test_development_remains_token_optional(self):
        self.assertEqual(
            service_request_status("/queue", "", "", production=False),
            "development",
        )


if __name__ == "__main__":
    unittest.main()
