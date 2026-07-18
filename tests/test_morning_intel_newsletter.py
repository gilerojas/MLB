import os
import unittest
from io import StringIO
from unittest.mock import patch

from morning_intel.morning_intel import (
    IntelReport,
    generate_editorial_glm,
    parse_mlb_news_rss,
    render_digest_html,
    send_resend_twilio,
)


RSS_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">
  <channel>
    <item>
      <title>Pitchers &amp; hitters open the second half</title>
      <link>https://www.mlb.com/news/second-half</link>
      <pubDate>Thu, 16 Jul 2026 12:00:00 GMT</pubDate>
      <dc:creator>MLB Writer</dc:creator>
      <image href="https://img.mlbstatic.com/example.jpg" />
    </item>
    <item>
      <title>Unsafe source</title>
      <link>https://example.com/not-mlb</link>
    </item>
  </channel>
</rss>"""


class MorningIntelNewsletterTests(unittest.TestCase):
    def test_parse_mlb_news_rss_accepts_only_mlb_articles(self):
        stories = parse_mlb_news_rss(RSS_FIXTURE)

        self.assertEqual(len(stories), 1)
        self.assertEqual(stories[0]["title"], "Pitchers & hitters open the second half")
        self.assertEqual(stories[0]["author"], "MLB Writer")
        self.assertTrue(stories[0]["published_at"].startswith("2026-07-16"))

    def test_parse_mlb_news_rss_handles_invalid_xml(self):
        self.assertEqual(parse_mlb_news_rss("not xml"), [])

    def test_render_digest_html_is_structured_and_escapes_content(self):
        report = IntelReport(anchor_date="2026-07-16", season=2026)
        report.news_stories = parse_mlb_news_rss(RSS_FIXTURE)
        report.yesterday_results = ["No games <scheduled>"]
        report.probables_today = ["NYY (Starter A) @ BOS (Starter B)"]
        report.editorial_brief = "First <paragraph> & detail.\n\nSecond paragraph."
        report.tweet_drafts = ["A useful <draft> & angle"]

        rendered = render_digest_html(report)

        self.assertIn("Mallitalytics", rendered)
        self.assertIn("The leadoff", rendered)
        self.assertIn("Private content notebook", rendered)
        self.assertIn("The read", rendered)
        self.assertIn("First &lt;paragraph&gt; &amp; detail.", rendered)
        self.assertIn("Pitchers &amp; hitters", rendered)
        self.assertIn("No games &lt;scheduled&gt;", rendered)
        self.assertIn("A useful &lt;draft&gt; &amp; angle", rendered)
        self.assertNotIn("<pre", rendered)

    @patch.dict(
        os.environ,
        {
            "GLM_API_KEY": "test-key",
            "GLM_MODEL": "glm-5.2",
            "GLM_BASE_URL": "https://api.z.ai/api/coding/paas/v4",
        },
        clear=True,
    )
    @patch("morning_intel.morning_intel.requests.post")
    def test_generate_editorial_glm_returns_grounded_brief_and_drafts(self, post):
        post.return_value.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": '{"morning_brief":"Watch the verified slate.\\n\\nThe data signals are measured.","tweet_drafts":["Draft one","Draft two"]}'
                    }
                }
            ]
        }

        brief, drafts = generate_editorial_glm('{"anchor_date":"2026-07-18"}', n=2)

        self.assertEqual(brief, "Watch the verified slate.\n\nThe data signals are measured.")
        self.assertEqual(drafts, ["Draft one", "Draft two"])
        post.assert_called_once()
        call = post.call_args
        self.assertEqual(call.args[0], "https://api.z.ai/api/coding/paas/v4/chat/completions")
        self.assertEqual(call.kwargs["json"]["model"], "glm-5.2")
        self.assertEqual(call.kwargs["json"]["max_tokens"], 4096)
        self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer test-key")

    @patch.dict(
        os.environ,
        {"GLM_API_KEY": "test-key"},
        clear=True,
    )
    @patch("morning_intel.morning_intel.requests.post")
    def test_generate_editorial_glm_repairs_invalid_json_once(self, post):
        first = unittest.mock.Mock()
        first.json.return_value = {
            "choices": [{"message": {"content": "Here is an invalid response."}}]
        }
        second = unittest.mock.Mock()
        second.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": '```json\n{"morning_brief":"Repaired brief.","tweet_drafts":["Draft"]}\n```'
                    }
                }
            ]
        }
        post.side_effect = [first, second]

        brief, drafts = generate_editorial_glm("{}", n=1)

        self.assertEqual(brief, "Repaired brief.")
        self.assertEqual(drafts, ["Draft"])
        self.assertEqual(post.call_count, 2)

    @patch.dict(
        os.environ,
        {
            "GMAIL_SMTP_USER": "sender@example.com",
            "GMAIL_APP_PASSWORD": "abcd efgh ijkl mnop",
            "MORNING_INTEL_TO_EMAIL": "reader@example.com",
        },
        clear=True,
    )
    @patch("morning_intel.morning_intel.log_notification")
    @patch("morning_intel.morning_intel.smtplib.SMTP_SSL")
    def test_gmail_fallback_sends_plain_and_html_parts(self, smtp_ssl, log_notification):
        smtp = smtp_ssl.return_value.__enter__.return_value

        send_resend_twilio(
            "Morning Intel | Jul 18",
            "<html><body>Newsletter</body></html>",
            "Newsletter",
            dry=False,
        )

        smtp_ssl.assert_called_once_with("smtp.gmail.com", 465, timeout=25)
        smtp.login.assert_called_once_with("sender@example.com", "abcdefghijklmnop")
        message = smtp.send_message.call_args.args[0]
        self.assertEqual(message["To"], "reader@example.com")
        self.assertTrue(message.is_multipart())
        self.assertEqual(
            {part.get_content_type() for part in message.iter_parts()},
            {"text/plain", "text/html"},
        )
        log_notification.assert_called_once()

    @patch.dict(
        os.environ,
        {
            "GMAIL_SMTP_USER": "sender@example.com",
            "GMAIL_APP_PASSWORD": "abcdefghijklmnop",
            "MORNING_INTEL_TO_EMAIL": "reader@example.com",
        },
        clear=True,
    )
    @patch("morning_intel.morning_intel.log_notification", side_effect=RuntimeError("audit unavailable"))
    @patch("morning_intel.morning_intel.smtplib.SMTP_SSL")
    def test_gmail_delivery_remains_sent_when_audit_log_fails(self, smtp_ssl, _log_notification):
        output = StringIO()

        with patch("sys.stdout", output):
            send_resend_twilio("Morning Intel", "<p>Edition</p>", "Edition", dry=False)

        smtp_ssl.return_value.__enter__.return_value.send_message.assert_called_once()
        self.assertIn("Gmail SMTP ok", output.getvalue())
        self.assertIn("Notification audit warning", output.getvalue())
        self.assertNotIn("Email not sent", output.getvalue())


if __name__ == "__main__":
    unittest.main()
