import os
import unittest
from unittest.mock import patch

from morning_intel.morning_intel import (
    IntelReport,
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
        report.tweet_drafts = ["A useful <draft> & angle"]

        rendered = render_digest_html(report)

        self.assertIn("Mallitalytics", rendered)
        self.assertIn("The leadoff", rendered)
        self.assertIn("Private content notebook", rendered)
        self.assertIn("Pitchers &amp; hitters", rendered)
        self.assertIn("No games &lt;scheduled&gt;", rendered)
        self.assertIn("A useful &lt;draft&gt; &amp; angle", rendered)
        self.assertNotIn("<pre", rendered)

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


if __name__ == "__main__":
    unittest.main()
