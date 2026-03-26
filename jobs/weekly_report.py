"""
Weekly performance report — runs at 9 AM ET every Monday.

1. Fetches Twitter engagement metrics for all posts from the past 7 days
2. Saves snapshots to twitter_metrics_snapshots table
3. Sends email + WhatsApp summary

Usage:
    python jobs/weekly_report.py
    python jobs/weekly_report.py --dry-run
"""
import argparse
import os
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).parent.parent
load_dotenv(REPO_ROOT / "jobs" / ".env")
sys.path.insert(0, str(REPO_ROOT))

from api.db.database import get_db, log_notification

TWITTER_BEARER = os.getenv("TWITTER_BEARER_TOKEN", "")


def fetch_tweet_metrics(tweet_id: str) -> dict | None:
    """Fetch public metrics for a tweet via Twitter API v2."""
    if not TWITTER_BEARER:
        return None
    try:
        resp = requests.get(
            f"https://api.twitter.com/2/tweets/{tweet_id}",
            params={"tweet.fields": "public_metrics"},
            headers={"Authorization": f"Bearer {TWITTER_BEARER}"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("public_metrics")
    except Exception:
        pass
    return None


def update_metrics_snapshot(item_id: int, metrics: dict):
    today = str(date.today())
    with get_db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO twitter_metrics_snapshots
                (content_queue_id, snapshot_date, likes, retweets, replies, impressions)
            VALUES (?,?,?,?,?,?)
            """,
            (
                item_id,
                today,
                metrics.get("like_count", 0),
                metrics.get("retweet_count", 0),
                metrics.get("reply_count", 0),
                metrics.get("impression_count", 0),
            ),
        )
        conn.execute(
            """
            UPDATE content_queue
            SET twitter_likes = ?, twitter_retweets = ?, twitter_replies = ?, twitter_impressions = ?
            WHERE id = ?
            """,
            (
                metrics.get("like_count", 0),
                metrics.get("retweet_count", 0),
                metrics.get("reply_count", 0),
                metrics.get("impression_count", 0),
                item_id,
            ),
        )


def get_weekly_posts() -> list[dict]:
    cutoff = str(date.today() - timedelta(days=7))
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, content_type, title, tweet_text, twitter_post_id,
                   twitter_likes, twitter_retweets, twitter_replies, twitter_impressions,
                   posted_at
            FROM content_queue
            WHERE status = 'posted' AND posted_at >= ?
            ORDER BY posted_at DESC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_weekly_counts() -> dict:
    cutoff = str(date.today() - timedelta(days=7))
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(CASE WHEN created_at >= ? THEN 1 END) as generated,
                COUNT(CASE WHEN status IN ('approved','posted') AND created_at >= ? THEN 1 END) as approved,
                COUNT(CASE WHEN status = 'posted' AND created_at >= ? THEN 1 END) as posted
            FROM content_queue
            """,
            (cutoff, cutoff, cutoff),
        ).fetchone()
        return dict(row) if row else {"generated": 0, "approved": 0, "posted": 0}


def build_by_type(posts: list[dict]) -> dict:
    groups = defaultdict(list)
    for p in posts:
        groups[p["content_type"]].append(p["twitter_likes"])
    return {
        ct: {
            "count": len(likes),
            "avg_likes": sum(likes) / len(likes) if likes else 0,
        }
        for ct, likes in groups.items()
    }


def send_report_via_hub(
    report_data: dict, dry_run: bool
):
    hub_url = os.getenv("HUB_URL", "http://localhost:3000")
    if dry_run:
        print(f"\n[DRY RUN] Weekly report data:")
        print(f"  Generated: {report_data['generated']}, Posted: {report_data['posted']}")
        print(f"  Top tweet likes: {report_data.get('best_likes', 0)}")
        return

    # Send email via Resend directly (import from hub is not available here,
    # so we call the hub API endpoint or use Resend Python SDK)
    resend_key = os.getenv("RESEND_API_KEY", "")
    resend_from = os.getenv("RESEND_FROM_EMAIL", "onboarding@resend.dev")
    resend_to = os.getenv("RESEND_TO_EMAIL", "")

    if not resend_key or not resend_to:
        print("  Resend not configured — skipping email.")
        return

    # Build minimal HTML report
    start_date = report_data["start_date"]
    end_date = report_data["end_date"]
    generated = report_data["generated"]
    approved = report_data["approved"]
    posted = report_data["posted"]
    best_likes = report_data.get("best_likes", 0)
    best_impressions = report_data.get("best_impressions", 0)
    avg_likes = report_data.get("avg_likes", 0)
    top_tweets = report_data.get("top_tweets", [])

    top_rows = "".join(
        f"<tr><td style='padding:6px'>{i+1}</td>"
        f"<td style='padding:6px;font-size:13px'>{(t.get('tweet_text') or t.get('title') or '—')[:60]}…</td>"
        f"<td style='padding:6px;text-align:center'>{t.get('twitter_likes',0)}</td>"
        f"<td style='padding:6px;text-align:center'>{t.get('twitter_impressions',0):,}</td>"
        "</tr>"
        for i, t in enumerate(top_tweets[:5])
    )

    html = f"""
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
<div style="background:#1a365d;color:#fff;padding:20px;border-radius:8px 8px 0 0">
  <h2 style="margin:0">📊 Mallitalytics Weekly Report</h2>
  <p style="margin:4px 0 0;opacity:.8">{start_date} – {end_date}</p>
</div>
<div style="padding:20px;background:#fff;border:1px solid #e2e8f0">
  <table style="width:100%;font-size:14px;margin-bottom:20px">
    <tr><td style="color:#718096">Cards generated</td><td style="font-weight:bold">{generated}</td></tr>
    <tr><td style="color:#718096">Cards approved</td><td style="font-weight:bold">{approved}</td></tr>
    <tr><td style="color:#718096">Cards posted</td><td style="font-weight:bold;color:#276749">{posted}</td></tr>
    <tr><td style="color:#718096">Best tweet</td><td style="font-weight:bold">{best_likes} likes · {best_impressions:,} impressions</td></tr>
    <tr><td style="color:#718096">Avg likes/tweet</td><td style="font-weight:bold">{avg_likes:.1f}</td></tr>
  </table>
  {"<h3>Top Tweets</h3><table style='width:100%;font-size:13px;border-collapse:collapse'><tr style='background:#f7fafc'><th style='padding:6px;text-align:left'>#</th><th style='padding:6px;text-align:left'>Tweet</th><th style='padding:6px'>Likes</th><th style='padding:6px'>Impressions</th></tr>" + top_rows + "</table>" if top_tweets else ""}
</div>
<div style="padding:12px;background:#f7fafc;text-align:center;font-size:12px;color:#a0aec0;border-radius:0 0 8px 8px">
  Mallitalytics · Your MLB Content Hub
</div>
</body></html>
"""

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={
                "from": resend_from,
                "to": resend_to,
                "subject": f"Mallitalytics Weekly | {start_date} – {end_date} · {posted} posts",
                "html": html,
            },
            timeout=15,
        )
        if resp.status_code in (200, 201):
            email_id = resp.json().get("id")
            print(f"  Email sent (id={email_id})")
            log_notification(
                "weekly_report", "email", resend_to,
                f"Mallitalytics Weekly | {start_date} – {end_date}",
                f"{posted} posts, best {best_likes} likes",
                "sent", email_id,
            )
        else:
            print(f"  Email failed: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"  Email error: {e}")

    # WhatsApp via Twilio
    twilio_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    twilio_token = os.getenv("TWILIO_AUTH_TOKEN", "")
    twilio_from = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
    twilio_to = os.getenv("TWILIO_WHATSAPP_TO", "")

    if twilio_sid and twilio_token and twilio_to:
        msg_body = (
            f"📊 Mallitalytics Weekly\n"
            f"{start_date} – {end_date}\n\n"
            f"Posted: {posted} cards\n"
            f"Best tweet: {best_likes} likes, {best_impressions:,} impressions\n"
            f"Avg: {avg_likes:.1f} likes/tweet\n\n"
            f"Full report sent to email."
        )
        try:
            resp = requests.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Messages.json",
                auth=(twilio_sid, twilio_token),
                data={"From": twilio_from, "To": twilio_to, "Body": msg_body},
                timeout=10,
            )
            if resp.status_code == 201:
                wa_sid = resp.json().get("sid")
                print(f"  WhatsApp sent (sid={wa_sid})")
            else:
                print(f"  WhatsApp failed: {resp.status_code}")
        except Exception as e:
            print(f"  WhatsApp error: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    today = date.today()
    start = today - timedelta(days=7)
    print(f"\nWeekly Report — {start} to {today}")

    posts = get_weekly_posts()
    counts = get_weekly_counts()
    print(f"  Posts this week: {len(posts)}")

    # Fetch updated metrics from Twitter
    print(f"  Fetching Twitter metrics for {len(posts)} tweets…")
    for p in posts:
        if p.get("twitter_post_id") and TWITTER_BEARER:
            metrics = fetch_tweet_metrics(p["twitter_post_id"])
            if metrics:
                update_metrics_snapshot(p["id"], metrics)
                p.update({
                    "twitter_likes": metrics.get("like_count", 0),
                    "twitter_impressions": metrics.get("impression_count", 0),
                })

    posts_sorted = sorted(posts, key=lambda x: x.get("twitter_likes", 0), reverse=True)
    by_type = build_by_type(posts)
    all_likes = [p.get("twitter_likes", 0) for p in posts]
    avg_likes = sum(all_likes) / len(all_likes) if all_likes else 0
    best = posts_sorted[0] if posts_sorted else {}

    report_data = {
        "start_date": str(start),
        "end_date": str(today),
        "generated": counts["generated"],
        "approved": counts["approved"],
        "posted": counts["posted"],
        "top_tweets": posts_sorted[:5],
        "by_type": by_type,
        "best_likes": best.get("twitter_likes", 0),
        "best_impressions": best.get("twitter_impressions", 0),
        "avg_likes": avg_likes,
    }

    send_report_via_hub(report_data, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
