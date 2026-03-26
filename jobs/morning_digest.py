"""
Morning digest job — runs at 8 AM ET daily.

Sends an email + WhatsApp summarizing:
  - Number of draft cards waiting for review
  - Today's scheduled games
  - Yesterday's posting performance

Usage:
    python jobs/morning_digest.py
    python jobs/morning_digest.py --dry-run     # print but don't send
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).parent.parent
load_dotenv(REPO_ROOT / "jobs" / ".env")
sys.path.insert(0, str(REPO_ROOT))

from api.db.database import get_db, log_notification


def get_draft_cards() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, player_name, title, content_type, game_date, tweet_text, image_url "
            "FROM content_queue WHERE status = 'draft' ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        return [dict(r) for r in rows]


def get_yesterday_posted() -> dict:
    yesterday = str(date.today() - timedelta(days=1))
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) as count,
                   COALESCE(SUM(twitter_likes), 0) as total_likes,
                   COALESCE(SUM(twitter_retweets), 0) as total_retweets,
                   COALESCE(SUM(twitter_impressions), 0) as total_impressions
            FROM content_queue
            WHERE status = 'posted' AND game_date = ?
            """,
            (yesterday,),
        ).fetchone()
        return dict(row) if row else {"count": 0, "total_likes": 0, "total_retweets": 0, "total_impressions": 0}


def get_games_today() -> int:
    try:
        resp = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": str(date.today())},
            timeout=10,
        )
        resp.raise_for_status()
        dates = resp.json().get("dates", [])
        if dates:
            return len(dates[0].get("games", []))
    except Exception:
        pass
    return 0


def send_via_hub_api(drafts: list[dict], games_today: int, posted: dict, dry_run: bool):
    """Delegate to Next.js /api/notify endpoint for email + WhatsApp."""
    hub_url = os.getenv("HUB_URL", "http://localhost:3000")
    if dry_run:
        print(f"\n[DRY RUN] Would POST to {hub_url}/api/notify with:")
        print(f"  Drafts: {len(drafts)}, Games today: {games_today}, Posted yesterday: {posted['count']}")
        return

    try:
        resp = requests.post(
            f"{hub_url}/api/notify",
            json={"type": "digest"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"  Email sent (id={data.get('email_id')})")
            print(f"  WhatsApp sent (sid={data.get('whatsapp_sid')})")
            log_notification(
                "morning_digest", "email",
                os.getenv("RESEND_TO_EMAIL", ""),
                f"Mallitalytics Daily | {len(drafts)} cards",
                f"{len(drafts)} drafts, {games_today} games today",
                "sent", data.get("email_id"),
            )
        else:
            print(f"  Hub API error: {resp.status_code} {resp.text[:200]}")
            log_notification(
                "morning_digest", "email",
                os.getenv("RESEND_TO_EMAIL", ""),
                None, None, "failed", None,
            )
    except Exception as e:
        print(f"  Error calling hub API: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    today_str = date.today().strftime("%A, %B %-d, %Y")
    print(f"\nMorning Digest — {today_str}")

    drafts = get_draft_cards()
    posted = get_yesterday_posted()
    games_today = get_games_today()

    print(f"  Drafts pending: {len(drafts)}")
    print(f"  Games today: {games_today}")
    print(f"  Posted yesterday: {posted['count']}")

    send_via_hub_api(drafts, games_today, posted, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
