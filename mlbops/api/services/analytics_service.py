"""Manual performance analytics for posted Mallitalytics queue items."""
from __future__ import annotations

import sqlite3
from typing import Any

METRIC_FIELDS = (
    "impressions",
    "likes",
    "replies",
    "reposts",
    "quote_tweets",
    "bookmarks",
    "profile_visits",
    "follows",
)

GROUP_FIELDS = {
    "content_pillar": "content_pillar",
    "hook_type": "hook_type",
    "content_type": "content_type",
}

TOP_POST_SORTS = {
    "bookmarks": "bookmarks",
    "replies": "replies",
    "reposts": "reposts",
    "follows": "follows",
    "engagement": "engagement_rate",
}


def _int_metric(value: Any) -> int:
    if value is None:
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def calculate_rates(metrics: dict[str, Any]) -> dict[str, float]:
    impressions = _int_metric(metrics.get("impressions"))
    likes = _int_metric(metrics.get("likes"))
    replies = _int_metric(metrics.get("replies"))
    reposts = _int_metric(metrics.get("reposts"))
    quote_tweets = _int_metric(metrics.get("quote_tweets"))
    bookmarks = _int_metric(metrics.get("bookmarks"))
    follows = _int_metric(metrics.get("follows"))
    engagements = likes + replies + reposts + quote_tweets + bookmarks
    if impressions <= 0:
        return {
            "engagement_rate": 0.0,
            "bookmark_rate": 0.0,
            "reply_rate": 0.0,
            "repost_rate": 0.0,
            "follows_per_1000_impressions": 0.0,
        }
    return {
        "engagement_rate": round(engagements / impressions, 6),
        "bookmark_rate": round(bookmarks / impressions, 6),
        "reply_rate": round(replies / impressions, 6),
        "repost_rate": round(reposts / impressions, 6),
        "follows_per_1000_impressions": round((follows / impressions) * 1000, 3),
    }


def _summary_row_from_mapping(row: dict[str, Any], label_key: str | None = None) -> dict[str, Any]:
    metrics = {field: _int_metric(row.get(field)) for field in METRIC_FIELDS}
    rates = calculate_rates(metrics)
    out = {
        "posts": _int_metric(row.get("posts")),
        **metrics,
        **rates,
        "bookmarks_per_1000_impressions": round(rates["bookmark_rate"] * 1000, 3),
        "replies_per_1000_impressions": round(rates["reply_rate"] * 1000, 3),
        "reposts_per_1000_impressions": round(rates["repost_rate"] * 1000, 3),
    }
    if label_key:
        out[label_key] = row.get(label_key) or "unknown"
    return out


def _empty_summary() -> dict[str, Any]:
    return _summary_row_from_mapping({"posts": 0}, None)


def build_performance_payload(queue_item: dict[str, Any], body: dict[str, Any]) -> dict[str, Any]:
    metrics = {field: _int_metric(body.get(field)) for field in METRIC_FIELDS}
    rates = calculate_rates(metrics)
    return {
        "queue_item_id": int(queue_item["id"]),
        "x_post_id": str(body.get("x_post_id") or queue_item.get("twitter_post_id") or "").strip(),
        "posted_at": body.get("posted_at") or queue_item.get("posted_at"),
        "content_type": queue_item.get("content_type"),
        "content_pillar": queue_item.get("content_pillar"),
        "hook_type": queue_item.get("hook_type"),
        "intended_kpi": queue_item.get("intended_kpi"),
        **metrics,
        **rates,
        "notes": str(body.get("notes") or "").strip(),
    }


def upsert_post_performance(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    columns = [
        "queue_item_id",
        "x_post_id",
        "posted_at",
        "content_type",
        "content_pillar",
        "hook_type",
        "intended_kpi",
        *METRIC_FIELDS,
        "engagement_rate",
        "bookmark_rate",
        "reply_rate",
        "repost_rate",
        "follows_per_1000_impressions",
        "notes",
    ]
    values = {col: payload.get(col) for col in columns}
    update_cols = [col for col in columns if col != "queue_item_id"]
    conn.execute(
        f"""
        INSERT INTO post_performance ({", ".join(columns)})
        VALUES ({", ".join(":" + col for col in columns)})
        ON CONFLICT(queue_item_id) DO UPDATE SET
            {", ".join(f"{col}=excluded.{col}" for col in update_cols)},
            updated_at = datetime('now')
        """,
        values,
    )
    row = conn.execute(
        "SELECT * FROM post_performance WHERE queue_item_id = ?",
        (payload["queue_item_id"],),
    ).fetchone()
    return dict(row)


def get_growth_summary(conn: sqlite3.Connection, days: int = 30) -> dict[str, Any]:
    """Build the v1 Growth dashboard payload from manual performance rows."""
    days = max(1, min(int(days or 30), 365))
    cutoff = f"-{days} days"

    summary_row = conn.execute(
        """
        SELECT
            COUNT(*) AS posts,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(likes), 0) AS likes,
            COALESCE(SUM(replies), 0) AS replies,
            COALESCE(SUM(reposts), 0) AS reposts,
            COALESCE(SUM(quote_tweets), 0) AS quote_tweets,
            COALESCE(SUM(bookmarks), 0) AS bookmarks,
            COALESCE(SUM(profile_visits), 0) AS profile_visits,
            COALESCE(SUM(follows), 0) AS follows
        FROM post_performance
        WHERE date(COALESCE(posted_at, updated_at, created_at)) >= date('now', ?)
        """,
        (cutoff,),
    ).fetchone()
    summary = _summary_row_from_mapping(dict(summary_row) if summary_row else {"posts": 0})

    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload_key, column in GROUP_FIELDS.items():
        rows = conn.execute(
            f"""
            SELECT
                COALESCE({column}, 'unknown') AS {payload_key},
                COUNT(*) AS posts,
                COALESCE(SUM(impressions), 0) AS impressions,
                COALESCE(SUM(likes), 0) AS likes,
                COALESCE(SUM(replies), 0) AS replies,
                COALESCE(SUM(reposts), 0) AS reposts,
                COALESCE(SUM(quote_tweets), 0) AS quote_tweets,
                COALESCE(SUM(bookmarks), 0) AS bookmarks,
                COALESCE(SUM(profile_visits), 0) AS profile_visits,
                COALESCE(SUM(follows), 0) AS follows
            FROM post_performance
            WHERE date(COALESCE(posted_at, updated_at, created_at)) >= date('now', ?)
            GROUP BY COALESCE({column}, 'unknown')
            ORDER BY bookmarks DESC, replies DESC, reposts DESC, posts DESC
            """,
            (cutoff,),
        ).fetchall()
        grouped[payload_key] = [_summary_row_from_mapping(dict(row), payload_key) for row in rows]

    top_posts: dict[str, list[dict[str, Any]]] = {}
    for key, sort_col in TOP_POST_SORTS.items():
        rows = conn.execute(
            f"""
            SELECT
                pp.queue_item_id,
                pp.x_post_id,
                pp.posted_at,
                pp.content_type,
                pp.content_pillar,
                pp.hook_type,
                pp.intended_kpi,
                pp.impressions,
                pp.likes,
                pp.replies,
                pp.reposts,
                pp.quote_tweets,
                pp.bookmarks,
                pp.profile_visits,
                pp.follows,
                pp.engagement_rate,
                pp.bookmark_rate,
                pp.reply_rate,
                pp.repost_rate,
                pp.follows_per_1000_impressions,
                cq.title,
                cq.player_name,
                cq.tweet_text
            FROM post_performance pp
            LEFT JOIN content_queue cq ON cq.id = pp.queue_item_id
            WHERE date(COALESCE(pp.posted_at, pp.updated_at, pp.created_at)) >= date('now', ?)
            ORDER BY pp.{sort_col} DESC, pp.impressions DESC
            LIMIT 5
            """,
            (cutoff,),
        ).fetchall()
        top_posts[key] = [dict(row) for row in rows]

    missing_rows = conn.execute(
        """
        SELECT
            cq.id AS queue_item_id,
            cq.title,
            cq.player_name,
            cq.content_type,
            cq.content_pillar,
            cq.hook_type,
            cq.intended_kpi,
            cq.priority_score,
            cq.posted_at,
            cq.twitter_post_id
        FROM content_queue cq
        LEFT JOIN post_performance pp ON pp.queue_item_id = cq.id
        WHERE cq.status = 'posted'
          AND cq.posted_at IS NOT NULL
          AND date(cq.posted_at) >= date('now', ?)
          AND pp.id IS NULL
        ORDER BY cq.posted_at DESC
        LIMIT 50
        """,
        (cutoff,),
    ).fetchall()
    missing_metrics = [dict(row) for row in missing_rows]

    queue_health_row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'draft' THEN 1 ELSE 0 END) AS queue_drafts,
            SUM(CASE WHEN status = 'posted' THEN 1 ELSE 0 END) AS queue_posted,
            SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS queue_rejected,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS queue_failed
        FROM content_queue
        """
    ).fetchone()
    queue_health = dict(queue_health_row) if queue_health_row else {}
    queue_health = {k: _int_metric(v) for k, v in queue_health.items()}
    total_finished = queue_health.get("queue_posted", 0) + queue_health.get("queue_failed", 0)
    queue_health["failed_post_rate"] = round(queue_health.get("queue_failed", 0) / total_finished, 6) if total_finished else 0.0
    queue_health["posted_without_metrics"] = len(missing_metrics)

    return {
        "days": days,
        "summary": summary,
        "by_pillar": grouped["content_pillar"],
        "by_hook_type": grouped["hook_type"],
        "by_content_type": grouped["content_type"],
        "top_posts": top_posts,
        "missing_metrics": missing_metrics,
        "queue_health": queue_health,
    }
