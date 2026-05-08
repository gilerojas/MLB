/**
 * POST /api/queue/[id]/approve
 * Uploads the card image to Twitter, posts the tweet, updates the DB.
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueItem, mergeQueueMeta, updateQueueItem } from "@/lib/db";
import { auditFromRequest, rateLimit, requireCsrf } from "@/lib/security";
import { postTweet, uploadMedia } from "@/lib/twitter";

export async function POST(
  req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  const csrf = await requireCsrf(req);
  if (csrf instanceof NextResponse) return csrf;
  const limited = await rateLimit(req, "queue_post", 12, 60_000);
  if (limited) return limited;

  const params = await context.params;
  const id = parseInt(params.id, 10);
  if (isNaN(id)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const item = getQueueItem(id);
  if (!item) {
    return NextResponse.json({ error: "Queue item not found" }, { status: 404 });
  }
  if (item.status !== "draft") {
    return NextResponse.json(
      { error: `Item is already ${item.status}` },
      { status: 400 }
    );
  }
  if (!item.tweet_text) {
    return NextResponse.json({ error: "No tweet text set" }, { status: 400 });
  }

  try {
    await auditFromRequest(req, "queue_post_attempt", "success", undefined, id);
    let mediaId: string | undefined;

    if (item.image_path) {
      try {
        mediaId = await uploadMedia(item.image_path);
      } catch (uploadErr) {
        console.error("Media upload failed:", uploadErr);
        updateQueueItem(id, {
          status: "failed",
          error_message: String(uploadErr),
          reviewed_at: new Date().toISOString(),
        });
        await auditFromRequest(req, "queue_post_media_upload", "failed", { detail: String(uploadErr) }, id);
        return NextResponse.json(
          { error: "Media upload failed", detail: String(uploadErr) },
          { status: 502 }
        );
      }
    }

    const { id: tweetId, url: tweetUrl } = await postTweet(
      item.tweet_text,
      mediaId
    );

    updateQueueItem(id, {
      status: "posted",
      twitter_post_id: tweetId,
      posted_at: new Date().toISOString(),
      reviewed_at: new Date().toISOString(),
    });
    mergeQueueMeta(id, {
      posted_via: req.headers.get("user-agent")?.toLowerCase().includes("mobile") ? "mobile" : "desktop",
      posted_source: "hub",
    });
    await auditFromRequest(req, "queue_post", "success", { tweet_id: tweetId }, id);

    return NextResponse.json({ success: true, tweet_id: tweetId, tweet_url: tweetUrl });
  } catch (err) {
    console.error("Tweet post failed:", err);
    updateQueueItem(id, {
      status: "failed",
      error_message: String(err),
      reviewed_at: new Date().toISOString(),
    });
    await auditFromRequest(req, "queue_post", "failed", { detail: String(err) }, id);
    return NextResponse.json(
      { error: "Tweet post failed", detail: String(err) },
      { status: 502 }
    );
  }
}
