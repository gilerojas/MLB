/**
 * POST /api/queue/[id]/approve
 * Uploads the card image to Twitter, posts the tweet, updates the DB.
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueItem, updateQueueItem } from "@/lib/db";
import { postTweet, uploadMedia } from "@/lib/twitter";

export async function POST(
  _req: NextRequest,
  { params }: { params: { id: string } }
) {
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

    return NextResponse.json({ success: true, tweet_id: tweetId, tweet_url: tweetUrl });
  } catch (err) {
    console.error("Tweet post failed:", err);
    updateQueueItem(id, {
      status: "failed",
      error_message: String(err),
      reviewed_at: new Date().toISOString(),
    });
    return NextResponse.json(
      { error: "Tweet post failed", detail: String(err) },
      { status: 502 }
    );
  }
}
