/**
 * PATCH /api/queue/[id]/tweet-text
 * Update the tweet text for a draft item.
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueItem, updateQueueItem } from "@/lib/db";
import {
  getTweetMaxCharsFromEnv,
  truncateTweetTextToCap,
} from "@/lib/tweetMaxChars";

export async function PATCH(
  req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  const params = await context.params;
  const id = parseInt(params.id, 10);
  if (isNaN(id)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const { tweet_text } = await req.json();
  if (typeof tweet_text !== "string") {
    return NextResponse.json({ error: "tweet_text required" }, { status: 400 });
  }

  const item = getQueueItem(id);
  if (!item) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  const cap = getTweetMaxCharsFromEnv();
  updateQueueItem(id, { tweet_text: truncateTweetTextToCap(tweet_text, cap) });
  return NextResponse.json({ success: true });
}
