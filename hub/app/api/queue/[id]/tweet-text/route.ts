/**
 * PATCH /api/queue/[id]/tweet-text
 * Update the tweet text for a draft item.
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueItem, updateQueueItem } from "@/lib/db";

export async function PATCH(
  req: NextRequest,
  { params }: { params: { id: string } }
) {
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

  updateQueueItem(id, { tweet_text: tweet_text.slice(0, 280) });
  return NextResponse.json({ success: true });
}
