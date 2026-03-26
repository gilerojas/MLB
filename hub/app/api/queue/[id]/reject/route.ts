/**
 * POST /api/queue/[id]/reject
 * Marks a draft item as rejected.
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueItem, updateQueueItem } from "@/lib/db";

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

  updateQueueItem(id, {
    status: "rejected",
    reviewed_at: new Date().toISOString(),
  });

  return NextResponse.json({ success: true, id });
}
