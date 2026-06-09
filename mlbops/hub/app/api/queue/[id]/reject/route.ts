/**
 * POST /api/queue/[id]/reject
 * Marks a draft item as rejected.
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueItem, updateQueueItem } from "@/lib/db";
import { auditFromRequest, rateLimit, requireCsrf } from "@/lib/security";

export async function POST(
  req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  const csrf = await requireCsrf(req);
  if (csrf instanceof NextResponse) return csrf;
  const limited = await rateLimit(req, "queue_reject", 60, 60_000);
  if (limited) return limited;

  const params = await context.params;
  const id = parseInt(params.id, 10);
  if (isNaN(id)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const item = await getQueueItem(id);
  if (!item) {
    return NextResponse.json({ error: "Queue item not found" }, { status: 404 });
  }
  if (item.status !== "draft") {
    return NextResponse.json(
      { error: `Item is already ${item.status}` },
      { status: 400 }
    );
  }

  await updateQueueItem(id, {
    status: "rejected",
    reviewed_at: new Date().toISOString(),
  });

  await auditFromRequest(req, "queue_reject", "success", undefined, id);
  return NextResponse.json({ success: true, id });
}
