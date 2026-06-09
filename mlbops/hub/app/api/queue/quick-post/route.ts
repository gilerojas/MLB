import { NextRequest, NextResponse } from "next/server";
import { insertQuickPost } from "@/lib/db";
import { getTweetMaxCharsFromEnv, truncateTweetTextToCap } from "@/lib/tweetMaxChars";
import { auditFromRequest, rateLimit, requireCsrf } from "@/lib/security";

export async function POST(req: NextRequest) {
  const csrf = await requireCsrf(req);
  if (csrf instanceof NextResponse) return csrf;
  const limited = await rateLimit(req, "quick_post", 40, 60_000);
  if (limited) return limited;

  const body = await req.json().catch(() => ({}));
  const text = typeof body.tweet_text === "string" ? body.tweet_text : "";
  const tweetText = truncateTweetTextToCap(text.trim(), getTweetMaxCharsFromEnv());
  if (!tweetText) {
    return NextResponse.json({ error: "tweet_text required" }, { status: 400 });
  }
  const userAgent = req.headers.get("user-agent") || "";
  const id = await insertQuickPost(tweetText, {
    source: "quick_post",
    source_module: "quick_post",
    content_pillar: "text_only",
    hook_type: "debate_prompt",
    intended_kpi: "replies",
    priority_score: 50,
    campaign: "daily_mlb",
    manual_or_ai: "manual",
    experiment_tag: "",
    creation_mode: "manual",
    ai_assisted: false,
    device: /mobile|iphone|android/i.test(userAgent) ? "mobile" : "desktop",
  });
  await auditFromRequest(req, "quick_post_create", "success", undefined, id);
  return NextResponse.json({ success: true, id });
}
