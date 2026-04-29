/**
 * POST /api/notify
 * Manual trigger to send the morning digest email + WhatsApp.
 * Body: { type: "digest" | "weekly" }
 */
import { NextRequest, NextResponse } from "next/server";
import { getQueueCounts, getQueueByStatus, getRecentPosted } from "@/lib/db";
import { sendMorningDigest } from "@/lib/resend";
import { sendMorningDigestWhatsApp } from "@/lib/twilio";

export async function POST(req: NextRequest) {
  const { type = "digest" } = await req.json().catch(() => ({}));

  if (type === "digest") {
    const today = new Date().toLocaleDateString("en-US", {
      weekday: "long",
      year: "numeric",
      month: "long",
      day: "numeric",
    });

    const drafts = getQueueByStatus("draft", 20, 0);
    const yesterday = getRecentPosted(1);

    const posted_summary = {
      count: yesterday.length,
      total_likes: yesterday.reduce((s, i) => s + i.twitter_likes, 0),
      total_retweets: yesterday.reduce((s, i) => s + i.twitter_retweets, 0),
      total_impressions: yesterday.reduce((s, i) => s + i.twitter_impressions, 0),
    };

    // Fetch today's game count from schedule
    let games_today = 0;
    try {
      const schedRes = await fetch(
        `${process.env.FASTAPI_BASE_URL || "http://localhost:8000"}/schedule/today`
      );
      if (schedRes.ok) {
        const sched = await schedRes.json();
        games_today = sched.games?.length || 0;
      }
    } catch {
      // schedule endpoint down — not fatal
    }

    const top_players = drafts
      .slice(0, 3)
      .map((d) => d.player_name || d.title || "Unknown");

    try {
      const emailId = await sendMorningDigest({
        date: today,
        cards: drafts,
        games_today,
        posted_yesterday: posted_summary,
      });

      let whatsappSid: string | null = null;
      try {
        whatsappSid = await sendMorningDigestWhatsApp({
          date: today,
          draft_count: drafts.length,
          top_players,
          games_today,
        });
      } catch (waErr) {
        console.error("WhatsApp digest failed:", waErr);
      }

      return NextResponse.json({
        success: true,
        email_id: emailId,
        whatsapp_sid: whatsappSid,
        drafts_count: drafts.length,
      });
    } catch (err) {
      return NextResponse.json(
        { error: "Notification failed", detail: String(err) },
        { status: 500 }
      );
    }
  }

  return NextResponse.json({ error: "Unknown type" }, { status: 400 });
}
