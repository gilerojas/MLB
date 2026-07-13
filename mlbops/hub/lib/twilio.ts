/**
 * WhatsApp notifications via Twilio.
 */
import twilio from "twilio";

function getClient() {
  return twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
}

const FROM = process.env.TWILIO_WHATSAPP_FROM || "whatsapp:+14155238886";
const TO = process.env.TWILIO_WHATSAPP_TO || "";

export async function sendWhatsApp(body: string): Promise<string> {
  const client = getClient();
  const msg = await client.messages.create({ from: FROM, to: TO, body });
  return msg.sid;
}

export async function sendMorningDigestWhatsApp(params: {
  date: string;
  draft_count: number;
  top_players: string[];
  games_today: number;
}): Promise<string> {
  const { date, draft_count, top_players, games_today } = params;
  const hubUrl = process.env.NEXT_PUBLIC_HUB_URL || "http://localhost:3000";

  const playerList = top_players
    .slice(0, 3)
    .map((p) => `  • ${p}`)
    .join("\n");

  const body = [
    `⚾ Mallitalytics Daily — ${date}`,
    ``,
    `Games today: ${games_today}`,
    `Cards ready to review: ${draft_count}`,
    draft_count > 0 ? `\nTop pending:\n${playerList}` : "",
    ``,
    `Review queue: ${hubUrl}/queue`,
  ]
    .filter((l) => l !== undefined)
    .join("\n");

  return sendWhatsApp(body);
}

export async function sendWeeklyReportWhatsApp(params: {
  start_date: string;
  end_date: string;
  posted: number;
  best_likes: number;
  best_impressions: number;
  avg_likes: number;
}): Promise<string> {
  const { start_date, end_date, posted, best_likes, best_impressions, avg_likes } = params;

  const body = [
    `📊 Mallitalytics Weekly`,
    `${start_date} – ${end_date}`,
    ``,
    `Posted: ${posted} cards`,
    `Best tweet: ${best_likes} likes, ${best_impressions.toLocaleString()} impressions`,
    `Avg engagement: ${avg_likes.toFixed(1)} likes/tweet`,
    ``,
    `Full report sent to email.`,
  ].join("\n");

  return sendWhatsApp(body);
}
