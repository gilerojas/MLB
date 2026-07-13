/**
 * Email notifications via Resend.
 * Used in API routes and job scripts (jobs use the Python Resend SDK or HTTP directly).
 */
import { Resend } from "resend";

let _resend: Resend | null = null;

function getResend(): Resend {
  const key = process.env.RESEND_API_KEY;
  if (!key) {
    throw new Error("RESEND_API_KEY is not set");
  }
  if (!_resend) {
    _resend = new Resend(key);
  }
  return _resend;
}

const FROM = process.env.RESEND_FROM_EMAIL || "onboarding@resend.dev";
const TO = process.env.RESEND_TO_EMAIL || "";

export interface DigestCard {
  id: number;
  player_name: string | null;
  content_type: string;
  title: string | null;
  tweet_text: string | null;
  image_url: string | null;
  game_date: string | null;
}

export interface PostedSummary {
  count: number;
  total_likes: number;
  total_retweets: number;
  total_impressions: number;
}

export async function sendMorningDigest(params: {
  date: string;
  cards: DigestCard[];
  games_today: number;
  posted_yesterday: PostedSummary;
}): Promise<string> {
  const { date, cards, games_today, posted_yesterday } = params;
  const hubUrl = process.env.NEXT_PUBLIC_HUB_URL || "http://localhost:3000";

  const cardRows = cards
    .map(
      (c) => `
    <tr style="border-bottom:1px solid #e2e8f0">
      <td style="padding:12px 8px;font-weight:600;color:#1a202c">${c.player_name || c.title || "—"}</td>
      <td style="padding:12px 8px;color:#4a5568;text-transform:capitalize">${c.content_type.replace("_", " ")}</td>
      <td style="padding:12px 8px;color:#718096">${c.game_date || "—"}</td>
      <td style="padding:12px 8px">
        <a href="${hubUrl}/queue" style="background:#2c7a7b;color:#fff;padding:4px 12px;border-radius:4px;text-decoration:none;font-size:13px">Review</a>
      </td>
    </tr>`
    )
    .join("");

  const html = `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"/></head>
<body style="font-family:Inter,system-ui,sans-serif;background:#f7fafc;margin:0;padding:0">
  <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)">

    <!-- Header -->
    <div style="background:#1a365d;padding:24px 32px">
      <h1 style="color:#fff;margin:0;font-size:22px;font-weight:700">⚾ Mallitalytics Daily</h1>
      <p style="color:#90cdf4;margin:4px 0 0;font-size:14px">${date}</p>
    </div>

    <!-- Stats row -->
    <div style="display:flex;background:#ebf8ff;padding:16px 32px;gap:24px">
      <div style="text-align:center">
        <div style="font-size:28px;font-weight:700;color:#2b6cb0">${cards.length}</div>
        <div style="font-size:12px;color:#4a5568;text-transform:uppercase;letter-spacing:0.05em">Cards to Review</div>
      </div>
      <div style="text-align:center">
        <div style="font-size:28px;font-weight:700;color:#276749">${games_today}</div>
        <div style="font-size:12px;color:#4a5568;text-transform:uppercase;letter-spacing:0.05em">Games Today</div>
      </div>
      <div style="text-align:center">
        <div style="font-size:28px;font-weight:700;color:#744210">${posted_yesterday.count}</div>
        <div style="font-size:12px;color:#4a5568;text-transform:uppercase;letter-spacing:0.05em">Posted Yesterday</div>
      </div>
    </div>

    <!-- Cards table -->
    <div style="padding:24px 32px">
      <h2 style="margin:0 0 16px;font-size:16px;color:#2d3748">Cards Ready for Review</h2>
      ${
        cards.length > 0
          ? `<table style="width:100%;border-collapse:collapse;font-size:14px">
               <thead>
                 <tr style="background:#f7fafc">
                   <th style="padding:8px;text-align:left;color:#718096;font-size:12px;text-transform:uppercase">Player</th>
                   <th style="padding:8px;text-align:left;color:#718096;font-size:12px;text-transform:uppercase">Type</th>
                   <th style="padding:8px;text-align:left;color:#718096;font-size:12px;text-transform:uppercase">Date</th>
                   <th style="padding:8px;text-align:left;color:#718096;font-size:12px;text-transform:uppercase">Action</th>
                 </tr>
               </thead>
               <tbody>${cardRows}</tbody>
             </table>`
          : `<p style="color:#718096;font-style:italic">No cards pending today.</p>`
      }
    </div>

    <!-- Yesterday's performance -->
    ${
      posted_yesterday.count > 0
        ? `<div style="padding:0 32px 24px">
             <h2 style="margin:0 0 12px;font-size:16px;color:#2d3748">Yesterday's Posting Performance</h2>
             <div style="background:#f0fff4;border-radius:6px;padding:16px;font-size:14px;color:#276749">
               ${posted_yesterday.count} tweets → ${posted_yesterday.total_likes} likes · ${posted_yesterday.total_retweets} retweets · ${posted_yesterday.total_impressions.toLocaleString()} impressions
             </div>
           </div>`
        : ""
    }

    <!-- CTA -->
    <div style="padding:24px 32px;text-align:center;border-top:1px solid #e2e8f0">
      <a href="${hubUrl}/queue" style="background:#2c7a7b;color:#fff;padding:12px 32px;border-radius:6px;text-decoration:none;font-size:15px;font-weight:600">Open Review Queue →</a>
    </div>

    <div style="padding:16px 32px;text-align:center;font-size:12px;color:#a0aec0">
      Mallitalytics · Your MLB Content Hub
    </div>
  </div>
</body>
</html>`;

  const { data, error } = await getResend().emails.send({
    from: FROM,
    to: TO,
    subject: `Mallitalytics Daily | ${cards.length} cards ready · ${date}`,
    html,
  });

  if (error) throw new Error(`Resend error: ${error.message}`);
  return data!.id;
}

export async function sendWeeklyReport(params: {
  start_date: string;
  end_date: string;
  generated: number;
  approved: number;
  posted: number;
  top_tweets: Array<{
    title: string | null;
    tweet_text: string | null;
    twitter_post_id: string | null;
    twitter_likes: number;
    twitter_retweets: number;
    twitter_impressions: number;
  }>;
  by_type: Record<string, { count: number; avg_likes: number }>;
}): Promise<string> {
  const { start_date, end_date, generated, approved, posted, top_tweets, by_type } = params;
  const approval_rate = generated > 0 ? ((approved / generated) * 100).toFixed(0) : "0";

  const topRows = top_tweets
    .slice(0, 5)
    .map(
      (t, i) => `
    <tr style="border-bottom:1px solid #e2e8f0">
      <td style="padding:10px 8px;color:#718096">${i + 1}</td>
      <td style="padding:10px 8px;font-size:13px">${(t.tweet_text || t.title || "—").slice(0, 60)}…</td>
      <td style="padding:10px 8px;text-align:center;font-weight:600;color:#276749">${t.twitter_likes}</td>
      <td style="padding:10px 8px;text-align:center">${t.twitter_retweets}</td>
      <td style="padding:10px 8px;text-align:center;color:#4a5568">${t.twitter_impressions.toLocaleString()}</td>
    </tr>`
    )
    .join("");

  const typeRows = Object.entries(by_type)
    .map(
      ([type, stats]) => `
    <tr style="border-bottom:1px solid #f7fafc">
      <td style="padding:8px;text-transform:capitalize">${type.replace("_", " ")}</td>
      <td style="padding:8px;text-align:center">${stats.count}</td>
      <td style="padding:8px;text-align:center">${stats.avg_likes.toFixed(1)}</td>
    </tr>`
    )
    .join("");

  const html = `
<!DOCTYPE html>
<html>
<body style="font-family:Inter,system-ui,sans-serif;background:#f7fafc;margin:0;padding:0">
  <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1)">
    <div style="background:#1a365d;padding:24px 32px">
      <h1 style="color:#fff;margin:0;font-size:22px">📊 Mallitalytics Weekly Report</h1>
      <p style="color:#90cdf4;margin:4px 0 0;font-size:14px">${start_date} — ${end_date}</p>
    </div>

    <div style="padding:24px 32px">
      <h2 style="margin:0 0 16px;font-size:16px;color:#2d3748">This Week in Numbers</h2>
      <table style="width:100%;font-size:14px">
        <tr><td style="padding:6px;color:#718096">Cards generated</td><td style="padding:6px;font-weight:600">${generated}</td></tr>
        <tr><td style="padding:6px;color:#718096">Cards approved</td><td style="padding:6px;font-weight:600">${approved}</td></tr>
        <tr><td style="padding:6px;color:#718096">Cards posted</td><td style="padding:6px;font-weight:600;color:#276749">${posted}</td></tr>
        <tr><td style="padding:6px;color:#718096">Approval rate</td><td style="padding:6px;font-weight:600">${approval_rate}%</td></tr>
      </table>
    </div>

    ${
      top_tweets.length > 0
        ? `<div style="padding:0 32px 24px">
             <h2 style="margin:0 0 16px;font-size:16px;color:#2d3748">Top Performing Content</h2>
             <table style="width:100%;border-collapse:collapse;font-size:13px">
               <thead>
                 <tr style="background:#f7fafc">
                   <th style="padding:8px;text-align:left;color:#718096">#</th>
                   <th style="padding:8px;text-align:left;color:#718096">Tweet</th>
                   <th style="padding:8px;text-align:center;color:#718096">Likes</th>
                   <th style="padding:8px;text-align:center;color:#718096">RT</th>
                   <th style="padding:8px;text-align:center;color:#718096">Impressions</th>
                 </tr>
               </thead>
               <tbody>${topRows}</tbody>
             </table>
           </div>`
        : ""
    }

    ${
      Object.keys(by_type).length > 0
        ? `<div style="padding:0 32px 24px">
             <h2 style="margin:0 0 16px;font-size:16px;color:#2d3748">Content Type Breakdown</h2>
             <table style="width:100%;border-collapse:collapse;font-size:13px">
               <thead>
                 <tr style="background:#f7fafc">
                   <th style="padding:8px;text-align:left;color:#718096">Type</th>
                   <th style="padding:8px;text-align:center;color:#718096">Posted</th>
                   <th style="padding:8px;text-align:center;color:#718096">Avg Likes</th>
                 </tr>
               </thead>
               <tbody>${typeRows}</tbody>
             </table>
           </div>`
        : ""
    }

    <div style="padding:16px 32px;text-align:center;font-size:12px;color:#a0aec0;border-top:1px solid #e2e8f0">
      Mallitalytics · Your MLB Content Hub
    </div>
  </div>
</body>
</html>`;

  const { data, error } = await getResend().emails.send({
    from: FROM,
    to: TO,
    subject: `Mallitalytics Weekly | ${start_date} – ${end_date} · ${posted} posts`,
    html,
  });

  if (error) throw new Error(`Resend error: ${error.message}`);
  return data!.id;
}
