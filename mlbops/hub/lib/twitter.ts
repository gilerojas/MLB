/**
 * Twitter API v2 client — OAuth 1.0a signing, media upload, tweet posting.
 * Used exclusively in API routes (server-side only).
 */
import crypto from "crypto";
import { execFile } from "child_process";
import fs from "fs";
import OAuth from "oauth-1.0a";
import { promisify } from "util";

const TWITTER_API_BASE = "https://api.twitter.com";
const MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json";
const execFileAsync = promisify(execFile);
const CURL_STATUS_MARKER = "\n__HTTP_STATUS__:";
const OUTPUTS_DIR = process.env.MLBOPS_OUTPUTS_DIR || "/outputs";

function getOAuthClient() {
  return new OAuth({
    consumer: {
      key: process.env.TWITTER_API_KEY!,
      secret: process.env.TWITTER_API_SECRET!,
    },
    signature_method: "HMAC-SHA1",
    hash_function(base_string, key) {
      return crypto
        .createHmac("sha1", key)
        .update(base_string)
        .digest("base64");
    },
  });
}

function getUserToken() {
  return {
    key: process.env.TWITTER_ACCESS_TOKEN!,
    secret: process.env.TWITTER_ACCESS_TOKEN_SECRET!,
  };
}

function assertTwitterOAuthEnv(): void {
  const missing = [
    "TWITTER_API_KEY",
    "TWITTER_API_SECRET",
    "TWITTER_ACCESS_TOKEN",
    "TWITTER_ACCESS_TOKEN_SECRET",
  ].filter((k) => !process.env[k]?.trim());
  if (missing.length) {
    throw new Error(
      `Missing Twitter OAuth env: ${missing.join(", ")}. Set them in MalliOps/.env (sourced by start_hub.sh) or MalliOps/hub/.env.local.`
    );
  }
}

function resolveMediaPath(imagePath: string): string {
  if (fs.existsSync(imagePath)) return imagePath;

  const legacyOutputsPrefix = "/app/outputs/";
  if (imagePath.startsWith(legacyOutputsPrefix)) {
    const candidate = `${OUTPUTS_DIR}/${imagePath.slice(legacyOutputsPrefix.length)}`;
    if (fs.existsSync(candidate)) return candidate;
  }

  throw new Error(`Media file not found: ${imagePath}. Hub container must mount outputs at ${OUTPUTS_DIR}.`);
}

function getAuthHeader(
  method: string,
  url: string,
  params?: Record<string, string>
): string {
  const oauth = getOAuthClient();
  const token = getUserToken();
  const header = oauth.toHeader(
    oauth.authorize({ url, method, data: params }, token)
  );
  return header.Authorization;
}

async function curlRequest(args: string[]): Promise<{ status: number; body: string }> {
  const { stdout, stderr } = await execFileAsync("curl", [
    "-sS",
    ...args,
    "-w",
    `${CURL_STATUS_MARKER}%{http_code}`,
  ], {
    maxBuffer: 20 * 1024 * 1024,
  });
  const markerAt = stdout.lastIndexOf(CURL_STATUS_MARKER);
  if (markerAt < 0) {
    throw new Error(`curl response missing status marker${stderr ? `: ${stderr}` : ""}`);
  }
  const body = stdout.slice(0, markerAt);
  const status = Number(stdout.slice(markerAt + CURL_STATUS_MARKER.length).trim());
  if (!Number.isFinite(status)) {
    throw new Error(`curl response had invalid status${stderr ? `: ${stderr}` : ""}`);
  }
  return { status, body };
}

/** Upload a PNG image and return the media_id_string. */
export async function uploadMedia(imagePath: string): Promise<string> {
  assertTwitterOAuthEnv();

  const mediaPath = resolveMediaPath(imagePath);
  const totalBytes = fs.statSync(mediaPath).size;
  const mimeType = "image/png";
  if (totalBytes > 5 * 1024 * 1024) {
    throw new Error(`Twitter media upload only supports local images up to 5 MB in this environment; got ${totalBytes} bytes.`);
  }

  // Node/Python DNS resolution is not reliable in this local Codex runtime, while
  // system curl resolves normally. Use curl for Twitter I/O from server routes.
  const authHeader = getAuthHeader("POST", MEDIA_UPLOAD_URL);
  const res = await curlRequest([
    "-X",
    "POST",
    MEDIA_UPLOAD_URL,
    "-H",
    `Authorization: ${authHeader}`,
    "-F",
    `media=@${mediaPath};type=${mimeType}`,
    "-F",
    "media_category=tweet_image",
  ]);
  if (res.status < 200 || res.status >= 300) {
    throw new Error(`Twitter media upload failed (${res.status}): ${res.body}`);
  }

  const data = JSON.parse(res.body);
  if (!data.media_id_string) {
    throw new Error(`Twitter media upload response missing media_id_string: ${res.body}`);
  }
  return data.media_id_string;
}

/** Post a tweet with an optional media attachment. Returns the tweet ID. */
export async function postTweet(
  text: string,
  mediaId?: string
): Promise<{ id: string; url: string }> {
  assertTwitterOAuthEnv();

  const url = `${TWITTER_API_BASE}/2/tweets`;
  const body: Record<string, unknown> = { text };
  if (mediaId) {
    body.media = { media_ids: [mediaId] };
  }

  const authHeader = getAuthHeader("POST", url);
  const payload = JSON.stringify(body);
  const res = await curlRequest([
    "-X",
    "POST",
    url,
    "-H",
    `Authorization: ${authHeader}`,
    "-H",
    "Content-Type: application/json",
    "--data-binary",
    payload,
  ]);

  if (res.status < 200 || res.status >= 300) {
    throw new Error(`Twitter post failed (${res.status}): ${res.body}`);
  }

  const data = JSON.parse(res.body);
  const tweetId: string = data.data.id;
  // Derive URL from your own handle — or use generic link
  const tweetUrl = `https://twitter.com/i/web/status/${tweetId}`;
  return { id: tweetId, url: tweetUrl };
}

/** Fetch public metrics for a tweet (likes, retweets, replies, impressions). */
export async function getTweetMetrics(tweetId: string): Promise<{
  like_count: number;
  retweet_count: number;
  reply_count: number;
  impression_count: number;
}> {
  const url = `${TWITTER_API_BASE}/2/tweets/${tweetId}?tweet.fields=public_metrics`;
  const authHeader = `Bearer ${process.env.TWITTER_BEARER_TOKEN}`;

  const res = await fetch(url, { headers: { Authorization: authHeader } });
  if (!res.ok) {
    throw new Error(`Twitter metrics fetch failed: ${await res.text()}`);
  }
  const data = await res.json();
  return data.data.public_metrics;
}
