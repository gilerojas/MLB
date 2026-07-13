/**
 * Keep in sync with `MalliOps/api/paths.py` `get_tweet_max_chars()`.
 * Next.js server routes use this; FastAPI uses the env on the API process.
 * Set the same `MALLIOPS_TWEET_MAX_CHARS` in MalliOps/.env if both run together.
 */
export function getTweetMaxCharsFromEnv(): number {
  const raw = process.env.MALLIOPS_TWEET_MAX_CHARS?.trim() || "10000";
  const n = parseInt(raw, 10);
  if (Number.isNaN(n)) return 10_000;
  return Math.max(1, Math.min(250_000, n));
}

/** Match FastAPI `truncate_tweet_text_to_cap` — avoid slicing mid-word when possible. */
export function truncateTweetTextToCap(text: string, cap?: number): string {
  const c = cap ?? getTweetMaxCharsFromEnv();
  if (c <= 0) return "";
  const s = text ?? "";
  if (s.length <= c) return s;
  let chunk = s.slice(0, c);
  if (/\s$/.test(chunk)) return chunk.trimEnd();
  const sp = chunk.lastIndexOf(" ");
  if (sp > 0 && sp >= Math.floor(c * 0.55)) chunk = chunk.slice(0, sp);
  return chunk.replace(/[ ,;:\u2014-]+$/, "").trimEnd();
}
