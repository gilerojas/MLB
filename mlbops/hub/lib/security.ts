import crypto from "crypto";
import net from "net";
import { cookies } from "next/headers";
import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";
import { logAuditEvent } from "@/lib/db";

export const SESSION_COOKIE = "mlbops_session";
const configuredSessionHours = Number(process.env.MLBOPS_SESSION_HOURS || "8");
export const SESSION_TTL_SECONDS =
  Math.max(1, Math.min(24, Number.isFinite(configuredSessionHours) ? configuredSessionHours : 8)) *
  60 *
  60;
const CSRF_HEADER = "x-csrf-token";
const MAX_RATE_BUCKETS = 1_000;

export type SessionPayload = {
  sub: string;
  iat: number;
  exp: number;
  sid: string;
  csrf: string;
};

type RateEntry = {
  count: number;
  resetAt: number;
};

const rateBuckets = new Map<string, RateEntry>();
let rateOperations = 0;

function base64url(input: Buffer | string): string {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function decodeBase64url(input: string): Buffer {
  const pad = "=".repeat((4 - (input.length % 4)) % 4);
  return Buffer.from(input.replace(/-/g, "+").replace(/_/g, "/") + pad, "base64");
}

function getSessionSecret(): string {
  const secret = process.env.MLBOPS_SESSION_SECRET || process.env.SESSION_SECRET || "";
  if (secret.trim().length >= 32) return secret;
  if (process.env.NODE_ENV === "production") {
    throw new Error("Set MLBOPS_SESSION_SECRET to a random value of at least 32 characters.");
  }
  return "dev-only-mlbops-session-secret-change-before-travel";
}

function sign(data: string): string {
  return base64url(crypto.createHmac("sha256", getSessionSecret()).update(data).digest());
}

function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && crypto.timingSafeEqual(ab, bb);
}

export function createSessionToken(): string {
  const now = Math.floor(Date.now() / 1000);
  const payload: SessionPayload = {
    sub: "owner",
    iat: now,
    exp: now + SESSION_TTL_SECONDS,
    sid: crypto.randomBytes(18).toString("hex"),
    csrf: crypto.randomBytes(24).toString("hex"),
  };
  const body = base64url(JSON.stringify(payload));
  return `${body}.${sign(body)}`;
}

export function parseSessionToken(token?: string | null): SessionPayload | null {
  if (!token) return null;
  const [body, sig, extra] = token.split(".");
  if (!body || !sig || extra || !safeEqual(sign(body), sig)) return null;
  try {
    const payload = JSON.parse(decodeBase64url(body).toString("utf8")) as SessionPayload;
    if (!payload.exp || payload.exp < Math.floor(Date.now() / 1000)) return null;
    if (!payload.csrf || !payload.sid) return null;
    return payload;
  } catch {
    return null;
  }
}

export async function getSession(): Promise<SessionPayload | null> {
  const store = await cookies();
  return parseSessionToken(store.get(SESSION_COOKIE)?.value);
}

export function passwordMatches(password: string): boolean {
  const configured = process.env.MLBOPS_APP_PASSWORD || process.env.APP_PASSWORD || "";
  const hash = process.env.MLBOPS_APP_PASSWORD_SHA256 || process.env.APP_PASSWORD_SHA256 || "";
  if (hash.trim()) {
    const actual = crypto.createHash("sha256").update(password).digest("hex");
    return safeEqual(actual, hash.trim().toLowerCase());
  }
  if (!configured.trim()) return process.env.NODE_ENV !== "production" && password === "mlbops-dev";
  return safeEqual(password, configured);
}

export async function requireSessionJson(): Promise<SessionPayload | NextResponse> {
  const session = await getSession();
  if (session) return session;
  return NextResponse.json({ error: "Authentication required" }, { status: 401 });
}

export async function requireCsrf(req: NextRequest): Promise<SessionPayload | NextResponse> {
  const sessionOrResponse = await requireSessionJson();
  if (sessionOrResponse instanceof NextResponse) return sessionOrResponse;
  const session = sessionOrResponse;
  const headerToken = req.headers.get(CSRF_HEADER) || "";
  if (!headerToken || !safeEqual(headerToken, session.csrf)) {
    await auditFromRequest(req, "csrf_rejected", "failed", { detail: "missing_or_invalid" });
    return NextResponse.json({ error: "Invalid CSRF token" }, { status: 403 });
  }
  const originResult = await requireSameOrigin(req);
  if (originResult) return originResult;
  return session;
}

export async function requireSameOrigin(req: NextRequest): Promise<NextResponse | null> {
  const method = req.method.toUpperCase();
  if (method === "GET" || method === "HEAD" || method === "OPTIONS") return null;
  const origin = req.headers.get("origin");
  if (!origin) return null;
  const host = req.headers.get("host");
  try {
    const originUrl = new URL(origin);
    if (originUrl.host === host) return null;
  } catch {
    // handled below
  }
  await auditFromRequest(req, "origin_rejected", "failed", { origin });
  return NextResponse.json({ error: "Invalid request origin" }, { status: 403 });
}

export async function rateLimit(
  req: NextRequest,
  action: string,
  limit: number,
  windowMs: number
): Promise<NextResponse | null> {
  const ip = requestClientAddress(req);
  const key = `${action}:${ip}`;
  const now = Date.now();
  rateOperations += 1;
  if (rateOperations % 100 === 0 || rateBuckets.size >= MAX_RATE_BUCKETS) {
    for (const [bucketKey, entry] of rateBuckets) {
      if (entry.resetAt <= now) rateBuckets.delete(bucketKey);
    }
    while (rateBuckets.size >= MAX_RATE_BUCKETS) {
      const oldest = rateBuckets.keys().next().value as string | undefined;
      if (!oldest) break;
      rateBuckets.delete(oldest);
    }
  }
  const current = rateBuckets.get(key);
  if (!current || current.resetAt <= now) {
    rateBuckets.set(key, { count: 1, resetAt: now + windowMs });
    return null;
  }
  current.count += 1;
  if (current.count <= limit) return null;
  await auditFromRequest(req, "rate_limited", "failed", { action });
  return NextResponse.json({ error: "Rate limit exceeded" }, { status: 429 });
}

function requestClientAddress(req: NextRequest): string {
  if (process.env.MLBOPS_TRUST_PROXY !== "1") return "direct";
  const candidates = [
    req.headers.get("x-forwarded-for")?.split(",")[0]?.trim(),
    req.headers.get("x-real-ip")?.trim(),
  ];
  return candidates.find((candidate) => candidate && net.isIP(candidate)) || "proxy-unknown";
}

export async function auditFromRequest(
  req: NextRequest,
  action: string,
  result: "success" | "failed",
  details?: Record<string, unknown>,
  contentQueueId?: number
): Promise<void> {
  try {
    const session = parseSessionToken(req.cookies.get(SESSION_COOKIE)?.value);
    await logAuditEvent({
      action,
      result,
      content_queue_id: contentQueueId ?? null,
      session_id: session?.sid ?? null,
      source_ip: requestClientAddress(req),
      user_agent: req.headers.get("user-agent") ?? null,
      details_json: details ? JSON.stringify(details) : null,
    });
  } catch (err) {
    console.error("audit log failed", err);
  }
}

export function sessionCookieOptions(req?: NextRequest) {
  const proto = req?.headers.get("x-forwarded-proto") || "";
  const secureCookieOverride = process.env.MLBOPS_SECURE_COOKIES;
  const isSecure =
    secureCookieOverride === "0"
      ? false
      : proto === "https" ||
        process.env.NODE_ENV === "production" ||
        secureCookieOverride === "1";
  return {
    httpOnly: true,
    sameSite: "lax" as const,
    secure: isSecure,
    path: "/",
    maxAge: SESSION_TTL_SECONDS,
  };
}
