import { NextRequest, NextResponse } from "next/server";

const SESSION_COOKIE = "mlbops_session";

function getSecret(): string {
  const secret = process.env.MLBOPS_SESSION_SECRET || process.env.SESSION_SECRET || "";
  if (secret.trim().length >= 32) return secret;
  return "dev-only-mlbops-session-secret-change-before-travel";
}

function decodeBase64url(input: string): string {
  const pad = "=".repeat((4 - (input.length % 4)) % 4);
  const b64 = input.replace(/-/g, "+").replace(/_/g, "/") + pad;
  return atob(b64);
}

function base64url(bytes: ArrayBuffer): string {
  const arr = Array.from(new Uint8Array(bytes));
  const str = String.fromCharCode(...arr);
  return btoa(str).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function hmac(data: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(getSecret()),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return base64url(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data)));
}

function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

async function hasValidSession(req: NextRequest): Promise<boolean> {
  const raw = req.cookies.get(SESSION_COOKIE)?.value;
  if (!raw) return false;
  const [body, sig, extra] = raw.split(".");
  if (!body || !sig || extra) return false;
  if (!safeEqual(await hmac(body), sig)) return false;
  try {
    const payload = JSON.parse(decodeBase64url(body)) as { exp?: number };
    return Boolean(payload.exp && payload.exp > Math.floor(Date.now() / 1000));
  } catch {
    return false;
  }
}

function isPublicPath(pathname: string): boolean {
  return (
    pathname === "/login" ||
    pathname.startsWith("/api/auth/login") ||
    pathname.startsWith("/_next/") ||
    pathname === "/favicon.ico" ||
    pathname === "/manifest.webmanifest" ||
    pathname.startsWith("/icons/")
  );
}

export async function proxy(req: NextRequest) {
  const { pathname } = req.nextUrl;
  if (isPublicPath(pathname)) return NextResponse.next();
  if (await hasValidSession(req)) {
    if (pathname === "/login") return NextResponse.redirect(new URL("/queue", req.url));
    return NextResponse.next();
  }
  if (pathname.startsWith("/api/")) {
    return NextResponse.json({ error: "Authentication required" }, { status: 401 });
  }
  const url = new URL("/login", req.url);
  url.searchParams.set("next", `${pathname}${req.nextUrl.search}`);
  return NextResponse.redirect(url);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|.*\\.(?:png|jpg|jpeg|gif|svg|ico|css|js|map)$).*)"],
};

