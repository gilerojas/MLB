import { NextRequest, NextResponse } from "next/server";
import {
  auditFromRequest,
  createSessionToken,
  passwordMatches,
  rateLimit,
  sessionCookieOptions,
  SESSION_COOKIE,
} from "@/lib/security";

export async function POST(req: NextRequest) {
  const limited = await rateLimit(req, "login", 10, 60_000);
  if (limited) return limited;

  let password = "";
  try {
    const body = await req.json();
    password = typeof body.password === "string" ? body.password : "";
  } catch {
    return NextResponse.json({ error: "Invalid login payload" }, { status: 400 });
  }

  if (!passwordMatches(password)) {
    await auditFromRequest(req, "login", "failed");
    return NextResponse.json({ error: "Invalid password" }, { status: 401 });
  }

  const token = createSessionToken();
  const res = NextResponse.json({ success: true });
  res.cookies.set(SESSION_COOKIE, token, sessionCookieOptions(req));
  await auditFromRequest(req, "login", "success");
  return res;
}
