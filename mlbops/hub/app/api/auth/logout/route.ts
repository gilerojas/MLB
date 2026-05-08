import { NextRequest, NextResponse } from "next/server";
import { auditFromRequest, requireCsrf, SESSION_COOKIE } from "@/lib/security";

export async function POST(req: NextRequest) {
  const csrf = await requireCsrf(req);
  if (csrf instanceof NextResponse) return csrf;
  await auditFromRequest(req, "logout", "success");
  const res = NextResponse.json({ success: true });
  res.cookies.set(SESSION_COOKIE, "", {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production" || process.env.MLBOPS_SECURE_COOKIES === "1",
    path: "/",
    maxAge: 0,
  });
  return res;
}
