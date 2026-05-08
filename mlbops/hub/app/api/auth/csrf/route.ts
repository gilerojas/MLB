import { NextResponse } from "next/server";
import { getSession } from "@/lib/security";

export async function GET() {
  const session = await getSession();
  if (!session) {
    return NextResponse.json({ error: "Authentication required" }, { status: 401 });
  }
  return NextResponse.json({ csrfToken: session.csrf });
}

