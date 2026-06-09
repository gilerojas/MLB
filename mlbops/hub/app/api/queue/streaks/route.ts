import { NextResponse } from "next/server";
import { getPostingStreakStats } from "@/lib/db";
import { requireSessionJson } from "@/lib/security";

export async function GET() {
  const session = await requireSessionJson();
  if (session instanceof NextResponse) return session;
  return NextResponse.json(await getPostingStreakStats());
}
