import { NextResponse } from "next/server";
import { getWatchlist } from "@/lib/db";

export async function GET() {
  const players = await getWatchlist(false);
  return NextResponse.json({ players });
}
