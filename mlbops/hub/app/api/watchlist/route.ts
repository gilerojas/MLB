import { NextResponse } from "next/server";
import { getWatchlist } from "@/lib/db";

export function GET() {
  const players = getWatchlist(false);
  return NextResponse.json({ players });
}
