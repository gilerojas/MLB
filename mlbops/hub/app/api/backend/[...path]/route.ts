import { NextRequest, NextResponse } from "next/server";
import { auditFromRequest, rateLimit, requireCsrf, requireSessionJson } from "@/lib/security";

type RouteContext = { params: Promise<{ path: string[] }> };

const WRITE_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

async function proxyToFastApi(req: NextRequest, context: RouteContext) {
  const session = WRITE_METHODS.has(req.method.toUpperCase())
    ? await requireCsrf(req)
    : await requireSessionJson();
  if (session instanceof NextResponse) return session;

  if (WRITE_METHODS.has(req.method.toUpperCase())) {
    const limited = await rateLimit(req, "backend_write", 80, 60_000);
    if (limited) return limited;
  }

  const params = await context.params;
  const path = params.path.join("/");
  const internalBase =
    process.env.FASTAPI_BASE_URL || process.env.INTERNAL_API_URL || "http://127.0.0.1:8000";
  const target = new URL(`/${path}${req.nextUrl.search}`, internalBase);
  const headers = new Headers(req.headers);
  headers.delete("host");
  headers.delete("cookie");
  headers.delete("x-csrf-token");

  const body =
    req.method === "GET" || req.method === "HEAD" ? undefined : await req.arrayBuffer();

  const upstream = await fetch(target, {
    method: req.method,
    headers,
    body,
    cache: "no-store",
    redirect: "manual",
  });

  if (WRITE_METHODS.has(req.method.toUpperCase())) {
    await auditFromRequest(
      req,
      `backend:${req.method.toUpperCase()}:${path}`,
      upstream.ok ? "success" : "failed",
      { status: upstream.status }
    );
  }

  const responseHeaders = new Headers(upstream.headers);
  responseHeaders.delete("content-encoding");
  responseHeaders.delete("transfer-encoding");
  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: responseHeaders,
  });
}

export const GET = proxyToFastApi;
export const POST = proxyToFastApi;
export const PATCH = proxyToFastApi;
export const PUT = proxyToFastApi;
export const DELETE = proxyToFastApi;
