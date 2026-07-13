/**
 * FastAPI base URL and JSON fetch helpers for server + client components.
 *
 * Browser: if you open the hub at http://127.0.0.1:3000 but the API URL defaults to
 * http://localhost:8000, some environments mis-resolve or treat that as a different
 * site — use the same hostname as the page unless NEXT_PUBLIC_FASTAPI_URL is set.
 * Server (RSC): prefer FASTAPI_BASE_URL / 127.0.0.1 to avoid Node → ::1 quirks.
 */

export function getApiBase(): string {
  const pub =
    (typeof process !== "undefined" &&
      process.env &&
      (process.env.NEXT_PUBLIC_FASTAPI_URL || process.env.NEXT_PUBLIC_API_URL)) ||
    "";

  if (typeof window !== "undefined") {
    if (pub) return pub.replace(/\/$/, "");
    return `${window.location.origin}/api/backend`;
  }

  if (pub) return pub.replace(/\/$/, "");
  const internal =
    (typeof process !== "undefined" &&
      process.env &&
      (process.env.FASTAPI_BASE_URL || process.env.INTERNAL_API_URL)) ||
    "";
  if (internal) return internal.replace(/\/$/, "");
  return "http://127.0.0.1:8000";
}

let csrfTokenPromise: Promise<string> | null = null;

export function resetCsrfTokenCache() {
  csrfTokenPromise = null;
}

export async function getCsrfToken(): Promise<string> {
  if (!csrfTokenPromise) {
    csrfTokenPromise = fetch("/api/auth/csrf", { cache: "no-store" })
      .then(async (res) => {
        if (!res.ok) throw new Error("Unable to read CSRF token");
        const data = (await res.json()) as { csrfToken?: string };
        if (!data.csrfToken) throw new Error("Missing CSRF token");
        return data.csrfToken;
      })
      .catch((err) => {
        csrfTokenPromise = null;
        throw err;
      });
  }
  return csrfTokenPromise;
}

export async function secureFetch(input: RequestInfo | URL, init: RequestInit = {}) {
  const method = (init.method || "GET").toUpperCase();
  const needsCsrf = !["GET", "HEAD", "OPTIONS"].includes(method);
  const headers = new Headers(init.headers);
  if (needsCsrf) {
    headers.set("x-csrf-token", await getCsrfToken());
  }
  return fetch(input, { ...init, headers });
}

export class ApiError extends Error {
  status: number;
  detail: unknown;

  constructor(message: string, status: number, detail?: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

export async function fetchJson<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const url = path.startsWith("http") ? path : `${getApiBase()}${path.startsWith("/") ? path : `/${path}`}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      ...init?.headers,
    },
  });
  const text = await res.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text) as unknown;
    } catch {
      data = text;
    }
  }
  if (!res.ok) {
    const detail =
      data && typeof data === "object" && data !== null && "detail" in data
        ? (data as { detail: unknown }).detail
        : data;
    throw new ApiError(
      typeof detail === "string" ? detail : res.statusText,
      res.status,
      detail
    );
  }
  return data as T;
}
