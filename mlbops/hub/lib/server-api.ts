import "server-only";

const SERVICE_HEADER = "x-mlbops-service-token";

function apiServiceToken(): string {
  const token = (process.env.MLBOPS_API_SERVICE_TOKEN || "").trim();
  if (process.env.NODE_ENV === "production" && token.length < 32) {
    throw new Error("Set MLBOPS_API_SERVICE_TOKEN to a random value of at least 32 characters.");
  }
  return token;
}

export function apiServiceHeaders(initial?: HeadersInit): Headers {
  const headers = new Headers(initial);
  headers.delete(SERVICE_HEADER);
  const token = apiServiceToken();
  if (token) headers.set(SERVICE_HEADER, token);
  return headers;
}

export function serverApiBase(): string {
  return (
    process.env.FASTAPI_BASE_URL ||
    process.env.INTERNAL_API_URL ||
    "http://127.0.0.1:8000"
  ).replace(/\/$/, "");
}

export function serverApiFetch(path: string, init: RequestInit = {}) {
  const url = path.startsWith("http")
    ? path
    : `${serverApiBase()}${path.startsWith("/") ? path : `/${path}`}`;
  return fetch(url, { ...init, headers: apiServiceHeaders(init.headers) });
}
