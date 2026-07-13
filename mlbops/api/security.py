"""Service-to-service authentication for the private FastAPI process."""

from __future__ import annotations

import os
import secrets

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

SERVICE_TOKEN_HEADER = "x-mlbops-service-token"
_PUBLIC_PATHS = frozenset({"/health"})


def is_production_runtime() -> bool:
    runtime = os.getenv("MLBOPS_RUNTIME", "").strip().lower()
    return runtime in {"vps", "production", "prod"}


def service_request_status(
    path: str,
    supplied_token: str,
    expected_token: str,
    *,
    production: bool,
) -> str:
    if path in _PUBLIC_PATHS or path.startswith("/static/"):
        return "public"
    if not expected_token:
        return "misconfigured" if production else "development"
    if production and len(expected_token) < 32:
        return "misconfigured"
    if supplied_token and secrets.compare_digest(supplied_token, expected_token):
        return "authorized"
    return "unauthorized"


class ServiceAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        status = service_request_status(
            request.url.path,
            request.headers.get(SERVICE_TOKEN_HEADER, ""),
            os.getenv("MLBOPS_API_SERVICE_TOKEN", "").strip(),
            production=is_production_runtime(),
        )
        if status in {"public", "development", "authorized"}:
            return await call_next(request)
        if status == "misconfigured":
            return JSONResponse(
                {"detail": "API service authentication is not configured."},
                status_code=503,
            )
        return JSONResponse({"detail": "Service authentication required."}, status_code=401)
