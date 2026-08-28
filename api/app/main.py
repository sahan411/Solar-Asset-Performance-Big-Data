"""SolarIQ serving API — FastAPI application factory.

Assembles configuration, the PostgreSQL pool, structured logging, request
metrics and every router into one app. No business SQL lives here — that
belongs in app/repositories/; routers only translate HTTP <-> repository calls.
"""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import Settings, cors_origins_from_env
from app.db import Database
from app.logging import get_logger
from app.metrics import ERROR_COUNT, REQUEST_COUNT, REQUEST_DURATION
from app.routers import alerts, plants, portfolio, system

log = get_logger("api")


class StrictJSONResponse(JSONResponse):
    """Refuses to emit NaN/Infinity — invalid JSON that `json.dumps` allows by
    default. The pipeline should never produce them (see docs/member-2-handoff.md,
    section 8); this is the safety net that fails loudly instead of shipping a
    response a browser's JSON.parse would silently choke on."""

    def render(self, content: Any) -> bytes:
        return json.dumps(content, allow_nan=False, separators=(",", ":")).encode("utf-8")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    database = Database(settings.database_url)
    app.state.settings = settings
    app.state.database = database
    log.info("api_startup", "SolarIQ API starting", port=settings.port)
    try:
        yield
    finally:
        database.close()
        log.info("api_shutdown", "SolarIQ API stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="SolarIQ Serving API",
        version="1",
        default_response_class=StrictJSONResponse,
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def _request_metrics(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        route = request.scope.get("route")
        route_path = route.path if route is not None else request.url.path
        labels = {"method": request.method, "route": route_path}

        REQUEST_COUNT.labels(**labels, status_code=response.status_code).inc()
        REQUEST_DURATION.labels(**labels).observe(duration)
        if response.status_code >= 500:
            ERROR_COUNT.labels(**labels).inc()

        return response

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_origins_from_env()),
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return StrictJSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return StrictJSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        # Never leak a stack trace or SQL error to the client; the full
        # exception still goes to the structured logs for debugging.
        log.exception("unhandled_api_error", f"Unhandled error on {request.method} {request.url.path}")
        return StrictJSONResponse(status_code=500, content={"detail": "Internal server error"})

    app.include_router(system.router)
    app.include_router(portfolio.router)
    app.include_router(plants.router)
    app.include_router(alerts.router)

    return app


app = create_app()
