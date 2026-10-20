from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
import httpx

from agentbus.auth import read_token_file


def create_gateway(
    *,
    agentbus_url: str,
    token: str,
    allowed_host: str = "127.0.0.1:5173",
    transport: httpx.BaseTransport | None = None,
    operational_url: str | None = None,
    operational_token: str | None = None,
    operational_transport: httpx.BaseTransport | None = None,
) -> FastAPI:
    parsed = urlparse(agentbus_url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("AgentBus gateway target must be loopback HTTP.")
    host_name = allowed_host.rsplit(":", 1)[0].strip("[]")
    if host_name not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Control Center Host must be loopback.")
    client = httpx.Client(
        base_url=agentbus_url,
        headers={"Authorization": f"Bearer {token}"},
        transport=transport,
    )
    operational_client = None
    if operational_url is not None:
        parsed_operations = urlparse(operational_url)
        if parsed_operations.scheme != "http" or parsed_operations.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Operational projection target must be loopback HTTP.")
        if not operational_token:
            raise ValueError("Operational projection token is required.")
        operational_client = httpx.Client(
            base_url=operational_url,
            headers={"Authorization": f"Bearer {operational_token}"},
            transport=operational_transport,
        )
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        client.close()
        if operational_client is not None:
            operational_client.close()

    app = FastAPI(title="AgentBus Control Center Gateway", lifespan=lifespan)

    @app.middleware("http")
    async def browser_boundary(request: Request, call_next):
        if request.headers.get("host") != allowed_host:
            return JSONResponse(status_code=400, content={"detail": {"code": "invalid_host"}})
        origin = request.headers.get("origin")
        if origin is not None and origin != f"http://{allowed_host}":
            return JSONResponse(status_code=403, content={"detail": {"code": "invalid_origin"}})
        if request.headers.get("sec-fetch-site") not in {None, "same-origin", "none"}:
            return JSONResponse(status_code=403, content={"detail": {"code": "cross_site_request"}})
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'"
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    def forward(path: str, params: dict[str, Any] | None = None) -> Response:
        response = client.get(path, params={k: v for k, v in (params or {}).items() if v is not None})
        headers = {key: value for key, value in response.headers.items()
                   if key.lower() in {"etag", "content-type"}}
        return Response(response.content, status_code=response.status_code, headers=headers)

    def forward_operations(path: str, params: dict[str, Any] | None = None) -> Response:
        if operational_client is None:
            return JSONResponse(status_code=503, content={"detail": {"code": "operational_projection_unavailable"}})
        response = operational_client.get(path, params={k: v for k, v in (params or {}).items() if v is not None})
        return Response(response.content, status_code=response.status_code,
                        media_type=response.headers.get("content-type", "application/json"))

    @app.get("/api/tasks")
    def tasks(
        limit: int = Query(default=25, ge=1, le=100), cursor: str | None = None,
        status: str | None = None, assigned_to: str | None = None,
        requested_by: str | None = None, correlation_id: UUID | None = None,
        type: str | None = None,
    ) -> Response:
        return forward("/tasks", locals())

    @app.get("/api/tasks/{task_id}")
    def task(task_id: UUID) -> Response:
        return forward(f"/tasks/{task_id}")

    @app.get("/api/correlations/{correlation_id}/trace")
    def trace(correlation_id: UUID) -> Response:
        return forward(f"/correlations/{correlation_id}/trace")

    @app.get("/api/approvals")
    def approvals(
        limit: int = Query(default=25, ge=1, le=100), cursor: str | None = None,
        status: str | None = None, assigned_to: str | None = None,
        task_id: UUID | None = None, correlation_id: UUID | None = None,
    ) -> Response:
        return forward("/approvals", locals())

    @app.get("/api/approvals/{approval_id}")
    def approval(approval_id: UUID) -> Response:
        return forward(f"/approvals/{approval_id}")

    @app.get("/api/operations")
    def operations(
        limit: int = Query(default=25, ge=1, le=100), cursor: str | None = None,
        kind: str | None = None, attention_class: str | None = None,
        task_id: UUID | None = None, approval_id: UUID | None = None,
        correlation_id: UUID | None = None, logical_owner: str | None = None,
    ) -> Response:
        return forward_operations("/operations", locals())

    @app.get("/api/operations/{operation_id}")
    def operation(operation_id: str) -> Response:
        return forward_operations(f"/operations/{operation_id}")

    @app.get("/api/operation-sources")
    def operation_sources() -> Response:
        return forward_operations("/sources")

    return app


def from_environment() -> FastAPI:
    token = read_token_file(os.environ["AGENTBUS_CONTROL_CENTER_TOKEN_FILE"])
    return create_gateway(
        agentbus_url=os.environ.get("AGENTBUS_URL", "http://127.0.0.1:8000"),
        token=token,
        allowed_host=os.environ.get("AGENTBUS_CONTROL_CENTER_HOST", "127.0.0.1:5173"),
        operational_url=os.environ.get("AGENTBUS_OPERATIONAL_URL"),
        operational_token=(read_token_file(os.environ["AGENTBUS_OPERATIONAL_READER_TOKEN_FILE"])
                           if os.environ.get("AGENTBUS_OPERATIONAL_READER_TOKEN_FILE") else None),
    )


app = from_environment() if os.environ.get("AGENTBUS_CONTROL_CENTER_TOKEN_FILE") else FastAPI()
