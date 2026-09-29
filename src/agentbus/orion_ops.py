from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field


ACTOR_LABEL = "orion-ops"
PERSONA_VERSION = "orion-ops/v1"


class OrionOpsError(RuntimeError):
    def __init__(self, status_code: int, code: str, message: str, detail: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.detail = detail


class CreateTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=10_000)
    input: dict[str, Any] = Field(default_factory=dict)
    assigned_to: str | None = Field(default=None, min_length=1, max_length=200)
    correlation_id: UUID
    causation_event_id: UUID | None = None
    idempotency_key: str = Field(min_length=1, max_length=200)


class ResourceIdInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID


class TaskIdInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: UUID


class RequestApprovalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: UUID
    gate: str = Field(min_length=1, max_length=200)
    request_reason: str = Field(min_length=1, max_length=10_000)
    assigned_to: str | None = Field(default=None, min_length=1, max_length=200)
    context: dict[str, Any] = Field(default_factory=dict)
    causation_event_id: UUID | None = None
    if_match: str = Field(pattern=r'^"v[1-9][0-9]*"$')
    idempotency_key: str = Field(min_length=1, max_length=200)


class ToolHttpMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: int
    etag: str | None = None
    task_etag: str | None = None
    approval_etag: str | None = None
    idempotency_replayed: str | None = None


class ToolResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: Any
    http: ToolHttpMetadata


@dataclass(frozen=True)
class ControlResponse:
    status_code: int
    body: Any
    headers: dict[str, str]


class ControlTransport(Protocol):
    def request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ControlResponse: ...


class HttpControlTransport:
    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("AgentBus base URL must be loopback HTTP.")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("AgentBus base URL cannot contain credentials, query, or fragment.")
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ControlResponse:
        try:
            response = self._client.request(method, path, json=json, headers=headers)
        except httpx.RequestError as error:
            raise OrionOpsError(503, "agentbus_unavailable", "AgentBus is unavailable.") from error
        try:
            body = response.json() if response.content else None
        except ValueError as error:
            raise OrionOpsError(
                502, "invalid_agentbus_response", "AgentBus returned invalid JSON."
            ) from error
        return ControlResponse(
            status_code=response.status_code,
            body=body,
            headers={key.lower(): value for key, value in response.headers.items()},
        )


class OrionOpsControlPlane:
    """A closed, coordinator-only projection over the AgentBus HTTP API."""

    def __init__(self, transport: ControlTransport, *, instance_id: UUID) -> None:
        self._transport = transport
        self.principal = f"local-coordinator:{instance_id}"

    @property
    def metadata(self) -> dict[str, str]:
        return {
            "principal": self.principal,
            "actor_label": ACTOR_LABEL,
            "persona_version": PERSONA_VERSION,
        }

    def _call(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> ToolResult:
        response = self._transport.request(method, path, json=body, headers=headers)
        if not 200 <= response.status_code < 300:
            detail = response.body.get("detail", {}) if isinstance(response.body, dict) else {}
            if isinstance(detail, str):
                code, message = "agentbus_error", detail
            else:
                code = detail.get("code", "agentbus_error")
                message = detail.get("message", f"AgentBus returned HTTP {response.status_code}.")
            raise OrionOpsError(response.status_code, code, message, detail)
        etags = {
            key.replace("-", "_"): value
            for key, value in response.headers.items()
            if key in {"etag", "task-etag", "approval-etag", "idempotency-replayed"}
        }
        return ToolResult(
            data=response.body,
            http=ToolHttpMetadata(status=response.status_code, **etags),
        )

    def create_task(self, command: CreateTaskInput) -> ToolResult:
        body = command.model_dump(mode="json", exclude={"idempotency_key"})
        body["requested_by"] = self.principal
        return self._call(
            "POST",
            "/tasks",
            body=body,
            headers={"Idempotency-Key": command.idempotency_key},
        )

    def get_task(self, command: ResourceIdInput) -> ToolResult:
        return self._call("GET", f"/tasks/{command.id}")

    def get_task_events(self, command: TaskIdInput) -> ToolResult:
        return self._call("GET", f"/tasks/{command.task_id}/events")

    def get_approval(self, command: ResourceIdInput) -> ToolResult:
        return self._call("GET", f"/approvals/{command.id}")

    def list_task_approvals(self, command: TaskIdInput) -> ToolResult:
        return self._call("GET", f"/tasks/{command.task_id}/approvals")

    def request_approval(self, command: RequestApprovalInput) -> ToolResult:
        body = command.model_dump(
            mode="json",
            exclude={"task_id", "if_match", "idempotency_key"},
        )
        body["requested_by"] = self.principal
        return self._call(
            "POST",
            f"/tasks/{command.task_id}/request-approval",
            body=body,
            headers={
                "Idempotency-Key": command.idempotency_key,
                "If-Match": command.if_match,
            },
        )
