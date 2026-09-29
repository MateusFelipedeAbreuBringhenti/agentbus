from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from fastapi.testclient import TestClient

from agentbus.app import create_app
from agentbus.orion_ops import (
    CreateTaskInput,
    ControlResponse,
    HttpControlTransport,
    OrionOpsControlPlane,
    OrionOpsError,
    RequestApprovalInput,
    ResourceIdInput,
    TaskIdInput,
)
from agentbus.orion_ops_mcp import create_server


@dataclass
class FakeTransport:
    responses: list[ControlResponse]
    calls: list[tuple[str, str, dict[str, Any] | None, dict[str, str] | None]] = field(
        default_factory=list
    )

    def request(self, method, path, *, json=None, headers=None):
        self.calls.append((method, path, json, headers))
        return self.responses.pop(0)


class AppClientTransport:
    def __init__(self, client: TestClient) -> None:
        self.client = client

    def request(self, method, path, *, json=None, headers=None):
        response = self.client.request(method, path, json=json, headers=headers)
        return ControlResponse(
            response.status_code,
            response.json() if response.content else None,
            {key.lower(): value for key, value in response.headers.items()},
        )


def task_response(task_id: UUID, correlation_id: UUID, *, replayed: str = "false"):
    return ControlResponse(
        201,
        {"id": str(task_id), "correlation_id": str(correlation_id), "status": "ready"},
        {"etag": '"v1"', "idempotency-replayed": replayed},
    )


def test_create_injects_fixed_identity_and_preserves_protocol_metadata():
    instance_id, task_id, correlation_id = uuid4(), uuid4(), uuid4()
    transport = FakeTransport([task_response(task_id, correlation_id)])
    control = OrionOpsControlPlane(transport, instance_id=instance_id)

    result = control.create_task(
        CreateTaskInput(
            type="combine",
            title="Combine A and B",
            input={"a": "A", "b": "B"},
            assigned_to="dex",
            correlation_id=correlation_id,
            idempotency_key="create-1",
        )
    )

    method, path, body, headers = transport.calls[0]
    assert (method, path) == ("POST", "/tasks")
    assert body["requested_by"] == f"local-coordinator:{instance_id}"
    assert "actor_label" not in body and "persona_version" not in body
    assert headers == {"Idempotency-Key": "create-1"}
    assert result.data["id"] == str(task_id)
    assert result.http.etag == '"v1"'
    assert result.http.idempotency_replayed == "false"


def test_identity_and_generic_transport_fields_are_structurally_rejected():
    base = {
        "type": "work",
        "title": "Do work",
        "correlation_id": str(uuid4()),
        "idempotency_key": "key",
    }
    for forbidden in (
        "requested_by",
        "principal",
        "url",
        "method",
        "headers",
        "sql",
        "shell",
        "filesystem",
    ):
        with pytest.raises(ValidationError):
            CreateTaskInput.model_validate({**base, forbidden: "attacker"})


def test_mcp_exports_exact_allowlist_with_closed_command_schemas():
    transport = FakeTransport([])
    server = create_server(OrionOpsControlPlane(transport, instance_id=uuid4()))
    tools = asyncio.run(server.list_tools())

    assert {tool.name for tool in tools} == {
        "create_task",
        "get_task",
        "get_task_events",
        "get_approval",
        "list_task_approvals",
        "request_approval",
    }
    assert not {"claim", "complete", "fail", "approve", "reject"} & {
        tool.name for tool in tools
    }
    for tool in tools:
        command_ref = tool.input_schema["properties"]["command"]["$ref"]
        definition = tool.input_schema["$defs"][command_ref.rsplit("/", 1)[1]]
        assert definition["additionalProperties"] is False


def test_all_paths_and_methods_are_fixed_by_the_control_plane():
    ok = ControlResponse(200, {}, {})
    transport = FakeTransport([ok, ok, ok, ok])
    control = OrionOpsControlPlane(transport, instance_id=uuid4())
    task_id, approval_id = uuid4(), uuid4()

    control.get_task(ResourceIdInput(id=task_id))
    control.get_task_events(TaskIdInput(task_id=task_id))
    control.get_approval(ResourceIdInput(id=approval_id))
    control.list_task_approvals(TaskIdInput(task_id=task_id))

    assert [(call[0], call[1]) for call in transport.calls] == [
        ("GET", f"/tasks/{task_id}"),
        ("GET", f"/tasks/{task_id}/events"),
        ("GET", f"/approvals/{approval_id}"),
        ("GET", f"/tasks/{task_id}/approvals"),
    ]


def test_request_approval_injects_identity_and_preserves_etags_and_causation():
    instance_id, task_id, cause = uuid4(), uuid4(), uuid4()
    transport = FakeTransport(
        [ControlResponse(200, {"task": {}, "approval": {}}, {
            "task-etag": '"v2"', "approval-etag": '"v1"'
        })]
    )
    control = OrionOpsControlPlane(transport, instance_id=instance_id)

    result = control.request_approval(
        RequestApprovalInput(
            task_id=task_id,
            gate="deployment.production",
            request_reason="Human authorization required",
            assigned_to="mateus",
            causation_event_id=cause,
            if_match='"v1"',
            idempotency_key="approval-1",
        )
    )

    _, path, body, headers = transport.calls[0]
    assert path == f"/tasks/{task_id}/request-approval"
    assert body["requested_by"] == f"local-coordinator:{instance_id}"
    assert body["causation_event_id"] == str(cause)
    assert headers == {"Idempotency-Key": "approval-1", "If-Match": '"v1"'}
    assert result.http.task_etag == '"v2"'
    assert result.http.approval_etag == '"v1"'


def test_agentbus_domain_error_is_translated_without_losing_code():
    transport = FakeTransport(
        [ControlResponse(409, {"detail": {
            "code": "idempotency_key_reused", "message": "already used"
        }}, {})]
    )
    control = OrionOpsControlPlane(transport, instance_id=uuid4())

    with pytest.raises(OrionOpsError) as captured:
        control.get_task(ResourceIdInput(id=uuid4()))
    assert captured.value.status_code == 409
    assert captured.value.code == "idempotency_key_reused"
    assert "[idempotency_key_reused]" in str(captured.value)


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1:8000",
        "http://example.com",
        "http://user:pass@localhost:8000",
        "http://localhost:8000?target=other",
    ],
)
def test_http_transport_rejects_non_loopback_or_decorated_urls(url):
    with pytest.raises(ValueError):
        HttpControlTransport(url)


def test_two_tasks_cannot_confuse_results():
    first, second, correlation = uuid4(), uuid4(), uuid4()
    transport = FakeTransport(
        [task_response(first, correlation), task_response(second, correlation)]
    )
    control = OrionOpsControlPlane(transport, instance_id=uuid4())

    common = dict(type="combine", title="Combine", correlation_id=correlation)
    result_a = control.create_task(CreateTaskInput(**common, idempotency_key="a"))
    result_b = control.create_task(CreateTaskInput(**common, idempotency_key="b"))

    assert result_a.data["id"] == str(first)
    assert result_b.data["id"] == str(second)


def test_real_agentbus_idempotency_and_correlation_are_preserved(tmp_path):
    app = create_app(tmp_path / "agentbus.sqlite3")
    instance_id, correlation_id = uuid4(), uuid4()
    with TestClient(app) as client:
        control = OrionOpsControlPlane(AppClientTransport(client), instance_id=instance_id)
        command = CreateTaskInput(
            type="combine",
            title="Combine A and B",
            input={"a": "A", "b": "B"},
            assigned_to="dex",
            correlation_id=correlation_id,
            idempotency_key="stable-create-key",
        )
        created = control.create_task(command)
        replayed = control.create_task(command)
        events = control.get_task_events(TaskIdInput(task_id=created.data["id"]))

    assert created.data == replayed.data
    assert created.data["requested_by"] == f"local-coordinator:{instance_id}"
    assert created.data["correlation_id"] == str(correlation_id)
    assert created.http.idempotency_replayed == "false"
    assert replayed.http.idempotency_replayed == "true"
    assert len(events.data) == 1
    assert events.data[0]["actor_id"] == f"local-coordinator:{instance_id}"
    assert events.data[0]["correlation_id"] == str(correlation_id)
