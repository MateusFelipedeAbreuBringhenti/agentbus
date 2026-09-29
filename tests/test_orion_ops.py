from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
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
from mcp.server.mcpserver.exceptions import ToolError


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


def task_response(
    task_id: UUID,
    correlation_id: UUID,
    *,
    replayed: str = "false",
    requested_by: str = "local-coordinator:test",
):
    now = datetime.now(UTC).isoformat()
    return ControlResponse(
        201,
        {
            "id": str(task_id),
            "type": "combine",
            "title": "Combine A and B",
            "description": None,
            "input": {"a": "A", "b": "B"},
            "output": None,
            "status": "ready",
            "requested_by": requested_by,
            "assigned_to": "dex",
            "failure_code": None,
            "failure_message": None,
            "retry_of": None,
            "waiting_on_approval_id": None,
            "correlation_id": str(correlation_id),
            "version": 1,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "finished_at": None,
        },
        {"etag": '"v1"', "idempotency-replayed": replayed},
    )


def approval_body(
    approval_id: UUID,
    task_id: UUID,
    correlation_id: UUID,
    *,
    requested_by: str = "local-coordinator:test",
):
    now = datetime.now(UTC).isoformat()
    return {
        "id": str(approval_id),
        "task_id": str(task_id),
        "gate": "deployment.production",
        "status": "pending",
        "request_reason": "Human authorization required",
        "context": {},
        "requested_by": requested_by,
        "assigned_to": "mateus",
        "decided_by": None,
        "decision_reason": None,
        "correlation_id": str(correlation_id),
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "decided_at": None,
    }


def test_create_injects_fixed_identity_and_preserves_protocol_metadata():
    instance_id, task_id, correlation_id = uuid4(), uuid4(), uuid4()
    principal = f"local-coordinator:{instance_id}"
    transport = FakeTransport([
        task_response(task_id, correlation_id, requested_by=principal)
    ])
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
        assert tool.input_schema["additionalProperties"] is False
        command_ref = tool.input_schema["properties"]["command"]["$ref"]
        definition = tool.input_schema["$defs"][command_ref.rsplit("/", 1)[1]]
        assert definition["additionalProperties"] is False


def test_mcp_runtime_rejects_undeclared_envelope_fields():
    server = create_server(OrionOpsControlPlane(FakeTransport([]), instance_id=uuid4()))

    async def call_with_smuggled_field():
        return await server.call_tool(
            "get_task_events",
            {
                "command": {"task_id": str(uuid4())},
                "shell": "ignored by the generated envelope before hardening",
            },
        )

    with pytest.raises(ToolError, match="Extra inputs are not permitted"):
        asyncio.run(call_with_smuggled_field())


def test_all_paths_and_methods_are_fixed_by_the_control_plane():
    task_id, approval_id, correlation_id = uuid4(), uuid4(), uuid4()
    task = task_response(task_id, correlation_id)
    transport = FakeTransport([
        ControlResponse(200, task.body, {"etag": '"v1"'}),
        ControlResponse(200, [], {}),
        ControlResponse(
            200,
            approval_body(approval_id, task_id, correlation_id),
            {"etag": '"v1"'},
        ),
        ControlResponse(200, [], {}),
    ])
    control = OrionOpsControlPlane(transport, instance_id=uuid4())

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
    correlation_id, approval_id = uuid4(), uuid4()
    principal = f"local-coordinator:{instance_id}"
    task = task_response(task_id, correlation_id, requested_by=principal).body
    task.update(
        status="waiting_approval",
        waiting_on_approval_id=str(approval_id),
        version=2,
    )
    transport = FakeTransport(
        [ControlResponse(200, {
            "task": task,
            "approval": approval_body(
                approval_id, task_id, correlation_id, requested_by=principal
            ),
        }, {
            "task-etag": '"v2"', "approval-etag": '"v1"',
            "idempotency-replayed": "false",
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


def test_http_transport_translates_non_json_response():
    transport = HttpControlTransport("http://127.0.0.1:8000")
    transport._client.close()
    transport._client = httpx.Client(
        base_url="http://127.0.0.1:8000",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(502, text="not-json", headers={"content-type": "text/plain"})
        ),
    )
    try:
        with pytest.raises(OrionOpsError) as captured:
            transport.request("GET", "/tasks/not-relevant")
        assert captured.value.status_code == 502
        assert captured.value.code == "invalid_agentbus_response"
    finally:
        transport.close()


def test_valid_but_cross_task_response_fails_closed():
    requested_id, returned_id, correlation = uuid4(), uuid4(), uuid4()
    response = task_response(returned_id, correlation)
    transport = FakeTransport([
        ControlResponse(200, response.body, {"etag": '"v1"'})
    ])
    control = OrionOpsControlPlane(transport, instance_id=uuid4())

    with pytest.raises(OrionOpsError) as captured:
        control.get_task(ResourceIdInput(id=requested_id))
    assert captured.value.code == "invalid_agentbus_response"


def test_two_tasks_cannot_confuse_results():
    first, second, correlation = uuid4(), uuid4(), uuid4()
    instance_id = uuid4()
    principal = f"local-coordinator:{instance_id}"
    transport = FakeTransport(
        [
            task_response(first, correlation, requested_by=principal),
            task_response(second, correlation, requested_by=principal),
        ]
    )
    control = OrionOpsControlPlane(transport, instance_id=instance_id)

    common = dict(type="combine", title="Combine", correlation_id=correlation)
    result_a = control.create_task(CreateTaskInput(**common, idempotency_key="a"))
    result_b = control.create_task(CreateTaskInput(**common, idempotency_key="b"))

    assert result_a.data["id"] == str(first)
    assert result_b.data["id"] == str(second)


@pytest.mark.parametrize(
    ("response", "operation"),
    [
        (ControlResponse(200, {"status": "ready"}, {"etag": '"v1"'}), "task"),
        (ControlResponse(200, [], {"etag": '"v999"'}), "task"),
        (ControlResponse(200, {"unexpected": True}, {}), "events"),
        (ControlResponse(200, [{"status": "pending"}], {}), "approvals"),
    ],
)
def test_malformed_success_responses_fail_closed(response, operation):
    transport = FakeTransport([response])
    control = OrionOpsControlPlane(transport, instance_id=uuid4())
    with pytest.raises(OrionOpsError) as captured:
        if operation == "task":
            control.get_task(ResourceIdInput(id=uuid4()))
        elif operation == "events":
            control.get_task_events(TaskIdInput(task_id=uuid4()))
        else:
            control.list_task_approvals(TaskIdInput(task_id=uuid4()))
    assert captured.value.status_code == 502
    assert captured.value.code == "invalid_agentbus_response"


def test_concurrent_tasks_and_results_remain_isolated_in_real_storage(tmp_path):
    app = create_app(tmp_path / "concurrent.sqlite3")
    instance_id = uuid4()
    correlations = [uuid4(), uuid4()]

    def create(index: int):
        with TestClient(app) as client:
            control = OrionOpsControlPlane(AppClientTransport(client), instance_id=instance_id)
            return control.create_task(CreateTaskInput(
                type="combine",
                title=f"Task {index}",
                input={"value": index},
                assigned_to="dex",
                correlation_id=correlations[index],
                idempotency_key=f"concurrent-{index}",
            ))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, (0, 1)))

    assert results[0].data["id"] != results[1].data["id"]
    assert [result.data["input"] for result in results] == [{"value": 0}, {"value": 1}]
    assert [result.data["correlation_id"] for result in results] == [
        str(correlations[0]),
        str(correlations[1]),
    ]

    with TestClient(app) as client:
        for index, result in enumerate(results):
            task_id = result.data["id"]
            claimed = client.post(
                f"/tasks/{task_id}/claim",
                json={"agent_id": "dex"},
                headers={"Idempotency-Key": f"claim-{index}"},
            )
            assert claimed.status_code == 200
            completed = client.post(
                f"/tasks/{task_id}/complete",
                json={"output": {"result": f"result-{index}"}},
                headers={
                    "Idempotency-Key": f"complete-{index}",
                    "If-Match": claimed.headers["etag"],
                },
            )
            assert completed.status_code == 200

    def read(index: int):
        with TestClient(app) as client:
            control = OrionOpsControlPlane(AppClientTransport(client), instance_id=instance_id)
            return control.get_task(ResourceIdInput(id=results[index].data["id"]))

    with ThreadPoolExecutor(max_workers=2) as pool:
        terminal = list(pool.map(read, (0, 1)))
    assert [result.data["output"] for result in terminal] == [
        {"result": "result-0"},
        {"result": "result-1"},
    ]


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


def test_real_approval_request_replays_with_causality_and_etags(tmp_path):
    app = create_app(tmp_path / "approval.sqlite3")
    correlation_id = uuid4()
    with TestClient(app) as client:
        control = OrionOpsControlPlane(AppClientTransport(client), instance_id=uuid4())
        task = control.create_task(CreateTaskInput(
            type="release",
            title="Prepare release",
            assigned_to="dex",
            correlation_id=correlation_id,
            idempotency_key="create-release",
        ))
        events = control.get_task_events(TaskIdInput(task_id=task.data["id"]))
        command = RequestApprovalInput(
            task_id=task.data["id"],
            gate="deployment.production",
            request_reason="Authorize publish",
            assigned_to="mateus",
            causation_event_id=events.data[0]["id"],
            if_match=task.http.etag,
            idempotency_key="request-release-approval",
        )
        requested = control.request_approval(command)
        replayed = control.request_approval(command)

        assert requested.data == replayed.data
        assert requested.http.task_etag == '"v2"'
        assert requested.http.approval_etag == '"v1"'
        assert requested.http.idempotency_replayed == "false"
        assert replayed.http.idempotency_replayed == "true"
        assert requested.data["task"]["correlation_id"] == str(correlation_id)
        assert requested.data["approval"]["correlation_id"] == str(correlation_id)

        with pytest.raises(OrionOpsError) as collision:
            control.request_approval(command.model_copy(
                update={"request_reason": "Changed after the fact"}
            ))
        assert collision.value.status_code == 409
        assert collision.value.code == "idempotency_key_reused"
