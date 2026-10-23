import json
from pathlib import Path
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import httpx

from agentbus.app import create_app
from agentbus.approval_confirm import ApprovalConfirmer
from agentbus.approval_intents import ApprovalIntentJournal, create_intent_app
from agentbus.auth import AuthRegistry, token_digest
from agentbus.control_center_gateway import create_gateway
from agentbus.coordinator import CoordinatorOutbox
from agentbus.execution_policy import ExecutionPolicy
from agentbus.operational_projection import (
    AgentBusReadClient, OperationalProjection, ProjectionManifest,
    create_projection_app,
)
from agentbus.orion_ops import (
    ControlResponse, CreateTaskInput, OrionOpsControlPlane, RequestApprovalInput,
)
from agentbus.release_reconciler import (
    AgentBusReleaseClient, ReleaseJournal, ReleasePolicy, ReleaseReconciler,
)
from agentbus.runner import (
    AgentRunner, BusResponse, DeterministicNoopExecutor, RunnerJournal,
)


SUBJECT = "local-coordinator:11111111-1111-4111-8111-111111111111"
TOKENS = {
    "coordinator": "e2e-coordinator-token-0000000000000000000",
    "approver": "e2e-approver-token-000000000000000000000",
    "runner": "e2e-runner-token-00000000000000000000000",
    "viewer": "e2e-viewer-token-00000000000000000000000",
    "projection": "e2e-projection-token-0000000000000000000",
    "projection_reader": "e2e-projection-reader-000000000000000000",
}


def _registry(tmp_path: Path) -> AuthRegistry:
    definitions = [
        ("coordinator-1", SUBJECT, "coordinator", [
            "task:create", "approval:request", "task:release", "task:read:any",
            "approval:read:any", "event:read:any", "trace:read:any",
        ]),
        ("approver-1", "mateus-e2e", "approver", [
            "approval:read:self", "approval:decide:self",
        ]),
        ("runner-1", "dex-e2e", "runner", [
            "inbox:read:self", "task:read:self", "task:claim:self",
            "task:complete:self", "task:fail:self",
        ]),
        ("viewer-1", "control-center", "viewer", [
            "task:read:any", "approval:read:any", "trace:read:any",
        ]),
        ("projection-1", "operational-projection", "projection", [
            "task:read:any",
        ]),
    ]
    path = tmp_path / "auth.json"
    path.write_text(json.dumps({"principals": [{
        "principal_id": principal, "subject_id": subject,
        "token_sha256": token_digest(TOKENS[token]), "permissions": permissions,
    } for principal, subject, token, permissions in definitions]}), encoding="utf-8")
    path.chmod(0o600)
    return AuthRegistry.from_file(path)


def _headers(token: str, **extra: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKENS[token]}", **extra}


def _transport(api: TestClient) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        response = api.request(
            request.method, request.url.path, params=dict(request.url.params),
            content=request.content, headers=dict(request.headers),
        )
        return httpx.Response(
            response.status_code, content=response.content,
            headers=dict(response.headers), request=request,
        )
    return httpx.MockTransport(handler)


class CoordinatorTransport:
    authenticated_subject_id = SUBJECT

    def __init__(self, api: TestClient): self.api = api

    def request(self, method, path, *, json=None, headers=None):
        response = self.api.request(
            method, path, json=json,
            headers={**_headers("coordinator"), **(headers or {})},
        )
        return ControlResponse(response.status_code, response.json(), {
            key.lower(): value for key, value in response.headers.items()
        })


class RunnerTransport:
    authenticated_subject_id = "dex-e2e"

    def __init__(self, api: TestClient): self.api = api

    def _response(self, response):
        return BusResponse(response.status_code, response.json(), {
            key.lower(): value for key, value in response.headers.items()
        })

    def get_inbox(self, agent_id):
        return self._response(self.api.get(
            f"/agents/{agent_id}/inbox", headers=_headers("runner"),
        ))

    def get_task(self, task_id):
        return self._response(self.api.get(
            f"/tasks/{task_id}", headers=_headers("runner"),
        ))

    def claim_task(self, task_id, agent_id, key, execution_id, instance_id):
        return self._response(self.api.post(f"/tasks/{task_id}/claim", json={
            "agent_id": agent_id, "execution_id": execution_id,
            "runner_instance_id": instance_id,
        }, headers=_headers("runner", **{"Idempotency-Key": key})))

    def complete_task(self, task_id, output, key, etag, execution_id, instance_id):
        return self._response(self.api.post(f"/tasks/{task_id}/complete", json={
            "output": output, "execution_id": execution_id,
            "runner_instance_id": instance_id,
        }, headers=_headers("runner", **{"Idempotency-Key": key, "If-Match": etag})))

    def fail_task(self, task_id, code, message, key, etag, execution_id, instance_id):
        return self._response(self.api.post(f"/tasks/{task_id}/fail", json={
            "failure_code": code, "failure_message": message,
            "execution_id": execution_id, "runner_instance_id": instance_id,
        }, headers=_headers("runner", **{"Idempotency-Key": key, "If-Match": etag})))


def _private_json(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o600)
    return path


def test_complete_authorized_flow_reaches_result_events_projection_and_gateway(tmp_path):
    execution_policy = ExecutionPolicy(
        "palace-e2e-v1", "e" * 64,
        {"dex-e2e": frozenset({"palace.e2e.noop"})},
    )
    database = tmp_path / "agentbus.sqlite3"
    with TestClient(create_app(
        database, authenticator=_registry(tmp_path), execution_policy=execution_policy,
    )) as api:
        outbox_path = tmp_path / "orion-outbox.sqlite3"
        coordinator = OrionOpsControlPlane(
            CoordinatorTransport(api),
            instance_id=UUID("11111111-1111-4111-8111-111111111111"),
            outbox=CoordinatorOutbox(outbox_path),
        )
        correlation_id = uuid4()
        task_result = coordinator.create_task(CreateTaskInput(
            type="palace.e2e.noop", title="Palace operational readiness",
            input={"message": "Palace operational readiness", "expected_result": "verified"},
            assigned_to="dex-e2e", correlation_id=correlation_id,
            idempotency_key="e2e-create",
        ))
        task = task_result.data
        requested = coordinator.request_approval(RequestApprovalInput(
            task_id=UUID(task["id"]), gate="palace.e2e.manual",
            request_reason="Human confirmation for isolated E2E",
            assigned_to="mateus-e2e", if_match=task_result.http.etag,
            idempotency_key="e2e-request-approval",
        ))
        approval = requested.data["approval"]

        intent_path = tmp_path / "approval-intents.sqlite3"
        intent_journal = ApprovalIntentJournal(intent_path)
        intent_app = create_intent_app(
            journal=intent_journal, agentbus_url="http://127.0.0.1:8000",
            token=TOKENS["viewer"], allowed_host="127.0.0.1:5173",
            transport=_transport(api),
        )
        with TestClient(intent_app, base_url="http://127.0.0.1:5173") as intents:
            session = intents.get("/intent-api/session").json()
            created_intent = intents.post("/intent-api/approval-intents", json={
                "approval_id": approval["id"], "decision": "approve",
                "decision_reason": "E2E approved", "if_match": '"v1"',
            }, headers={
                "Origin": "http://127.0.0.1:5173", "Sec-Fetch-Site": "same-origin",
                "X-AgentBus-CSRF": session["csrf_token"], "Intent-Key": "e2e-intent",
            })
            assert created_intent.status_code == 201
            intent_id = created_intent.json()["intent_id"]

        confirmer = ApprovalConfirmer(
            journal=intent_journal, journal_path=intent_path,
            agentbus_url="http://127.0.0.1:8000", token=TOKENS["approver"],
            transport=_transport(api), prompt=lambda _: "APROVAR", output=lambda _: None,
        )
        try: confirmer.confirm(intent_id)
        finally: confirmer.close()
        waiting = api.get(f"/tasks/{task['id']}", headers=_headers("coordinator")).json()
        assert waiting["status"] == "waiting_approval"

        release_path = tmp_path / "release.sqlite3"
        release_policy = ReleasePolicy.from_file(_private_json(
            tmp_path / "release-policy.json", {"allowed_gates": ["palace.e2e.manual"]},
        ))
        release_client = AgentBusReleaseClient(
            "http://127.0.0.1:8000", TOKENS["coordinator"], transport=_transport(api),
        )
        try:
            released = ReleaseReconciler(
                journal=ReleaseJournal(release_path), client=release_client,
                policy=release_policy, expected_subject_id=SUBJECT,
            ).reconcile(UUID(approval["id"]))
        finally: release_client.close()
        assert released["state"] == "completed"

        runner_path = tmp_path / "runner.sqlite3"
        with RunnerJournal(runner_path) as journal:
            AgentRunner(
                agent_id="dex-e2e", client=RunnerTransport(api), journal=journal,
                executor=DeterministicNoopExecutor(),
                allowed_task_types=frozenset({"palace.e2e.noop"}),
            ).poll_once()
            record = journal.get(task["id"])
            assert record is not None and record.reported_at is not None

        final = api.get(f"/tasks/{task['id']}", headers=_headers("coordinator")).json()
        assert final["status"] == "succeeded"
        assert final["output"]["status"] == "verified"
        events = api.get(
            f"/tasks/{task['id']}/events", headers=_headers("coordinator"),
        ).json()
        assert [event["type"] for event in events] == [
            "task.created", "task.approval_requested", "approval.requested",
            "approval.approved", "task.released", "task.started", "task.completed",
        ]
        assert events[4]["causation_event_id"] == events[3]["id"]

        manifest_path = _private_json(tmp_path / "operations.json", {"sources": [
            {"source_id": "approval", "kind": "approval_intent", "path": str(intent_path), "logical_owner": "mateus-e2e"},
            {"source_id": "release", "kind": "release", "path": str(release_path), "logical_owner": SUBJECT},
            {"source_id": "runner", "kind": "runner", "path": str(runner_path), "logical_owner": "dex-e2e"},
            {"source_id": "outbox", "kind": "outbox", "path": str(outbox_path), "logical_owner": SUBJECT},
        ]})
        projection_bus = AgentBusReadClient(
            "http://127.0.0.1:8000", TOKENS["projection"], transport=_transport(api),
        )
        projection_bus.verify_read_only()
        projection_app = create_projection_app(
            projection=OperationalProjection(
                ProjectionManifest.from_file(manifest_path), projection_bus,
            ), reader_token=TOKENS["projection_reader"],
        )
        def project(request: httpx.Request) -> httpx.Response:
            with TestClient(projection_app) as projection_api:
                response = projection_api.get(
                    request.url.path, params=dict(request.url.params),
                    headers={"Authorization": f"Bearer {TOKENS['projection_reader']}"},
                )
            return httpx.Response(
                response.status_code, content=response.content,
                headers=dict(response.headers), request=request,
            )

        projection_transport = httpx.MockTransport(project)
        gateway = create_gateway(
            agentbus_url="http://127.0.0.1:8000", token=TOKENS["viewer"],
            transport=_transport(api), allowed_host="127.0.0.1:5173",
            operational_url="http://127.0.0.1:8003",
            operational_token=TOKENS["projection_reader"],
            operational_transport=projection_transport,
        )
        with TestClient(gateway, base_url="http://127.0.0.1:5173") as panel:
            visible = panel.get(f"/api/tasks/{task['id']}")
            operations = panel.get(f"/api/operations?task_id={task['id']}")
            trace = panel.get(f"/api/correlations/{correlation_id}/trace")
        projection_bus.close()
        assert visible.json()["status"] == "succeeded"
        assert {item["kind"] for item in operations.json()["items"]} >= {
            "approval_intent", "release", "runner",
        }
        assert len(trace.json()["events"]) == 7
