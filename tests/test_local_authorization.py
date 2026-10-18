import json
import sqlite3
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus import database as storage
from agentbus.app import create_app
from agentbus.auth import AuthRegistry, TestAuthenticator, token_digest
from agentbus.database import Database
from agentbus.models import TaskCreate
from agentbus.service import create_task
from agentbus.orion_ops import ControlResponse, CreateTaskInput, OrionOpsControlPlane
from agentbus.runner import AgentRunner, BusResponse, DeterministicWorkExecutor, RunnerJournal


class _AuthenticatedRunnerClient:
    authenticated_subject_id = "dex"

    def __init__(self, api):
        self.api = api

    def _call(self, method, path, **kwargs):
        headers = {**kwargs.pop("headers", {}), **_headers("dex")}
        response = self.api.request(method, path, headers=headers, **kwargs)
        return BusResponse(response.status_code, response.json(), dict(response.headers))

    def get_inbox(self, agent_id): return self._call("GET", f"/agents/{agent_id}/inbox")
    def get_task(self, task_id): return self._call("GET", f"/tasks/{task_id}")
    def claim_task(self, task_id, agent_id, key):
        return self._call("POST", f"/tasks/{task_id}/claim", json={"agent_id": agent_id},
                          headers={"Idempotency-Key": key})
    def complete_task(self, task_id, output, key, etag):
        return self._call("POST", f"/tasks/{task_id}/complete", json={"output": output},
                          headers={"Idempotency-Key": key, "If-Match": etag})
    def fail_task(self, task_id, code, message, key, etag):
        return self._call("POST", f"/tasks/{task_id}/fail",
                          json={"failure_code": code, "failure_message": message},
                          headers={"Idempotency-Key": key, "If-Match": etag})


class _AuthenticatedControlTransport:
    def __init__(self, api, subject_id):
        self.api = api
        self.authenticated_subject_id = subject_id
    def request(self, method, path, *, json=None, headers=None):
        response = self.api.request(method, path, json=json,
                                    headers={**(headers or {}), **_headers("orion")})
        return ControlResponse(response.status_code, response.json(), dict(response.headers))


TOKENS = {
    "orion": "orion-token-0000000000000000000000000000",
    "dex": "dex-token-000000000000000000000000000000",
    "mateus": "mateus-token-000000000000000000000000000",
    "viewer": "viewer-token-000000000000000000000000000",
}


def _registry(tmp_path, *, second_orion=False, coordinator_subject="orion"):
    entries = [
        ("coordinator-1", coordinator_subject, TOKENS["orion"], [
            "task:create", "task:create:delegated", "task:create:successor",
            "approval:request", "task:release", "task:read:any", "event:read:any",
            "trace:read:any", "approval:read:any",
        ]),
        ("runner-dex-1", "dex", TOKENS["dex"], [
            "inbox:read:self", "task:read:self", "task:claim:self",
            "task:complete:self", "task:fail:self",
        ]),
        ("human-mateus-1", "mateus", TOKENS["mateus"], [
            "inbox:read:self", "approval:read:self", "approval:decide:self",
        ]),
        ("control-center-1", "control-center", TOKENS["viewer"], [
            "task:read:any", "event:read:any", "trace:read:any", "approval:read:any",
        ]),
    ]
    if second_orion:
        entries.append(("coordinator-2", "orion", "rotated-token-00000000000000000000000000", entries[0][3]))
    path = tmp_path / "auth.json"
    path.write_text(json.dumps({"principals": [{
        "principal_id": pid, "subject_id": subject,
        "token_sha256": token_digest(token), "permissions": permissions,
    } for pid, subject, token, permissions in entries]}), encoding="utf-8")
    path.chmod(0o600)
    return AuthRegistry.from_file(path)


def _headers(identity, **extra):
    return {"Authorization": f"Bearer {TOKENS[identity]}", **extra}


def _payload(*, requested_by="orion", assigned_to="dex"):
    return {"type": "secure", "title": "Authorized work", "input": {},
            "requested_by": requested_by, "assigned_to": assigned_to,
            "correlation_id": str(uuid4())}


def test_authentication_is_required_and_invalid_token_is_rejected(tmp_path):
    with TestClient(create_app(tmp_path / "db.sqlite3", authenticator=_registry(tmp_path))) as api:
        missing = api.get("/tasks")
        invalid = api.get("/tasks", headers={"Authorization": "Bearer " + "x" * 32})
    assert missing.status_code == 401
    assert missing.json()["detail"]["code"] == "authentication_required"
    assert invalid.status_code == 401
    assert invalid.json()["detail"]["code"] == "invalid_credential"


def test_registry_is_an_immutable_startup_snapshot(tmp_path):
    registry = _registry(tmp_path)
    path = tmp_path / "auth.json"
    payload = json.loads(path.read_text())
    payload["principals"] = []
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert registry.authenticate("Bearer " + TOKENS["orion"]).subject_id == "orion"


def test_missing_production_config_fails_before_database_migration(tmp_path, monkeypatch):
    database = tmp_path / "must-not-be-created.sqlite3"
    monkeypatch.delenv("AGENTBUS_AUTH_CONFIG", raising=False)
    with pytest.raises(RuntimeError, match="AGENTBUS_AUTH_CONFIG is required"):
        with TestClient(create_app(database)):
            pass
    assert not database.exists()


def test_identity_mismatch_is_rejected_before_any_state(tmp_path):
    database = tmp_path / "db.sqlite3"
    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        response = api.post("/tasks", json=_payload(requested_by="mateus"), headers=_headers(
            "dex", **{"Idempotency-Key": "forged"}
        ))
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "permission_denied"
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM idempotency_records").fetchone()[0] == 0


def test_delegation_is_audited_without_becoming_authority(tmp_path):
    database = tmp_path / "db.sqlite3"
    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        created = api.post("/tasks", json=_payload(requested_by="mateus"), headers=_headers(
            "orion", **{"Idempotency-Key": "delegated"}
        ))
    assert created.status_code == 201
    assert created.json()["requested_by"] == "mateus"
    with sqlite3.connect(database) as db:
        event = db.execute("SELECT actor_id, authenticated_principal_id FROM events").fetchone()
        record = db.execute("SELECT authenticated_principal_id FROM idempotency_records").fetchone()
    assert event == ("orion", "coordinator-1")
    assert record == ("coordinator-1",)


def test_worker_cannot_claim_as_another_subject_or_read_other_inbox(tmp_path):
    with TestClient(create_app(tmp_path / "db.sqlite3", authenticator=_registry(tmp_path))) as api:
        created = api.post("/tasks", json=_payload(assigned_to="other"), headers=_headers(
            "orion", **{"Idempotency-Key": "create"}
        )).json()
        inbox = api.get("/agents/other/inbox", headers=_headers("dex"))
        claim = api.post(f"/tasks/{created['id']}/claim", json={"agent_id": "other"},
                         headers=_headers("dex", **{"Idempotency-Key": "claim"}))
    assert inbox.status_code == 403
    assert inbox.json()["detail"]["code"] == "principal_mismatch"
    assert claim.status_code == 403
    assert claim.json()["detail"]["code"] == "principal_mismatch"


def test_readonly_principal_cannot_mutate(tmp_path):
    with TestClient(create_app(tmp_path / "db.sqlite3", authenticator=_registry(tmp_path))) as api:
        assert api.get("/tasks", headers=_headers("viewer")).status_code == 200
        denied = api.post("/tasks", json=_payload(), headers=_headers(
            "viewer", **{"Idempotency-Key": "denied"}
        ))
    assert denied.status_code == 403
    assert denied.json()["detail"]["code"] == "permission_denied"


def test_historical_replay_requires_same_authorized_subject(tmp_path, monkeypatch):
    database = tmp_path / "legacy.sqlite3"
    migrations = storage.MIGRATIONS
    monkeypatch.setattr(storage, "MIGRATIONS", migrations[:3])
    db = Database(database)
    db.migrate()
    payload = TaskCreate.model_validate(_payload())
    original = create_task(db, payload, "legacy-key", TestAuthenticator.context).task
    monkeypatch.setattr(storage, "MIGRATIONS", migrations)

    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        replay = api.post("/tasks", json=payload.model_dump(mode="json"), headers=_headers(
            "orion", **{"Idempotency-Key": "legacy-key"}
        ))
        denied = api.post("/tasks", json=payload.model_dump(mode="json"), headers=_headers(
            "viewer", **{"Idempotency-Key": "legacy-key"}
        ))
    assert replay.status_code == 201
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["id"] == str(original.id)
    assert denied.status_code == 403
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT authenticated_principal_id FROM events"
        ).fetchone() == (None,)
        assert connection.execute(
            "SELECT authenticated_principal_id FROM idempotency_records"
        ).fetchone() == (None,)


def test_new_database_triggers_reject_unauthenticated_audit_rows(tmp_path):
    database = Database(tmp_path / "db.sqlite3")
    database.migrate()
    with database.transaction() as db:
        with pytest.raises(sqlite3.IntegrityError, match="authenticated principal required"):
            db.execute("""INSERT INTO idempotency_records(
                scope,key,request_hash,command_id,correlation_id,resource_type,resource_id,
                response_status,response_body,created_at,authenticated_principal_id
            ) VALUES ('s','k','h',?,?, 'task',?,200,'{}','now',NULL)""",
                       (str(uuid4()), str(uuid4()), str(uuid4())))


@pytest.mark.parametrize(("path", "body"), [
    ("/tasks", _payload()),
    (f"/tasks/{uuid4()}/successors", {"type": "x", "title": "x", "input": {}, "requested_by": "orion"}),
    (f"/tasks/{uuid4()}/claim", {"agent_id": "dex"}),
    (f"/tasks/{uuid4()}/complete", {"output": {}}),
    (f"/tasks/{uuid4()}/fail", {"failure_code": "x", "failure_message": "x"}),
    (f"/tasks/{uuid4()}/request-approval", {"gate": "g", "request_reason": "r", "requested_by": "orion"}),
    (f"/approvals/{uuid4()}/approve", {"decided_by": "mateus"}),
    (f"/approvals/{uuid4()}/reject", {"decided_by": "mateus"}),
    (f"/tasks/{uuid4()}/release", {"approval_id": str(uuid4()), "actor_id": "orion"}),
])
def test_every_mutation_rejects_missing_authentication(tmp_path, path, body):
    with TestClient(create_app(tmp_path / "db.sqlite3", authenticator=_registry(tmp_path))) as api:
        response = api.post(path, json=body, headers={"Idempotency-Key": "key", "If-Match": '"v1"'})
    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "authentication_required"


def test_only_assigned_human_can_decide_approval(tmp_path):
    with TestClient(create_app(tmp_path / "db.sqlite3", authenticator=_registry(tmp_path))) as api:
        task = api.post("/tasks", json=_payload(), headers=_headers(
            "orion", **{"Idempotency-Key": "create"}
        )).json()
        requested = api.post(f"/tasks/{task['id']}/request-approval", json={
            "gate": "deployment.production", "request_reason": "Human decision",
            "requested_by": "orion", "assigned_to": "mateus", "context": {},
        }, headers=_headers("orion", **{"Idempotency-Key": "request", "If-Match": '"v1"'}))
        approval = requested.json()["approval"]
        coordinator = api.post(f"/approvals/{approval['id']}/approve", json={
            "decided_by": "orion"
        }, headers=_headers("orion", **{"Idempotency-Key": "wrong", "If-Match": '"v1"'}))
        human = api.post(f"/approvals/{approval['id']}/approve", json={
            "decided_by": "mateus"
        }, headers=_headers("mateus", **{"Idempotency-Key": "right", "If-Match": '"v1"'}))
    assert coordinator.status_code == 403
    assert human.status_code == 200
    assert human.json()["decided_by"] == "mateus"


def test_rotated_credential_for_same_subject_replays_without_rewriting_audit(tmp_path):
    database = tmp_path / "db.sqlite3"
    registry = _registry(tmp_path, second_orion=True)
    payload = _payload()
    with TestClient(create_app(database, authenticator=registry)) as api:
        first = api.post("/tasks", json=payload, headers=_headers(
            "orion", **{"Idempotency-Key": "rotation"}
        ))
        replay = api.post("/tasks", json=payload, headers={
            "Authorization": "Bearer rotated-token-00000000000000000000000000",
            "Idempotency-Key": "rotation",
        })
    assert replay.status_code == 201
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json() == first.json()
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT authenticated_principal_id FROM events").fetchone() == (
            "coordinator-1",
        )


def test_authenticated_orion_ops_to_runner_heartbeat(tmp_path):
    database = tmp_path / "heartbeat.sqlite3"
    instance = uuid4()
    coordinator_subject = f"local-coordinator:{instance}"
    with TestClient(create_app(
        database, authenticator=_registry(tmp_path, coordinator_subject=coordinator_subject)
    )) as api:
        control = OrionOpsControlPlane(
            _AuthenticatedControlTransport(api, coordinator_subject), instance_id=instance
        )
        created = control.create_task(CreateTaskInput(
            type="heartbeat", title="Authenticated heartbeat", input={"value": 7},
            assigned_to="dex", correlation_id=uuid4(), idempotency_key="heartbeat-create",
        ))
        with RunnerJournal(tmp_path / "runner.sqlite3") as journal:
            runner = AgentRunner(
                agent_id="dex", client=_AuthenticatedRunnerClient(api), journal=journal,
                executor=DeterministicWorkExecutor(),
            )
            runner.poll_once()
        terminal = api.get(f"/tasks/{created.data['id']}", headers=_headers("viewer"))
        trace = api.get(
            f"/correlations/{created.data['correlation_id']}/trace", headers=_headers("viewer")
        )
    assert terminal.json()["status"] == "succeeded"
    assert [event["type"] for event in trace.json()["events"]] == [
        "task.created", "task.started", "task.completed"
    ]
    assert all(event["authenticated_principal_id"] for event in trace.json()["events"])
