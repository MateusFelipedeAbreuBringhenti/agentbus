import json
import sqlite3
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_app
from agentbus.auth import AuthRegistry, token_digest
from agentbus.execution_policy import ExecutionPolicy
from agentbus.runner import AgentRunner, BusResponse, DeterministicWorkExecutor, RunnerJournal


TOKENS = {
    "orion": "orion-execution-token-00000000000000000",
    "dex-1": "dex-execution-token-one-000000000000000",
    "dex-2": "dex-execution-token-two-000000000000000",
}


def registry(tmp_path):
    path = tmp_path / "principals.json"
    path.write_text(json.dumps({"principals": [
        {"principal_id": "orion-1", "subject_id": "orion",
         "token_sha256": token_digest(TOKENS["orion"]),
         "permissions": ["task:create", "task:read:any", "event:read:any"]},
        {"principal_id": "dex-1", "subject_id": "dex",
         "token_sha256": token_digest(TOKENS["dex-1"]),
         "permissions": ["inbox:read:self", "task:read:self", "task:claim:self",
                         "task:complete:self", "task:fail:self"]},
        {"principal_id": "dex-2", "subject_id": "dex",
         "token_sha256": token_digest(TOKENS["dex-2"]),
         "permissions": ["inbox:read:self", "task:read:self", "task:claim:self",
                         "task:complete:self", "task:fail:self"]},
    ]}), encoding="utf-8")
    path.chmod(0o600)
    return AuthRegistry.from_file(path)


def auth(name):
    return {"Authorization": f"Bearer {TOKENS[name]}"}


def create(api, task_type="allowed"):
    response = api.post("/tasks", json={
        "type": task_type, "title": "Controlled execution", "input": {},
        "requested_by": "orion", "assigned_to": "dex",
        "correlation_id": str(uuid4()),
    }, headers={**auth("orion"), "Idempotency-Key": str(uuid4())})
    assert response.status_code == 201
    return response.json()


def claim(api, task, execution_id=None, runner_instance_id=None, key="claim"):
    execution_id = execution_id or str(uuid4())
    runner_instance_id = runner_instance_id or str(uuid4())
    response = api.post(f"/tasks/{task['id']}/claim", json={
        "agent_id": "dex", "execution_id": execution_id,
        "runner_instance_id": runner_instance_id,
    }, headers={**auth("dex-1"), "Idempotency-Key": key})
    return response, execution_id, runner_instance_id


def policy(*task_types, policy_id="execution-v1"):
    return ExecutionPolicy(policy_id, policy_id.ljust(64, "0")[:64], {
        "dex": frozenset(task_types),
    })


def test_execution_policy_file_is_closed_and_requires_mode_0600(tmp_path):
    path = tmp_path / "execution-policy.json"
    path.write_text(json.dumps({
        "policy_id": "local-v1", "subjects": {"dex": ["allowed"]},
    }), encoding="utf-8")
    path.chmod(0o644)
    with pytest.raises(PermissionError):
        ExecutionPolicy.from_file(path)
    path.chmod(0o600)
    loaded = ExecutionPolicy.from_file(path)
    loaded.require("dex", "allowed")
    with pytest.raises(Exception):
        loaded.require("dex", "not-allowed")


def test_server_denies_direct_claim_for_task_type_outside_subject_policy(tmp_path):
    with TestClient(create_app(
        tmp_path / "bus.sqlite3", authenticator=registry(tmp_path),
        execution_policy=policy("allowed"),
    )) as api:
        task = create(api, "denied")
        response, _, _ = claim(api, task)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "task_type_not_allowed"


def test_claim_persists_execution_and_policy_provenance_in_task_and_event(tmp_path):
    with TestClient(create_app(
        tmp_path / "bus.sqlite3", authenticator=registry(tmp_path),
        execution_policy=policy("allowed"),
    )) as api:
        task = create(api)
        response, execution_id, instance_id = claim(api, task)
        events = api.get(f"/tasks/{task['id']}/events", headers=auth("orion")).json()
    assert response.status_code == 200
    body = response.json()
    assert body["execution_id"] == execution_id
    assert body["runner_instance_id"] == instance_id
    assert body["execution_subject_id"] == "dex"
    assert body["claimed_principal_id"] == "dex-1"
    started = next(event for event in events if event["type"] == "task.started")
    assert started["data"]["execution_id"] == execution_id
    assert started["data"]["policy_id"] == "execution-v1"


def test_execution_id_cannot_be_reused_for_another_task(tmp_path):
    with TestClient(create_app(
        tmp_path / "bus.sqlite3", authenticator=registry(tmp_path),
        execution_policy=policy("allowed"),
    )) as api:
        first, second = create(api), create(api)
        execution_id = str(uuid4())
        assert claim(api, first, execution_id=execution_id, key="first")[0].status_code == 200
        response = claim(api, second, execution_id=execution_id, key="second")[0]
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "execution_id_conflict"


@pytest.mark.parametrize(
    ("field", "code"),
    [("execution_id", "execution_id_conflict"),
     ("runner_instance_id", "execution_instance_conflict")],
)
def test_terminal_report_must_match_claimed_execution(tmp_path, field, code):
    with TestClient(create_app(
        tmp_path / "bus.sqlite3", authenticator=registry(tmp_path),
        execution_policy=policy("allowed"),
    )) as api:
        task = create(api)
        claimed, execution_id, instance_id = claim(api, task)
        assert claimed.status_code == 200
        command = {"output": {"ok": True}, "execution_id": execution_id,
                   "runner_instance_id": instance_id}
        command[field] = str(uuid4())
        response = api.post(f"/tasks/{task['id']}/complete", json=command, headers={
            **auth("dex-1"), "Idempotency-Key": "complete", "If-Match": '"v2"',
        })
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == code


def test_rotated_principal_replays_committed_result_after_policy_revocation(tmp_path):
    database = tmp_path / "bus.sqlite3"
    auth_registry = registry(tmp_path)
    execution_id = instance_id = task = None
    with TestClient(create_app(
        database, authenticator=auth_registry, execution_policy=policy("allowed"),
    )) as api:
        task = create(api)
        claimed, execution_id, instance_id = claim(api, task)
        command = {"output": {"ok": True}, "execution_id": execution_id,
                   "runner_instance_id": instance_id}
        first = api.post(f"/tasks/{task['id']}/complete", json=command, headers={
            **auth("dex-1"), "Idempotency-Key": "complete", "If-Match": '"v2"',
        })
        assert first.status_code == 200
    with TestClient(create_app(
        database, authenticator=auth_registry, execution_policy=policy(policy_id="execution-v2"),
    )) as api:
        replay = api.post(f"/tasks/{task['id']}/complete", json=command, headers={
            **auth("dex-2"), "Idempotency-Key": "complete", "If-Match": '"v2"',
        })
    assert replay.status_code == 200
    assert replay.headers["Idempotency-Replayed"] == "true"


def test_revoked_type_blocks_a_new_terminal_mutation(tmp_path):
    database = tmp_path / "bus.sqlite3"
    auth_registry = registry(tmp_path)
    with TestClient(create_app(
        database, authenticator=auth_registry, execution_policy=policy("allowed"),
    )) as api:
        task = create(api)
        claimed, execution_id, instance_id = claim(api, task)
        assert claimed.status_code == 200
    with TestClient(create_app(
        database, authenticator=auth_registry, execution_policy=policy(policy_id="execution-v2"),
    )) as api:
        response = api.post(f"/tasks/{task['id']}/complete", json={
            "output": {"ok": True}, "execution_id": execution_id,
            "runner_instance_id": instance_id,
        }, headers={**auth("dex-2"), "Idempotency-Key": "new-complete", "If-Match": '"v2"'})
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "task_type_not_allowed"


def test_historical_running_task_without_provenance_fails_closed(tmp_path):
    database = tmp_path / "bus.sqlite3"
    with TestClient(create_app(
        database, authenticator=registry(tmp_path), execution_policy=policy("allowed"),
    )) as api:
        task = create(api)
        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE tasks SET status='running', assigned_to='dex', version=2 WHERE id=?",
                (task["id"],),
            )
        response = api.post(f"/tasks/{task['id']}/complete", json={
            "output": {"unsafe": False}, "execution_id": str(uuid4()),
            "runner_instance_id": str(uuid4()),
        }, headers={**auth("dex-1"), "Idempotency-Key": "historical", "If-Match": '"v2"'})
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "execution_provenance_required"


class TerminalConflictClient:
    authenticated_subject_id = "dex"

    def __init__(self):
        self.task_id = str(uuid4())

    def get_inbox(self, _agent_id):
        return BusResponse(200, {"items": [{"kind": "task", "task": {
            "id": self.task_id, "type": "allowed", "status": "ready",
        }}]}, {})

    def claim_task(self, *_args):
        return BusResponse(409, {"detail": {"code": "task_state_conflict"}}, {})


def test_runner_persists_terminal_claim_conflict_instead_of_retrying(tmp_path):
    client = TerminalConflictClient()
    with RunnerJournal(tmp_path / "runner.sqlite3") as journal:
        runner = AgentRunner(
            agent_id="dex", client=client, journal=journal,
            executor=DeterministicWorkExecutor(), allowed_task_types=frozenset({"allowed"}),
        )
        runner.poll_once()
        record = journal.get(client.task_id)
        assert record is not None
        assert record.stopped_at is not None
        assert record.stop_code == "task_state_conflict"
        assert record.last_http_status == 409
        assert journal.pending() == []
