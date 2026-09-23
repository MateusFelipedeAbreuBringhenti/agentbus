from concurrent.futures import ThreadPoolExecutor
import sqlite3
from threading import Barrier
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_app


@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "agentbus-execution.sqlite3"


@pytest.fixture
def client(database_path):
    app = create_app(database_path)
    with TestClient(app) as test_client:
        yield test_client


def create_ready_task(client, *, correlation_id: str | None = None) -> dict:
    correlation_id = correlation_id or str(uuid4())
    response = client.post(
        "/tasks",
        json={
            "type": "execute_work",
            "title": "Execute a unit of work",
            "input": {"work": "slice-002"},
            "requested_by": "orion",
            "correlation_id": correlation_id,
        },
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 201
    assert response.headers["etag"] == '"v1"'
    return response.json()


def claim(client, task_id: str, *, key: str = "claim-001", cause: str | None = None):
    body = {"agent_id": "dex"}
    if cause is not None:
        body["causation_event_id"] = cause
    return client.post(
        f"/tasks/{task_id}/claim",
        json=body,
        headers={"Idempotency-Key": key},
    )


def test_claim_atomically_moves_ready_task_to_running(client):
    task = create_ready_task(client)

    response = claim(client, task["id"])

    assert response.status_code == 200
    assert response.headers["etag"] == '"v2"'
    assert response.json()["status"] == "running"
    assert response.json()["assigned_to"] == "dex"
    assert response.json()["started_at"] is not None
    assert response.json()["version"] == 2


def test_two_concurrent_claims_only_allow_one_agent(client):
    task = create_ready_task(client)
    barrier = Barrier(2)

    def attempt(agent_id: str):
        barrier.wait()
        return client.post(
            f"/tasks/{task['id']}/claim",
            json={"agent_id": agent_id},
            headers={"Idempotency-Key": f"claim-{agent_id}"},
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(attempt, ["dex-a", "dex-b"]))

    assert sorted(response.status_code for response in responses) == [200, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["detail"]["code"] == "task_state_conflict"
    persisted = client.get(f"/tasks/{task['id']}").json()
    assert persisted["status"] == "running"
    assert persisted["assigned_to"] in {"dex-a", "dex-b"}


def test_claim_replay_returns_original_response(client):
    task = create_ready_task(client)

    first = claim(client, task["id"], key="replay-claim")
    replay = claim(client, task["id"], key="replay-claim")

    assert first.status_code == 200
    assert replay.status_code == 200
    assert first.json() == replay.json()
    assert replay.headers["etag"] == '"v2"'
    assert replay.headers["Idempotency-Replayed"] == "true"


def test_complete_moves_running_task_to_succeeded(client):
    task = create_ready_task(client)
    running = claim(client, task["id"]).json()

    response = client.post(
        f"/tasks/{task['id']}/complete",
        json={"output": {"artifact": "release.tar.gz"}},
        headers={"Idempotency-Key": "complete-001", "If-Match": '"v2"'},
    )

    assert running["version"] == 2
    assert response.status_code == 200
    assert response.headers["etag"] == '"v3"'
    assert response.json()["status"] == "succeeded"
    assert response.json()["output"] == {"artifact": "release.tar.gz"}
    assert response.json()["finished_at"] is not None
    assert response.json()["version"] == 3


def test_fail_moves_running_task_to_failed(client):
    task = create_ready_task(client)
    claim(client, task["id"])

    response = client.post(
        f"/tasks/{task['id']}/fail",
        json={"failure_code": "execution_error", "failure_message": "Worker failed."},
        headers={"Idempotency-Key": "fail-001", "If-Match": '"v2"'},
    )

    assert response.status_code == 200
    assert response.headers["etag"] == '"v3"'
    assert response.json()["status"] == "failed"
    assert response.json()["failure_code"] == "execution_error"
    assert response.json()["failure_message"] == "Worker failed."
    assert response.json()["finished_at"] is not None
    events = client.get(f"/tasks/{task['id']}/events").json()
    assert [event["sequence"] for event in events] == [1, 2, 3]
    assert events[-1]["type"] == "task.failed"


def test_complete_replay_returns_original_response_without_new_mutation(client):
    task = create_ready_task(client)
    claim(client, task["id"])
    body = {"output": {"artifact": "release.tar.gz"}}
    headers = {"Idempotency-Key": "replay-complete", "If-Match": '"v2"'}

    first = client.post(f"/tasks/{task['id']}/complete", json=body, headers=headers)
    replay = client.post(f"/tasks/{task['id']}/complete", json=body, headers=headers)

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.headers["etag"] == replay.headers["etag"] == '"v3"'
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert client.get(f"/tasks/{task['id']}").json() == first.json()
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 3


def test_fail_replay_returns_original_response_without_new_mutation(client):
    task = create_ready_task(client)
    claim(client, task["id"])
    body = {"failure_code": "execution_error", "failure_message": "Worker failed."}
    headers = {"Idempotency-Key": "replay-fail", "If-Match": '"v2"'}

    first = client.post(f"/tasks/{task['id']}/fail", json=body, headers=headers)
    replay = client.post(f"/tasks/{task['id']}/fail", json=body, headers=headers)

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.headers["etag"] == replay.headers["etag"] == '"v3"'
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert client.get(f"/tasks/{task['id']}").json() == first.json()
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 3


def test_terminal_command_idempotency_collision_precedes_terminal_state_rejection(client):
    task = create_ready_task(client)
    claim(client, task["id"])
    endpoint = f"/tasks/{task['id']}/complete"
    key = "colliding-complete"
    first = client.post(
        endpoint,
        json={"output": {"result": "original"}},
        headers={"Idempotency-Key": key, "If-Match": '"v2"'},
    )

    different_content = client.post(
        endpoint,
        json={"output": {"result": "different"}},
        headers={"Idempotency-Key": key, "If-Match": '"v2"'},
    )
    different_if_match = client.post(
        endpoint,
        json={"output": {"result": "original"}},
        headers={"Idempotency-Key": key, "If-Match": '"v3"'},
    )

    assert first.status_code == 200
    for collision in (different_content, different_if_match):
        assert collision.status_code == 409
        assert collision.json()["detail"]["code"] == "idempotency_key_reused"
    assert client.get(f"/tasks/{task['id']}").json()["version"] == 3
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 3


def test_wrong_if_match_returns_version_conflict(client):
    task = create_ready_task(client)
    claim(client, task["id"])

    response = client.post(
        f"/tasks/{task['id']}/complete",
        json={"output": {}},
        headers={"Idempotency-Key": "wrong-version", "If-Match": '"v1"'},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "task_version_conflict",
        "message": "Task version differs from If-Match.",
        "expected": 1,
        "actual": 2,
    }


@pytest.mark.parametrize("command", ["complete", "fail"])
def test_if_match_is_required_for_terminal_commands(client, command):
    task = create_ready_task(client)
    claim(client, task["id"])
    body = (
        {"output": {}}
        if command == "complete"
        else {"failure_code": "error", "failure_message": "Failed."}
    )

    response = client.post(
        f"/tasks/{task['id']}/{command}",
        json=body,
        headers={"Idempotency-Key": f"missing-if-match-{command}"},
    )

    assert response.status_code == 428
    assert response.json()["detail"]["code"] == "if_match_required"


def test_command_incompatible_with_current_state_returns_conflict(client):
    task = create_ready_task(client)

    response = client.post(
        f"/tasks/{task['id']}/complete",
        json={"output": {}},
        headers={"Idempotency-Key": "complete-ready", "If-Match": '"v1"'},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "task_state_conflict"
    assert response.json()["detail"]["expected"] == "running"
    assert response.json()["detail"]["actual"] == "ready"


def test_events_continue_sequence_and_preserve_correlation_and_causation(client):
    correlation_id = str(uuid4())
    task = create_ready_task(client, correlation_id=correlation_id)
    created_event = client.get(f"/tasks/{task['id']}/events").json()[0]
    running = claim(client, task["id"], cause=created_event["id"]).json()
    started_event = client.get(f"/tasks/{task['id']}/events").json()[1]

    completed = client.post(
        f"/tasks/{task['id']}/complete",
        json={"output": {}, "causation_event_id": started_event["id"]},
        headers={"Idempotency-Key": "causal-complete", "If-Match": '"v2"'},
    )

    assert running["correlation_id"] == correlation_id
    assert completed.status_code == 200
    events = client.get(f"/tasks/{task['id']}/events").json()
    assert [event["sequence"] for event in events] == [1, 2, 3]
    assert [event["type"] for event in events] == [
        "task.created",
        "task.started",
        "task.completed",
    ]
    assert all(event["correlation_id"] == correlation_id for event in events)
    assert events[1]["causation_event_id"] == events[0]["id"]
    assert events[2]["causation_event_id"] == events[1]["id"]


def test_claim_replay_after_completion_returns_original_running_representation(client):
    task = create_ready_task(client)
    original_claim = claim(client, task["id"], key="claim-before-complete")
    client.post(
        f"/tasks/{task['id']}/complete",
        json={"output": {"done": True}},
        headers={"Idempotency-Key": "complete-after-claim", "If-Match": '"v2"'},
    )

    replay = claim(client, task["id"], key="claim-before-complete")

    assert replay.status_code == 200
    assert replay.json() == original_claim.json()
    assert replay.json()["status"] == "running"
    assert replay.json()["version"] == 2
    assert client.get(f"/tasks/{task['id']}").json()["status"] == "succeeded"


def test_event_failure_rolls_back_claim_and_idempotency(client, database_path):
    task = create_ready_task(client)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER force_started_event_failure
            BEFORE INSERT ON events
            WHEN NEW.type = 'task.started'
            BEGIN
                SELECT RAISE(ABORT, 'forced started event failure');
            END
            """
        )

    response = claim(client, task["id"], key="claim-must-roll-back")

    assert response.status_code == 500
    persisted = client.get(f"/tasks/{task['id']}").json()
    assert persisted["status"] == "ready"
    assert persisted["assigned_to"] is None
    assert persisted["started_at"] is None
    assert persisted["version"] == 1
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM idempotency_records").fetchone()[0] == 1
