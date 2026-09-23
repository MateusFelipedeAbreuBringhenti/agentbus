import sqlite3
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_app


@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "agentbus.sqlite3"


@pytest.fixture
def client(database_path):
    app = create_app(database_path)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def correlation_id():
    return str(uuid4())


def task_payload(correlation_id: str) -> dict:
    return {
        "type": "prepare_release",
        "title": "Prepare AgentBus release",
        "description": "Build the local release candidate.",
        "input": {"version": "0.1.0"},
        "requested_by": "orion",
        "assigned_to": "dex",
        "correlation_id": correlation_id,
    }


def test_create_task_persists_ready_task_and_first_event(client, correlation_id):
    response = client.post(
        "/tasks",
        json=task_payload(correlation_id),
        headers={"Idempotency-Key": "create-release-001"},
    )

    assert response.status_code == 201
    assert response.headers["Idempotency-Replayed"] == "false"
    created = response.json()
    assert created["status"] == "ready"
    assert created["version"] == 1
    assert created["correlation_id"] == correlation_id

    persisted_response = client.get(f"/tasks/{created['id']}")
    assert persisted_response.status_code == 200
    assert persisted_response.json() == created

    events_response = client.get(f"/tasks/{created['id']}/events")
    assert events_response.status_code == 200
    events = events_response.json()
    assert len(events) == 1
    assert events[0]["task_id"] == created["id"]
    assert events[0]["type"] == "task.created"
    assert events[0]["sequence"] == 1
    assert events[0]["event_index"] == 0
    assert events[0]["correlation_id"] == correlation_id
    assert events[0]["causation_event_id"] is None
    assert events[0]["data"]["status"] == "ready"


def test_same_key_and_body_replays_original_response(client, correlation_id):
    payload = task_payload(correlation_id)
    headers = {"Idempotency-Key": "stable-create-key"}

    first = client.post("/tasks", json=payload, headers=headers)
    replay = client.post("/tasks", json=payload, headers=headers)

    assert first.status_code == 201
    assert replay.status_code == 201
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json() == first.json()

    events = client.get(f"/tasks/{first.json()['id']}/events").json()
    assert len(events) == 1


def test_same_key_with_different_body_returns_conflict(client, correlation_id):
    headers = {"Idempotency-Key": "colliding-create-key"}
    first_payload = task_payload(correlation_id)
    different_payload = {**first_payload, "title": "A different task"}

    first = client.post("/tasks", json=first_payload, headers=headers)
    conflict = client.post("/tasks", json=different_payload, headers=headers)

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "idempotency_key_reused"


def test_different_requesters_can_reuse_same_idempotency_key(client, correlation_id):
    headers = {"Idempotency-Key": "shared-between-identities"}
    orion_payload = task_payload(correlation_id)
    dex_payload = {**orion_payload, "requested_by": "dex"}

    orion_response = client.post("/tasks", json=orion_payload, headers=headers)
    dex_response = client.post("/tasks", json=dex_payload, headers=headers)

    assert orion_response.status_code == 201
    assert dex_response.status_code == 201
    assert orion_response.json()["id"] != dex_response.json()["id"]
    assert orion_response.json()["requested_by"] == "orion"
    assert dex_response.json()["requested_by"] == "dex"


def test_existing_event_in_same_correlation_is_accepted_as_cause(client, correlation_id):
    root = client.post(
        "/tasks",
        json=task_payload(correlation_id),
        headers={"Idempotency-Key": "causal-root"},
    )
    root_event = client.get(f"/tasks/{root.json()['id']}/events").json()[0]
    child_payload = {
        **task_payload(correlation_id),
        "title": "Causally related task",
        "causation_event_id": root_event["id"],
    }

    child = client.post(
        "/tasks",
        json=child_payload,
        headers={"Idempotency-Key": "causal-child"},
    )

    assert child.status_code == 201
    child_event = client.get(f"/tasks/{child.json()['id']}/events").json()[0]
    assert child_event["causation_event_id"] == root_event["id"]
    assert child_event["correlation_id"] == correlation_id


def test_missing_causation_event_returns_domain_error_without_partial_state(
    client,
    database_path,
    correlation_id,
):
    payload = {
        **task_payload(correlation_id),
        "causation_event_id": str(uuid4()),
    }

    response = client.post(
        "/tasks",
        json=payload,
        headers={"Idempotency-Key": "missing-cause"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "causation_event_not_found"
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM idempotency_records").fetchone()[0] == 0


def test_cause_from_another_correlation_returns_conflict_without_partial_state(
    client,
    database_path,
    correlation_id,
):
    root = client.post(
        "/tasks",
        json=task_payload(correlation_id),
        headers={"Idempotency-Key": "other-correlation-root"},
    )
    root_event = client.get(f"/tasks/{root.json()['id']}/events").json()[0]
    payload = {
        **task_payload(str(uuid4())),
        "causation_event_id": root_event["id"],
    }

    response = client.post(
        "/tasks",
        json=payload,
        headers={"Idempotency-Key": "mismatched-cause"},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "causation_correlation_mismatch"
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM idempotency_records").fetchone()[0] == 1


def test_event_failure_rolls_back_task_event_and_idempotency(
    client,
    database_path,
    correlation_id,
):
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER force_event_failure
            BEFORE INSERT ON events
            BEGIN
                SELECT RAISE(ABORT, 'forced event failure');
            END
            """
        )

    response = client.post(
        "/tasks",
        json=task_payload(correlation_id),
        headers={"Idempotency-Key": "must-roll-back"},
    )

    assert response.status_code == 500
    assert response.json()["detail"]["code"] == "database_error"

    with sqlite3.connect(database_path) as connection:
        task_count = connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
        event_count = connection.execute("SELECT count(*) FROM events").fetchone()[0]
        idempotency_count = connection.execute(
            "SELECT count(*) FROM idempotency_records"
        ).fetchone()[0]

    assert task_count == 0
    assert event_count == 0
    assert idempotency_count == 0
