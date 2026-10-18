import sqlite3
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_test_app as create_app


@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "causal.sqlite3"


@pytest.fixture
def client(database_path):
    with TestClient(create_app(database_path)) as client:
        yield client


def succeeded_task(client, correlation_id=None):
    correlation_id = correlation_id or str(uuid4())
    created = client.post(
        "/tasks",
        json={
            "type": "producer", "title": "Produce", "input": {"a": "A"},
            "requested_by": "orion-ops", "assigned_to": "worker-a",
            "correlation_id": correlation_id,
        },
        headers={"Idempotency-Key": str(uuid4())},
    ).json()
    client.post(
        f"/tasks/{created['id']}/claim",
        json={"agent_id": "worker-a"},
        headers={"Idempotency-Key": str(uuid4())},
    )
    completed = client.post(
        f"/tasks/{created['id']}/complete",
        json={"output": {"artifact": {"content": {"value": 3}, "sha256": "abc"}}},
        headers={"Idempotency-Key": str(uuid4()), "If-Match": '"v2"'},
    ).json()
    event = client.get(f"/tasks/{created['id']}/events").json()[-1]
    return completed, event


def successor_body(title="Consume"):
    return {
        "type": "consumer", "title": title,
        "input": {"artifact": {"content": {"value": 3}, "sha256": "abc"}},
        "requested_by": "orion-ops", "assigned_to": "worker-b",
    }


def test_successor_inherits_server_owned_correlation_and_completion_cause(client):
    parent, completed_event = succeeded_task(client)
    response = client.post(
        f"/tasks/{parent['id']}/successors",
        json=successor_body(), headers={"Idempotency-Key": "successor-1"},
    )
    assert response.status_code == 201
    child = response.json()
    assert child["status"] == "ready"
    assert child["correlation_id"] == parent["correlation_id"]
    events = client.get(f"/tasks/{child['id']}/events").json()
    assert events[0]["causation_event_id"] == completed_event["id"]
    assert events[0]["data"]["predecessor_task_id"] == parent["id"]


def test_successor_schema_rejects_client_owned_causality(client):
    parent, _ = succeeded_task(client)
    for field in ("correlation_id", "causation_event_id"):
        response = client.post(
            f"/tasks/{parent['id']}/successors",
            json={**successor_body(), field: str(uuid4())},
            headers={"Idempotency-Key": f"forbidden-{field}"},
        )
        assert response.status_code == 422


def test_successor_requires_succeeded_predecessor(client):
    correlation = str(uuid4())
    parent = client.post(
        "/tasks",
        json={
            "type": "producer", "title": "Not done", "input": {},
            "requested_by": "orion", "correlation_id": correlation,
        },
        headers={"Idempotency-Key": "not-done"},
    ).json()
    response = client.post(
        f"/tasks/{parent['id']}/successors", json=successor_body(),
        headers={"Idempotency-Key": "too-early"},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "predecessor_not_succeeded"


def test_successor_replay_and_collision(client):
    parent, _ = succeeded_task(client)
    url = f"/tasks/{parent['id']}/successors"
    headers = {"Idempotency-Key": "stable-successor"}
    first = client.post(url, json=successor_body(), headers=headers)
    client.post(
        f"/tasks/{first.json()['id']}/claim", json={"agent_id": "worker-b"},
        headers={"Idempotency-Key": "advance-child"},
    )
    replay = client.post(url, json=successor_body(), headers=headers)
    collision = client.post(url, json=successor_body("Other"), headers=headers)
    assert replay.status_code == 201
    assert replay.json() == first.json()
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert collision.status_code == 409
    assert collision.json()["detail"]["code"] == "idempotency_key_reused"


def test_event_insert_failure_rolls_back_task_and_idempotency(client, database_path):
    parent, _ = succeeded_task(client)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            f"""CREATE TRIGGER reject_successor_event
            BEFORE INSERT ON events
            WHEN NEW.task_id != '{parent["id"]}'
            BEGIN SELECT RAISE(ABORT, 'forced event failure'); END"""
        )
    response = client.post(
        f"/tasks/{parent['id']}/successors", json=successor_body(),
        headers={"Idempotency-Key": "rollback-event"},
    )
    assert response.status_code == 500
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        assert connection.execute(
            "SELECT count(*) FROM idempotency_records WHERE key = 'rollback-event'"
        ).fetchone()[0] == 0


def test_invalid_completion_event_rolls_back_successor(client, database_path):
    parent, completed_event = succeeded_task(client)
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP TRIGGER events_are_append_only_on_update")
        connection.execute(
            "UPDATE events SET data_json = ? WHERE id = ?",
            ('{"version":999}', completed_event["id"]),
        )
    response = client.post(
        f"/tasks/{parent['id']}/successors", json=successor_body(),
        headers={"Idempotency-Key": "broken-event"},
    )
    assert response.status_code == 409
    trace = client.get(f"/correlations/{parent['correlation_id']}/trace").json()
    assert len(trace["tasks"]) == 1


def test_trace_is_deterministic_factual_and_isolated(client):
    parent, _ = succeeded_task(client)
    child = client.post(
        f"/tasks/{parent['id']}/successors", json=successor_body(),
        headers={"Idempotency-Key": "trace-child"},
    ).json()
    succeeded_task(client)
    trace = client.get(f"/correlations/{parent['correlation_id']}/trace").json()
    assert trace["correlation_id"] == parent["correlation_id"]
    assert [task["id"] for task in trace["tasks"]] == [parent["id"], child["id"]]
    assert "complete" not in trace
    assert {event["task_id"] for event in trace["events"]} == {parent["id"], child["id"]}
