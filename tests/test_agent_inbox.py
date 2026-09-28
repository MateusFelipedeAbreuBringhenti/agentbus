from concurrent.futures import ThreadPoolExecutor
import sqlite3
from threading import Barrier
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus import database as storage
from agentbus.app import create_app
from agentbus.database import Database
from agentbus.models import TaskCreate
from agentbus.service import create_task as create_task_record


@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "agentbus-inbox.sqlite3"


@pytest.fixture
def client(database_path):
    with TestClient(create_app(database_path)) as api:
        yield api


def create_task(client, *, assigned_to="dex", correlation_id=None, title="Work"):
    body = {
        "type": "work",
        "title": title,
        "requested_by": "orion",
        "correlation_id": correlation_id or str(uuid4()),
    }
    if assigned_to is not None:
        body["assigned_to"] = assigned_to
    response = client.post(
        "/tasks",
        json=body,
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 201
    return response.json()


def claim(client, task, *, agent_id="dex", key=None):
    return client.post(
        f"/tasks/{task['id']}/claim",
        json={"agent_id": agent_id},
        headers={"Idempotency-Key": key or str(uuid4())},
    )


def request_approval(client, task, *, assigned_to="mateus", key=None):
    response = client.post(
        f"/tasks/{task['id']}/request-approval",
        json={
            "gate": "deployment.production",
            "request_reason": "Human decision required.",
            "requested_by": "orion",
            "assigned_to": assigned_to,
        },
        headers={
            "Idempotency-Key": key or str(uuid4()),
            "If-Match": f'"v{task["version"]}"',
        },
    )
    assert response.status_code == 200
    return response.json()


def inbox(client, agent_id):
    response = client.get(f"/agents/{agent_id}/inbox")
    assert response.status_code == 200
    return response.json()


def test_inbox_isolates_identities_and_excludes_unassigned_work(client):
    dex = create_task(client, assigned_to="dex", title="Dex work")
    orion = create_task(client, assigned_to="orion", title="Orion work")
    unassigned = create_task(client, assigned_to=None, title="Open work")

    dex_items = inbox(client, "dex")["items"]
    orion_items = inbox(client, "orion")["items"]

    assert [item["task"]["id"] for item in dex_items] == [dex["id"]]
    assert [item["task"]["id"] for item in orion_items] == [orion["id"]]
    assert unassigned["id"] not in {
        item["task"]["id"] for item in dex_items + orion_items
    }


@pytest.mark.parametrize("terminal", ["complete", "fail"])
def test_task_moves_through_inbox_and_etag_drives_terminal_command(client, terminal):
    task = create_task(client)
    ready = inbox(client, "dex")["items"]
    assert len(ready) == 1
    assert ready[0]["kind"] == "task"
    assert ready[0]["etag"] == '"v1"'
    assert ready[0]["available_actions"] == ["claim"]
    assert ready[0]["task"]["correlation_id"] == task["correlation_id"]

    claimed = claim(client, task).json()
    running = inbox(client, "dex")["items"]
    assert len(running) == 1
    assert running[0]["task"]["status"] == "running"
    assert running[0]["task"]["version"] == claimed["version"] == 2
    assert running[0]["etag"] == '"v2"'
    assert running[0]["available_actions"] == ["complete", "fail"]

    body = (
        {"output": {"done": True}}
        if terminal == "complete"
        else {"failure_code": "execution_error", "failure_message": "Failed."}
    )
    completed = client.post(
        f"/tasks/{task['id']}/{terminal}",
        json=body,
        headers={
            "Idempotency-Key": f"{terminal}-from-inbox",
            "If-Match": running[0]["etag"],
        },
    )
    assert completed.status_code == 200
    assert inbox(client, "dex")["items"] == []


def test_unassigned_task_appears_only_after_agent_claims_it(client):
    task = create_task(client, assigned_to=None)
    assert inbox(client, "dex")["items"] == []

    response = claim(client, task)

    assert response.status_code == 200
    items = inbox(client, "dex")["items"]
    assert [item["task"]["id"] for item in items] == [task["id"]]
    assert items[0]["task"]["status"] == "running"


def test_reserved_task_cannot_be_claimed_by_another_identity(client):
    task = create_task(client, assigned_to="dex")

    response = claim(client, task, agent_id="orion", key="wrong-agent")

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "task_assignment_conflict",
        "message": "Task is reserved for another agent identity.",
        "requested": "orion",
        "assigned_to": "dex",
    }
    assert inbox(client, "dex")["items"][0]["task"]["status"] == "ready"
    assert inbox(client, "orion")["items"] == []


def test_independent_connections_report_concurrent_claim_loser_as_running(
    client, database_path
):
    task = create_task(client, assigned_to=None)
    barrier = Barrier(2)
    with TestClient(create_app(database_path)) as other:
        def attempt(api, agent_id):
            barrier.wait(timeout=5)
            return claim(
                api,
                task,
                agent_id=agent_id,
                key=f"claim-{agent_id}",
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(
                executor.map(
                    lambda arguments: attempt(*arguments),
                    [(client, "dex"), (other, "orion")],
                )
            )

    assert sorted(response.status_code for response in responses) == [200, 409]
    winner = next(response for response in responses if response.status_code == 200)
    loser = next(response for response in responses if response.status_code == 409)
    assert loser.json()["detail"] == {
        "code": "task_state_conflict",
        "message": "Task is not in the state required by this command.",
        "expected": "ready",
        "actual": "running",
    }
    persisted = client.get(f"/tasks/{task['id']}").json()
    assert persisted == winner.json()
    assert persisted["status"] == "running"
    assert persisted["assigned_to"] in {"dex", "orion"}


def test_independent_connections_same_agent_and_key_write_once_then_replay(
    client, database_path
):
    task = create_task(client, assigned_to="dex")
    barrier = Barrier(2)
    with TestClient(create_app(database_path)) as other:
        def attempt(api):
            barrier.wait(timeout=5)
            return claim(api, task, agent_id="dex", key="same-concurrent-claim")

        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(attempt, [client, other]))

    assert [response.status_code for response in responses] == [200, 200]
    assert responses[0].json() == responses[1].json()
    assert sorted(
        response.headers["Idempotency-Replayed"] for response in responses
    ) == ["false", "true"]
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM events WHERE task_id=? AND type='task.started'",
            (task["id"],),
        ).fetchone() == (1,)
        assert connection.execute(
            """SELECT count(*) FROM idempotency_records
            WHERE key='same-concurrent-claim'"""
        ).fetchone() == (1,)


@pytest.mark.parametrize("decision", ["approve", "reject"])
def test_pending_approval_disappears_after_decision(client, decision):
    task = create_task(client)
    approval = request_approval(client, task)["approval"]
    items = inbox(client, "mateus")["items"]
    assert len(items) == 1
    assert items[0]["kind"] == "approval"
    assert items[0]["approval"]["id"] == approval["id"]
    assert items[0]["approval"]["correlation_id"] == task["correlation_id"]
    assert items[0]["etag"] == '"v1"'
    assert items[0]["available_actions"] == ["approve", "reject"]

    response = client.post(
        f"/approvals/{approval['id']}/{decision}",
        json={"decided_by": "mateus"},
        headers={
            "Idempotency-Key": f"{decision}-from-inbox",
            "If-Match": items[0]["etag"],
        },
    )

    assert response.status_code == 200
    assert inbox(client, "mateus")["items"] == []


def test_unassigned_and_terminal_approvals_are_not_personal_attention(client):
    task = create_task(client)
    approval = request_approval(client, task, assigned_to=None)["approval"]
    assert inbox(client, "mateus")["items"] == []
    response = client.post(
        f"/approvals/{approval['id']}/approve",
        json={"decided_by": "mateus"},
        headers={"Idempotency-Key": "approve-unassigned", "If-Match": '"v1"'},
    )
    assert response.status_code == 200
    assert inbox(client, "mateus")["items"] == []


def test_approval_boundary_moves_attention_between_agent_inboxes(client):
    task = create_task(client, assigned_to="dex")
    requested = request_approval(client, task)
    approval = requested["approval"]
    assert inbox(client, "dex")["items"] == []
    assert inbox(client, "mateus")["items"][0]["approval"]["id"] == approval["id"]

    decision = client.post(
        f"/approvals/{approval['id']}/approve",
        json={"decided_by": "mateus"},
        headers={"Idempotency-Key": "approve-boundary", "If-Match": '"v1"'},
    )
    assert decision.status_code == 200
    assert inbox(client, "mateus")["items"] == []
    assert inbox(client, "dex")["items"] == []

    released = client.post(
        f"/tasks/{task['id']}/release",
        json={"approval_id": approval["id"], "actor_id": "orion"},
        headers={"Idempotency-Key": "release-boundary", "If-Match": '"v2"'},
    )
    assert released.status_code == 200
    dex_items = inbox(client, "dex")["items"]
    assert len(dex_items) == 1
    assert dex_items[0]["task"]["status"] == "ready"
    assert dex_items[0]["etag"] == '"v3"'


def test_inbox_order_is_oldest_attention_first_with_stable_tiebreakers(
    client, database_path
):
    correlation_id = str(uuid4())
    later = create_task(client, correlation_id=correlation_id, title="Later")
    first = create_task(client, correlation_id=correlation_id, title="First")
    approval_task = create_task(client, assigned_to="orion", correlation_id=correlation_id)
    approval = request_approval(client, approval_task, assigned_to="dex")["approval"]
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE tasks SET created_at='2026-01-02T00:00:00+00:00' WHERE id=?",
            (later["id"],),
        )
        connection.execute(
            "UPDATE tasks SET created_at='2026-01-01T00:00:00+00:00' WHERE id=?",
            (first["id"],),
        )
        connection.execute(
            "UPDATE approvals SET created_at='2026-01-01T00:00:00+00:00' WHERE id=?",
            (approval["id"],),
        )

    items = inbox(client, "dex")["items"]
    identities = [
        (item["kind"], item[item["kind"]]["id"])
        for item in items
    ]
    expected_tied = sorted(
        [("task", first["id"]), ("approval", approval["id"])],
        key=lambda value: (value[0], value[1]),
    )
    assert identities == [*expected_tied, ("task", later["id"])]
    assert len(identities) == len(set(identities))
    assert {item[item["kind"]]["correlation_id"] for item in items} == {
        correlation_id
    }


def test_existing_v1_assigned_task_survives_upgrade_and_appears_in_inbox(
    tmp_path, monkeypatch
):
    path = tmp_path / "legacy-inbox.sqlite3"
    migrations = storage.MIGRATIONS
    monkeypatch.setattr(storage, "MIGRATIONS", migrations[:1])
    database = Database(path)
    database.migrate()
    task = create_task_record(
        database,
        TaskCreate(
            type="legacy",
            title="Legacy assigned work",
            requested_by="orion",
            assigned_to="dex",
            correlation_id=uuid4(),
        ),
        "legacy-create",
    ).task
    monkeypatch.setattr(storage, "MIGRATIONS", migrations)

    with TestClient(create_app(path)) as api:
        items = inbox(api, "dex")["items"]

    assert len(items) == 1
    assert items[0]["task"]["id"] == str(task.id)
    assert items[0]["task"]["version"] == 1
    assert items[0]["etag"] == '"v1"'
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
