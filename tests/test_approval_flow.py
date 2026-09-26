from concurrent.futures import ThreadPoolExecutor
import sqlite3
from threading import Barrier
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_app


@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "agentbus-approval.sqlite3"


@pytest.fixture
def client(database_path):
    app = create_app(database_path)
    with TestClient(app) as test_client:
        yield test_client


def create_task(client, *, correlation_id: str | None = None) -> dict:
    response = client.post(
        "/tasks",
        json={
            "type": "deploy",
            "title": "Deploy release",
            "input": {"environment": "production"},
            "requested_by": "orion",
            "correlation_id": correlation_id or str(uuid4()),
        },
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 201
    return response.json()


def request_approval(
    client,
    task_id: str,
    *,
    key: str = "request-approval-001",
    cause: str | None = None,
):
    body = {
        "gate": "deployment.production",
        "request_reason": "Production deployment requires review.",
        "requested_by": "orion",
        "assigned_to": "mateus",
        "context": {"release": "0.1.0"},
    }
    if cause is not None:
        body["causation_event_id"] = cause
    return client.post(
        f"/tasks/{task_id}/request-approval",
        json=body,
        headers={"Idempotency-Key": key, "If-Match": '"v1"'},
    )


def decide(
    client,
    approval_id: str,
    decision: str,
    *,
    key: str,
    cause: str | None = None,
    decided_by: str = "mateus",
):
    body = {"decided_by": decided_by, "decision_reason": f"Decision: {decision}."}
    if cause is not None:
        body["causation_event_id"] = cause
    return client.post(
        f"/approvals/{approval_id}/{decision}",
        json=body,
        headers={"Idempotency-Key": key, "If-Match": '"v1"'},
    )


def release(
    client,
    task_id: str,
    approval_id: str,
    *,
    key: str = "release-001",
    cause: str | None = None,
):
    body = {"approval_id": approval_id, "actor_id": "orion"}
    if cause is not None:
        body["causation_event_id"] = cause
    return client.post(
        f"/tasks/{task_id}/release",
        json=body,
        headers={"Idempotency-Key": key, "If-Match": '"v2"'},
    )


def test_full_approved_flow_preserves_boundary_versions_and_event_history(
    client,
    database_path,
):
    task = create_task(client)
    created_event = client.get(f"/tasks/{task['id']}/events").json()[0]

    requested = request_approval(client, task["id"], cause=created_event["id"])
    assert requested.status_code == 200
    assert requested.headers["Task-ETag"] == '"v2"'
    assert requested.headers["Approval-ETag"] == '"v1"'
    requested_body = requested.json()
    approval = requested_body["approval"]
    assert requested_body["task"]["status"] == "waiting_approval"
    assert requested_body["task"]["version"] == 2
    assert requested_body["task"]["waiting_on_approval_id"] == approval["id"]
    assert approval["status"] == "pending"
    assert approval["version"] == 1
    assert approval["gate"] == "deployment.production"

    events = client.get(f"/tasks/{task['id']}/events").json()
    approved = decide(
        client,
        approval["id"],
        "approve",
        key="approve-001",
        cause=events[2]["id"],
    )
    assert approved.status_code == 200
    assert approved.headers["etag"] == '"v2"'
    assert approved.json()["status"] == "approved"
    assert approved.json()["version"] == 2
    blocked_task = client.get(f"/tasks/{task['id']}").json()
    assert blocked_task["status"] == "waiting_approval"
    assert blocked_task["version"] == 2
    assert blocked_task["waiting_on_approval_id"] == approval["id"]

    approval_event = client.get(f"/tasks/{task['id']}/events").json()[3]
    released = release(
        client,
        task["id"],
        approval["id"],
        cause=approval_event["id"],
    )
    assert released.status_code == 200
    assert released.headers["etag"] == '"v3"'
    assert released.json()["status"] == "ready"
    assert released.json()["version"] == 3
    assert released.json()["waiting_on_approval_id"] is None
    unchanged_approval = client.get(f"/approvals/{approval['id']}")
    assert unchanged_approval.json()["status"] == "approved"
    assert unchanged_approval.json()["version"] == 2

    events = client.get(f"/tasks/{task['id']}/events").json()
    assert [event["sequence"] for event in events] == [1, 2, 3, 4, 5]
    assert [event["type"] for event in events] == [
        "task.created",
        "task.approval_requested",
        "approval.requested",
        "approval.approved",
        "task.released",
    ]
    assert all(event["correlation_id"] == task["correlation_id"] for event in events)

    with sqlite3.connect(database_path) as connection:
        request_events = connection.execute(
            """
            SELECT command_id, event_index
            FROM events
            WHERE type IN ('task.approval_requested', 'approval.requested')
            ORDER BY sequence
            """
        ).fetchall()
    assert request_events[0][0] == request_events[1][0]
    assert [row[1] for row in request_events] == [0, 1]


def test_rejected_approval_leaves_task_waiting(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]

    rejected = decide(client, approval["id"], "reject", key="reject-001")

    assert rejected.status_code == 200
    assert rejected.json()["status"] == "rejected"
    assert rejected.json()["version"] == 2
    blocked_task = client.get(f"/tasks/{task['id']}").json()
    assert blocked_task["status"] == "waiting_approval"
    assert blocked_task["version"] == 2
    assert blocked_task["waiting_on_approval_id"] == approval["id"]


def test_concurrent_approve_and_reject_only_allow_one_decision(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    barrier = Barrier(2)

    def attempt(decision: str):
        barrier.wait()
        return decide(
            client,
            approval["id"],
            decision,
            key=f"concurrent-{decision}",
            decided_by=f"reviewer-{decision}",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(attempt, ["approve", "reject"]))

    assert sorted(response.status_code for response in responses) == [200, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["detail"]["code"] == "approval_state_conflict"
    persisted = client.get(f"/approvals/{approval['id']}").json()
    assert persisted["status"] in {"approved", "rejected"}
    assert persisted["version"] == 2
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 4


def test_release_rejects_missing_approval(client):
    task = create_task(client)
    request_approval(client, task["id"])

    response = release(client, task["id"], str(uuid4()), key="missing-approval")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "approval_not_found"


def test_release_rejects_approval_from_another_task(client):
    first_task = create_task(client)
    first_approval = request_approval(
        client,
        first_task["id"],
        key="first-request",
    ).json()["approval"]
    decide(client, first_approval["id"], "approve", key="first-approve")
    second_task = create_task(client)
    request_approval(client, second_task["id"], key="second-request")

    response = release(
        client,
        second_task["id"],
        first_approval["id"],
        key="wrong-task-release",
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "approval_task_mismatch"


def test_release_rejects_approval_from_another_correlation(client, database_path):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    decide(client, approval["id"], "approve", key="approve-before-corruption")
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE approvals SET correlation_id = ? WHERE id = ?",
            (str(uuid4()), approval["id"]),
        )

    response = release(client, task["id"], approval["id"], key="wrong-correlation")

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "approval_correlation_mismatch"


def test_release_rejects_non_approved_approval(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]

    response = release(client, task["id"], approval["id"], key="pending-release")

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "approval_state_conflict"
    assert response.json()["detail"]["expected"] == "approved"
    assert response.json()["detail"]["actual"] == "pending"


def test_request_approval_replay_is_exact_and_does_not_add_events(client):
    task = create_task(client)

    first = request_approval(client, task["id"], key="replay-request")
    replay = request_approval(client, task["id"], key="replay-request")

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.headers["Task-ETag"] == replay.headers["Task-ETag"] == '"v2"'
    assert first.headers["Approval-ETag"] == replay.headers["Approval-ETag"] == '"v1"'
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["task"]["waiting_on_approval_id"] == replay.json()["approval"]["id"]
    assert client.get(f"/tasks/{task['id']}").json()["waiting_on_approval_id"] == replay.json()["approval"]["id"]
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 3


@pytest.mark.parametrize("decision", ["approve", "reject"])
def test_approval_decision_replay_is_exact(client, decision):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    key = f"replay-{decision}"

    first = decide(client, approval["id"], decision, key=key)
    replay = decide(client, approval["id"], decision, key=key)

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.headers["etag"] == replay.headers["etag"] == '"v2"'
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert client.get(f"/tasks/{task['id']}").json()["waiting_on_approval_id"] == approval["id"]
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 4


def test_release_replay_is_exact_and_does_not_add_events(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    decide(client, approval["id"], "approve", key="approve-for-release-replay")

    first = release(client, task["id"], approval["id"], key="replay-release")
    replay = release(client, task["id"], approval["id"], key="replay-release")

    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.headers["etag"] == replay.headers["etag"] == '"v3"'
    assert replay.headers["Idempotency-Replayed"] == "true"
    assert replay.json()["waiting_on_approval_id"] is None
    assert client.get(f"/tasks/{task['id']}").json()["waiting_on_approval_id"] is None
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 5


def test_release_idempotency_hash_includes_specific_approval_id(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    decide(client, approval["id"], "approve", key="approve-before-release-collision")
    first = release(client, task["id"], approval["id"], key="release-id-collision")

    collision = release(
        client,
        task["id"],
        str(uuid4()),
        key="release-id-collision",
    )

    assert first.status_code == 200
    assert collision.status_code == 409
    assert collision.json()["detail"]["code"] == "idempotency_key_reused"
    assert len(client.get(f"/tasks/{task['id']}/events").json()) == 5


def test_approval_decision_idempotency_collision_returns_conflict(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    first = decide(client, approval["id"], "approve", key="decision-collision")

    collision = client.post(
        f"/approvals/{approval['id']}/approve",
        json={"decided_by": "mateus", "decision_reason": "Different reason."},
        headers={"Idempotency-Key": "decision-collision", "If-Match": '"v1"'},
    )

    assert first.status_code == 200
    assert collision.status_code == 409
    assert collision.json()["detail"]["code"] == "idempotency_key_reused"


def test_if_match_is_required_and_versions_are_checked(client):
    task = create_task(client)
    missing = client.post(
        f"/tasks/{task['id']}/request-approval",
        json={
            "gate": "deployment.production",
            "request_reason": "Review required.",
            "requested_by": "orion",
        },
        headers={"Idempotency-Key": "missing-if-match"},
    )
    wrong = client.post(
        f"/tasks/{task['id']}/request-approval",
        json={
            "gate": "deployment.production",
            "request_reason": "Review required.",
            "requested_by": "orion",
        },
        headers={"Idempotency-Key": "wrong-if-match", "If-Match": '"v2"'},
    )

    assert missing.status_code == 428
    assert wrong.status_code == 409
    assert wrong.json()["detail"]["code"] == "task_version_conflict"


def test_approval_decision_requires_matching_if_match(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    body = {"decided_by": "mateus"}
    endpoint = f"/approvals/{approval['id']}/approve"

    missing = client.post(
        endpoint,
        json=body,
        headers={"Idempotency-Key": "decision-missing-if-match"},
    )
    wrong = client.post(
        endpoint,
        json=body,
        headers={"Idempotency-Key": "decision-wrong-if-match", "If-Match": '"v2"'},
    )

    assert missing.status_code == 428
    assert wrong.status_code == 409
    assert wrong.json()["detail"]["code"] == "approval_version_conflict"


def test_release_requires_matching_if_match(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]
    decide(client, approval["id"], "approve", key="approve-before-version-tests")
    body = {"approval_id": approval["id"], "actor_id": "orion"}
    endpoint = f"/tasks/{task['id']}/release"

    missing = client.post(
        endpoint,
        json=body,
        headers={"Idempotency-Key": "release-missing-if-match"},
    )
    wrong = client.post(
        endpoint,
        json=body,
        headers={"Idempotency-Key": "release-wrong-if-match", "If-Match": '"v3"'},
    )

    assert missing.status_code == 428
    assert wrong.status_code == 409
    assert wrong.json()["detail"]["code"] == "task_version_conflict"


def test_event_failure_rolls_back_task_approval_events_and_idempotency(
    client,
    database_path,
):
    task = create_task(client)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER force_approval_requested_event_failure
            BEFORE INSERT ON events
            WHEN NEW.type = 'approval.requested'
            BEGIN
                SELECT RAISE(ABORT, 'forced approval event failure');
            END
            """
        )

    response = request_approval(client, task["id"], key="rollback-request")

    assert response.status_code == 500
    persisted = client.get(f"/tasks/{task['id']}").json()
    assert persisted["status"] == "ready"
    assert persisted["version"] == 1
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT count(*) FROM approvals").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM idempotency_records").fetchone()[0] == 1


def test_pending_gate_unique_index_is_enforced_by_sqlite(client, database_path):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]

    with sqlite3.connect(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO approvals(
                    id, task_id, gate, status, request_reason, context_json,
                    requested_by, assigned_to, decided_by, decision_reason,
                    correlation_id, version, created_at, updated_at, decided_at
                )
                SELECT ?, task_id, gate, 'pending', request_reason, context_json,
                       requested_by, assigned_to, NULL, NULL, correlation_id,
                       1, created_at, updated_at, NULL
                FROM approvals WHERE id = ?
                """,
                (str(uuid4()), approval["id"]),
            )


def test_request_approval_accepts_valid_cause_and_rejects_invalid_causes(client):
    valid_task = create_task(client)
    valid_event = client.get(f"/tasks/{valid_task['id']}/events").json()[0]
    valid = request_approval(
        client,
        valid_task["id"],
        key="valid-cause",
        cause=valid_event["id"],
    )
    assert valid.status_code == 200

    missing_task = create_task(client)
    missing = request_approval(
        client,
        missing_task["id"],
        key="missing-cause",
        cause=str(uuid4()),
    )
    assert missing.status_code == 422
    assert missing.json()["detail"]["code"] == "causation_event_not_found"

    mismatched_task = create_task(client)
    mismatched = request_approval(
        client,
        mismatched_task["id"],
        key="mismatched-cause",
        cause=valid_event["id"],
    )
    assert mismatched.status_code == 409
    assert mismatched.json()["detail"]["code"] == "causation_correlation_mismatch"


def test_approval_reads_return_entity_and_task_collection(client):
    task = create_task(client)
    approval = request_approval(client, task["id"]).json()["approval"]

    entity = client.get(f"/approvals/{approval['id']}")
    collection = client.get(f"/tasks/{task['id']}/approvals")

    assert entity.status_code == 200
    assert entity.headers["etag"] == '"v1"'
    assert entity.json() == approval
    assert collection.status_code == 200
    assert collection.json() == [approval]
