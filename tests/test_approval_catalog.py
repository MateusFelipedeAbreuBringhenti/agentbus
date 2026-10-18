from uuid import uuid4

from fastapi.testclient import TestClient

from agentbus.app import create_test_app


def _approval(client: TestClient, assigned_to: str, index: int) -> dict:
    task = client.post("/tasks", headers={"Idempotency-Key": f"task-{index}"}, json={
        "type": "approval.catalog", "title": f"Task {index}", "input": {},
        "requested_by": "orion", "assigned_to": "dex",
        "correlation_id": str(uuid4()),
    }).json()
    return client.post(
        f"/tasks/{task['id']}/request-approval",
        headers={"Idempotency-Key": f"approval-{index}", "If-Match": '"v1"'},
        json={
            "gate": f"gate.{index}", "request_reason": "Review",
            "requested_by": "orion", "assigned_to": assigned_to, "context": {},
        },
    ).json()["approval"]


def test_approval_catalog_filters_pages_and_binds_cursor(tmp_path):
    with TestClient(create_test_app(tmp_path / "db.sqlite3")) as client:
        first = _approval(client, "mateus", 1)
        second = _approval(client, "mateus", 2)
        _approval(client, "alice", 3)
        page = client.get("/approvals", params={
            "status": "pending", "assigned_to": "mateus", "limit": 1,
        })
        assert page.status_code == 200
        assert [item["id"] for item in page.json()["items"]] == [second["id"]]
        cursor = page.json()["next_cursor"]
        next_page = client.get("/approvals", params={
            "status": "pending", "assigned_to": "mateus", "limit": 1,
            "cursor": cursor,
        })
        assert [item["id"] for item in next_page.json()["items"]] == [first["id"]]
        wrong = client.get("/approvals", params={
            "status": "pending", "assigned_to": "alice", "limit": 1,
            "cursor": cursor,
        })
        assert wrong.status_code == 400
        assert wrong.json()["detail"]["code"] == "invalid_approval_cursor"


def test_auth_me_comes_only_from_authenticated_context(tmp_path):
    with TestClient(create_test_app(tmp_path / "db.sqlite3")) as client:
        response = client.get("/auth/me", params={"subject_id": "forged"})
    assert response.status_code == 200
    assert response.json()["principal_id"] == "test-suite"
    assert response.json()["subject_id"] == "*"
