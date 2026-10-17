from datetime import UTC, datetime
from uuid import uuid4

from fastapi.testclient import TestClient

from agentbus.app import create_app


def _create(client: TestClient, *, title: str, assigned_to: str | None = None) -> dict:
    response = client.post(
        "/tasks",
        headers={"Idempotency-Key": f"create-{title}"},
        json={
            "type": "catalog.test",
            "title": title,
            "input": {},
            "requested_by": "catalog-suite",
            "assigned_to": assigned_to,
            "correlation_id": str(uuid4()),
        },
    )
    assert response.status_code == 201
    return response.json()


def test_task_catalog_filters_and_returns_deterministic_cursor(tmp_path) -> None:
    app = create_app(tmp_path / "agentbus.sqlite3")
    with TestClient(app) as client:
        first = _create(client, title="first", assigned_to="dex")
        second = _create(client, title="second", assigned_to="dex")
        _create(client, title="other", assigned_to="orion")

        page_one = client.get("/tasks", params={"assigned_to": "dex", "limit": 1})
        assert page_one.status_code == 200
        assert [item["id"] for item in page_one.json()["items"]] == [second["id"]]
        cursor = page_one.json()["next_cursor"]
        assert cursor

        page_two = client.get(
            "/tasks", params={"assigned_to": "dex", "limit": 1, "cursor": cursor}
        )
        assert page_two.status_code == 200
        assert [item["id"] for item in page_two.json()["items"]] == [first["id"]]
        assert page_two.json()["next_cursor"] is None


def test_task_catalog_cursor_is_bound_to_filters(tmp_path) -> None:
    app = create_app(tmp_path / "agentbus.sqlite3")
    with TestClient(app) as client:
        _create(client, title="one", assigned_to="dex")
        _create(client, title="two", assigned_to="dex")
        cursor = client.get(
            "/tasks", params={"assigned_to": "dex", "limit": 1}
        ).json()["next_cursor"]

        response = client.get(
            "/tasks", params={"assigned_to": "orion", "limit": 1, "cursor": cursor}
        )
        assert response.status_code == 400
        assert response.json()["detail"]["code"] == "invalid_task_cursor"


def test_task_catalog_orders_uuid_ties_and_does_not_duplicate(tmp_path) -> None:
    app = create_app(tmp_path / "agentbus.sqlite3")
    with TestClient(app) as client:
        created = [_create(client, title=f"task-{index}") for index in range(3)]
        same_time = datetime(2026, 1, 1, tzinfo=UTC).isoformat()
        with app.state.database.transaction() as connection:
            connection.execute("UPDATE tasks SET created_at = ?", (same_time,))

        seen: list[str] = []
        cursor = None
        while True:
            params: dict[str, str | int] = {"limit": 1}
            if cursor:
                params["cursor"] = cursor
            response = client.get("/tasks", params=params)
            assert response.status_code == 200
            seen.extend(item["id"] for item in response.json()["items"])
            cursor = response.json()["next_cursor"]
            if cursor is None:
                break

        assert seen == sorted((item["id"] for item in created), reverse=True)
        assert len(seen) == len(set(seen)) == 3
