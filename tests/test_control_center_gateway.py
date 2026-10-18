import json

from fastapi.testclient import TestClient
import httpx

from agentbus.control_center_gateway import create_gateway
import pytest


def _gateway(captured):
    def upstream(request: httpx.Request):
        captured.append(request)
        return httpx.Response(200, json={"items": [], "next_cursor": None})
    return create_gateway(
        agentbus_url="http://127.0.0.1:8000", token="r" * 32,
        allowed_host="127.0.0.1:5173", transport=httpx.MockTransport(upstream),
    )


def test_gateway_forwards_only_enumerated_get_with_read_token():
    captured = []
    with TestClient(_gateway(captured), base_url="http://127.0.0.1:5173") as client:
        response = client.get("/api/tasks?limit=10", headers={
            "Origin": "http://127.0.0.1:5173", "Sec-Fetch-Site": "same-origin"
        })
        mutation = client.post("/api/tasks", json={})
        arbitrary = client.get("/api/proxy/http://example.com")
    assert response.status_code == 200
    assert captured[0].headers["authorization"] == "Bearer " + "r" * 32
    assert captured[0].url.path == "/tasks"
    assert mutation.status_code == 405
    assert arbitrary.status_code == 404


def test_gateway_forwards_only_enumerated_approval_reads():
    captured = []
    approval_id = "11111111-1111-4111-8111-111111111111"
    with TestClient(_gateway(captured), base_url="http://127.0.0.1:5173") as client:
        catalog = client.get("/api/approvals?status=pending&assigned_to=mateus")
        detail = client.get(f"/api/approvals/{approval_id}")
        mutation = client.post(f"/api/approvals/{approval_id}/approve", json={})
    assert catalog.status_code == detail.status_code == 200
    assert captured[0].url.path == "/approvals"
    assert captured[0].url.params["assigned_to"] == "mateus"
    assert captured[1].url.path == f"/approvals/{approval_id}"
    assert mutation.status_code == 404


def test_gateway_rejects_host_origin_and_cross_site_requests():
    captured = []
    with TestClient(_gateway(captured), base_url="http://127.0.0.1:5173") as client:
        bad_host = client.get("/api/tasks", headers={"Host": "agentbus.attacker.test"})
        bad_origin = client.get("/api/tasks", headers={"Origin": "https://attacker.test"})
        cross_site = client.get("/api/tasks", headers={"Sec-Fetch-Site": "cross-site"})
    assert bad_host.status_code == 400
    assert bad_origin.status_code == 403
    assert cross_site.status_code == 403
    assert captured == []


def test_gateway_rejects_non_loopback_target_and_host():
    with pytest.raises(ValueError, match="target must be loopback"):
        create_gateway(agentbus_url="https://example.com", token="r" * 32)
    with pytest.raises(ValueError, match="Host must be loopback"):
        create_gateway(agentbus_url="http://127.0.0.1:8000", token="r" * 32,
                       allowed_host="example.com:5173")
