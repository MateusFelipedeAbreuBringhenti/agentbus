import json
import hashlib
from pathlib import Path
import sqlite3

from fastapi.testclient import TestClient
import httpx
import pytest

from agentbus.control_center_gateway import create_gateway
from agentbus.operational_projection import (
    AgentBusReadClient, OperationalProjection, ProjectionManifest, create_projection_app,
)


TOKEN = "operational-reader-token-000000000000000000"


def _manifest(tmp_path: Path, sources: list[dict]) -> ProjectionManifest:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"sources": sources}), encoding="utf-8")
    path.chmod(0o600)
    return ProjectionManifest.from_file(path)


def _approval_journal(path: Path, rows=2):
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TABLE approval_intents(
            intent_id TEXT, approval_id TEXT, task_id TEXT, correlation_id TEXT,
            state TEXT, created_at TEXT, confirmed_at TEXT, sending_at TEXT,
            completed_at TEXT, intended_subject_id TEXT, last_http_status INTEGER,
            last_error_code TEXT, command_body_json TEXT, command_sha256 TEXT)""")
        for number in range(rows):
            db.execute("INSERT INTO approval_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                f"intent-{number}", f"approval-{number}", f"task-{number}", f"corr-{number}",
                "pending", f"2026-10-20T00:00:0{number}+00:00", None, None, None,
                "mateus", None, None, '{"secret":"must-not-leak"}', "command-hash",
            ))


def _app(tmp_path: Path, sources: list[dict]):
    manifest = _manifest(tmp_path, sources)
    transport = httpx.MockTransport(lambda request: httpx.Response(404, request=request))
    agentbus = AgentBusReadClient("http://127.0.0.1:8000", "a" * 32, transport=transport)
    return agentbus, create_projection_app(
        projection=OperationalProjection(manifest, agentbus), reader_token=TOKEN,
    )


def test_manifest_is_closed_private_and_allowlisted(tmp_path):
    journal = tmp_path / "intent.sqlite3"; _approval_journal(journal)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"sources": [{"source_id": "intents", "kind": "approval_intent", "path": str(journal), "logical_owner": "human", "extra": True}]}))
    path.chmod(0o600)
    with pytest.raises(ValueError): ProjectionManifest.from_file(path)
    path.write_text(json.dumps({"sources": []})); path.chmod(0o644)
    with pytest.raises(PermissionError): ProjectionManifest.from_file(path)


def test_catalog_is_authenticated_redacted_and_cursor_bound_to_filters(tmp_path):
    journal = tmp_path / "intent.sqlite3"; _approval_journal(journal)
    with sqlite3.connect(journal) as db:
        db.execute("UPDATE approval_intents SET last_error_code='private/internal/error' WHERE intent_id='intent-1'")
    before = hashlib.sha256(journal.read_bytes()).hexdigest()
    agentbus, app = _app(tmp_path, [{"source_id": "intents", "kind": "approval_intent", "path": str(journal), "logical_owner": "human"}])
    with TestClient(app) as client:
        assert client.get("/operations").status_code == 401
        headers = {"Authorization": f"Bearer {TOKEN}"}
        first = client.get("/operations?limit=1", headers=headers)
        assert first.status_code == 200
        body = first.json(); assert len(body["items"]) == 1 and body["next_cursor"]
        serialized = json.dumps(body)
        assert "must-not-leak" not in serialized and "command-hash" not in serialized
        assert "private/internal/error" not in serialized and "operation_error" in serialized
        assert str(journal) not in serialized and body["snapshot_scope"] == "per_request_not_global"
        second = client.get(f"/operations?limit=1&cursor={body['next_cursor']}", headers=headers)
        assert second.status_code == 200 and second.json()["items"][0]["operation_id"] != body["items"][0]["operation_id"]
        mismatch = client.get(f"/operations?limit=1&kind=release&cursor={body['next_cursor']}", headers=headers)
        assert mismatch.status_code == 400
        assert client.post("/operations", headers=headers).status_code == 405
        assert hashlib.sha256(journal.read_bytes()).hexdigest() == before
    agentbus.close()


def test_source_status_is_partial_for_missing_and_incompatible_sources(tmp_path):
    broken = tmp_path / "broken.sqlite3"
    with sqlite3.connect(broken) as db: db.execute("CREATE TABLE approval_intents(intent_id TEXT)")
    agentbus, app = _app(tmp_path, [
        {"source_id": "missing", "kind": "runner", "path": str(tmp_path / "missing.sqlite3"), "logical_owner": "dex"},
        {"source_id": "broken", "kind": "approval_intent", "path": str(broken), "logical_owner": "human"},
    ])
    with TestClient(app) as client:
        body = client.get("/operations", headers={"Authorization": f"Bearer {TOKEN}"}).json()
        assert {(source["source_id"], source["status"]) for source in body["sources"]} == {("missing", "unavailable"), ("broken", "incompatible")}
        assert body["items"] == []
    agentbus.close()


def test_runner_without_canonical_task_reports_insufficient_evidence(tmp_path):
    journal = tmp_path / "runner.sqlite3"
    with sqlite3.connect(journal) as db:
        db.execute("""CREATE TABLE executions(task_id TEXT, runner_instance_id TEXT,
            claimed_etag TEXT, claim_rejected_at TEXT, result_kind TEXT, reported_at TEXT,
            created_at TEXT, updated_at TEXT)""")
        db.execute("INSERT INTO executions VALUES(?,?,?,?,?,?,?,?)", ("task-x", "runner-x", '"v2"', None, None, None, "2026-10-20T00:00:00+00:00", "2026-10-20T00:01:00+00:00"))
    agentbus, app = _app(tmp_path, [{"source_id": "runner", "kind": "runner", "path": str(journal), "logical_owner": "dex"}])
    with TestClient(app) as client:
        item = client.get("/operations", headers={"Authorization": f"Bearer {TOKEN}"}).json()["items"][0]
        assert item["attention_class"] == "insufficient_evidence"
        assert item["correlation_id"] is None
        assert item["persisted_state"] == "execution_unconfirmed"
    agentbus.close()


def test_gateway_exposes_only_enumerated_operational_gets(tmp_path):
    journal = tmp_path / "intent.sqlite3"; _approval_journal(journal, rows=1)
    agentbus, projection_app = _app(tmp_path, [{"source_id": "intents", "kind": "approval_intent", "path": str(journal), "logical_owner": "human"}])
    projection_client = TestClient(projection_app)
    def operational_handler(request: httpx.Request):
        response = projection_client.request(request.method, request.url.path, params=dict(request.url.params), headers=dict(request.headers))
        return httpx.Response(response.status_code, content=response.content, headers=dict(response.headers), request=request)
    unavailable_agentbus = httpx.MockTransport(lambda request: httpx.Response(503, request=request))
    gateway = create_gateway(agentbus_url="http://127.0.0.1:8000", token="b" * 32,
        allowed_host="127.0.0.1:5173", transport=unavailable_agentbus,
        operational_url="http://127.0.0.1:8003", operational_token=TOKEN,
        operational_transport=httpx.MockTransport(operational_handler))
    with TestClient(gateway) as client:
        headers = {"Host": "127.0.0.1:5173", "Origin": "http://127.0.0.1:5173", "Sec-Fetch-Site": "same-origin"}
        assert client.get("/api/operations", headers=headers).status_code == 200
        assert client.post("/api/operations", headers=headers).status_code == 405
        assert client.get("/api/operations/raw/sqlite", headers=headers).status_code in {404, 422}
        assert client.get("/api/operations", headers={**headers, "Origin": "https://evil.invalid"}).status_code == 403
    projection_client.close(); agentbus.close()


def test_agentbus_technical_identity_must_be_strictly_read_only():
    def identity(permissions):
        return httpx.MockTransport(lambda request: httpx.Response(200, json={
            "principal_id": "ops-reader", "subject_id": "ops-reader", "permissions": permissions,
        }, request=request))
    accepted = AgentBusReadClient("http://127.0.0.1:8000", "a" * 32,
                                  transport=identity(["task:read:any", "approval:read:any"]))
    accepted.verify_read_only(); accepted.close()
    elevated = AgentBusReadClient("http://127.0.0.1:8000", "a" * 32,
                                  transport=identity(["task:read:any", "task:release"]))
    with pytest.raises(RuntimeError, match="read-only"):
        elevated.verify_read_only()
    elevated.close()


def test_concurrent_uncommitted_change_is_not_invented(tmp_path):
    journal = tmp_path / "intent.sqlite3"; _approval_journal(journal, rows=1)
    agentbus, app = _app(tmp_path, [{"source_id": "intents", "kind": "approval_intent", "path": str(journal), "logical_owner": "human"}])
    writer = sqlite3.connect(journal, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    writer.execute("UPDATE approval_intents SET state='completed',completed_at='2026-10-20T00:01:00+00:00'")
    with TestClient(app) as client:
        item = client.get("/operations", headers={"Authorization": f"Bearer {TOKEN}"}).json()["items"][0]
        assert item["persisted_state"] == "pending"
    writer.rollback(); writer.close(); agentbus.close()


def test_release_and_outbox_projection_use_composite_ids_and_closed_facts(tmp_path):
    release = tmp_path / "release.sqlite3"
    with sqlite3.connect(release) as db:
        db.execute("""CREATE TABLE release_commands(approval_id TEXT,task_id TEXT,
            correlation_id TEXT,state TEXT,created_at TEXT,prepared_at TEXT,completed_at TEXT,
            expected_subject_id TEXT,last_http_status INTEGER,last_error_code TEXT)""")
        db.execute("INSERT INTO release_commands VALUES(?,?,?,?,?,?,?,?,?,?)", (
            "same-id", "task-release", "corr-release", "auth_required",
            "2026-10-20T00:00:00+00:00", "2026-10-20T00:01:00+00:00", None,
            "orion-ops", 401, "authentication_required"))
    outbox = tmp_path / "outbox.sqlite3"
    with sqlite3.connect(outbox) as db:
        db.execute("""CREATE TABLE successor_commands(coordination_id TEXT,
            predecessor_task_id TEXT,successor_task_id TEXT,created_at TEXT,confirmed_at TEXT)""")
        db.execute("INSERT INTO successor_commands VALUES(?,?,?,?,?)", (
            "same-id", "task-parent", None, "2026-10-20T00:00:00+00:00", None))
    agentbus, app = _app(tmp_path, [
        {"source_id": "release", "kind": "release", "path": str(release), "logical_owner": "orion-ops"},
        {"source_id": "outbox", "kind": "outbox", "path": str(outbox), "logical_owner": "orion-ops"},
    ])
    with TestClient(app) as client:
        items = client.get("/operations", headers={"Authorization": f"Bearer {TOKEN}"}).json()["items"]
        assert len({item["operation_id"] for item in items}) == 2
        release_item = next(item for item in items if item["kind"] == "release")
        assert release_item["attention_class"] == "authentication_required"
        outbox_item = next(item for item in items if item["kind"] == "outbox")
        assert outbox_item["persisted_state"] == "unconfirmed"
        assert outbox_item["attention_class"] == "insufficient_evidence"
    agentbus.close()
