import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from fastapi.testclient import TestClient
import httpx
import pytest

from agentbus.app import create_app
from agentbus.approval_confirm import ApprovalConfirmer, ConfirmationError, NOTICE
from agentbus.approval_intents import ApprovalIntentJournal, IntentChanged, create_intent_app
from agentbus.auth import AuthRegistry, token_digest


TOKENS = {
    "orion": "orion-token-0000000000000000000000000000",
    "mateus": "mateus-token-000000000000000000000000000",
    "viewer": "viewer-token-000000000000000000000000000",
}


def _registry(tmp_path):
    entries = [
        ("orion-1", "orion", TOKENS["orion"], ["task:create", "approval:request", "task:read:any"]),
        ("mateus-1", "mateus", TOKENS["mateus"], ["approval:read:self", "approval:decide:self"]),
        ("viewer-1", "control-center", TOKENS["viewer"], ["approval:read:any"]),
    ]
    path = tmp_path / "auth.json"
    path.write_text(json.dumps({"principals": [{
        "principal_id": principal, "subject_id": subject,
        "token_sha256": token_digest(token), "permissions": permissions,
    } for principal, subject, token, permissions in entries]}))
    path.chmod(0o600)
    return AuthRegistry.from_file(path)


def _headers(name, **headers):
    return {"Authorization": f"Bearer {TOKENS[name]}", **headers}


def _pending_approval(api):
    task = api.post("/tasks", headers=_headers("orion", **{"Idempotency-Key": "create"}), json={
        "type": "human.review", "title": "Human review", "input": {},
        "requested_by": "orion", "assigned_to": "dex", "correlation_id": str(uuid4()),
    }).json()
    response = api.post(
        f"/tasks/{task['id']}/request-approval",
        headers=_headers("orion", **{"Idempotency-Key": "request", "If-Match": '"v1"'}),
        json={
            "gate": "deployment.production", "request_reason": "Review safely",
            "requested_by": "orion", "assigned_to": "mateus", "context": {},
        },
    )
    return task, response.json()["approval"]


def _transport(api, *, lose_first_decision=False):
    lost = False
    def handler(request: httpx.Request):
        nonlocal lost
        response = api.request(
            request.method, request.url.path,
            params=dict(request.url.params), content=request.content,
            headers=dict(request.headers),
        )
        if lose_first_decision and request.method == "POST" and not lost:
            lost = True
            raise httpx.ReadError("response lost after commit", request=request)
        return httpx.Response(
            response.status_code, content=response.content,
            headers=dict(response.headers), request=request,
        )
    return httpx.MockTransport(handler)


def _intent_client(tmp_path, api, approval):
    journal_path = tmp_path / "intents.sqlite3"
    journal = ApprovalIntentJournal(journal_path)
    app = create_intent_app(
        journal=journal, agentbus_url="http://127.0.0.1:8000",
        token=TOKENS["viewer"], allowed_host="127.0.0.1:5173",
        transport=_transport(api),
    )
    client = TestClient(app, base_url="http://127.0.0.1:5173")
    client.__enter__()
    session = client.get("/intent-api/session").json()
    body = {
        "approval_id": approval["id"], "decision": "approve",
        "decision_reason": "Reviewed", "if_match": '"v1"',
    }
    return journal_path, journal, client, session, body


def test_intent_journal_is_created_private_and_rejects_unsafe_existing_file(tmp_path):
    path = tmp_path / "private.sqlite3"
    ApprovalIntentJournal(path)
    assert path.stat().st_mode & 0o777 == 0o600

    unsafe = tmp_path / "unsafe.sqlite3"
    unsafe.touch(mode=0o644)
    with pytest.raises(PermissionError):
        ApprovalIntentJournal(unsafe)


def test_intent_boundary_csrf_idempotency_limits_and_public_shape(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _pending_approval(api)
        _, journal, client, session, body = _intent_client(tmp_path, api, approval)
        try:
            missing = client.post("/intent-api/approval-intents", json=body,
                                  headers={"Intent-Key": "one"})
            assert missing.status_code == 422
            headers = {"Intent-Key": "one", "X-AgentBus-CSRF": session["csrf_token"]}
            first = client.post("/intent-api/approval-intents", json=body, headers=headers)
            replay = client.post("/intent-api/approval-intents", json=body, headers=headers)
            assert first.status_code == 201
            assert replay.status_code == 200
            assert replay.headers["Intent-Replayed"] == "true"
            collision = client.post(
                "/intent-api/approval-intents",
                json={**body, "decision": "reject"}, headers=headers,
            )
            assert collision.status_code == 409
            assert collision.json()["detail"]["code"] == "intent_key_reused"
            public = client.get(f"/intent-api/approval-intents/{first.json()['intent_id']}").json()
            assert set(public) == {
                "intent_id", "approval", "decision", "decision_reason", "state",
                "created_at", "confirmed_at", "completed_at", "result", "public_error",
                "replayed",
            }
            assert "idempotency_key" not in json.dumps(public)
            for index in (2, 3):
                created = client.post("/intent-api/approval-intents", json=body, headers={
                    "Intent-Key": f"key-{index}", "X-AgentBus-CSRF": session["csrf_token"],
                })
                assert created.status_code == 201
            limited = client.post("/intent-api/approval-intents", json=body, headers={
                "Intent-Key": "key-4", "X-AgentBus-CSRF": session["csrf_token"],
            })
            assert limited.status_code == 429
            # Replay is checked before the now-exhausted capacity limit.
            assert client.post("/intent-api/approval-intents", json=body, headers=headers).status_code == 200
        finally:
            client.__exit__(None, None, None)


def test_intent_http_boundary_rejects_cross_site_oversize_and_other_session(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _pending_approval(api)
        _, _, client, session, body = _intent_client(tmp_path, api, approval)
        try:
            bad_origin = client.get("/intent-api/session", headers={
                "Origin": "https://attacker.example", "Sec-Fetch-Site": "cross-site",
            })
            assert bad_origin.status_code == 403
            oversized = client.post(
                "/intent-api/approval-intents",
                content=json.dumps({**body, "padding": "x" * 17_000}),
                headers={
                    "Content-Type": "application/json", "Intent-Key": "oversized",
                    "X-AgentBus-CSRF": session["csrf_token"],
                },
            )
            assert oversized.status_code == 413
            intent = client.post("/intent-api/approval-intents", json=body, headers={
                "Intent-Key": "private", "X-AgentBus-CSRF": session["csrf_token"],
            }).json()
            other = TestClient(client.app, base_url="http://127.0.0.1:5173")
            with other:
                other.get("/intent-api/session")
                hidden = other.get(f"/intent-api/approval-intents/{intent['intent_id']}")
            assert hidden.status_code == 404
        finally:
            client.__exit__(None, None, None)


def test_unconfirmed_intent_is_never_recovered(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _pending_approval(api)
        path, journal, client, session, body = _intent_client(tmp_path, api, approval)
        try:
            intent = client.post("/intent-api/approval-intents", json=body, headers={
                "Intent-Key": "pending-only", "X-AgentBus-CSRF": session["csrf_token"],
            }).json()
        finally:
            client.__exit__(None, None, None)
        confirmer = ApprovalConfirmer(
            journal=journal, journal_path=path, agentbus_url="http://127.0.0.1:8000",
            token=TOKENS["mateus"], transport=_transport(api),
            prompt=lambda _: pytest.fail("recovery must not prompt"), output=lambda _: None,
        )
        assert confirmer.recover() == []
        confirmer.close()
        assert journal.read(intent["intent_id"])["state"] == "pending"


def test_confirmation_has_no_prompt_lock_and_detects_local_change(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _pending_approval(api)
        path, journal, client, session, body = _intent_client(tmp_path, api, approval)
        try:
            intent = client.post("/intent-api/approval-intents", json=body, headers={
                "Intent-Key": "confirm", "X-AgentBus-CSRF": session["csrf_token"],
            }).json()
        finally:
            client.__exit__(None, None, None)
        def mutate_while_prompting(_):
            with journal.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("UPDATE approval_intents SET revision=revision+1 WHERE intent_id=?",
                           (intent["intent_id"],))
                db.commit()
            return "APROVAR"
        confirmer = ApprovalConfirmer(
            journal=journal, journal_path=path, agentbus_url="http://127.0.0.1:8000",
            token=TOKENS["mateus"], transport=_transport(api),
            prompt=mutate_while_prompting, output=lambda text: None,
        )
        with pytest.raises(IntentChanged):
            confirmer.confirm(intent["intent_id"])
        assert journal.read(intent["intent_id"])["state"] == "pending"
        confirmer.close()


def test_lost_success_response_recovers_exact_decision_without_releasing_task(tmp_path):
    database = tmp_path / "bus.sqlite3"
    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        task, approval = _pending_approval(api)
        path, journal, client, session, body = _intent_client(tmp_path, api, approval)
        try:
            intent = client.post("/intent-api/approval-intents", json=body, headers={
                "Intent-Key": "lost-response", "X-AgentBus-CSRF": session["csrf_token"],
            }).json()
        finally:
            client.__exit__(None, None, None)
        messages = []
        first = ApprovalConfirmer(
            journal=journal, journal_path=path, agentbus_url="http://127.0.0.1:8000",
            token=TOKENS["mateus"], transport=_transport(api, lose_first_decision=True),
            prompt=lambda _: "APROVAR", output=messages.append,
        )
        with pytest.raises(ConfirmationError, match="ambiguous"):
            first.confirm(intent["intent_id"])
        first.close()
        assert NOTICE in messages[0]
        assert journal.read(intent["intent_id"])["state"] == "sending"

        recovered = ApprovalConfirmer(
            journal=journal, journal_path=path, agentbus_url="http://127.0.0.1:8000",
            token=TOKENS["mateus"], transport=_transport(api),
            prompt=lambda _: pytest.fail("recovery must not prompt"), output=lambda _: None,
        ).recover()
        assert len(recovered) == 1
        final = journal.read(intent["intent_id"])
        assert final["state"] == "completed"
        assert final["replayed"] == 1
        persisted_task = api.get(f"/tasks/{task['id']}", headers=_headers("orion")).json()
        assert persisted_task["status"] == "waiting_approval"
        with sqlite3.connect(database) as db:
            assert db.execute(
                "SELECT count(*) FROM events WHERE type='approval.approved'"
            ).fetchone()[0] == 1
