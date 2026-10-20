import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from fastapi.testclient import TestClient
import httpx
import pytest

from agentbus.app import create_app
from agentbus.auth import AuthRegistry, token_digest
from agentbus.release_reconciler import (
    AgentBusReleaseClient,
    AmbiguousRelease,
    ReconciliationError,
    ReconciliationStopped,
    ReleaseJournal,
    ReleasePolicy,
    ReleaseReconciler,
)


TOKENS = {
    "orion": "orion-release-token-00000000000000000000000",
    "rotated": "orion-rotated-token-000000000000000000000",
    "other": "other-release-token-00000000000000000000000",
    "mateus": "mateus-release-token-0000000000000000000000",
}
SUBJECT = "local-coordinator:11111111-1111-1111-1111-111111111111"


def _registry(tmp_path):
    entries = [
        ("orion-1", SUBJECT, TOKENS["orion"], [
            "task:create", "approval:request", "task:release", "task:read:any",
            "approval:read:any", "event:read:any",
        ]),
        ("orion-2", SUBJECT, TOKENS["rotated"], [
            "task:create", "approval:request", "task:release", "task:read:any",
            "approval:read:any", "event:read:any",
        ]),
        ("other-1", "other-coordinator", TOKENS["other"], [
            "task:release", "task:read:any", "approval:read:any", "event:read:any",
        ]),
        ("human-1", "mateus", TOKENS["mateus"], [
            "approval:read:self", "approval:decide:self",
        ]),
    ]
    path = tmp_path / "auth.json"
    path.write_text(json.dumps({"principals": [{
        "principal_id": principal, "subject_id": subject,
        "token_sha256": token_digest(token), "permissions": permissions,
    } for principal, subject, token, permissions in entries]}), encoding="utf-8")
    path.chmod(0o600)
    return AuthRegistry.from_file(path)


def _headers(name, **extra):
    return {"Authorization": f"Bearer {TOKENS[name]}", **extra}


def _approved(api, *, gate="deployment.production"):
    task = api.post("/tasks", json={
        "type": "deploy", "title": "Deploy controlled release", "input": {},
        "requested_by": SUBJECT, "assigned_to": "dex", "correlation_id": str(uuid4()),
    }, headers=_headers("orion", **{"Idempotency-Key": "create"})).json()
    requested = api.post(f"/tasks/{task['id']}/request-approval", json={
        "gate": gate, "request_reason": "Human authorization required",
        "requested_by": SUBJECT, "assigned_to": "mateus", "context": {},
    }, headers=_headers("orion", **{
        "Idempotency-Key": "request", "If-Match": '"v1"',
    })).json()
    approval = requested["approval"]
    decided = api.post(f"/approvals/{approval['id']}/approve", json={
        "decided_by": "mateus", "decision_reason": "Approved",
    }, headers=_headers("mateus", **{
        "Idempotency-Key": "approve", "If-Match": '"v1"',
    }))
    assert decided.status_code == 200
    return task, decided.json()


def _transport(api, *, lose_release=False, release_401=False):
    lost = False
    unauthorized = False

    def handler(request: httpx.Request):
        nonlocal lost, unauthorized
        if release_401 and request.method == "POST" and "/release" in request.url.path and not unauthorized:
            unauthorized = True
            return httpx.Response(401, json={"detail": {"code": "invalid_credential"}}, request=request)
        response = api.request(
            request.method, request.url.path, params=dict(request.url.params),
            content=request.content, headers=dict(request.headers),
        )
        if lose_release and request.method == "POST" and "/release" in request.url.path and not lost:
            lost = True
            raise httpx.ReadError("release response lost after commit", request=request)
        return httpx.Response(
            response.status_code, content=response.content,
            headers=dict(response.headers), request=request,
        )

    return httpx.MockTransport(handler)


def _policy(tmp_path, gates=("deployment.production",)):
    path = tmp_path / "release-policy.json"
    path.write_text(json.dumps({"allowed_gates": list(gates)}), encoding="utf-8")
    path.chmod(0o600)
    return ReleasePolicy.from_file(path)


def _reconciler(tmp_path, api, *, token="orion", transport=None):
    journal = ReleaseJournal(tmp_path / "release.sqlite3")
    client = AgentBusReleaseClient(
        "http://127.0.0.1:8000", TOKENS[token],
        transport=transport or _transport(api),
    )
    return journal, client, ReleaseReconciler(
        journal=journal, client=client, policy=_policy(tmp_path),
        expected_subject_id=SUBJECT,
    )


def test_reconciler_releases_allowlisted_approved_task(tmp_path):
    database = tmp_path / "bus.sqlite3"
    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        task, approval = _approved(api)
        journal, client, reconciler = _reconciler(tmp_path, api)
        result = reconciler.reconcile(approval["id"])
        client.close()
        assert result["state"] == "completed"
        persisted = api.get(f"/tasks/{task['id']}", headers=_headers("orion")).json()
        assert persisted["status"] == "ready"
        assert persisted["waiting_on_approval_id"] is None
        events = api.get(f"/tasks/{task['id']}/events", headers=_headers("orion")).json()
        assert [event["type"] for event in events].count("task.released") == 1
        released = [event for event in events if event["type"] == "task.released"][0]
        approved = [event for event in events if event["type"] == "approval.approved"][0]
        assert released["causation_event_id"] == approved["id"]
        assert journal.read(approval["id"])["replayed"] == 0


def test_requester_identity_cannot_be_forged(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        task = api.post("/tasks", json={
            "type": "deploy", "title": "Forged requester", "input": {},
            "requested_by": SUBJECT, "assigned_to": "dex", "correlation_id": str(uuid4()),
        }, headers=_headers("orion", **{"Idempotency-Key": "forged-create"})).json()
        response = api.post(f"/tasks/{task['id']}/request-approval", json={
            "gate": "deployment.production", "request_reason": "Try forged authority",
            "requested_by": "quack:diretoria", "assigned_to": "mateus", "context": {},
        }, headers=_headers("orion", **{
            "Idempotency-Key": "forged-request", "If-Match": '"v1"',
        }))
        assert response.status_code == 403
        assert response.json()["detail"]["code"] == "principal_mismatch"


def test_lost_release_response_recovers_exact_command_without_duplicate_event(tmp_path):
    database = tmp_path / "bus.sqlite3"
    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        task, approval = _approved(api)
        journal, first_client, first = _reconciler(
            tmp_path, api, transport=_transport(api, lose_release=True),
        )
        with pytest.raises(AmbiguousRelease):
            first.reconcile(approval["id"])
        first_client.close()
        before = journal.read(approval["id"])
        assert before["state"] == "sending"
        body, key, etag = (
            before["command_body_json"], before["idempotency_key"], before["task_etag"],
        )

        second_client = AgentBusReleaseClient(
            "http://127.0.0.1:8000", TOKENS["rotated"], transport=_transport(api),
        )
        recovered = ReleaseReconciler(
            journal=ReleaseJournal(journal.path), client=second_client,
            policy=_policy(tmp_path), expected_subject_id=SUBJECT,
        ).recover()
        second_client.close()
        assert len(recovered) == 1
        final = journal.read(approval["id"])
        assert final["state"] == "completed" and final["replayed"] == 1
        assert (final["command_body_json"], final["idempotency_key"], final["task_etag"]) == (
            body, key, etag,
        )
        events = api.get(f"/tasks/{task['id']}/events", headers=_headers("orion")).json()
        assert [event["type"] for event in events].count("task.released") == 1


def test_401_suspends_exact_command_until_same_subject_reauthenticates(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _approved(api)
        journal, client, reconciler = _reconciler(
            tmp_path, api, transport=_transport(api, release_401=True),
        )
        with pytest.raises(ReconciliationError, match="reauthentication"):
            reconciler.reconcile(approval["id"])
        client.close()
        suspended = journal.read(approval["id"])
        assert suspended["state"] == "auth_required"
        exact = (
            suspended["command_body_json"], suspended["idempotency_key"], suspended["task_etag"],
        )
        rotated = AgentBusReleaseClient(
            "http://127.0.0.1:8000", TOKENS["rotated"], transport=_transport(api),
        )
        ReleaseReconciler(
            journal=ReleaseJournal(journal.path), client=rotated,
            policy=_policy(tmp_path), expected_subject_id=SUBJECT,
        ).recover()
        rotated.close()
        completed = journal.read(approval["id"])
        assert completed["state"] == "completed"
        assert (completed["command_body_json"], completed["idempotency_key"], completed["task_etag"]) == exact


def test_policy_and_subject_are_not_controlled_by_approval_text(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        task, approval = _approved(api, gate="quack.unlisted")
        journal = ReleaseJournal(tmp_path / "release.sqlite3")
        client = AgentBusReleaseClient(
            "http://127.0.0.1:8000", TOKENS["orion"], transport=_transport(api),
        )
        reconciler = ReleaseReconciler(
            journal=journal, client=client, policy=_policy(tmp_path), expected_subject_id=SUBJECT,
        )
        with pytest.raises(ReconciliationStopped, match="allowlisted"):
            reconciler.reconcile(approval["id"])
        client.close()
        assert journal.read(approval["id"])["state"] == "stopped"
        assert api.get(f"/tasks/{task['id']}", headers=_headers("orion")).json()["status"] == "waiting_approval"


def test_release_rejects_other_subject_before_replay_and_historical_provenance(tmp_path):
    database = tmp_path / "bus.sqlite3"
    with TestClient(create_app(database, authenticator=_registry(tmp_path))) as api:
        task, approval = _approved(api)
        approved_event = next(
            event for event in api.get(f"/tasks/{task['id']}/events", headers=_headers("orion")).json()
            if event["type"] == "approval.approved"
        )
        body = {
            "approval_id": approval["id"], "actor_id": SUBJECT,
            "causation_event_id": approved_event["id"],
        }
        first = api.post(f"/tasks/{task['id']}/release", json=body, headers=_headers(
            "orion", **{"Idempotency-Key": "release", "If-Match": '"v2"'},
        ))
        stolen = api.post(f"/tasks/{task['id']}/release", json={
            **body, "actor_id": "other-coordinator",
        }, headers=_headers(
            "other", **{"Idempotency-Key": "release", "If-Match": '"v2"'},
        ))
        assert first.status_code == 200
        assert stolen.status_code == 403

    historical_database = tmp_path / "historical.sqlite3"
    with TestClient(create_app(historical_database, authenticator=_registry(tmp_path))) as api:
        task, approval = _approved(api)
        events = api.get(f"/tasks/{task['id']}/events", headers=_headers("orion")).json()
        approved_event = next(event for event in events if event["type"] == "approval.approved")
        with sqlite3.connect(historical_database) as db:
            db.execute("DROP TRIGGER events_are_append_only_on_update")
            db.execute(
                "UPDATE events SET authenticated_principal_id=NULL WHERE type='approval.requested'"
            )
        blocked = api.post(f"/tasks/{task['id']}/release", json={
            "approval_id": approval["id"], "actor_id": SUBJECT,
            "causation_event_id": approved_event["id"],
        }, headers=_headers(
            "orion", **{"Idempotency-Key": "historical", "If-Match": '"v2"'},
        ))
        assert blocked.status_code == 403
        assert blocked.json()["detail"]["code"] == "approval_request_provenance_invalid"


def test_journal_compare_and_swap_prevents_two_simultaneous_sends(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _approved(api)
        journal, client, reconciler = _reconciler(
            tmp_path, api, transport=_transport(api, release_401=True),
        )
        with pytest.raises(ReconciliationError):
            reconciler.reconcile(approval["id"])
        client.close()
        current = journal.read(approval["id"])
        first = journal.begin_send(approval["id"], expected_revision=current["revision"])
        assert first["state"] == "sending"
        with pytest.raises(ReconciliationStopped, match="concurrently"):
            journal.begin_send(approval["id"], expected_revision=current["revision"])


def test_recovery_with_another_subject_fails_closed(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3", authenticator=_registry(tmp_path))) as api:
        _, approval = _approved(api)
        journal, client, reconciler = _reconciler(
            tmp_path, api, transport=_transport(api, release_401=True),
        )
        with pytest.raises(ReconciliationError):
            reconciler.reconcile(approval["id"])
        client.close()
        other = AgentBusReleaseClient(
            "http://127.0.0.1:8000", TOKENS["other"], transport=_transport(api),
        )
        wrong_identity = ReleaseReconciler(
            journal=ReleaseJournal(journal.path), client=other,
            policy=_policy(tmp_path), expected_subject_id=SUBJECT,
        )
        with pytest.raises(ReconciliationStopped, match="cannot recover"):
            wrong_identity.recover()
        other.close()
        assert journal.read(approval["id"])["state"] == "stopped"
