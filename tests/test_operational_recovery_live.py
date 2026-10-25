from __future__ import annotations

import json
import os
from pathlib import Path
import hashlib
import multiprocessing
import selectors
import shutil
import sqlite3
import socket
import subprocess
import sys
import time
from uuid import UUID, uuid4

import httpx
import pytest

from agentbus.auth import token_digest
from agentbus.approval_confirm import ApprovalConfirmer
from agentbus.approval_intents import ApprovalIntentJournal, IntentCreate
from agentbus.coordinator import CoordinatorOutbox
from agentbus.models import ApprovalRead
from agentbus.orion_ops import (
    CreateSuccessorInput, HttpControlTransport, OrionOpsControlPlane,
)
from agentbus.release_reconciler import (
    AgentBusReleaseClient, ReconciliationStopped, ReleaseJournal, ReleasePolicy,
    ReleaseReconciler,
)
from agentbus.runner import RunnerJournal


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "tests/support/recovery_probe.py"
TOKEN = "recovery-runner-token-000000000000000000"
CREATOR_TOKEN = "recovery-creator-token-00000000000000000"
COORD_TOKEN = "recovery-coordinator-token-000000000000000"
ROTATED_TOKEN = "recovery-coordinator-rotated-000000000000"
APPROVER_TOKEN = "recovery-approver-token-0000000000000000"
COORD_INSTANCE = "33333333-3333-4333-8333-333333333333"
COORD_SUBJECT = f"local-coordinator:{COORD_INSTANCE}"
PROJECTION_TOKEN = "recovery-projection-token-000000000000000"
READER_TOKEN = "recovery-reader-token-00000000000000000000"
OTHER_SUBJECT_TOKEN = "recovery-other-subject-token-000000000000000"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _wait_for(url: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=0.2).status_code == 401:
                return
        except httpx.TransportError:
            pass
        time.sleep(0.05)
    raise AssertionError("AgentBus did not start")


@pytest.fixture
def live_agentbus(tmp_path):
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    auth = tmp_path / "auth.json"
    auth.write_text(json.dumps({"principals": [
        {
            "principal_id": "runner-recovery", "subject_id": "dex-recovery",
            "token_sha256": token_digest(TOKEN),
            "permissions": ["inbox:read:self", "task:read:self", "task:claim:self",
                            "task:complete:self", "task:fail:self"],
        },
        {
            "principal_id": "coordinator-old", "subject_id": COORD_SUBJECT,
            "token_sha256": token_digest(COORD_TOKEN),
            "permissions": ["task:create", "task:create:successor", "approval:request", "task:release",
                            "task:read:any", "approval:read:any", "event:read:any",
                            "trace:read:any"],
        },
        {
            "principal_id": "projection", "subject_id": "recovery-projection",
            "token_sha256": token_digest(PROJECTION_TOKEN),
            "permissions": ["task:read:any"],
        },
        {
            "principal_id": "coordinator-rotated", "subject_id": COORD_SUBJECT,
            "token_sha256": token_digest(ROTATED_TOKEN),
            "permissions": ["task:release", "task:read:any", "approval:read:any",
                            "event:read:any"],
        },
        {
            "principal_id": "other-coordinator", "subject_id": "local-coordinator:other",
            "token_sha256": token_digest(OTHER_SUBJECT_TOKEN),
            "permissions": ["task:release", "task:read:any", "approval:read:any",
                            "event:read:any"],
        },
        {
            "principal_id": "approver", "subject_id": "mateus-recovery",
            "token_sha256": token_digest(APPROVER_TOKEN),
            "permissions": ["approval:read:self", "approval:decide:self"],
        },
        {
            "principal_id": "recovery-harness", "subject_id": "recovery-harness",
            "token_sha256": token_digest(CREATOR_TOKEN),
            "permissions": ["task:create", "task:read:any", "event:read:any"],
        },
    ]}), encoding="utf-8")
    auth.chmod(0o600)
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({
        "policy_id": "recovery-v1",
        "subjects": {"dex-recovery": ["palace.recovery.probe"]},
    }), encoding="utf-8")
    policy.chmod(0o600)
    process = subprocess.Popen([
        sys.executable, "-m", "uvicorn", "agentbus.app:create_app", "--factory",
        "--host", "127.0.0.1", "--port", str(port),
    ], cwd=ROOT, env={
        **os.environ, "PYTHONPATH": str(ROOT / "src"),
        "AGENTBUS_DATABASE": str(tmp_path / "agentbus.sqlite3"),
        "AGENTBUS_AUTH_CONFIG": str(auth),
        "AGENTBUS_EXECUTION_POLICY": str(policy),
    }, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_for(f"{url}/auth/me")
        created = httpx.post(f"{url}/tasks", json={
                "type": "palace.recovery.probe",
            "title": "Controlled recovery probe", "input": {},
                "requested_by": "recovery-harness", "assigned_to": "dex-recovery",
            "correlation_id": str(uuid4()),
        }, headers={
            "Authorization": f"Bearer {CREATOR_TOKEN}",
            "Idempotency-Key": "recovery-create",
        })
        assert created.status_code == 201
        yield url, created.json()["id"]
    finally:
        _terminate_child(process)


def _auth(token: str, **extra: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", **extra}


def _pending_approval(url: str, key: str) -> tuple[dict, dict, str]:
    created = httpx.post(f"{url}/tasks", json={
        "type": "palace.recovery.probe", "title": f"Recovery {key}", "input": {},
        "requested_by": COORD_SUBJECT, "assigned_to": "dex-recovery",
        "correlation_id": str(uuid4()),
    }, headers=_auth(COORD_TOKEN, **{"Idempotency-Key": f"{key}-create"}))
    assert created.status_code == 201
    task = created.json()
    requested = httpx.post(f"{url}/tasks/{task['id']}/request-approval", json={
        "gate": "palace.recovery", "request_reason": key,
        "requested_by": COORD_SUBJECT, "assigned_to": "mateus-recovery", "context": {},
    }, headers=_auth(COORD_TOKEN, **{
        "Idempotency-Key": f"{key}-request", "If-Match": created.headers["etag"],
    }))
    assert requested.status_code == 200
    return task, requested.json()["approval"], requested.headers["approval-etag"]


def _approved(url: str, key: str) -> tuple[dict, dict]:
    task, approval, etag = _pending_approval(url, key)
    decided = httpx.post(f"{url}/approvals/{approval['id']}/approve", json={
        "decided_by": "mateus-recovery", "decision_reason": "recovery fixture",
    }, headers=_auth(APPROVER_TOKEN, **{
        "Idempotency-Key": f"{key}-decide", "If-Match": etag,
    }))
    assert decided.status_code == 200
    return task, decided.json()


class _BlockingReleaseJournal(ReleaseJournal):
    checkpoint = None

    def complete(self, *args, **kwargs):
        self.checkpoint.send("upstream_release_confirmed")
        multiprocessing.Event().wait()


def _release_child(url: str, journal: str, policy: str, approval_id: str, send) -> None:
    _BlockingReleaseJournal.checkpoint = send
    client = AgentBusReleaseClient(url, COORD_TOKEN)
    try:
        ReleaseReconciler(
            journal=_BlockingReleaseJournal(journal), client=client,
            policy=ReleasePolicy.from_file(policy), expected_subject_id=COORD_SUBJECT,
        ).reconcile(approval_id)
    finally:
        client.close()


class _RevokedDuringReleaseClient(AgentBusReleaseClient):
    def release(self, row):
        return httpx.post(
            f"{self.client.base_url}/tasks/{row['task_id']}/release",
            json=json.loads(row["command_body_json"]),
            headers={
                "Authorization": "Bearer revoked-recovery-token",
                "Idempotency-Key": row["idempotency_key"],
                "If-Match": row["task_etag"],
            },
        )


def _auth_required_child(url: str, journal: str, policy: str, approval_id: str) -> None:
    client = _RevokedDuringReleaseClient(url, COORD_TOKEN)
    try:
        try:
            ReleaseReconciler(
                journal=ReleaseJournal(journal), client=client,
                policy=ReleasePolicy.from_file(policy), expected_subject_id=COORD_SUBJECT,
            ).reconcile(approval_id)
        except Exception:
            pass
    finally:
        client.close()


class _BlockingApprovalJournal(ApprovalIntentJournal):
    checkpoint = None

    def record_completed(self, *args, **kwargs):
        self.checkpoint.send("upstream_approval_confirmed")
        multiprocessing.Event().wait()


def _approval_child(url: str, journal: str, intent_id: str, send) -> None:
    _BlockingApprovalJournal.checkpoint = send
    confirmer = ApprovalConfirmer(
        journal=_BlockingApprovalJournal(Path(journal)), journal_path=Path(journal),
        agentbus_url=url, token=APPROVER_TOKEN,
        prompt=lambda _: "APROVAR", output=lambda _: None,
    )
    try:
        confirmer.confirm(intent_id)
    finally:
        confirmer.close()


def _kill_owned(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
    process.join(timeout=5)
    if process.is_alive():
        process.kill(); process.join(timeout=5)


class _BlockingOutbox(CoordinatorOutbox):
    checkpoint = None

    def confirm(self, *args, **kwargs):
        self.checkpoint.send("upstream_successor_confirmed")
        multiprocessing.Event().wait()


def _outbox_child(url: str, journal: str, predecessor_id: str, send) -> None:
    _BlockingOutbox.checkpoint = send
    transport = HttpControlTransport(url, token=COORD_TOKEN, subject_id=COORD_SUBJECT)
    try:
        OrionOpsControlPlane(
            transport, instance_id=UUID(COORD_INSTANCE),
            outbox=_BlockingOutbox(journal),
        ).create_successor_task(CreateSuccessorInput(
            coordination_id=uuid4(), predecessor_task_id=predecessor_id,
            type="palace.recovery.probe", title="Recovered successor",
            input={}, assigned_to="dex-recovery",
        ))
    finally:
        transport.close()


def _start(
    tmp_path: Path, mode: str, agentbus_url: str, task_id: str,
) -> tuple[subprocess.Popen, Path, Path]:
    journal = tmp_path / "runner.sqlite3"
    ledger = tmp_path / "effects.sqlite3"
    process = subprocess.Popen(
        [
            sys.executable, str(PROBE), "--journal", str(journal),
            "--ledger", str(ledger), "--mode", mode,
            "--agentbus-url", agentbus_url, "--token", TOKEN,
            "--task-id", task_id,
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return process, journal, ledger


def _checkpoint(process: subprocess.Popen, expected: str) -> dict:
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            if selector.select(timeout=0.2):
                line = process.stdout.readline()
                if not line:
                    break
                payload = json.loads(line)
                if payload["checkpoint"] == expected:
                    return payload
    finally:
        selector.close()
    if process.poll() is None:
        _terminate_child(process)
    stderr = process.stderr.read() if process.stderr is not None else ""
    raise AssertionError(f"checkpoint {expected!r} was not reached: {stderr}")


def _terminate_child(process: subprocess.Popen) -> None:
    assert process.pid is not None
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _finish(tmp_path: Path, agentbus_url: str, task_id: str) -> dict:
    process, _, _ = _start(tmp_path, "finish", agentbus_url, task_id)
    try:
        checkpoint = _checkpoint(process, "finished")
        assert process.wait(timeout=10) == 0
        return checkpoint
    finally:
        if process.poll() is None:
            _terminate_child(process)


def _events(ledger: Path) -> list[tuple[str, str]]:
    with sqlite3.connect(ledger) as connection:
        return connection.execute(
            "SELECT execution_id, phase FROM probe_events ORDER BY sequence"
        ).fetchall()


def test_h1_crash_after_synthetic_effect_reexecutes_same_logical_execution(tmp_path, live_agentbus):
    agentbus_url, task_id = live_agentbus
    process, journal, ledger = _start(tmp_path, "block-after-effect", agentbus_url, task_id)
    try:
        first = _checkpoint(process, "synthetic_effect_committed")
    finally:
        if process.poll() is None:
            _terminate_child(process)

    with RunnerJournal(journal) as durable:
        interrupted = durable.get(task_id)
        assert interrupted is not None
        assert interrupted.claimed_etag == '"v2"'
        assert interrupted.result_kind is None
        assert interrupted.reported_at is None

    recovered = _finish(tmp_path, agentbus_url, task_id)
    events = _events(ledger)
    assert events == [
        (first["execution_id"], "invocation_started"),
        (first["execution_id"], "synthetic_effect_committed"),
        (first["execution_id"], "invocation_started"),
        (first["execution_id"], "synthetic_effect_committed"),
    ]
    assert recovered["execution_id"] == first["execution_id"]
    with RunnerJournal(journal) as durable:
        assert durable.get(task_id).reported_at is not None
    events = httpx.get(f"{agentbus_url}/tasks/{task_id}/events", headers={
        "Authorization": f"Bearer {CREATOR_TOKEN}",
    }).json()
    assert [event["type"] for event in events] == [
        "task.created", "task.started", "task.completed",
    ]


def test_h2_crash_after_durable_result_reports_without_reexecution(tmp_path, live_agentbus):
    agentbus_url, task_id = live_agentbus
    process, journal, ledger = _start(tmp_path, "block-before-report", agentbus_url, task_id)
    try:
        effect = _checkpoint(process, "synthetic_effect_committed")
        durable_checkpoint = _checkpoint(process, "result_durable_before_report")
        assert durable_checkpoint["execution_id"] == effect["execution_id"]
    finally:
        if process.poll() is None:
            _terminate_child(process)

    with RunnerJournal(journal) as durable:
        interrupted = durable.get(task_id)
        assert interrupted is not None
        assert interrupted.result_kind == "succeeded"
        assert interrupted.report_key is not None
        assert interrupted.reported_at is None

    recovered = _finish(tmp_path, agentbus_url, task_id)
    assert recovered["execution_id"] == effect["execution_id"]
    assert _events(ledger) == [
        (effect["execution_id"], "invocation_started"),
        (effect["execution_id"], "synthetic_effect_committed"),
    ]
    with RunnerJournal(journal) as durable:
        assert durable.get(task_id).reported_at is not None
    events = httpx.get(f"{agentbus_url}/tasks/{task_id}/events", headers={
        "Authorization": f"Bearer {CREATOR_TOKEN}",
    }).json()
    assert [event["type"] for event in events] == [
        "task.created", "task.started", "task.completed",
    ]


def test_h3_lost_confirmed_terminal_response_replays_without_duplicate_event(
    tmp_path, live_agentbus,
):
    agentbus_url, task_id = live_agentbus
    process, journal, ledger = _start(tmp_path, "drop-terminal", agentbus_url, task_id)
    try:
        upstream = _checkpoint(process, "upstream_terminal_confirmed")
        _checkpoint(process, "finished")
        assert process.wait(timeout=10) == 0
    finally:
        if process.poll() is None:
            _terminate_child(process)

    with RunnerJournal(journal) as durable:
        interrupted = durable.get(task_id)
        assert interrupted is not None
        assert interrupted.result_kind == "succeeded"
        assert interrupted.reported_at is None
    assert httpx.get(
        f"{agentbus_url}/tasks/{task_id}",
        headers={"Authorization": f"Bearer {CREATOR_TOKEN}"},
    ).json()["status"] == "succeeded"

    recovered = _finish(tmp_path, agentbus_url, task_id)
    assert recovered["execution_id"] == upstream["execution_id"]
    assert len(_events(ledger)) == 2
    events = httpx.get(f"{agentbus_url}/tasks/{task_id}/events", headers={
        "Authorization": f"Bearer {CREATOR_TOKEN}",
    }).json()
    assert [event["type"] for event in events] == [
        "task.created", "task.started", "task.completed",
    ]


def test_h4_release_process_crash_recovers_exact_command_and_single_event(
    tmp_path, live_agentbus,
):
    url, _ = live_agentbus
    task, approval = _approved(url, "h4")
    journal = tmp_path / "release-h4.sqlite3"
    policy = tmp_path / "release-policy.json"
    policy.write_text(json.dumps({"allowed_gates": ["palace.recovery"]}), encoding="utf-8")
    policy.chmod(0o600)
    receive, send = multiprocessing.Pipe(duplex=False)
    process = multiprocessing.get_context("fork").Process(
        target=_release_child,
        args=(url, str(journal), str(policy), approval["id"], send),
    )
    process.start()
    try:
        assert receive.poll(10) and receive.recv() == "upstream_release_confirmed"
    finally:
        _kill_owned(process)
    before = ReleaseJournal(journal).read(approval["id"])
    assert before["state"] == "sending"
    exact = (before["command_body_json"], before["idempotency_key"], before["task_etag"])
    client = AgentBusReleaseClient(url, ROTATED_TOKEN)
    try:
        recovered = ReleaseReconciler(
            journal=ReleaseJournal(journal), client=client,
            policy=ReleasePolicy.from_file(policy), expected_subject_id=COORD_SUBJECT,
        ).recover()
    finally:
        client.close()
    assert len(recovered) == 1 and recovered[0]["state"] == "completed"
    after = ReleaseJournal(journal).read(approval["id"])
    assert (after["command_body_json"], after["idempotency_key"], after["task_etag"]) == exact
    events = httpx.get(f"{url}/tasks/{task['id']}/events", headers=_auth(COORD_TOKEN)).json()
    assert [event["type"] for event in events].count("task.released") == 1


def test_h5_auth_required_process_recovers_with_new_principal_same_subject(
    tmp_path, live_agentbus,
):
    url, _ = live_agentbus
    _, approval = _approved(url, "h5")
    journal = tmp_path / "release-h5.sqlite3"
    policy = tmp_path / "release-policy-h5.json"
    policy.write_text(json.dumps({"allowed_gates": ["palace.recovery"]}), encoding="utf-8")
    policy.chmod(0o600)
    process = multiprocessing.get_context("fork").Process(
        target=_auth_required_child,
        args=(url, str(journal), str(policy), approval["id"]),
    )
    process.start(); process.join(timeout=10)
    try:
        assert process.exitcode == 0
    finally:
        _kill_owned(process)
    suspended = ReleaseJournal(journal).read(approval["id"])
    assert suspended["state"] == "auth_required"
    assert suspended["last_http_status"] == 401
    assert suspended["last_error_code"] == "authentication_required"
    exact = (suspended["command_body_json"], suspended["idempotency_key"])

    adversarial_journal = tmp_path / "release-h5-other-subject.sqlite3"
    shutil.copy2(journal, adversarial_journal)
    other_client = AgentBusReleaseClient(url, OTHER_SUBJECT_TOKEN)
    try:
        with pytest.raises(ReconciliationStopped):
            ReleaseReconciler(
                journal=ReleaseJournal(adversarial_journal), client=other_client,
                policy=ReleasePolicy.from_file(policy),
                expected_subject_id=COORD_SUBJECT,
            ).recover()
    finally:
        other_client.close()
    rejected = ReleaseJournal(adversarial_journal).read(approval["id"])
    assert rejected["state"] == "stopped"
    assert rejected["last_error_code"] == "credential_identity_or_permission_changed"

    client = AgentBusReleaseClient(url, ROTATED_TOKEN)
    try:
        ReleaseReconciler(
            journal=ReleaseJournal(journal), client=client,
            policy=ReleasePolicy.from_file(policy), expected_subject_id=COORD_SUBJECT,
        ).recover()
    finally:
        client.close()
    completed = ReleaseJournal(journal).read(approval["id"])
    assert completed["state"] == "completed"
    assert (completed["command_body_json"], completed["idempotency_key"]) == exact


def test_h6_confirmed_approval_process_crash_recovers_without_new_prompt(
    tmp_path, live_agentbus,
):
    url, _ = live_agentbus
    task, approval_data, approval_etag = _pending_approval(url, "h6")
    journal_path = tmp_path / "approval-h6.sqlite3"
    journal = ApprovalIntentJournal(journal_path)
    intent, _ = journal.create(
        session_id="isolated-session", intent_key="h6-intent",
        command=IntentCreate(
            approval_id=approval_data["id"], decision="approve",
            decision_reason="recovery", if_match=approval_etag,
        ),
        approval=ApprovalRead.model_validate(approval_data), etag=approval_etag,
    )
    receive, send = multiprocessing.Pipe(duplex=False)
    process = multiprocessing.get_context("fork").Process(
        target=_approval_child,
        args=(url, str(journal_path), intent["intent_id"], send),
    )
    process.start()
    try:
        assert receive.poll(10) and receive.recv() == "upstream_approval_confirmed"
    finally:
        _kill_owned(process)
    assert journal.read(intent["intent_id"])["state"] == "sending"
    confirmer = ApprovalConfirmer(
        journal=journal, journal_path=journal_path, agentbus_url=url,
        token=APPROVER_TOKEN,
        prompt=lambda _: (_ for _ in ()).throw(AssertionError("no recovery prompt")),
        output=lambda _: None,
    )
    try:
        recovered = confirmer.recover()
    finally:
        confirmer.close()
    assert len(recovered) == 1
    assert journal.read(intent["intent_id"])["state"] == "completed"
    assert httpx.get(f"{url}/tasks/{task['id']}", headers=_auth(COORD_TOKEN)).json()["status"] == "waiting_approval"


def test_h7_orion_ops_process_crash_recovers_same_successor(tmp_path, live_agentbus):
    url, predecessor_id = live_agentbus
    _finish(tmp_path, url, predecessor_id)
    journal = tmp_path / "outbox-h7.sqlite3"
    receive, send = multiprocessing.Pipe(duplex=False)
    process = multiprocessing.get_context("fork").Process(
        target=_outbox_child,
        args=(url, str(journal), predecessor_id, send),
    )
    process.start()
    try:
        assert receive.poll(10) and receive.recv() == "upstream_successor_confirmed"
    finally:
        _kill_owned(process)
    pending = CoordinatorOutbox(journal).pending()
    assert len(pending) == 1 and pending[0].successor_task_id is None
    predecessor = httpx.get(
        f"{url}/tasks/{predecessor_id}", headers=_auth(COORD_TOKEN),
    ).json()
    trace_before = httpx.get(
        f"{url}/correlations/{predecessor['correlation_id']}/trace",
        headers=_auth(COORD_TOKEN),
    )
    assert trace_before.status_code == 200
    canonical_successors = [
        task for task in trace_before.json()["tasks"] if task["id"] != predecessor_id
    ]
    assert len(canonical_successors) == 1
    canonical_successor_id = canonical_successors[0]["id"]
    transport = HttpControlTransport(url, token=COORD_TOKEN, subject_id=COORD_SUBJECT)
    try:
        recovered = OrionOpsControlPlane(
            transport, instance_id=UUID(COORD_INSTANCE),
            outbox=CoordinatorOutbox(journal),
        ).recover_pending_successors()
    finally:
        transport.close()
    assert len(recovered) == 1
    assert recovered[0].http.idempotency_replayed == "true"
    assert CoordinatorOutbox(journal).pending() == []
    with sqlite3.connect(journal) as connection:
        persisted_successor_id = connection.execute(
            "SELECT successor_task_id FROM successor_commands",
        ).fetchone()[0]
    assert persisted_successor_id == canonical_successor_id
    successor_events = httpx.get(
        f"{url}/tasks/{canonical_successor_id}/events", headers=_auth(COORD_TOKEN),
    )
    assert successor_events.status_code == 200
    assert [event["type"] for event in successor_events.json()].count("task.created") == 1


def test_h8_projection_process_recovers_isolated_source_without_mutation(
    tmp_path, live_agentbus,
):
    url, _ = live_agentbus
    source = tmp_path / "isolated-release.sqlite3"
    ReleaseJournal(source)
    original_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"sources": [{
        "source_id": "isolated-release", "kind": "release", "path": str(source),
        "logical_owner": COORD_SUBJECT,
    }]}), encoding="utf-8")
    manifest.chmod(0o600)
    projection_token = tmp_path / "projection.token"
    projection_token.write_text(PROJECTION_TOKEN + "\n", encoding="utf-8")
    projection_token.chmod(0o600)
    reader_token = tmp_path / "reader.token"
    reader_token.write_text(READER_TOKEN + "\n", encoding="utf-8")
    reader_token.chmod(0o600)
    port = _free_port()
    projection_url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen([
        sys.executable, "-m", "uvicorn", "agentbus.operational_projection:app",
        "--host", "127.0.0.1", "--port", str(port),
    ], cwd=ROOT, env={
        **os.environ, "PYTHONPATH": str(ROOT / "src"), "AGENTBUS_URL": url,
        "AGENTBUS_OPERATIONAL_MANIFEST": str(manifest),
        "AGENTBUS_OPERATIONAL_AGENTBUS_TOKEN_FILE": str(projection_token),
        "AGENTBUS_OPERATIONAL_READER_TOKEN_FILE": str(reader_token),
    }, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    headers = _auth(READER_TOKEN)
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                response = httpx.get(f"{projection_url}/operations", headers=headers)
                if response.status_code == 200:
                    break
            except httpx.TransportError:
                pass
            time.sleep(0.05)
        else:
            raise AssertionError("projection did not start")
        assert response.json()["sources"][0]["status"] == "available"
        hidden = tmp_path / "isolated-release.hidden"
        source.rename(hidden)
        assert httpx.get(f"{projection_url}/operations", headers=headers).json()["sources"][0]["status"] == "unavailable"
        hidden.rename(source)
        assert httpx.get(f"{projection_url}/operations", headers=headers).json()["sources"][0]["status"] == "available"
        assert hashlib.sha256(source.read_bytes()).hexdigest() == original_hash
    finally:
        _terminate_child(process)
