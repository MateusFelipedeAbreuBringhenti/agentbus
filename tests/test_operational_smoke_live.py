import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from uuid import UUID, uuid4

import httpx

from agentbus.approval_intents import ApprovalIntentJournal
from agentbus.auth import token_digest
from agentbus.coordinator import CoordinatorOutbox
from agentbus.orion_ops import (
    CreateSuccessorInput,
    CreateTaskInput,
    HttpControlTransport,
    OrionOpsControlPlane,
    RequestApprovalInput,
)
from agentbus.release_reconciler import ReleaseJournal
from agentbus.runner import RunnerJournal


ROOT = Path(__file__).resolve().parents[1]
SUBJECT = "local-coordinator:11111111-1111-4111-8111-111111111111"
TOKENS = {
    "coordinator": "live-coordinator-token-000000000000000000",
    "approver": "live-approver-token-00000000000000000000",
    "runner": "live-runner-token-0000000000000000000000",
    "viewer": "live-viewer-token-0000000000000000000000",
    "projection": "live-projection-token-000000000000000000",
    "projection_reader": "live-projection-reader-0000000000000000",
}


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _start(command: list[str], environment: dict[str, str], cwd: Path) -> subprocess.Popen:
    return subprocess.Popen(
        command, cwd=cwd, env=environment,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _wait_for(url: str, *, expected: int | None = None) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=0.3)
            if expected is None or response.status_code == expected:
                return
        except httpx.TransportError:
            pass
        time.sleep(0.05)
    raise AssertionError(f"service did not become ready: {url}")


def _private(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)
    return path


def _token_file(tmp_path: Path, name: str) -> Path:
    return _private(tmp_path / f"{name}.token", TOKENS[name] + "\n")


def _headers(name: str, **extra: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKENS[name]}", **extra}


def _chromium_dom(chromium: str, url: str) -> str:
    rendered = subprocess.run([
        chromium, "--headless", "--disable-gpu", "--no-sandbox",
        "--virtual-time-budget=4000", "--dump-dom", url,
    ], check=True, capture_output=True, text=True, timeout=40)
    return rendered.stdout


def test_real_processes_cli_runner_and_browser_observe_one_completed_task(tmp_path):
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    assert chromium is not None, "Chromium is mandatory for the approved operational E2E"
    bus_port, gateway_port, intent_port, projection_port, vite_port = (
        _free_port() for _ in range(5)
    )
    bus_url = f"http://127.0.0.1:{bus_port}"

    definitions = [
        ("coordinator-1", SUBJECT, "coordinator", [
            "task:create", "task:create:successor", "approval:request",
            "task:release", "task:read:any",
            "approval:read:any", "event:read:any", "trace:read:any",
        ]),
        ("approver-1", "mateus-e2e", "approver", [
            "approval:read:self", "approval:decide:self",
        ]),
        ("runner-1", "dex-e2e", "runner", [
            "inbox:read:self", "task:read:self", "task:claim:self",
            "task:complete:self", "task:fail:self",
        ]),
        ("viewer-1", "control-center", "viewer", [
            "task:read:any", "approval:read:any", "event:read:any", "trace:read:any",
        ]),
        ("projection-1", "operational-projection", "projection", ["task:read:any"]),
    ]
    auth = _private(tmp_path / "auth.json", json.dumps({"principals": [{
        "principal_id": principal, "subject_id": subject,
        "token_sha256": token_digest(TOKENS[token]), "permissions": permissions,
    } for principal, subject, token, permissions in definitions]}))
    execution_policy = _private(tmp_path / "execution-policy.json", json.dumps({
        "policy_id": "palace-e2e-live-v1",
        "subjects": {"dex-e2e": ["palace.e2e.noop"]},
    }))
    release_policy = _private(tmp_path / "release-policy.json", json.dumps({
        "allowed_gates": ["palace.e2e.manual"],
    }))
    token_files = {name: _token_file(tmp_path, name) for name in TOKENS}

    intent_journal = tmp_path / "approval-intents.sqlite3"
    release_journal = tmp_path / "release.sqlite3"
    runner_journal = tmp_path / "runner.sqlite3"
    outbox_journal = tmp_path / "orion-outbox.sqlite3"
    ApprovalIntentJournal(intent_journal)
    ReleaseJournal(release_journal)
    with RunnerJournal(runner_journal): pass
    CoordinatorOutbox(outbox_journal)
    manifest = _private(tmp_path / "operations.json", json.dumps({"sources": [
        {"source_id": "approval", "kind": "approval_intent", "path": str(intent_journal), "logical_owner": "mateus-e2e"},
        {"source_id": "release", "kind": "release", "path": str(release_journal), "logical_owner": SUBJECT},
        {"source_id": "runner", "kind": "runner", "path": str(runner_journal), "logical_owner": "dex-e2e"},
        {"source_id": "outbox", "kind": "outbox", "path": str(outbox_journal), "logical_owner": SUBJECT},
    ]}))

    base_env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    processes: list[subprocess.Popen] = []
    try:
        processes.append(_start([
            sys.executable, "-m", "uvicorn", "agentbus.app:create_app", "--factory",
            "--host", "127.0.0.1", "--port", str(bus_port),
        ], {**base_env, "AGENTBUS_DATABASE": str(tmp_path / "agentbus.sqlite3"),
            "AGENTBUS_AUTH_CONFIG": str(auth),
            "AGENTBUS_EXECUTION_POLICY": str(execution_policy)}, ROOT))
        _wait_for(f"{bus_url}/auth/me", expected=401)

        processes.append(_start([
            sys.executable, "-m", "uvicorn", "agentbus.operational_projection:app",
            "--host", "127.0.0.1", "--port", str(projection_port),
        ], {**base_env, "AGENTBUS_URL": bus_url,
            "AGENTBUS_OPERATIONAL_MANIFEST": str(manifest),
            "AGENTBUS_OPERATIONAL_AGENTBUS_TOKEN_FILE": str(token_files["projection"]),
            "AGENTBUS_OPERATIONAL_READER_TOKEN_FILE": str(token_files["projection_reader"])}, ROOT))
        _wait_for(f"http://127.0.0.1:{projection_port}/operations", expected=401)

        processes.append(_start([
            sys.executable, "-m", "uvicorn", "agentbus.control_center_gateway:app",
            "--host", "127.0.0.1", "--port", str(gateway_port),
        ], {**base_env, "AGENTBUS_URL": bus_url,
            "AGENTBUS_CONTROL_CENTER_TOKEN_FILE": str(token_files["viewer"]),
            "AGENTBUS_CONTROL_CENTER_HOST": f"127.0.0.1:{vite_port}",
            "AGENTBUS_OPERATIONAL_URL": f"http://127.0.0.1:{projection_port}",
            "AGENTBUS_OPERATIONAL_READER_TOKEN_FILE": str(token_files["projection_reader"])}, ROOT))

        processes.append(_start([
            sys.executable, "-m", "uvicorn", "agentbus.approval_intents:app",
            "--host", "127.0.0.1", "--port", str(intent_port),
        ], {**base_env, "AGENTBUS_URL": bus_url,
            "AGENTBUS_CONTROL_CENTER_TOKEN_FILE": str(token_files["viewer"]),
            "AGENTBUS_CONTROL_CENTER_HOST": f"127.0.0.1:{vite_port}",
            "AGENTBUS_APPROVAL_INTENT_JOURNAL": str(intent_journal)}, ROOT))

        processes.append(_start([
            "npm", "run", "dev", "--", "--port", str(vite_port), "--strictPort",
        ], {**os.environ,
            "AGENTBUS_CONTROL_CENTER_GATEWAY": f"http://127.0.0.1:{gateway_port}",
            "AGENTBUS_APPROVAL_INTENT_SERVICE": f"http://127.0.0.1:{intent_port}"},
            ROOT / "frontend"))
        _wait_for(f"http://127.0.0.1:{vite_port}/")

        correlation_id = uuid4()
        coordinator_transport = HttpControlTransport(
            bus_url, token=TOKENS["coordinator"], subject_id=SUBJECT,
        )
        try:
            coordinator = OrionOpsControlPlane(
                coordinator_transport,
                instance_id=UUID("11111111-1111-4111-8111-111111111111"),
                outbox=CoordinatorOutbox(outbox_journal),
            )
            seed = coordinator.create_task(CreateTaskInput(
                type="palace.e2e.noop", title="Palace live test-drive seed",
                input={"purpose": "causal predecessor only"}, assigned_to="dex-e2e",
                correlation_id=correlation_id, idempotency_key="live-seed-create",
            ))

            seed_run = subprocess.run([
                sys.executable, "-m", "agentbus.runner_cli", "run",
                "--agent-id", "dex-e2e", "--journal", str(runner_journal),
                "--token-file", str(token_files["runner"]),
                "--executor", "deterministic-noop",
                "--allow-task-type", "palace.e2e.noop", "--agentbus-url", bus_url,
                "--once",
            ], cwd=ROOT, env=base_env, capture_output=True, text=True, timeout=20)
            assert seed_run.returncode == 0, seed_run.stderr

            coordination_id = uuid4()
            task_result = coordinator.create_successor_task(CreateSuccessorInput(
                coordination_id=coordination_id, predecessor_task_id=UUID(seed.data["id"]),
                type="palace.e2e.noop", title="Palace live operational test drive",
                input={"message": "Palace operational readiness", "expected_result": "verified"},
                assigned_to="dex-e2e",
            ))
            task = task_result.data
            requested = coordinator.request_approval(RequestApprovalInput(
                task_id=UUID(task["id"]), gate="palace.e2e.manual",
                request_reason="Live human boundary", assigned_to="mateus-e2e",
                if_match=task_result.http.etag,
                idempotency_key="live-request",
            ))
            approval = requested.data["approval"]
            approval_etag = requested.http.approval_etag
        finally:
            coordinator_transport.close()

        with sqlite3.connect(outbox_journal) as outbox_db:
            persisted_successor = outbox_db.execute(
                "SELECT predecessor_task_id, successor_task_id, confirmed_at "
                "FROM successor_commands WHERE coordination_id = ?",
                (str(coordination_id),),
            ).fetchone()
        assert persisted_successor is not None
        assert persisted_successor[:2] == (seed.data["id"], task["id"])
        assert persisted_successor[2] is not None

        with httpx.Client(base_url=f"http://127.0.0.1:{vite_port}") as browser_http:
            boundary = {"Origin": f"http://127.0.0.1:{vite_port}", "Sec-Fetch-Site": "same-origin"}
            session = browser_http.get("/intent-api/session", headers=boundary).json()
            intent = browser_http.post("/intent-api/approval-intents", json={
                "approval_id": approval["id"], "decision": "approve",
                "decision_reason": "Automated isolated identity",
                "if_match": approval_etag,
            }, headers={**boundary, "Intent-Key": "live-intent",
                        "X-AgentBus-CSRF": session["csrf_token"]})
        assert intent.status_code == 201

        confirmed = subprocess.run([
            sys.executable, "-m", "agentbus.approval_confirm", "confirm",
            intent.json()["intent_id"], "--journal", str(intent_journal),
            "--token-file", str(token_files["approver"]), "--agentbus-url", bus_url,
        ], cwd=ROOT, env=base_env, input="APROVAR\n", text=True,
            capture_output=True, timeout=20)
        assert confirmed.returncode == 0, confirmed.stderr
        assert httpx.get(f"{bus_url}/tasks/{task['id']}", headers=_headers("coordinator")).json()["status"] == "waiting_approval"

        released = subprocess.run([
            sys.executable, "-m", "agentbus.release_reconciler", "reconcile",
            approval["id"], "--journal", str(release_journal),
            "--policy", str(release_policy), "--token-file", str(token_files["coordinator"]),
            "--subject", SUBJECT, "--agentbus-url", bus_url,
        ], cwd=ROOT, env=base_env, capture_output=True, text=True, timeout=20)
        assert released.returncode == 0, released.stderr

        ran = subprocess.run([
            sys.executable, "-m", "agentbus.runner_cli", "run",
            "--agent-id", "dex-e2e", "--journal", str(runner_journal),
            "--token-file", str(token_files["runner"]),
            "--executor", "deterministic-noop",
            "--allow-task-type", "palace.e2e.noop", "--agentbus-url", bus_url, "--once",
        ], cwd=ROOT, env=base_env, capture_output=True, text=True, timeout=20)
        assert ran.returncode == 0, ran.stderr

        final = httpx.get(f"{bus_url}/tasks/{task['id']}", headers=_headers("coordinator"))
        assert final.json()["status"] == "succeeded"
        events = httpx.get(f"{bus_url}/tasks/{task['id']}/events", headers=_headers("coordinator")).json()
        assert [event["type"] for event in events][-3:] == [
            "task.released", "task.started", "task.completed",
        ]

        task_dom = _chromium_dom(chromium, f"http://127.0.0.1:{vite_port}/tasks")
        trace_dom = _chromium_dom(chromium, f"http://127.0.0.1:{vite_port}/correlations/{correlation_id}")
        operations_dom = _chromium_dom(chromium, f"http://127.0.0.1:{vite_port}/operations")
        assert "Palace live operational test drive" in task_dom and "succeeded" in task_dom
        assert "task.completed" in trace_dom and "task.released" in trace_dom
        for source in ("approval", "release", "runner", "outbox"):
            assert f"{source}: available" in operations_dom
        assert "approval_intent" in operations_dom
        assert "release" in operations_dom
        assert "runner" in operations_dom
    finally:
        for process in reversed(processes):
            process.terminate()
        for process in reversed(processes):
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=5)
