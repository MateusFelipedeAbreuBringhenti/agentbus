from __future__ import annotations

import json
import os
from pathlib import Path
import selectors
import sqlite3
import socket
import subprocess
import sys
import time
from uuid import uuid4

import httpx
import pytest

from agentbus.auth import token_digest
from agentbus.runner import RunnerJournal


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "tests/support/recovery_probe.py"
TOKEN = "recovery-runner-token-000000000000000000"
CREATOR_TOKEN = "recovery-creator-token-00000000000000000"


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
