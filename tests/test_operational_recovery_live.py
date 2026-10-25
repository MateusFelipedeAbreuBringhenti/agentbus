from __future__ import annotations

import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import sys
import time

from agentbus.runner import RunnerJournal


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "tests/support/recovery_probe.py"
TASK_ID = "11111111-1111-4111-8111-111111111111"


def _start(tmp_path: Path, mode: str) -> tuple[subprocess.Popen, Path, Path]:
    journal = tmp_path / "runner.sqlite3"
    ledger = tmp_path / "effects.sqlite3"
    process = subprocess.Popen(
        [
            sys.executable, str(PROBE), "--journal", str(journal),
            "--ledger", str(ledger), "--mode", mode,
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


def _finish(tmp_path: Path) -> dict:
    process, _, _ = _start(tmp_path, "finish")
    checkpoint = _checkpoint(process, "finished")
    assert process.wait(timeout=10) == 0
    return checkpoint


def _events(ledger: Path) -> list[tuple[str, str]]:
    with sqlite3.connect(ledger) as connection:
        return connection.execute(
            "SELECT execution_id, phase FROM probe_events ORDER BY sequence"
        ).fetchall()


def test_h1_crash_after_synthetic_effect_reexecutes_same_logical_execution(tmp_path):
    process, journal, ledger = _start(tmp_path, "block-after-effect")
    try:
        first = _checkpoint(process, "synthetic_effect_committed")
    finally:
        if process.poll() is None:
            _terminate_child(process)

    with RunnerJournal(journal) as durable:
        interrupted = durable.get(TASK_ID)
        assert interrupted is not None
        assert interrupted.claimed_etag == '"v2"'
        assert interrupted.result_kind is None
        assert interrupted.reported_at is None

    recovered = _finish(tmp_path)
    events = _events(ledger)
    assert events == [
        (first["execution_id"], "invocation_started"),
        (first["execution_id"], "synthetic_effect_committed"),
        (first["execution_id"], "invocation_started"),
        (first["execution_id"], "synthetic_effect_committed"),
    ]
    assert recovered["execution_id"] == first["execution_id"]
    with RunnerJournal(journal) as durable:
        assert durable.get(TASK_ID).reported_at is not None


def test_h2_crash_after_durable_result_reports_without_reexecution(tmp_path):
    process, journal, ledger = _start(tmp_path, "block-before-report")
    try:
        effect = _checkpoint(process, "synthetic_effect_committed")
        durable_checkpoint = _checkpoint(process, "result_durable_before_report")
        assert durable_checkpoint["execution_id"] == effect["execution_id"]
    finally:
        if process.poll() is None:
            _terminate_child(process)

    with RunnerJournal(journal) as durable:
        interrupted = durable.get(TASK_ID)
        assert interrupted is not None
        assert interrupted.result_kind == "succeeded"
        assert interrupted.report_key is not None
        assert interrupted.reported_at is None

    recovered = _finish(tmp_path)
    assert recovered["execution_id"] == effect["execution_id"]
    assert _events(ledger) == [
        (effect["execution_id"], "invocation_started"),
        (effect["execution_id"], "synthetic_effect_committed"),
    ]
    with RunnerJournal(journal) as durable:
        assert durable.get(TASK_ID).reported_at is not None
