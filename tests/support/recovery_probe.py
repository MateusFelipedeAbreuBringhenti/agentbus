"""Isolated recovery probe used only by the #009H test harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from threading import Event
from uuid import UUID

from agentbus.runner import (
    AgentRunner,
    BusResponse,
    ExecutionRequest,
    ExecutionSucceeded,
    RunnerJournal,
)


AGENT_ID = "dex-recovery"
TASK_ID = "11111111-1111-4111-8111-111111111111"
CORRELATION_ID = "22222222-2222-4222-8222-222222222222"


def emit(checkpoint: str, **fields: str) -> None:
    print(json.dumps({"checkpoint": checkpoint, **fields}, sort_keys=True), flush=True)


class ProbeExecutor:
    def __init__(self, ledger: Path, *, block_after_effect: bool) -> None:
        self.ledger = ledger
        self.block_after_effect = block_after_effect

    def _append(self, execution_id: UUID, phase: str) -> None:
        with sqlite3.connect(self.ledger) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS probe_events ("
                "sequence INTEGER PRIMARY KEY AUTOINCREMENT, execution_id TEXT NOT NULL, "
                "phase TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO probe_events(execution_id, phase) VALUES(?, ?)",
                (str(execution_id), phase),
            )

    def execute(self, request: ExecutionRequest) -> ExecutionSucceeded:
        self._append(request.execution_id, "invocation_started")
        self._append(request.execution_id, "synthetic_effect_committed")
        emit("synthetic_effect_committed", execution_id=str(request.execution_id))
        if self.block_after_effect:
            Event().wait()
        return ExecutionSucceeded(output={
            "status": "probe_completed",
            "execution_id": str(request.execution_id),
        })


class ProbeClient:
    authenticated_subject_id = AGENT_ID

    def __init__(self, *, block_before_report: bool) -> None:
        self.block_before_report = block_before_report

    def get_inbox(self, agent_id: str) -> BusResponse:
        assert agent_id == AGENT_ID
        return BusResponse(200, {"items": [{
            "kind": "task",
            "task": {"id": TASK_ID, "status": "ready", "type": "palace.recovery.probe"},
        }]}, {})

    def claim_task(
        self, task_id: str, agent_id: str, idempotency_key: str,
        execution_id: str, runner_instance_id: str,
    ) -> BusResponse:
        return BusResponse(200, {}, {"etag": '"v2"'})

    def get_task(self, task_id: str) -> BusResponse:
        return BusResponse(200, {
            "id": TASK_ID,
            "status": "running",
            "type": "palace.recovery.probe",
            "title": "Controlled recovery probe",
            "description": None,
            "input": {},
            "correlation_id": CORRELATION_ID,
        }, {"etag": '"v2"'})

    def complete_task(
        self, task_id: str, output: dict, idempotency_key: str, if_match: str,
        execution_id: str, runner_instance_id: str,
    ) -> BusResponse:
        if self.block_before_report:
            emit("result_durable_before_report", execution_id=execution_id)
            Event().wait()
        return BusResponse(200, {"id": task_id, "status": "succeeded"}, {"etag": '"v3"'})

    def fail_task(self, *args, **kwargs) -> BusResponse:
        raise AssertionError("The recovery probe never fails its synthetic Task.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument(
        "--mode", required=True,
        choices=("block-after-effect", "block-before-report", "finish"),
    )
    arguments = parser.parse_args()
    with RunnerJournal(arguments.journal) as journal:
        runner = AgentRunner(
            agent_id=AGENT_ID,
            client=ProbeClient(block_before_report=arguments.mode == "block-before-report"),
            journal=journal,
            executor=ProbeExecutor(
                arguments.ledger,
                block_after_effect=arguments.mode == "block-after-effect",
            ),
            allowed_task_types=frozenset({"palace.recovery.probe"}),
        )
        runner.poll_once()
        record = journal.get(TASK_ID)
        assert record is not None
        emit("finished", execution_id=record.execution_id)


if __name__ == "__main__":
    main()
