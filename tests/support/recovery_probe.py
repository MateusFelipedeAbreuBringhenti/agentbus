"""Isolated recovery probe used only by the #009H test harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
from threading import Event
from uuid import UUID

from agentbus.runner import (
    AgentBusTransportError,
    AgentRunner,
    ExecutionRequest,
    ExecutionSucceeded,
    HttpAgentBusClient,
    RunnerJournal,
)


AGENT_ID = "dex-recovery"
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


class BlockingReportClient:
    def __init__(
        self, client: HttpAgentBusClient, *, block_before_report: bool,
        drop_terminal_response: bool,
    ) -> None:
        self.client = client
        self.block_before_report = block_before_report
        self.drop_terminal_response = drop_terminal_response
        self.authenticated_subject_id = client.authenticated_subject_id

    def __getattr__(self, name: str):
        return getattr(self.client, name)

    def complete_task(self, *args, **kwargs):
        execution_id = kwargs.get("execution_id") or args[4]
        if self.block_before_report:
            emit("result_durable_before_report", execution_id=str(execution_id))
            Event().wait()
        response = self.client.complete_task(*args, **kwargs)
        if self.drop_terminal_response:
            emit("upstream_terminal_confirmed", execution_id=str(execution_id))
            raise AgentBusTransportError("test harness discarded confirmed response")
        return response


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--agentbus-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument(
        "--mode", required=True,
        choices=("block-after-effect", "block-before-report", "drop-terminal", "finish"),
    )
    arguments = parser.parse_args()
    http_client = HttpAgentBusClient(
        arguments.agentbus_url, token=arguments.token, subject_id=AGENT_ID,
    )
    try:
        with RunnerJournal(arguments.journal) as journal:
            runner = AgentRunner(
                agent_id=AGENT_ID,
                client=BlockingReportClient(
                    http_client,
                    block_before_report=arguments.mode == "block-before-report",
                    drop_terminal_response=arguments.mode == "drop-terminal",
                ),
                journal=journal,
                executor=ProbeExecutor(
                    arguments.ledger,
                    block_after_effect=arguments.mode == "block-after-effect",
                ),
                allowed_task_types=frozenset({"palace.recovery.probe"}),
            )
            runner.poll_once()
            record = journal.get(arguments.task_id)
            assert record is not None
            emit("finished", execution_id=record.execution_id)
    finally:
        http_client.close()


if __name__ == "__main__":
    main()
