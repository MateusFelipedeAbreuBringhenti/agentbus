from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import fcntl
import json
from pathlib import Path
import sqlite3
from threading import Event
import time
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4, uuid5

import httpx


class JournalLockedError(Exception):
    """Another process already owns this runner journal."""


class JournalIdentityMismatch(Exception):
    """The journal belongs to a different runner instance."""


class AgentBusTransportError(Exception):
    """The AgentBus response was unavailable or lost."""


@dataclass(frozen=True)
class BusResponse:
    status_code: int
    body: dict[str, Any]
    headers: dict[str, str]


class AgentBusClient(Protocol):
    def get_inbox(self, agent_id: str) -> BusResponse: ...

    def get_task(self, task_id: str) -> BusResponse: ...

    def claim_task(
        self,
        task_id: str,
        agent_id: str,
        idempotency_key: str,
    ) -> BusResponse: ...

    def complete_task(
        self,
        task_id: str,
        output: dict[str, Any],
        idempotency_key: str,
        if_match: str,
    ) -> BusResponse: ...

    def fail_task(
        self,
        task_id: str,
        failure_code: str,
        failure_message: str,
        idempotency_key: str,
        if_match: str,
    ) -> BusResponse: ...


class HttpAgentBusClient:
    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpAgentBusClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> BusResponse:
        try:
            response = self._client.request(method, path, **kwargs)
        except httpx.RequestError as error:
            raise AgentBusTransportError from error
        body = response.json() if response.content else {}
        return BusResponse(
            status_code=response.status_code,
            body=body,
            headers={key.lower(): value for key, value in response.headers.items()},
        )

    def get_inbox(self, agent_id: str) -> BusResponse:
        return self._request("GET", f"/agents/{agent_id}/inbox")

    def get_task(self, task_id: str) -> BusResponse:
        return self._request("GET", f"/tasks/{task_id}")

    def claim_task(
        self,
        task_id: str,
        agent_id: str,
        idempotency_key: str,
    ) -> BusResponse:
        return self._request(
            "POST",
            f"/tasks/{task_id}/claim",
            json={"agent_id": agent_id},
            headers={"Idempotency-Key": idempotency_key},
        )

    def complete_task(
        self,
        task_id: str,
        output: dict[str, Any],
        idempotency_key: str,
        if_match: str,
    ) -> BusResponse:
        return self._request(
            "POST",
            f"/tasks/{task_id}/complete",
            json={"output": output},
            headers={"Idempotency-Key": idempotency_key, "If-Match": if_match},
        )

    def fail_task(
        self,
        task_id: str,
        failure_code: str,
        failure_message: str,
        idempotency_key: str,
        if_match: str,
    ) -> BusResponse:
        return self._request(
            "POST",
            f"/tasks/{task_id}/fail",
            json={
                "failure_code": failure_code,
                "failure_message": failure_message,
            },
            headers={"Idempotency-Key": idempotency_key, "If-Match": if_match},
        )


@dataclass(frozen=True)
class ExecutionRequest:
    execution_id: UUID
    task_id: UUID
    task_type: str
    title: str
    description: str | None
    input: dict[str, Any]
    correlation_id: UUID
    task_etag: str
    agent_id: str


@dataclass(frozen=True)
class ExecutionSucceeded:
    output: dict[str, Any]


@dataclass(frozen=True)
class ExecutionFailed:
    failure_code: str
    failure_message: str


ExecutionResult = ExecutionSucceeded | ExecutionFailed


class WorkExecutor(Protocol):
    def execute(self, request: ExecutionRequest) -> ExecutionResult: ...


class DeterministicWorkExecutor:
    """A vendor-neutral executor used to prove the runner protocol."""

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        if request.input.get("should_fail") is True:
            return ExecutionFailed(
                failure_code="deterministic_failure",
                failure_message="The deterministic executor was asked to fail.",
            )
        return ExecutionSucceeded(
            output={
                "echo": request.input,
                "execution_id": str(request.execution_id),
            }
        )


@dataclass(frozen=True)
class JournalRecord:
    task_id: str
    runner_instance_id: str
    claim_key: str
    execution_id: str
    claimed_etag: str | None
    claim_rejected_at: str | None
    result_kind: Literal["succeeded", "failed"] | None
    result_payload: dict[str, Any] | None
    report_key: str | None
    reported_at: str | None


class RunnerJournal:
    def __init__(
        self,
        path: str | Path,
        *,
        runner_instance_id: UUID | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = open(f"{self.path}.lock", "a+b")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._lock_file.close()
            raise JournalLockedError from error
        self._connection = sqlite3.connect(self.path)
        self._connection.row_factory = sqlite3.Row
        self._create_schema()
        self.runner_instance_id = self._load_or_create_identity(runner_instance_id)

    def close(self) -> None:
        self._connection.close()
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
        self._lock_file.close()

    def __enter__(self) -> RunnerJournal:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _create_schema(self) -> None:
        with self._connection:
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS runner_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    runner_instance_id TEXT NOT NULL
                )"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS executions (
                    task_id TEXT PRIMARY KEY,
                    runner_instance_id TEXT NOT NULL,
                    claim_key TEXT NOT NULL UNIQUE,
                    execution_id TEXT NOT NULL UNIQUE,
                    claimed_etag TEXT,
                    claim_rejected_at TEXT,
                    result_kind TEXT CHECK (
                        result_kind IS NULL OR result_kind IN ('succeeded', 'failed')
                    ),
                    result_payload_json TEXT,
                    report_key TEXT UNIQUE,
                    reported_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    CHECK ((result_kind IS NULL) = (result_payload_json IS NULL)),
                    CHECK ((result_kind IS NULL) = (report_key IS NULL)),
                    CHECK (claimed_etag IS NULL OR claim_rejected_at IS NULL),
                    CHECK (reported_at IS NULL OR report_key IS NOT NULL)
                )"""
            )

    def _load_or_create_identity(self, requested: UUID | None) -> UUID:
        row = self._connection.execute(
            "SELECT runner_instance_id FROM runner_metadata WHERE singleton = 1"
        ).fetchone()
        if row is not None:
            existing = UUID(row["runner_instance_id"])
            if requested is not None and requested != existing:
                self.close()
                raise JournalIdentityMismatch
            return existing
        identity = requested or uuid4()
        with self._connection:
            self._connection.execute(
                """INSERT INTO runner_metadata(singleton, runner_instance_id)
                VALUES (1, ?)""",
                (str(identity),),
            )
        return identity

    def prepare(self, task_id: str) -> JournalRecord:
        now = datetime.now(UTC).isoformat()
        execution_id = uuid5(self.runner_instance_id, f"task:{task_id}")
        claim_key = f"runner:{self.runner_instance_id}:task:{task_id}:claim"
        with self._connection:
            self._connection.execute(
                """INSERT OR IGNORE INTO executions(
                    task_id, runner_instance_id, claim_key, execution_id,
                    claimed_etag, claim_rejected_at, result_kind, result_payload_json,
                    report_key, reported_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)""",
                (
                    task_id,
                    str(self.runner_instance_id),
                    claim_key,
                    str(execution_id),
                    now,
                    now,
                ),
            )
        record = self.get(task_id)
        assert record is not None
        return record

    def get(self, task_id: str) -> JournalRecord | None:
        row = self._connection.execute(
            "SELECT * FROM executions WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        return self._record(row) if row is not None else None

    def pending(self) -> list[JournalRecord]:
        rows = self._connection.execute(
            """SELECT * FROM executions
            WHERE reported_at IS NULL AND claim_rejected_at IS NULL
            ORDER BY created_at, task_id"""
        ).fetchall()
        return [self._record(row) for row in rows]

    def confirm_claim(self, task_id: str, etag: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connection:
            row = self._connection.execute(
                "SELECT claimed_etag FROM executions WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["claimed_etag"] not in (None, etag):
                raise ValueError("Claim replay returned a different ETag.")
            self._connection.execute(
                """UPDATE executions SET claimed_etag = ?, updated_at = ?
                WHERE task_id = ?""",
                (etag, now, task_id),
            )

    def reject_claim(self, task_id: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connection:
            self._connection.execute(
                """UPDATE executions
                SET claim_rejected_at = ?, updated_at = ?
                WHERE task_id = ? AND claimed_etag IS NULL""",
                (now, now, task_id),
            )

    def record_result(self, task_id: str, result: ExecutionResult) -> None:
        if isinstance(result, ExecutionSucceeded):
            kind = "succeeded"
            payload = {"output": result.output}
        else:
            kind = "failed"
            payload = {
                "failure_code": result.failure_code,
                "failure_message": result.failure_message,
            }
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        report_key = f"runner:{self.runner_instance_id}:task:{task_id}:{kind}"
        now = datetime.now(UTC).isoformat()
        with self._connection:
            row = self._connection.execute(
                """SELECT result_kind, result_payload_json, report_key
                FROM executions WHERE task_id = ?""",
                (task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["result_kind"] is not None:
                if tuple(row) != (kind, payload_json, report_key):
                    raise ValueError("Executor returned a different durable result.")
                return
            self._connection.execute(
                """UPDATE executions
                SET result_kind = ?, result_payload_json = ?, report_key = ?,
                    updated_at = ?
                WHERE task_id = ?""",
                (kind, payload_json, report_key, now, task_id),
            )

    def mark_reported(self, task_id: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connection:
            self._connection.execute(
                """UPDATE executions SET reported_at = ?, updated_at = ?
                WHERE task_id = ? AND report_key IS NOT NULL""",
                (now, now, task_id),
            )

    @staticmethod
    def _record(row: sqlite3.Row) -> JournalRecord:
        payload = (
            json.loads(row["result_payload_json"])
            if row["result_payload_json"] is not None
            else None
        )
        return JournalRecord(
            task_id=row["task_id"],
            runner_instance_id=row["runner_instance_id"],
            claim_key=row["claim_key"],
            execution_id=row["execution_id"],
            claimed_etag=row["claimed_etag"],
            claim_rejected_at=row["claim_rejected_at"],
            result_kind=row["result_kind"],
            result_payload=payload,
            report_key=row["report_key"],
            reported_at=row["reported_at"],
        )


class AgentRunner:
    def __init__(
        self,
        *,
        agent_id: str,
        client: AgentBusClient,
        journal: RunnerJournal,
        executor: WorkExecutor,
    ) -> None:
        self.agent_id = agent_id
        self.client = client
        self.journal = journal
        self.executor = executor

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 1.0,
        stop_event: Event | None = None,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")
        while stop_event is None or not stop_event.is_set():
            try:
                self.poll_once()
            except AgentBusTransportError:
                pass
            if stop_event is None:
                time.sleep(poll_interval_seconds)
            else:
                stop_event.wait(poll_interval_seconds)

    def poll_once(self) -> None:
        inbox = self.client.get_inbox(self.agent_id)
        if inbox.status_code != 200:
            return
        for item in inbox.body.get("items", []):
            if item.get("kind") != "task":
                continue
            task = item["task"]
            if task["status"] == "ready":
                self.journal.prepare(task["id"])

        for record in self.journal.pending():
            self._resume(record.task_id)

    def _resume(self, task_id: str) -> None:
        record = self.journal.get(task_id)
        if record is None or record.reported_at is not None:
            return
        if record.claimed_etag is None:
            try:
                response = self.client.claim_task(
                    task_id,
                    self.agent_id,
                    record.claim_key,
                )
            except AgentBusTransportError:
                return
            if response.status_code != 200:
                detail = response.body.get("detail", {})
                if response.status_code == 409 and detail.get("code") in {
                    "task_state_conflict",
                    "task_assignment_conflict",
                }:
                    self.journal.reject_claim(task_id)
                return
            etag = response.headers.get("etag")
            if etag is None:
                return
            self.journal.confirm_claim(task_id, etag)
            record = self.journal.get(task_id)
            assert record is not None

        if record.result_kind is None:
            try:
                current = self.client.get_task(task_id)
            except AgentBusTransportError:
                return
            if current.status_code != 200:
                return
            if (
                current.body.get("status") != "running"
                or current.headers.get("etag") != record.claimed_etag
            ):
                return
            request = ExecutionRequest(
                execution_id=UUID(record.execution_id),
                task_id=UUID(task_id),
                task_type=current.body["type"],
                title=current.body["title"],
                description=current.body.get("description"),
                input=current.body["input"],
                correlation_id=UUID(current.body["correlation_id"]),
                task_etag=record.claimed_etag,
                agent_id=self.agent_id,
            )
            result = self.executor.execute(request)
            self.journal.record_result(task_id, result)
            record = self.journal.get(task_id)
            assert record is not None

        assert record.result_payload is not None
        assert record.report_key is not None
        assert record.claimed_etag is not None
        try:
            if record.result_kind == "succeeded":
                response = self.client.complete_task(
                    task_id,
                    record.result_payload["output"],
                    record.report_key,
                    record.claimed_etag,
                )
            else:
                response = self.client.fail_task(
                    task_id,
                    record.result_payload["failure_code"],
                    record.result_payload["failure_message"],
                    record.report_key,
                    record.claimed_etag,
                )
        except AgentBusTransportError:
            return
        if response.status_code == 200:
            self.journal.mark_reported(task_id)
