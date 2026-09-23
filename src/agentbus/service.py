from dataclasses import dataclass
from datetime import UTC, datetime
from contextlib import closing
import hashlib
import json
import sqlite3
from uuid import UUID, uuid4

from pydantic import BaseModel

from agentbus.database import Database
from agentbus.models import (
    EventRead,
    TaskClaim,
    TaskComplete,
    TaskCreate,
    TaskFail,
    TaskRead,
    TaskStatus,
)


CREATE_TASK_OPERATION = "POST:/tasks"
CLAIM_TASK_OPERATION = "POST:/tasks/{task_id}/claim"
COMPLETE_TASK_OPERATION = "POST:/tasks/{task_id}/complete"
FAIL_TASK_OPERATION = "POST:/tasks/{task_id}/fail"


class IdempotencyConflict(Exception):
    """An idempotency key was reused with a different request."""


class CausationEventNotFound(Exception):
    """The declared causation event does not exist."""


class CausationCorrelationMismatch(Exception):
    """The causation event belongs to another correlation."""


class TaskNotFound(Exception):
    """The target Task does not exist."""


class TaskStateConflict(Exception):
    def __init__(self, expected: TaskStatus, actual: TaskStatus) -> None:
        self.expected = expected
        self.actual = actual


class TaskVersionConflict(Exception):
    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual


@dataclass(frozen=True)
class CreateTaskResult:
    task: TaskRead
    replayed: bool


@dataclass(frozen=True)
class TaskCommandResult:
    task: TaskRead
    replayed: bool


def _canonical_request_hash(request: TaskCreate) -> str:
    payload = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _idempotency_scope(requested_by: str) -> str:
    return json.dumps(
        {"identity": requested_by, "operation": CREATE_TASK_OPERATION},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _command_request_hash(
    task_id: UUID,
    request: BaseModel,
    expected_version: int | None = None,
) -> str:
    payload = json.dumps(
        {
            "body": request.model_dump(mode="json"),
            "expected_version": expected_version,
            "task_id": str(task_id),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _command_scope(identity: str, operation: str) -> str:
    return json.dumps(
        {"identity": identity, "operation": operation},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _replayed_task(
    connection: sqlite3.Connection,
    scope: str,
    idempotency_key: str,
    request_hash: str,
) -> TaskRead | None:
    existing = connection.execute(
        """
        SELECT request_hash, response_body
        FROM idempotency_records
        WHERE scope = ? AND key = ?
        """,
        (scope, idempotency_key),
    ).fetchone()
    if existing is None:
        return None
    if existing["request_hash"] != request_hash:
        raise IdempotencyConflict
    return TaskRead.model_validate_json(existing["response_body"])


def _validate_causation(
    connection: sqlite3.Connection,
    causation_event_id: UUID | None,
    correlation_id: str,
) -> None:
    if causation_event_id is None:
        return
    cause = connection.execute(
        "SELECT correlation_id FROM events WHERE id = ?",
        (str(causation_event_id),),
    ).fetchone()
    if cause is None:
        raise CausationEventNotFound
    if cause["correlation_id"] != correlation_id:
        raise CausationCorrelationMismatch


def _next_event_sequence(connection: sqlite3.Connection, task_id: UUID) -> int:
    row = connection.execute(
        "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM events WHERE task_id = ?",
        (str(task_id),),
    ).fetchone()
    return row["next_sequence"]


def _insert_task_event(
    connection: sqlite3.Connection,
    *,
    task: TaskRead,
    event_type: str,
    data: dict,
    actor_id: str,
    causation_event_id: UUID | None,
    command_id: UUID,
    occurred_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO events(
            id, task_id, sequence, type, data_json, actor_id,
            correlation_id, causation_event_id, command_id,
            event_index, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """,
        (
            str(uuid4()),
            str(task.id),
            _next_event_sequence(connection, task.id),
            event_type,
            json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            actor_id,
            str(task.correlation_id),
            str(causation_event_id) if causation_event_id else None,
            str(command_id),
            occurred_at,
        ),
    )


def _store_command_idempotency(
    connection: sqlite3.Connection,
    *,
    scope: str,
    idempotency_key: str,
    request_hash: str,
    command_id: UUID,
    task: TaskRead,
    causation_event_id: UUID | None,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO idempotency_records(
            scope, key, request_hash, command_id, correlation_id,
            causation_event_id, resource_type, resource_id,
            response_status, response_body, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'task', ?, 200, ?, ?)
        """,
        (
            scope,
            idempotency_key,
            request_hash,
            str(command_id),
            str(task.correlation_id),
            str(causation_event_id) if causation_event_id else None,
            str(task.id),
            task.model_dump_json(),
            created_at,
        ),
    )


def _task_from_row(row: sqlite3.Row) -> TaskRead:
    return TaskRead(
        id=row["id"],
        type=row["type"],
        title=row["title"],
        description=row["description"],
        input=json.loads(row["input_json"]),
        output=json.loads(row["output_json"]) if row["output_json"] else None,
        status=row["status"],
        requested_by=row["requested_by"],
        assigned_to=row["assigned_to"],
        failure_code=row["failure_code"],
        failure_message=row["failure_message"],
        retry_of=row["retry_of"],
        correlation_id=row["correlation_id"],
        version=row["version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def _event_from_row(row: sqlite3.Row) -> EventRead:
    return EventRead(
        id=row["id"],
        task_id=row["task_id"],
        sequence=row["sequence"],
        type=row["type"],
        data=json.loads(row["data_json"]),
        actor_id=row["actor_id"],
        correlation_id=row["correlation_id"],
        causation_event_id=row["causation_event_id"],
        command_id=row["command_id"],
        event_index=row["event_index"],
        occurred_at=row["occurred_at"],
    )


def create_task(
    database: Database,
    request: TaskCreate,
    idempotency_key: str,
) -> CreateTaskResult:
    request_hash = _canonical_request_hash(request)
    idempotency_scope = _idempotency_scope(request.requested_by)

    with database.transaction() as connection:
        existing = connection.execute(
            """
            SELECT request_hash, response_body
            FROM idempotency_records
            WHERE scope = ? AND key = ?
            """,
            (idempotency_scope, idempotency_key),
        ).fetchone()
        if existing is not None:
            if existing["request_hash"] != request_hash:
                raise IdempotencyConflict
            return CreateTaskResult(
                task=TaskRead.model_validate_json(existing["response_body"]),
                replayed=True,
            )

        _validate_causation(
            connection,
            request.causation_event_id,
            str(request.correlation_id),
        )

        task_id = uuid4()
        event_id = uuid4()
        command_id = uuid4()
        now = datetime.now(UTC)
        now_text = now.isoformat()
        input_json = json.dumps(
            request.input,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

        connection.execute(
            """
            INSERT INTO tasks(
                id, type, title, description, input_json, output_json, status,
                requested_by, assigned_to, failure_code, failure_message,
                retry_of, correlation_id, version, created_at, updated_at,
                started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, ?, 1, ?, ?, NULL, NULL)
            """,
            (
                str(task_id),
                request.type,
                request.title,
                request.description,
                input_json,
                TaskStatus.READY.value,
                request.requested_by,
                request.assigned_to,
                str(request.correlation_id),
                now_text,
                now_text,
            ),
        )

        event_data = {
            "task_id": str(task_id),
            "type": request.type,
            "status": TaskStatus.READY.value,
        }
        connection.execute(
            """
            INSERT INTO events(
                id, task_id, sequence, type, data_json, actor_id,
                correlation_id, causation_event_id, command_id,
                event_index, occurred_at
            ) VALUES (?, ?, 1, 'task.created', ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                str(event_id),
                str(task_id),
                json.dumps(event_data, separators=(",", ":"), sort_keys=True),
                request.requested_by,
                str(request.correlation_id),
                str(request.causation_event_id) if request.causation_event_id else None,
                str(command_id),
                now_text,
            ),
        )

        task = TaskRead(
            id=task_id,
            type=request.type,
            title=request.title,
            description=request.description,
            input=request.input,
            output=None,
            status=TaskStatus.READY,
            requested_by=request.requested_by,
            assigned_to=request.assigned_to,
            failure_code=None,
            failure_message=None,
            retry_of=None,
            correlation_id=request.correlation_id,
            version=1,
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
        )
        connection.execute(
            """
            INSERT INTO idempotency_records(
                scope, key, request_hash, command_id, correlation_id,
                causation_event_id, resource_type, resource_id,
                response_status, response_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'task', ?, 201, ?, ?)
            """,
            (
                idempotency_scope,
                idempotency_key,
                request_hash,
                str(command_id),
                str(request.correlation_id),
                str(request.causation_event_id) if request.causation_event_id else None,
                str(task_id),
                task.model_dump_json(),
                now_text,
            ),
        )

        return CreateTaskResult(task=task, replayed=False)


def claim_task(
    database: Database,
    task_id: UUID,
    request: TaskClaim,
    idempotency_key: str,
) -> TaskCommandResult:
    scope = _command_scope(request.agent_id, CLAIM_TASK_OPERATION)
    request_hash = _command_request_hash(task_id, request)

    with database.transaction() as connection:
        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            return TaskCommandResult(task=replayed, replayed=True)

        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        _validate_causation(
            connection,
            request.causation_event_id,
            row["correlation_id"],
        )

        now = datetime.now(UTC).isoformat()
        updated = connection.execute(
            """
            UPDATE tasks
            SET status = 'running', assigned_to = ?, started_at = ?,
                updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'ready'
            """,
            (request.agent_id, now, now, str(task_id)),
        )
        if updated.rowcount != 1:
            raise TaskStateConflict(TaskStatus.READY, TaskStatus(row["status"]))

        task = _task_from_row(
            connection.execute("SELECT * FROM tasks WHERE id = ?", (str(task_id),)).fetchone()
        )
        command_id = uuid4()
        _insert_task_event(
            connection,
            task=task,
            event_type="task.started",
            data={
                "assigned_to": request.agent_id,
                "status": task.status.value,
                "task_id": str(task.id),
                "version": task.version,
            },
            actor_id=request.agent_id,
            causation_event_id=request.causation_event_id,
            command_id=command_id,
            occurred_at=now,
        )
        _store_command_idempotency(
            connection,
            scope=scope,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            command_id=command_id,
            task=task,
            causation_event_id=request.causation_event_id,
            created_at=now,
        )
        return TaskCommandResult(task=task, replayed=False)


def _assigned_task_identity(row: sqlite3.Row) -> str:
    if row["assigned_to"] is None:
        raise TaskStateConflict(TaskStatus.RUNNING, TaskStatus(row["status"]))
    return row["assigned_to"]


def complete_task(
    database: Database,
    task_id: UUID,
    request: TaskComplete,
    idempotency_key: str,
    expected_version: int,
) -> TaskCommandResult:
    request_hash = _command_request_hash(task_id, request, expected_version)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        actor_id = _assigned_task_identity(row)
        scope = _command_scope(actor_id, COMPLETE_TASK_OPERATION)
        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            return TaskCommandResult(task=replayed, replayed=True)

        _validate_causation(connection, request.causation_event_id, row["correlation_id"])
        if row["status"] != TaskStatus.RUNNING.value:
            raise TaskStateConflict(TaskStatus.RUNNING, TaskStatus(row["status"]))
        if row["version"] != expected_version:
            raise TaskVersionConflict(expected_version, row["version"])

        now = datetime.now(UTC).isoformat()
        output_json = json.dumps(
            request.output,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        updated = connection.execute(
            """
            UPDATE tasks
            SET status = 'succeeded', output_json = ?, finished_at = ?,
                updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'running' AND version = ?
            """,
            (output_json, now, now, str(task_id), expected_version),
        )
        if updated.rowcount != 1:
            raise TaskVersionConflict(expected_version, row["version"])

        task = _task_from_row(
            connection.execute("SELECT * FROM tasks WHERE id = ?", (str(task_id),)).fetchone()
        )
        command_id = uuid4()
        _insert_task_event(
            connection,
            task=task,
            event_type="task.completed",
            data={
                "output": request.output,
                "status": task.status.value,
                "task_id": str(task.id),
                "version": task.version,
            },
            actor_id=actor_id,
            causation_event_id=request.causation_event_id,
            command_id=command_id,
            occurred_at=now,
        )
        _store_command_idempotency(
            connection,
            scope=scope,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            command_id=command_id,
            task=task,
            causation_event_id=request.causation_event_id,
            created_at=now,
        )
        return TaskCommandResult(task=task, replayed=False)


def fail_task(
    database: Database,
    task_id: UUID,
    request: TaskFail,
    idempotency_key: str,
    expected_version: int,
) -> TaskCommandResult:
    request_hash = _command_request_hash(task_id, request, expected_version)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        actor_id = _assigned_task_identity(row)
        scope = _command_scope(actor_id, FAIL_TASK_OPERATION)
        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            return TaskCommandResult(task=replayed, replayed=True)

        _validate_causation(connection, request.causation_event_id, row["correlation_id"])
        if row["status"] != TaskStatus.RUNNING.value:
            raise TaskStateConflict(TaskStatus.RUNNING, TaskStatus(row["status"]))
        if row["version"] != expected_version:
            raise TaskVersionConflict(expected_version, row["version"])

        now = datetime.now(UTC).isoformat()
        updated = connection.execute(
            """
            UPDATE tasks
            SET status = 'failed', failure_code = ?, failure_message = ?,
                finished_at = ?, updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'running' AND version = ?
            """,
            (
                request.failure_code,
                request.failure_message,
                now,
                now,
                str(task_id),
                expected_version,
            ),
        )
        if updated.rowcount != 1:
            raise TaskVersionConflict(expected_version, row["version"])

        task = _task_from_row(
            connection.execute("SELECT * FROM tasks WHERE id = ?", (str(task_id),)).fetchone()
        )
        command_id = uuid4()
        _insert_task_event(
            connection,
            task=task,
            event_type="task.failed",
            data={
                "failure_code": request.failure_code,
                "failure_message": request.failure_message,
                "status": task.status.value,
                "task_id": str(task.id),
                "version": task.version,
            },
            actor_id=actor_id,
            causation_event_id=request.causation_event_id,
            command_id=command_id,
            occurred_at=now,
        )
        _store_command_idempotency(
            connection,
            scope=scope,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            command_id=command_id,
            task=task,
            causation_event_id=request.causation_event_id,
            created_at=now,
        )
        return TaskCommandResult(task=task, replayed=False)


def get_task(database: Database, task_id: UUID) -> TaskRead | None:
    with closing(database.connect()) as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
    return _task_from_row(row) if row is not None else None


def get_task_events(database: Database, task_id: UUID) -> list[EventRead]:
    with closing(database.connect()) as connection:
        rows = connection.execute(
            "SELECT * FROM events WHERE task_id = ? ORDER BY sequence",
            (str(task_id),),
        ).fetchall()
    return [_event_from_row(row) for row in rows]
