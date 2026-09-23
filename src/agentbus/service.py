from dataclasses import dataclass
from datetime import UTC, datetime
from contextlib import closing
import hashlib
import json
import sqlite3
from uuid import UUID, uuid4

from agentbus.database import Database
from agentbus.models import EventRead, TaskCreate, TaskRead, TaskStatus


IDEMPOTENCY_SCOPE = "POST:/tasks"


class IdempotencyConflict(Exception):
    """An idempotency key was reused with a different request."""


@dataclass(frozen=True)
class CreateTaskResult:
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

    with database.transaction() as connection:
        existing = connection.execute(
            """
            SELECT request_hash, response_body
            FROM idempotency_records
            WHERE scope = ? AND key = ?
            """,
            (IDEMPOTENCY_SCOPE, idempotency_key),
        ).fetchone()
        if existing is not None:
            if existing["request_hash"] != request_hash:
                raise IdempotencyConflict
            return CreateTaskResult(
                task=TaskRead.model_validate_json(existing["response_body"]),
                replayed=True,
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
                IDEMPOTENCY_SCOPE,
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
