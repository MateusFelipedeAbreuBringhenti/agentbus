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
    ApprovalDecision,
    ApprovalRead,
    ApprovalStatus,
    EventRead,
    RequestApproval,
    RequestApprovalResult,
    TaskClaim,
    TaskComplete,
    TaskCreate,
    TaskFail,
    TaskRead,
    TaskRelease,
    TaskStatus,
)


CREATE_TASK_OPERATION = "POST:/tasks"
CLAIM_TASK_OPERATION = "POST:/tasks/{task_id}/claim"
COMPLETE_TASK_OPERATION = "POST:/tasks/{task_id}/complete"
FAIL_TASK_OPERATION = "POST:/tasks/{task_id}/fail"
REQUEST_APPROVAL_OPERATION = "POST:/tasks/{task_id}/request-approval"
APPROVE_APPROVAL_OPERATION = "POST:/approvals/{approval_id}/approve"
REJECT_APPROVAL_OPERATION = "POST:/approvals/{approval_id}/reject"
RELEASE_TASK_OPERATION = "POST:/tasks/{task_id}/release"


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


class ApprovalNotFound(Exception):
    """The target Approval does not exist."""


class ApprovalStateConflict(Exception):
    def __init__(self, expected: ApprovalStatus, actual: ApprovalStatus) -> None:
        self.expected = expected
        self.actual = actual


class ApprovalVersionConflict(Exception):
    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual


class ApprovalTaskMismatch(Exception):
    """The Approval belongs to a different Task."""


class ApprovalCorrelationMismatch(Exception):
    """The Approval belongs to a different correlation."""


class PendingApprovalConflict(Exception):
    """A pending Approval already exists for this Task and gate."""


@dataclass(frozen=True)
class CreateTaskResult:
    task: TaskRead
    replayed: bool


@dataclass(frozen=True)
class TaskCommandResult:
    task: TaskRead
    replayed: bool


@dataclass(frozen=True)
class ApprovalCommandResult:
    approval: ApprovalRead
    replayed: bool


@dataclass(frozen=True)
class RequestApprovalCommandResult:
    result: RequestApprovalResult
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
    resource_id: UUID,
    request: BaseModel,
    expected_version: int | None = None,
) -> str:
    payload = json.dumps(
        {
            "body": request.model_dump(mode="json"),
            "expected_version": expected_version,
            "resource_id": str(resource_id),
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


def _replayed_body(
    connection: sqlite3.Connection,
    scope: str,
    idempotency_key: str,
    request_hash: str,
) -> str | None:
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
    return existing["response_body"]


def _replayed_task(
    connection: sqlite3.Connection,
    scope: str,
    idempotency_key: str,
    request_hash: str,
) -> TaskRead | None:
    body = _replayed_body(connection, scope, idempotency_key, request_hash)
    return TaskRead.model_validate_json(body) if body is not None else None


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
    event_index: int = 0,
) -> None:
    connection.execute(
        """
        INSERT INTO events(
            id, task_id, sequence, type, data_json, actor_id,
            correlation_id, causation_event_id, command_id,
            event_index, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            event_index,
            occurred_at,
        ),
    )


def _store_idempotency_response(
    connection: sqlite3.Connection,
    *,
    scope: str,
    idempotency_key: str,
    request_hash: str,
    command_id: UUID,
    correlation_id: UUID,
    causation_event_id: UUID | None,
    resource_type: str,
    resource_id: UUID,
    response_body: str,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO idempotency_records(
            scope, key, request_hash, command_id, correlation_id,
            causation_event_id, resource_type, resource_id,
            response_status, response_body, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 200, ?, ?)
        """,
        (
            scope,
            idempotency_key,
            request_hash,
            str(command_id),
            str(correlation_id),
            str(causation_event_id) if causation_event_id else None,
            resource_type,
            str(resource_id),
            response_body,
            created_at,
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
    _store_idempotency_response(
        connection,
        scope=scope,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        command_id=command_id,
        correlation_id=task.correlation_id,
        causation_event_id=causation_event_id,
        resource_type="task",
        resource_id=task.id,
        response_body=task.model_dump_json(),
        created_at=created_at,
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


def _approval_from_row(row: sqlite3.Row) -> ApprovalRead:
    return ApprovalRead(
        id=row["id"],
        task_id=row["task_id"],
        gate=row["gate"],
        status=row["status"],
        request_reason=row["request_reason"],
        context=json.loads(row["context_json"]),
        requested_by=row["requested_by"],
        assigned_to=row["assigned_to"],
        decided_by=row["decided_by"],
        decision_reason=row["decision_reason"],
        correlation_id=row["correlation_id"],
        version=row["version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        decided_at=row["decided_at"],
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


def request_task_approval(
    database: Database,
    task_id: UUID,
    request: RequestApproval,
    idempotency_key: str,
    expected_version: int,
) -> RequestApprovalCommandResult:
    scope = _command_scope(request.requested_by, REQUEST_APPROVAL_OPERATION)
    request_hash = _command_request_hash(task_id, request, expected_version)

    with database.transaction() as connection:
        replayed_body = _replayed_body(connection, scope, idempotency_key, request_hash)
        if replayed_body is not None:
            return RequestApprovalCommandResult(
                result=RequestApprovalResult.model_validate_json(replayed_body),
                replayed=True,
            )

        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        _validate_causation(connection, request.causation_event_id, row["correlation_id"])
        if row["status"] != TaskStatus.READY.value:
            raise TaskStateConflict(TaskStatus.READY, TaskStatus(row["status"]))
        if row["version"] != expected_version:
            raise TaskVersionConflict(expected_version, row["version"])

        now = datetime.now(UTC).isoformat()
        updated = connection.execute(
            """
            UPDATE tasks
            SET status = 'waiting_approval', updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'ready' AND version = ?
            """,
            (now, str(task_id), expected_version),
        )
        if updated.rowcount != 1:
            raise TaskVersionConflict(expected_version, row["version"])

        approval_id = uuid4()
        try:
            connection.execute(
                """
                INSERT INTO approvals(
                    id, task_id, gate, status, request_reason, context_json,
                    requested_by, assigned_to, decided_by, decision_reason,
                    correlation_id, version, created_at, updated_at, decided_at
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, NULL, NULL, ?, 1, ?, ?, NULL)
                """,
                (
                    str(approval_id),
                    str(task_id),
                    request.gate,
                    request.request_reason,
                    json.dumps(
                        request.context,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    request.requested_by,
                    request.assigned_to,
                    row["correlation_id"],
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as error:
            if "approvals.task_id, approvals.gate" in str(error):
                raise PendingApprovalConflict from error
            raise

        task = _task_from_row(
            connection.execute("SELECT * FROM tasks WHERE id = ?", (str(task_id),)).fetchone()
        )
        approval = _approval_from_row(
            connection.execute(
                "SELECT * FROM approvals WHERE id = ?",
                (str(approval_id),),
            ).fetchone()
        )
        command_id = uuid4()
        _insert_task_event(
            connection,
            task=task,
            event_type="task.approval_requested",
            data={
                "approval_id": str(approval.id),
                "gate": approval.gate,
                "status": task.status.value,
                "task_id": str(task.id),
                "version": task.version,
            },
            actor_id=request.requested_by,
            causation_event_id=request.causation_event_id,
            command_id=command_id,
            occurred_at=now,
            event_index=0,
        )
        _insert_task_event(
            connection,
            task=task,
            event_type="approval.requested",
            data={
                "approval_id": str(approval.id),
                "gate": approval.gate,
                "status": approval.status.value,
                "task_id": str(task.id),
                "version": approval.version,
            },
            actor_id=request.requested_by,
            causation_event_id=request.causation_event_id,
            command_id=command_id,
            occurred_at=now,
            event_index=1,
        )
        result = RequestApprovalResult(task=task, approval=approval)
        _store_idempotency_response(
            connection,
            scope=scope,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            command_id=command_id,
            correlation_id=task.correlation_id,
            causation_event_id=request.causation_event_id,
            resource_type="approval",
            resource_id=approval.id,
            response_body=result.model_dump_json(),
            created_at=now,
        )
        return RequestApprovalCommandResult(result=result, replayed=False)


def decide_approval(
    database: Database,
    approval_id: UUID,
    request: ApprovalDecision,
    idempotency_key: str,
    expected_version: int,
    decision: ApprovalStatus,
) -> ApprovalCommandResult:
    operation = (
        APPROVE_APPROVAL_OPERATION
        if decision is ApprovalStatus.APPROVED
        else REJECT_APPROVAL_OPERATION
    )
    scope = _command_scope(request.decided_by, operation)
    request_hash = _command_request_hash(approval_id, request, expected_version)

    with database.transaction() as connection:
        replayed_body = _replayed_body(connection, scope, idempotency_key, request_hash)
        if replayed_body is not None:
            return ApprovalCommandResult(
                approval=ApprovalRead.model_validate_json(replayed_body),
                replayed=True,
            )

        row = connection.execute(
            "SELECT * FROM approvals WHERE id = ?",
            (str(approval_id),),
        ).fetchone()
        if row is None:
            raise ApprovalNotFound
        _validate_causation(connection, request.causation_event_id, row["correlation_id"])
        if row["status"] != ApprovalStatus.PENDING.value:
            raise ApprovalStateConflict(ApprovalStatus.PENDING, ApprovalStatus(row["status"]))
        if row["version"] != expected_version:
            raise ApprovalVersionConflict(expected_version, row["version"])

        now = datetime.now(UTC).isoformat()
        updated = connection.execute(
            """
            UPDATE approvals
            SET status = ?, decided_by = ?, decision_reason = ?,
                decided_at = ?, updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'pending' AND version = ?
            """,
            (
                decision.value,
                request.decided_by,
                request.decision_reason,
                now,
                now,
                str(approval_id),
                expected_version,
            ),
        )
        if updated.rowcount != 1:
            current = connection.execute(
                "SELECT status, version FROM approvals WHERE id = ?",
                (str(approval_id),),
            ).fetchone()
            if current["status"] != ApprovalStatus.PENDING.value:
                raise ApprovalStateConflict(
                    ApprovalStatus.PENDING,
                    ApprovalStatus(current["status"]),
                )
            raise ApprovalVersionConflict(expected_version, current["version"])

        approval = _approval_from_row(
            connection.execute(
                "SELECT * FROM approvals WHERE id = ?",
                (str(approval_id),),
            ).fetchone()
        )
        task = _task_from_row(
            connection.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (str(approval.task_id),),
            ).fetchone()
        )
        command_id = uuid4()
        _insert_task_event(
            connection,
            task=task,
            event_type=f"approval.{decision.value}",
            data={
                "approval_id": str(approval.id),
                "gate": approval.gate,
                "status": approval.status.value,
                "task_id": str(task.id),
                "version": approval.version,
            },
            actor_id=request.decided_by,
            causation_event_id=request.causation_event_id,
            command_id=command_id,
            occurred_at=now,
        )
        _store_idempotency_response(
            connection,
            scope=scope,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            command_id=command_id,
            correlation_id=approval.correlation_id,
            causation_event_id=request.causation_event_id,
            resource_type="approval",
            resource_id=approval.id,
            response_body=approval.model_dump_json(),
            created_at=now,
        )
        return ApprovalCommandResult(approval=approval, replayed=False)


def release_task(
    database: Database,
    task_id: UUID,
    request: TaskRelease,
    idempotency_key: str,
    expected_version: int,
) -> TaskCommandResult:
    scope = _command_scope(request.actor_id, RELEASE_TASK_OPERATION)
    request_hash = _command_request_hash(task_id, request, expected_version)

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
        _validate_causation(connection, request.causation_event_id, row["correlation_id"])

        approval_row = connection.execute(
            "SELECT * FROM approvals WHERE id = ?",
            (str(request.approval_id),),
        ).fetchone()
        if approval_row is None:
            raise ApprovalNotFound
        if approval_row["task_id"] != str(task_id):
            raise ApprovalTaskMismatch
        if approval_row["correlation_id"] != row["correlation_id"]:
            raise ApprovalCorrelationMismatch
        if approval_row["status"] != ApprovalStatus.APPROVED.value:
            raise ApprovalStateConflict(
                ApprovalStatus.APPROVED,
                ApprovalStatus(approval_row["status"]),
            )
        if row["status"] != TaskStatus.WAITING_APPROVAL.value:
            raise TaskStateConflict(
                TaskStatus.WAITING_APPROVAL,
                TaskStatus(row["status"]),
            )
        if row["version"] != expected_version:
            raise TaskVersionConflict(expected_version, row["version"])

        now = datetime.now(UTC).isoformat()
        updated = connection.execute(
            """
            UPDATE tasks
            SET status = 'ready', updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'waiting_approval' AND version = ?
            """,
            (now, str(task_id), expected_version),
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
            event_type="task.released",
            data={
                "approval_id": str(request.approval_id),
                "status": task.status.value,
                "task_id": str(task.id),
                "version": task.version,
            },
            actor_id=request.actor_id,
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


def get_approval(database: Database, approval_id: UUID) -> ApprovalRead | None:
    with closing(database.connect()) as connection:
        row = connection.execute(
            "SELECT * FROM approvals WHERE id = ?",
            (str(approval_id),),
        ).fetchone()
    return _approval_from_row(row) if row is not None else None


def get_task_approvals(database: Database, task_id: UUID) -> list[ApprovalRead]:
    with closing(database.connect()) as connection:
        rows = connection.execute(
            "SELECT * FROM approvals WHERE task_id = ? ORDER BY created_at, id",
            (str(task_id),),
        ).fetchall()
    return [_approval_from_row(row) for row in rows]
