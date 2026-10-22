from dataclasses import dataclass
from datetime import UTC, datetime
from contextlib import closing
import base64
import binascii
import hashlib
import json
import sqlite3
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel

from agentbus.auth import AuthContext, ResourceAccessDenied
from agentbus.database import Database
from agentbus.execution_policy import ExecutionPolicy, TestExecutionPolicy
from agentbus.models import (
    AgentInbox,
    ApprovalDecision,
    ApprovalPage,
    ApprovalRead,
    ApprovalStatus,
    EventRead,
    InboxApprovalItem,
    InboxTaskItem,
    RequestApproval,
    RequestApprovalResult,
    CorrelationTrace,
    SuccessorTaskCreate,
    TaskClaim,
    TaskComplete,
    TaskCreate,
    TaskFail,
    TaskRead,
    TaskPage,
    TaskRelease,
    TaskStatus,
)


CREATE_TASK_OPERATION = "POST:/tasks"
CREATE_SUCCESSOR_OPERATION = "POST:/tasks/{task_id}/successors"
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


class InvalidTaskCursor(Exception):
    """The Task catalog cursor is malformed or belongs to other filters."""


class InvalidApprovalCursor(Exception):
    """The Approval catalog cursor is malformed or belongs to other filters."""


class TaskStateConflict(Exception):
    def __init__(self, expected: TaskStatus, actual: TaskStatus) -> None:
        self.expected = expected
        self.actual = actual


class PredecessorNotSucceeded(Exception):
    def __init__(self, actual: TaskStatus) -> None:
        self.actual = actual


class PredecessorCompletionEventInvalid(Exception):
    """The succeeded predecessor has no unique matching completion Event."""


class TaskVersionConflict(Exception):
    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual


class TaskAssignmentConflict(Exception):
    def __init__(self, requested: str, assigned_to: str) -> None:
        self.requested = requested
        self.assigned_to = assigned_to


class ExecutionIdConflict(Exception):
    pass


class ExecutionInstanceConflict(Exception):
    pass


class ExecutionSubjectConflict(Exception):
    pass


class ExecutionProvenanceRequired(Exception):
    pass


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


class ApprovalNotCurrentForTask(Exception):
    """The Approval did not open the Task's current wait."""


class ApprovalRequestProvenanceInvalid(Exception):
    """The Approval has no unique authenticated request Event."""


class ApprovalDecisionCausationInvalid(Exception):
    """Release does not cite the unique matching authenticated approval Event."""


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
            "body": request.model_dump(mode="json", exclude_none=True),
            "expected_version": expected_version,
            # Preserve persisted Slice #002 fingerprints across the v2 upgrade.
            ("task_id" if isinstance(request, (TaskClaim, TaskComplete, TaskFail))
             else "resource_id"): str(resource_id),
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
    authenticated_principal_id: str,
    causation_event_id: UUID | None,
    command_id: UUID,
    occurred_at: str,
    event_index: int = 0,
) -> None:
    if "authenticated_principal_id" not in {
        row[1] for row in connection.execute("PRAGMA table_info(events)")
    }:
        connection.execute(
            """INSERT INTO events(
                id, task_id, sequence, type, data_json, actor_id, correlation_id,
                causation_event_id, command_id, event_index, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (str(uuid4()), str(task.id), _next_event_sequence(connection, task.id),
             event_type, json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
             actor_id, str(task.correlation_id),
             str(causation_event_id) if causation_event_id else None,
             str(command_id), event_index, occurred_at),
        )
        return
    connection.execute(
        """
        INSERT INTO events(
            id, task_id, sequence, type, data_json, actor_id,
            authenticated_principal_id, correlation_id, causation_event_id, command_id,
            event_index, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            str(task.id),
            _next_event_sequence(connection, task.id),
            event_type,
            json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            actor_id,
            authenticated_principal_id,
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
    authenticated_principal_id: str,
    response_status: int = 200,
) -> None:
    if "authenticated_principal_id" not in {
        row[1] for row in connection.execute("PRAGMA table_info(idempotency_records)")
    }:
        connection.execute(
            """INSERT INTO idempotency_records(
                scope, key, request_hash, command_id, correlation_id, causation_event_id,
                resource_type, resource_id, response_status, response_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (scope, idempotency_key, request_hash, str(command_id), str(correlation_id),
             str(causation_event_id) if causation_event_id else None, resource_type,
             str(resource_id), response_status, response_body, created_at),
        )
        return
    connection.execute(
        """
        INSERT INTO idempotency_records(
            scope, key, request_hash, command_id, correlation_id,
            causation_event_id, resource_type, resource_id,
            response_status, response_body, created_at, authenticated_principal_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            response_status,
            response_body,
            created_at,
            authenticated_principal_id,
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
    authenticated_principal_id: str,
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
        authenticated_principal_id=authenticated_principal_id,
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
        waiting_on_approval_id=(
            row["waiting_on_approval_id"]
            if "waiting_on_approval_id" in row.keys()
            else None
        ),
        correlation_id=row["correlation_id"],
        version=row["version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        execution_id=row["execution_id"] if "execution_id" in row.keys() else None,
        runner_instance_id=(row["runner_instance_id"] if "runner_instance_id" in row.keys() else None),
        execution_subject_id=(row["execution_subject_id"] if "execution_subject_id" in row.keys() else None),
        claimed_principal_id=(row["claimed_principal_id"] if "claimed_principal_id" in row.keys() else None),
        claim_policy_id=row["claim_policy_id"] if "claim_policy_id" in row.keys() else None,
        claim_policy_sha256=(row["claim_policy_sha256"] if "claim_policy_sha256" in row.keys() else None),
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
        authenticated_principal_id=(
            row["authenticated_principal_id"]
            if "authenticated_principal_id" in row.keys() else None
        ),
        correlation_id=row["correlation_id"],
        causation_event_id=row["causation_event_id"],
        command_id=row["command_id"],
        event_index=row["event_index"],
        occurred_at=row["occurred_at"],
    )


def _task_filter_fingerprint(
    *,
    status: TaskStatus | None,
    assigned_to: str | None,
    requested_by: str | None,
    correlation_id: UUID | None,
    task_type: str | None,
) -> str:
    filters = json.dumps(
        {
            "assigned_to": assigned_to,
            "correlation_id": str(correlation_id) if correlation_id else None,
            "requested_by": requested_by,
            "status": status.value if status else None,
            "type": task_type,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(filters.encode("utf-8")).hexdigest()[:16]


def _encode_task_cursor(created_at: str, task_id: str, fingerprint: str) -> str:
    payload = json.dumps(
        {"created_at": created_at, "filter": fingerprint, "id": task_id, "v": 1},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_task_cursor(cursor: str, fingerprint: str) -> tuple[str, str]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        if (
            payload.get("v") != 1
            or payload.get("filter") != fingerprint
            or not isinstance(payload.get("created_at"), str)
            or not isinstance(payload.get("id"), str)
        ):
            raise InvalidTaskCursor
        UUID(payload["id"])
        return payload["created_at"], payload["id"]
    except (
        AttributeError,
        binascii.Error,
        KeyError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise InvalidTaskCursor from None


def create_task(
    database: Database,
    request: TaskCreate,
    idempotency_key: str,
    auth: AuthContext,
) -> CreateTaskResult:
    request_hash = _canonical_request_hash(request)
    actor_id = request.requested_by if auth.subject_id == "*" else auth.subject_id
    idempotency_scope = _idempotency_scope(actor_id)

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
        event_columns = {row[1] for row in connection.execute("PRAGMA table_info(events)")}
        if "authenticated_principal_id" in event_columns:
            connection.execute(
                """INSERT INTO events(
                    id, task_id, sequence, type, data_json, actor_id,
                    authenticated_principal_id, correlation_id, causation_event_id,
                    command_id, event_index, occurred_at
                ) VALUES (?, ?, 1, 'task.created', ?, ?, ?, ?, ?, ?, 0, ?)""",
                (str(event_id), str(task_id),
                 json.dumps(event_data, separators=(",", ":"), sort_keys=True), actor_id,
                 auth.principal_id, str(request.correlation_id),
                 str(request.causation_event_id) if request.causation_event_id else None,
                 str(command_id), now_text),
            )
        else:
            connection.execute(
                """INSERT INTO events(
                    id, task_id, sequence, type, data_json, actor_id, correlation_id,
                    causation_event_id, command_id, event_index, occurred_at
                ) VALUES (?, ?, 1, 'task.created', ?, ?, ?, ?, ?, 0, ?)""",
                (str(event_id), str(task_id),
                 json.dumps(event_data, separators=(",", ":"), sort_keys=True), actor_id,
                 str(request.correlation_id),
                 str(request.causation_event_id) if request.causation_event_id else None,
                 str(command_id), now_text),
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
            waiting_on_approval_id=None,
            correlation_id=request.correlation_id,
            version=1,
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
        )
        _store_idempotency_response(
            connection, scope=idempotency_scope, idempotency_key=idempotency_key,
            request_hash=request_hash, command_id=command_id,
            correlation_id=request.correlation_id,
            causation_event_id=request.causation_event_id, resource_type="task",
            resource_id=task_id, response_body=task.model_dump_json(),
            created_at=now_text, response_status=201,
            authenticated_principal_id=auth.principal_id,
        )

        return CreateTaskResult(task=task, replayed=False)


def create_successor_task(
    database: Database,
    predecessor_id: UUID,
    request: SuccessorTaskCreate,
    idempotency_key: str,
    auth: AuthContext,
) -> CreateTaskResult:
    request_hash = _command_request_hash(predecessor_id, request)
    actor_id = request.requested_by if auth.subject_id == "*" else auth.subject_id
    scope = _command_scope(actor_id, CREATE_SUCCESSOR_OPERATION)

    with database.transaction() as connection:
        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            return CreateTaskResult(task=replayed, replayed=True)

        predecessor_row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?", (str(predecessor_id),)
        ).fetchone()
        if predecessor_row is None:
            raise TaskNotFound
        predecessor = _task_from_row(predecessor_row)
        if predecessor.status is not TaskStatus.SUCCEEDED:
            raise PredecessorNotSucceeded(predecessor.status)

        completion_events = connection.execute(
            """
            SELECT * FROM events
            WHERE task_id = ? AND type = 'task.completed'
              AND correlation_id = ?
            ORDER BY sequence
            """,
            (str(predecessor_id), str(predecessor.correlation_id)),
        ).fetchall()
        if len(completion_events) != 1:
            raise PredecessorCompletionEventInvalid
        completion = _event_from_row(completion_events[0])
        if completion.data.get("version") != predecessor.version:
            raise PredecessorCompletionEventInvalid

        task_id, command_id = uuid4(), uuid4()
        now = datetime.now(UTC)
        now_text = now.isoformat()
        connection.execute(
            """
            INSERT INTO tasks(
                id, type, title, description, input_json, output_json, status,
                requested_by, assigned_to, failure_code, failure_message,
                retry_of, correlation_id, version, created_at, updated_at,
                started_at, finished_at, waiting_on_approval_id
            ) VALUES (?, ?, ?, ?, ?, NULL, 'ready', ?, ?, NULL, NULL, NULL,
                      ?, 1, ?, ?, NULL, NULL, NULL)
            """,
            (
                str(task_id), request.type, request.title, request.description,
                json.dumps(request.input, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                request.requested_by, request.assigned_to,
                str(predecessor.correlation_id), now_text, now_text,
            ),
        )
        task = _task_from_row(
            connection.execute("SELECT * FROM tasks WHERE id = ?", (str(task_id),)).fetchone()
        )
        _insert_task_event(
            connection,
            task=task,
            event_type="task.created",
            data={
                "predecessor_task_id": str(predecessor.id),
                "status": task.status.value,
                "task_id": str(task.id),
                "type": task.type,
            },
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
            causation_event_id=completion.id,
            command_id=command_id,
            occurred_at=now_text,
        )
        _store_idempotency_response(
            connection,
            scope=scope,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            command_id=command_id,
            correlation_id=task.correlation_id,
            causation_event_id=completion.id,
            resource_type="task",
            resource_id=task.id,
            response_body=task.model_dump_json(),
            created_at=now_text,
            response_status=201,
            authenticated_principal_id=auth.principal_id,
        )
        return CreateTaskResult(task=task, replayed=False)


def claim_task(
    database: Database,
    task_id: UUID,
    request: TaskClaim,
    idempotency_key: str,
    auth: AuthContext,
    policy: ExecutionPolicy | None = None,
) -> TaskCommandResult:
    policy = policy or TestExecutionPolicy()
    actor_id = request.agent_id if auth.subject_id == "*" else auth.subject_id
    scope = _command_scope(actor_id, CLAIM_TASK_OPERATION)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        if auth.subject_id != "*" and row["assigned_to"] not in {None, auth.subject_id}:
            raise ResourceAccessDenied
        has_execution_provenance = "execution_id" in row.keys()
        original_request_hash = _command_request_hash(task_id, request)
        legacy_replay = None
        if not has_execution_provenance or row["execution_id"] is None:
            legacy_replay = _replayed_task(
                connection, scope, idempotency_key, original_request_hash
            )
            if legacy_replay is not None:
                return TaskCommandResult(task=legacy_replay, replayed=True)
        execution_id = request.execution_id
        runner_instance_id = request.runner_instance_id
        if (
            has_execution_provenance
            and auth.subject_id == "*"
            and execution_id is None
            and runner_instance_id is None
        ):
            execution_id = uuid5(UUID(int=0), f"test:{actor_id}:{task_id}")
            runner_instance_id = UUID(int=0)
            request = request.model_copy(update={
                "execution_id": execution_id, "runner_instance_id": runner_instance_id,
            })
        request_hash = _command_request_hash(task_id, request)
        replayed = legacy_replay or _replayed_task(
            connection, scope, idempotency_key, request_hash
        )
        if replayed is not None:
            if has_execution_provenance and row["execution_id"] is not None:
                if request.execution_id is None or str(request.execution_id) != row["execution_id"]:
                    raise ExecutionIdConflict
                if request.runner_instance_id is None or str(request.runner_instance_id) != row["runner_instance_id"]:
                    raise ExecutionInstanceConflict
            return TaskCommandResult(task=replayed, replayed=True)

        if has_execution_provenance and (execution_id is None or runner_instance_id is None):
            raise ExecutionProvenanceRequired
        _validate_causation(
            connection,
            request.causation_event_id,
            row["correlation_id"],
        )
        if row["status"] != TaskStatus.READY.value:
            raise TaskStateConflict(TaskStatus.READY, TaskStatus(row["status"]))
        if row["assigned_to"] is not None and row["assigned_to"] != request.agent_id:
            raise TaskAssignmentConflict(request.agent_id, row["assigned_to"])
        if has_execution_provenance:
            policy.require(actor_id, row["type"])
            reused = connection.execute(
                "SELECT id FROM tasks WHERE execution_id = ? AND id != ?",
                (str(execution_id), str(task_id)),
            ).fetchone()
            if reused is not None:
                raise ExecutionIdConflict

        now = datetime.now(UTC).isoformat()
        if has_execution_provenance:
            updated = connection.execute(
                """
                UPDATE tasks
                SET status = 'running', assigned_to = ?, started_at = ?,
                    updated_at = ?, version = version + 1,
                    execution_id = ?, runner_instance_id = ?, execution_subject_id = ?,
                    claimed_principal_id = ?, claim_policy_id = ?, claim_policy_sha256 = ?
                WHERE id = ? AND status = 'ready'
                  AND (assigned_to IS NULL OR assigned_to = ?)
                """,
                (request.agent_id, now, now, str(execution_id), str(runner_instance_id),
                 actor_id, auth.principal_id, policy.policy_id, policy.policy_sha256,
                 str(task_id), request.agent_id),
            )
        else:
            updated = connection.execute(
                """UPDATE tasks SET status = 'running', assigned_to = ?, started_at = ?,
                    updated_at = ?, version = version + 1
                WHERE id = ? AND status = 'ready'
                  AND (assigned_to IS NULL OR assigned_to = ?)""",
                (request.agent_id, now, now, str(task_id), request.agent_id),
            )
        if updated.rowcount != 1:
            current = connection.execute(
                "SELECT status, assigned_to FROM tasks WHERE id = ?",
                (str(task_id),),
            ).fetchone()
            if current is None:
                raise TaskNotFound
            current_status = TaskStatus(current["status"])
            if current_status is not TaskStatus.READY:
                raise TaskStateConflict(TaskStatus.READY, current_status)
            if (
                current["assigned_to"] is not None
                and current["assigned_to"] != request.agent_id
            ):
                raise TaskAssignmentConflict(
                    request.agent_id,
                    current["assigned_to"],
                )
            raise RuntimeError("Claim acquisition failed without a domain conflict.")

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
                "execution_id": str(execution_id),
                "runner_instance_id": str(runner_instance_id),
                "execution_subject_id": actor_id,
                "task_type": task.type,
                "policy_id": policy.policy_id,
                "policy_sha256": policy.policy_sha256,
            },
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            authenticated_principal_id=auth.principal_id,
        )
        return TaskCommandResult(task=task, replayed=False)


def _assigned_task_identity(row: sqlite3.Row) -> str:
    if row["assigned_to"] is None:
        raise TaskStateConflict(TaskStatus.RUNNING, TaskStatus(row["status"]))
    return row["assigned_to"]


def _validate_execution_command(
    row: sqlite3.Row,
    execution_id: UUID | None,
    runner_instance_id: UUID | None,
    auth: AuthContext,
) -> None:
    if "execution_id" not in row.keys():
        return
    if row["execution_id"] is None:
        raise ExecutionProvenanceRequired
    if auth.subject_id != "*" and row["execution_subject_id"] != auth.subject_id:
        raise ExecutionSubjectConflict
    if auth.subject_id == "*" and execution_id is None and runner_instance_id is None:
        return
    if execution_id is None or str(execution_id) != row["execution_id"]:
        raise ExecutionIdConflict
    if runner_instance_id is None or str(runner_instance_id) != row["runner_instance_id"]:
        raise ExecutionInstanceConflict


def complete_task(
    database: Database,
    task_id: UUID,
    request: TaskComplete,
    idempotency_key: str,
    expected_version: int,
    auth: AuthContext,
    policy: ExecutionPolicy | None = None,
) -> TaskCommandResult:
    policy = policy or TestExecutionPolicy()
    request_hash = _command_request_hash(task_id, request, expected_version)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        assigned_identity = _assigned_task_identity(row)
        actor_id = assigned_identity if auth.subject_id == "*" else auth.subject_id
        if auth.subject_id != "*" and assigned_identity != auth.subject_id:
            raise ResourceAccessDenied
        scope = _command_scope(actor_id, COMPLETE_TASK_OPERATION)
        if "execution_id" in row.keys() and row["execution_id"] is not None:
            _validate_execution_command(
                row, request.execution_id, request.runner_instance_id, auth
            )
        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            if "execution_id" not in row.keys() or row["execution_id"] is None:
                return TaskCommandResult(task=replayed, replayed=True)
            return TaskCommandResult(task=replayed, replayed=True)

        _validate_execution_command(row, request.execution_id, request.runner_instance_id, auth)
        policy.require(actor_id, row["type"])
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
                "execution_id": row["execution_id"] if "execution_id" in row.keys() else None,
                "runner_instance_id": row["runner_instance_id"] if "runner_instance_id" in row.keys() else None,
                "execution_subject_id": row["execution_subject_id"] if "execution_subject_id" in row.keys() else None,
                "claim_policy_id": row["claim_policy_id"] if "claim_policy_id" in row.keys() else None,
                "claim_policy_sha256": row["claim_policy_sha256"] if "claim_policy_sha256" in row.keys() else None,
                "report_policy_id": policy.policy_id,
                "report_policy_sha256": policy.policy_sha256,
            },
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            authenticated_principal_id=auth.principal_id,
        )
        return TaskCommandResult(task=task, replayed=False)


def fail_task(
    database: Database,
    task_id: UUID,
    request: TaskFail,
    idempotency_key: str,
    expected_version: int,
    auth: AuthContext,
    policy: ExecutionPolicy | None = None,
) -> TaskCommandResult:
    policy = policy or TestExecutionPolicy()
    request_hash = _command_request_hash(task_id, request, expected_version)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
        assigned_identity = _assigned_task_identity(row)
        actor_id = assigned_identity if auth.subject_id == "*" else auth.subject_id
        if auth.subject_id != "*" and assigned_identity != auth.subject_id:
            raise ResourceAccessDenied
        scope = _command_scope(actor_id, FAIL_TASK_OPERATION)
        if "execution_id" in row.keys() and row["execution_id"] is not None:
            _validate_execution_command(
                row, request.execution_id, request.runner_instance_id, auth
            )
        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            if "execution_id" not in row.keys() or row["execution_id"] is None:
                return TaskCommandResult(task=replayed, replayed=True)
            return TaskCommandResult(task=replayed, replayed=True)

        _validate_execution_command(row, request.execution_id, request.runner_instance_id, auth)
        policy.require(actor_id, row["type"])
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
                "execution_id": row["execution_id"] if "execution_id" in row.keys() else None,
                "runner_instance_id": row["runner_instance_id"] if "runner_instance_id" in row.keys() else None,
                "execution_subject_id": row["execution_subject_id"] if "execution_subject_id" in row.keys() else None,
                "claim_policy_id": row["claim_policy_id"] if "claim_policy_id" in row.keys() else None,
                "claim_policy_sha256": row["claim_policy_sha256"] if "claim_policy_sha256" in row.keys() else None,
                "report_policy_id": policy.policy_id,
                "report_policy_sha256": policy.policy_sha256,
            },
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            authenticated_principal_id=auth.principal_id,
        )
        return TaskCommandResult(task=task, replayed=False)


def request_task_approval(
    database: Database,
    task_id: UUID,
    request: RequestApproval,
    idempotency_key: str,
    expected_version: int,
    auth: AuthContext,
) -> RequestApprovalCommandResult:
    actor_id = request.requested_by if auth.subject_id == "*" else auth.subject_id
    scope = _command_scope(actor_id, REQUEST_APPROVAL_OPERATION)
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

        updated = connection.execute(
            """
            UPDATE tasks
            SET status = 'waiting_approval', waiting_on_approval_id = ?,
                updated_at = ?, version = version + 1
            WHERE id = ? AND status = 'ready' AND version = ?
            """,
            (str(approval_id), now, str(task_id), expected_version),
        )
        if updated.rowcount != 1:
            raise TaskVersionConflict(expected_version, row["version"])

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
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            authenticated_principal_id=auth.principal_id,
        )
        return RequestApprovalCommandResult(result=result, replayed=False)


def decide_approval(
    database: Database,
    approval_id: UUID,
    request: ApprovalDecision,
    idempotency_key: str,
    expected_version: int,
    decision: ApprovalStatus,
    auth: AuthContext,
) -> ApprovalCommandResult:
    operation = (
        APPROVE_APPROVAL_OPERATION
        if decision is ApprovalStatus.APPROVED
        else REJECT_APPROVAL_OPERATION
    )
    actor_id = request.decided_by if auth.subject_id == "*" else auth.subject_id
    scope = _command_scope(actor_id, operation)
    request_hash = _command_request_hash(approval_id, request, expected_version)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM approvals WHERE id = ?",
            (str(approval_id),),
        ).fetchone()
        if row is None:
            raise ApprovalNotFound
        if auth.subject_id != "*" and row["assigned_to"] != auth.subject_id:
            raise ResourceAccessDenied
        replayed_body = _replayed_body(connection, scope, idempotency_key, request_hash)
        if replayed_body is not None:
            return ApprovalCommandResult(
                approval=ApprovalRead.model_validate_json(replayed_body),
                replayed=True,
            )
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
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            authenticated_principal_id=auth.principal_id,
        )
        return ApprovalCommandResult(approval=approval, replayed=False)


def release_task(
    database: Database,
    task_id: UUID,
    request: TaskRelease,
    idempotency_key: str,
    expected_version: int,
    auth: AuthContext,
) -> TaskCommandResult:
    actor_id = request.actor_id if auth.subject_id == "*" else auth.subject_id
    scope = _command_scope(actor_id, RELEASE_TASK_OPERATION)
    request_hash = _command_request_hash(task_id, request, expected_version)

    with database.transaction() as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
        if row is None:
            raise TaskNotFound
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
        if auth.subject_id != "*" and approval_row["requested_by"] != auth.subject_id:
            raise ResourceAccessDenied

        request_events = connection.execute(
            """SELECT * FROM events
            WHERE task_id = ? AND type = 'approval.requested'
            ORDER BY sequence""",
            (str(task_id),),
        ).fetchall()
        matching_requests = []
        for event in request_events:
            try:
                data = json.loads(event["data_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if (
                data.get("approval_id") == str(request.approval_id)
                and data.get("task_id") == str(task_id)
                and event["correlation_id"] == row["correlation_id"]
                and event["actor_id"] == approval_row["requested_by"]
                and event["authenticated_principal_id"] is not None
            ):
                matching_requests.append(event)
        if len(matching_requests) != 1:
            raise ApprovalRequestProvenanceInvalid

        replayed = _replayed_task(connection, scope, idempotency_key, request_hash)
        if replayed is not None:
            return TaskCommandResult(task=replayed, replayed=True)

        if row["waiting_on_approval_id"] != str(request.approval_id):
            raise ApprovalNotCurrentForTask
        if approval_row["status"] != ApprovalStatus.APPROVED.value:
            raise ApprovalStateConflict(
                ApprovalStatus.APPROVED,
                ApprovalStatus(approval_row["status"]),
            )
        if request.causation_event_id is None:
            raise ApprovalDecisionCausationInvalid
        decision_event = connection.execute(
            "SELECT * FROM events WHERE id = ?",
            (str(request.causation_event_id),),
        ).fetchone()
        if decision_event is None:
            raise ApprovalDecisionCausationInvalid
        try:
            decision_data = json.loads(decision_event["data_json"])
        except (TypeError, json.JSONDecodeError):
            raise ApprovalDecisionCausationInvalid from None
        if not (
            decision_event["type"] == "approval.approved"
            and decision_event["task_id"] == str(task_id)
            and decision_event["correlation_id"] == row["correlation_id"]
            and decision_event["authenticated_principal_id"] is not None
            and decision_data.get("approval_id") == str(request.approval_id)
            and decision_data.get("task_id") == str(task_id)
            and decision_data.get("status") == ApprovalStatus.APPROVED.value
        ):
            raise ApprovalDecisionCausationInvalid
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
            SET status = 'ready', waiting_on_approval_id = NULL,
                updated_at = ?, version = version + 1
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
            actor_id=actor_id,
            authenticated_principal_id=auth.principal_id,
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
            authenticated_principal_id=auth.principal_id,
        )
        return TaskCommandResult(task=task, replayed=False)


def get_task(database: Database, task_id: UUID) -> TaskRead | None:
    with closing(database.connect()) as connection:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (str(task_id),),
        ).fetchone()
    return _task_from_row(row) if row is not None else None


def list_tasks(
    database: Database,
    *,
    limit: int,
    cursor: str | None = None,
    status: TaskStatus | None = None,
    assigned_to: str | None = None,
    requested_by: str | None = None,
    correlation_id: UUID | None = None,
    task_type: str | None = None,
) -> TaskPage:
    fingerprint = _task_filter_fingerprint(
        status=status,
        assigned_to=assigned_to,
        requested_by=requested_by,
        correlation_id=correlation_id,
        task_type=task_type,
    )
    clauses: list[str] = []
    parameters: list[object] = []
    for column, value in (
        ("status", status.value if status else None),
        ("assigned_to", assigned_to),
        ("requested_by", requested_by),
        ("correlation_id", str(correlation_id) if correlation_id else None),
        ("type", task_type),
    ):
        if value is not None:
            clauses.append(f"{column} = ?")
            parameters.append(value)
    if cursor is not None:
        created_at, task_id = _decode_task_cursor(cursor, fingerprint)
        clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
        parameters.extend((created_at, created_at, task_id))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with closing(database.connect()) as connection:
        rows = connection.execute(
            f"""
            SELECT * FROM tasks
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (*parameters, limit + 1),
        ).fetchall()
    page_rows = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page_rows:
        last = page_rows[-1]
        next_cursor = _encode_task_cursor(last["created_at"], last["id"], fingerprint)
    return TaskPage(
        items=[_task_from_row(row) for row in page_rows],
        next_cursor=next_cursor,
    )


def get_task_events(database: Database, task_id: UUID) -> list[EventRead]:
    with closing(database.connect()) as connection:
        rows = connection.execute(
            "SELECT * FROM events WHERE task_id = ? ORDER BY sequence",
            (str(task_id),),
        ).fetchall()
        return [_event_from_row(row) for row in rows]


def get_correlation_trace(database: Database, correlation_id: UUID) -> CorrelationTrace:
    with closing(database.connect()) as connection:
        connection.execute("BEGIN")
        task_rows = connection.execute(
            """SELECT * FROM tasks WHERE correlation_id = ?
            ORDER BY created_at, id""",
            (str(correlation_id),),
        ).fetchall()
        event_rows = connection.execute(
            """SELECT * FROM events WHERE correlation_id = ?
            ORDER BY occurred_at, task_id, sequence""",
            (str(correlation_id),),
        ).fetchall()
    return CorrelationTrace(
        correlation_id=correlation_id,
        tasks=[_task_from_row(row) for row in task_rows],
        events=[_event_from_row(row) for row in event_rows],
    )


def get_approval(database: Database, approval_id: UUID) -> ApprovalRead | None:
    with closing(database.connect()) as connection:
        row = connection.execute(
            "SELECT * FROM approvals WHERE id = ?",
            (str(approval_id),),
        ).fetchone()
    return _approval_from_row(row) if row is not None else None


def list_approvals(
    database: Database,
    *,
    limit: int,
    cursor: str | None = None,
    status: ApprovalStatus | None = None,
    assigned_to: str | None = None,
    task_id: UUID | None = None,
    correlation_id: UUID | None = None,
) -> ApprovalPage:
    filters = {
        "status": status.value if status else None,
        "assigned_to": assigned_to,
        "task_id": str(task_id) if task_id else None,
        "correlation_id": str(correlation_id) if correlation_id else None,
    }
    fingerprint = hashlib.sha256(json.dumps(
        filters, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()
    clauses: list[str] = []
    parameters: list[object] = []
    for column, value in filters.items():
        if value is not None:
            clauses.append(f"{column} = ?")
            parameters.append(value)
    if cursor is not None:
        try:
            decoded = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
            if set(decoded) != {"created_at", "id", "fingerprint"}:
                raise ValueError
            if decoded["fingerprint"] != fingerprint:
                raise ValueError
            created_at, approval_id = decoded["created_at"], decoded["id"]
            if not isinstance(created_at, str) or not isinstance(approval_id, str):
                raise ValueError
        except (ValueError, TypeError, json.JSONDecodeError, binascii.Error):
            raise InvalidApprovalCursor from None
        clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
        parameters.extend((created_at, created_at, approval_id))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with closing(database.connect()) as connection:
        rows = connection.execute(
            f"SELECT * FROM approvals {where} ORDER BY created_at DESC, id DESC LIMIT ?",
            (*parameters, limit + 1),
        ).fetchall()
    page_rows = rows[:limit]
    next_cursor = None
    if len(rows) > limit and page_rows:
        last = page_rows[-1]
        payload = json.dumps({
            "created_at": last["created_at"], "id": last["id"],
            "fingerprint": fingerprint,
        }, sort_keys=True, separators=(",", ":")).encode()
        next_cursor = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    return ApprovalPage(
        items=[_approval_from_row(row) for row in page_rows],
        next_cursor=next_cursor,
    )


def get_task_approvals(database: Database, task_id: UUID) -> list[ApprovalRead]:
    with closing(database.connect()) as connection:
        rows = connection.execute(
            "SELECT * FROM approvals WHERE task_id = ? ORDER BY created_at, id",
            (str(task_id),),
        ).fetchall()
    return [_approval_from_row(row) for row in rows]


def get_agent_inbox(database: Database, agent_id: str) -> AgentInbox:
    with closing(database.connect()) as connection:
        connection.execute("BEGIN")
        task_rows = connection.execute(
            """SELECT * FROM tasks
            WHERE assigned_to = ? AND status IN ('ready', 'running')""",
            (agent_id,),
        ).fetchall()
        approval_rows = connection.execute(
            """SELECT * FROM approvals
            WHERE assigned_to = ? AND status = 'pending'""",
            (agent_id,),
        ).fetchall()

    items: list[InboxTaskItem | InboxApprovalItem] = []
    for row in task_rows:
        task = _task_from_row(row)
        items.append(
            InboxTaskItem(
                attention_since=(task.created_at if task.status is TaskStatus.READY else task.updated_at),
                etag=f'"v{task.version}"',
                available_actions=(
                    ["claim"] if task.status is TaskStatus.READY else ["complete", "fail"]
                ),
                task=task,
            )
        )
    for row in approval_rows:
        approval = _approval_from_row(row)
        items.append(
            InboxApprovalItem(
                attention_since=approval.created_at,
                etag=f'"v{approval.version}"',
                available_actions=["approve", "reject"],
                approval=approval,
            )
        )
    items.sort(
        key=lambda item: (
            item.attention_since,
            item.kind,
            str(item.task.id if isinstance(item, InboxTaskItem) else item.approval.id),
        )
    )
    return AgentInbox(agent_id=agent_id, items=items)
