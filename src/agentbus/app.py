from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import os
from pathlib import Path
import re
import sqlite3
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse

from agentbus.auth import (
    AuthContext, Authenticator, AuthenticationRequired, EnvironmentAuthenticator,
    InvalidCredential, PermissionDenied, PrincipalMismatch, ResourceAccessDenied,
    TestAuthenticator,
)

from agentbus.database import Database
from agentbus.models import (
    AgentInbox,
    ApprovalDecision,
    ApprovalRead,
    ApprovalStatus,
    EventRead,
    CorrelationTrace,
    RequestApproval,
    RequestApprovalResult,
    TaskClaim,
    TaskComplete,
    TaskCreate,
    TaskFail,
    TaskRead,
    TaskPage,
    TaskStatus,
    TaskRelease,
    SuccessorTaskCreate,
)
from agentbus.service import (
    ApprovalCorrelationMismatch,
    ApprovalNotFound,
    ApprovalNotCurrentForTask,
    ApprovalStateConflict,
    ApprovalTaskMismatch,
    ApprovalVersionConflict,
    CausationCorrelationMismatch,
    CausationEventNotFound,
    IdempotencyConflict,
    InvalidTaskCursor,
    PendingApprovalConflict,
    PredecessorCompletionEventInvalid,
    PredecessorNotSucceeded,
    TaskNotFound,
    TaskAssignmentConflict,
    TaskStateConflict,
    TaskVersionConflict,
    claim_task,
    complete_task,
    create_task,
    create_successor_task,
    decide_approval,
    fail_task,
    get_approval,
    get_agent_inbox,
    get_task,
    list_tasks,
    get_task_approvals,
    get_task_events,
    get_correlation_trace,
    release_task,
    request_task_approval,
)


ETAG_PATTERN = re.compile(r'^"v([1-9][0-9]*)"$')


def _etag(task: TaskRead) -> str:
    return f'"v{task.version}"'


def _task_response(
    task: TaskRead,
    *,
    status_code: int,
    replayed: bool | None = None,
) -> JSONResponse:
    headers = {"ETag": _etag(task)}
    if replayed is not None:
        headers["Idempotency-Replayed"] = str(replayed).lower()
    return JSONResponse(
        status_code=status_code,
        content=task.model_dump(mode="json"),
        headers=headers,
    )


def _approval_response(
    approval: ApprovalRead,
    *,
    replayed: bool | None = None,
) -> JSONResponse:
    headers = {"ETag": f'"v{approval.version}"'}
    if replayed is not None:
        headers["Idempotency-Replayed"] = str(replayed).lower()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=approval.model_dump(mode="json"),
        headers=headers,
    )


def _request_approval_response(
    result: RequestApprovalResult,
    replayed: bool,
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result.model_dump(mode="json"),
        headers={
            "Task-ETag": f'"v{result.task.version}"',
            "Approval-ETag": f'"v{result.approval.version}"',
            "Idempotency-Replayed": str(replayed).lower(),
        },
    )


def _expected_version(if_match: str | None) -> int:
    if if_match is None:
        raise HTTPException(
            status_code=status.HTTP_428_PRECONDITION_REQUIRED,
            detail={
                "code": "if_match_required",
                "message": "If-Match is required for this command.",
            },
        )
    matched = ETAG_PATTERN.fullmatch(if_match)
    if matched is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "invalid_if_match",
                "message": 'If-Match must be a strong ETag in the form "vN".',
            },
        )
    return int(matched.group(1))


def create_app(
    database_path: str | Path | None = None,
    *,
    authenticator: Authenticator | None = None,
) -> FastAPI:
    path = database_path or os.environ.get("AGENTBUS_DATABASE", "agentbus.sqlite3")
    database = Database(path)
    auth_backend = authenticator or EnvironmentAuthenticator()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if isinstance(auth_backend, EnvironmentAuthenticator):
            auth_backend.load()
        database.migrate()
        yield

    app = FastAPI(title="AgentBus", version="0.1.0", lifespan=lifespan)
    app.state.database = database

    def authenticated(
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> AuthContext:
        return auth_backend.authenticate(authorization)

    def require(auth: AuthContext, permission: str) -> None:
        auth.require(permission)

    def same_subject(auth: AuthContext, declared: str) -> None:
        if auth.subject_id not in {"*", declared}:
            raise PrincipalMismatch

    def direct_or_delegated(auth: AuthContext, requested_by: str) -> None:
        if auth.subject_id in {"*", requested_by}:
            return
        auth.require("task:create:delegated")

    @app.exception_handler(AuthenticationRequired)
    async def authentication_required_handler(_: Request, __: AuthenticationRequired) -> JSONResponse:
        return JSONResponse(status_code=401, content={"detail": {
            "code": "authentication_required", "message": "Bearer authentication is required."
        }}, headers={"WWW-Authenticate": "Bearer"})

    @app.exception_handler(InvalidCredential)
    async def invalid_credential_handler(_: Request, __: InvalidCredential) -> JSONResponse:
        return JSONResponse(status_code=401, content={"detail": {
            "code": "invalid_credential", "message": "Bearer credential is invalid."
        }}, headers={"WWW-Authenticate": "Bearer"})

    @app.exception_handler(PermissionDenied)
    async def permission_denied_handler(_: Request, __: PermissionDenied) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": {
            "code": "permission_denied", "message": "Principal lacks the required permission."
        }})

    @app.exception_handler(PrincipalMismatch)
    async def principal_mismatch_handler(_: Request, __: PrincipalMismatch) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": {
            "code": "principal_mismatch", "message": "Declared identity differs from authenticated subject."
        }})

    @app.exception_handler(ResourceAccessDenied)
    async def resource_access_denied_handler(_: Request, __: ResourceAccessDenied) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": {
            "code": "resource_access_denied", "message": "Principal cannot access this resource."
        }})

    @app.exception_handler(PredecessorNotSucceeded)
    async def predecessor_not_succeeded_handler(
        _: Request, error: PredecessorNotSucceeded,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": {
                "code": "predecessor_not_succeeded",
                "message": "A successor requires a succeeded predecessor.",
                "actual": error.actual.value,
            }},
        )

    @app.exception_handler(PredecessorCompletionEventInvalid)
    async def predecessor_completion_event_invalid_handler(
        _: Request, __: PredecessorCompletionEventInvalid,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": {
                "code": "predecessor_completion_event_invalid",
                "message": "The predecessor has no unique matching completion Event.",
            }},
        )

    @app.exception_handler(IdempotencyConflict)
    async def idempotency_conflict_handler(
        _: Request,
        __: IdempotencyConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "idempotency_key_reused",
                    "message": "Idempotency-Key was already used with a different request.",
                }
            },
        )

    @app.exception_handler(CausationEventNotFound)
    async def causation_event_not_found_handler(
        _: Request,
        __: CausationEventNotFound,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={
                "detail": {
                    "code": "causation_event_not_found",
                    "message": "The declared causation event does not exist.",
                }
            },
        )

    @app.exception_handler(CausationCorrelationMismatch)
    async def causation_correlation_mismatch_handler(
        _: Request,
        __: CausationCorrelationMismatch,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "causation_correlation_mismatch",
                    "message": "The causation event belongs to another correlation.",
                }
            },
        )

    @app.exception_handler(TaskNotFound)
    async def task_not_found_handler(_: Request, __: TaskNotFound) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": {"code": "task_not_found", "message": "Task not found."}},
        )

    @app.exception_handler(InvalidTaskCursor)
    async def invalid_task_cursor_handler(
        _: Request, __: InvalidTaskCursor,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": {
                "code": "invalid_task_cursor",
                "message": "Task cursor is invalid or does not match the active filters.",
            }},
        )

    @app.exception_handler(TaskStateConflict)
    async def task_state_conflict_handler(
        _: Request,
        error: TaskStateConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "task_state_conflict",
                    "message": "Task is not in the state required by this command.",
                    "expected": error.expected.value,
                    "actual": error.actual.value,
                }
            },
        )

    @app.exception_handler(TaskVersionConflict)
    async def task_version_conflict_handler(
        _: Request,
        error: TaskVersionConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "task_version_conflict",
                    "message": "Task version differs from If-Match.",
                    "expected": error.expected,
                    "actual": error.actual,
                }
            },
        )

    @app.exception_handler(ApprovalNotFound)
    async def approval_not_found_handler(_: Request, __: ApprovalNotFound) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "detail": {"code": "approval_not_found", "message": "Approval not found."}
            },
        )

    @app.exception_handler(ApprovalStateConflict)
    async def approval_state_conflict_handler(
        _: Request,
        error: ApprovalStateConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "approval_state_conflict",
                    "message": "Approval is not in the state required by this command.",
                    "expected": error.expected.value,
                    "actual": error.actual.value,
                }
            },
        )

    @app.exception_handler(ApprovalVersionConflict)
    async def approval_version_conflict_handler(
        _: Request,
        error: ApprovalVersionConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "approval_version_conflict",
                    "message": "Approval version differs from If-Match.",
                    "expected": error.expected,
                    "actual": error.actual,
                }
            },
        )

    @app.exception_handler(ApprovalTaskMismatch)
    async def approval_task_mismatch_handler(
        _: Request,
        __: ApprovalTaskMismatch,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "approval_task_mismatch",
                    "message": "Approval belongs to another Task.",
                }
            },
        )

    @app.exception_handler(ApprovalCorrelationMismatch)
    async def approval_correlation_mismatch_handler(
        _: Request,
        __: ApprovalCorrelationMismatch,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "approval_correlation_mismatch",
                    "message": "Approval belongs to another correlation.",
                }
            },
        )

    @app.exception_handler(ApprovalNotCurrentForTask)
    async def approval_not_current_for_task_handler(
        _: Request,
        __: ApprovalNotCurrentForTask,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "approval_not_current_for_task",
                    "message": "Approval did not open the Task's current wait.",
                }
            },
        )

    @app.exception_handler(PendingApprovalConflict)
    async def pending_approval_conflict_handler(
        _: Request,
        __: PendingApprovalConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "pending_approval_conflict",
                    "message": "A pending Approval already exists for this Task and gate.",
                }
            },
        )

    @app.exception_handler(sqlite3.DatabaseError)
    async def database_error_handler(_: Request, __: sqlite3.DatabaseError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": {"code": "database_error", "message": "Database operation failed."}},
        )

    @app.exception_handler(TaskAssignmentConflict)
    async def task_assignment_conflict_handler(
        _: Request,
        error: TaskAssignmentConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "detail": {
                    "code": "task_assignment_conflict",
                    "message": "Task is reserved for another agent identity.",
                    "requested": error.requested,
                    "assigned_to": error.assigned_to,
                }
            },
        )

    @app.post("/tasks", response_model=TaskRead, status_code=status.HTTP_201_CREATED)
    def create_task_endpoint(
        task: TaskCreate,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "task:create")
        direct_or_delegated(auth, task.requested_by)
        result = create_task(database, task, idempotency_key, auth)
        return _task_response(
            result.task,
            status_code=status.HTTP_201_CREATED,
            replayed=result.replayed,
        )

    @app.get("/tasks/{task_id}", response_model=TaskRead)
    def get_task_endpoint(task_id: UUID, auth: AuthContext = Depends(authenticated)) -> JSONResponse:
        task = get_task(database, task_id)
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        if "task:read:any" not in auth.permissions:
            require(auth, "task:read:self")
            if auth.subject_id != "*" and auth.subject_id != task.assigned_to:
                raise ResourceAccessDenied
        return _task_response(task, status_code=status.HTTP_200_OK)

    @app.get("/tasks", response_model=TaskPage)
    def list_tasks_endpoint(
        limit: int = Query(default=25, ge=1, le=100),
        cursor: str | None = Query(default=None, min_length=1, max_length=1000),
        task_status: TaskStatus | None = Query(default=None, alias="status"),
        assigned_to: str | None = Query(default=None, min_length=1, max_length=200),
        requested_by: str | None = Query(default=None, min_length=1, max_length=200),
        correlation_id: UUID | None = None,
        task_type: str | None = Query(default=None, alias="type", min_length=1, max_length=100),
        auth: AuthContext = Depends(authenticated),
    ) -> TaskPage:
        require(auth, "task:read:any")
        return list_tasks(
            database,
            limit=limit,
            cursor=cursor,
            status=task_status,
            assigned_to=assigned_to,
            requested_by=requested_by,
            correlation_id=correlation_id,
            task_type=task_type,
        )

    @app.post(
        "/tasks/{task_id}/successors",
        response_model=TaskRead,
        status_code=status.HTTP_201_CREATED,
    )
    def create_successor_task_endpoint(
        task_id: UUID,
        command: SuccessorTaskCreate,
        idempotency_key: str = Header(
            alias="Idempotency-Key", min_length=1, max_length=200,
        ),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "task:create:successor")
        direct_or_delegated(auth, command.requested_by)
        result = create_successor_task(database, task_id, command, idempotency_key, auth)
        return _task_response(
            result.task,
            status_code=status.HTTP_201_CREATED,
            replayed=result.replayed,
        )

    @app.get("/correlations/{correlation_id}/trace", response_model=CorrelationTrace)
    def get_correlation_trace_endpoint(correlation_id: UUID, auth: AuthContext = Depends(authenticated)) -> CorrelationTrace:
        require(auth, "trace:read:any")
        return get_correlation_trace(database, correlation_id)

    @app.get("/agents/{agent_id}/inbox", response_model=AgentInbox)
    def get_agent_inbox_endpoint(agent_id: str, auth: AuthContext = Depends(authenticated)) -> AgentInbox:
        require(auth, "inbox:read:self")
        same_subject(auth, agent_id)
        return get_agent_inbox(database, agent_id)

    @app.post("/tasks/{task_id}/claim", response_model=TaskRead)
    def claim_task_endpoint(
        task_id: UUID,
        command: TaskClaim,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "task:claim:self")
        same_subject(auth, command.agent_id)
        result = claim_task(database, task_id, command, idempotency_key, auth)
        return _task_response(
            result.task,
            status_code=status.HTTP_200_OK,
            replayed=result.replayed,
        )

    @app.post("/tasks/{task_id}/complete", response_model=TaskRead)
    def complete_task_endpoint(
        task_id: UUID,
        command: TaskComplete,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        if_match: str | None = Header(default=None, alias="If-Match"),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "task:complete:self")
        task = get_task(database, task_id)
        if task is not None and auth.subject_id not in {"*", task.assigned_to}:
            raise ResourceAccessDenied
        result = complete_task(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            auth,
        )
        return _task_response(
            result.task,
            status_code=status.HTTP_200_OK,
            replayed=result.replayed,
        )

    @app.post("/tasks/{task_id}/fail", response_model=TaskRead)
    def fail_task_endpoint(
        task_id: UUID,
        command: TaskFail,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        if_match: str | None = Header(default=None, alias="If-Match"),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "task:fail:self")
        task = get_task(database, task_id)
        if task is not None and auth.subject_id not in {"*", task.assigned_to}:
            raise ResourceAccessDenied
        result = fail_task(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            auth,
        )
        return _task_response(
            result.task,
            status_code=status.HTTP_200_OK,
            replayed=result.replayed,
        )

    @app.post("/tasks/{task_id}/request-approval", response_model=RequestApprovalResult)
    def request_task_approval_endpoint(
        task_id: UUID,
        command: RequestApproval,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        if_match: str | None = Header(default=None, alias="If-Match"),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "approval:request")
        same_subject(auth, command.requested_by)
        result = request_task_approval(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            auth,
        )
        return _request_approval_response(result.result, result.replayed)

    @app.post("/approvals/{approval_id}/approve", response_model=ApprovalRead)
    def approve_approval_endpoint(
        approval_id: UUID,
        command: ApprovalDecision,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        if_match: str | None = Header(default=None, alias="If-Match"),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "approval:decide:self")
        same_subject(auth, command.decided_by)
        approval = get_approval(database, approval_id)
        if approval is not None and auth.subject_id not in {"*", approval.assigned_to}:
            raise ResourceAccessDenied
        result = decide_approval(
            database,
            approval_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            ApprovalStatus.APPROVED,
            auth,
        )
        return _approval_response(result.approval, replayed=result.replayed)

    @app.post("/approvals/{approval_id}/reject", response_model=ApprovalRead)
    def reject_approval_endpoint(
        approval_id: UUID,
        command: ApprovalDecision,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        if_match: str | None = Header(default=None, alias="If-Match"),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "approval:decide:self")
        same_subject(auth, command.decided_by)
        approval = get_approval(database, approval_id)
        if approval is not None and auth.subject_id not in {"*", approval.assigned_to}:
            raise ResourceAccessDenied
        result = decide_approval(
            database,
            approval_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            ApprovalStatus.REJECTED,
            auth,
        )
        return _approval_response(result.approval, replayed=result.replayed)

    @app.post("/tasks/{task_id}/release", response_model=TaskRead)
    def release_task_endpoint(
        task_id: UUID,
        command: TaskRelease,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
        if_match: str | None = Header(default=None, alias="If-Match"),
        auth: AuthContext = Depends(authenticated),
    ) -> JSONResponse:
        require(auth, "task:release")
        same_subject(auth, command.actor_id)
        result = release_task(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            auth,
        )
        return _task_response(
            result.task,
            status_code=status.HTTP_200_OK,
            replayed=result.replayed,
        )

    @app.get("/approvals/{approval_id}", response_model=ApprovalRead)
    def get_approval_endpoint(approval_id: UUID, auth: AuthContext = Depends(authenticated)) -> JSONResponse:
        approval = get_approval(database, approval_id)
        if approval is None:
            raise ApprovalNotFound
        if "approval:read:any" not in auth.permissions:
            require(auth, "approval:read:self")
            if auth.subject_id != "*" and auth.subject_id != approval.assigned_to:
                raise ResourceAccessDenied
        return _approval_response(approval)

    @app.get("/tasks/{task_id}/approvals", response_model=list[ApprovalRead])
    def get_task_approvals_endpoint(task_id: UUID, auth: AuthContext = Depends(authenticated)) -> list[ApprovalRead]:
        require(auth, "approval:read:any")
        task = get_task(database, task_id)
        if task is None:
            raise TaskNotFound
        return get_task_approvals(database, task_id)

    @app.get("/tasks/{task_id}/events", response_model=list[EventRead])
    def get_task_events_endpoint(task_id: UUID, auth: AuthContext = Depends(authenticated)) -> list[EventRead]:
        require(auth, "event:read:any")
        task = get_task(database, task_id)
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        return get_task_events(database, task_id)

    return app


def create_test_app(database_path: str | Path) -> FastAPI:
    return create_app(database_path, authenticator=TestAuthenticator())


app = create_app()
