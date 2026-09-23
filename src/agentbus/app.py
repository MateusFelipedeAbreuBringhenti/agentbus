from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import os
from pathlib import Path
import re
import sqlite3
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

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
)
from agentbus.service import (
    ApprovalCorrelationMismatch,
    ApprovalNotFound,
    ApprovalStateConflict,
    ApprovalTaskMismatch,
    ApprovalVersionConflict,
    CausationCorrelationMismatch,
    CausationEventNotFound,
    IdempotencyConflict,
    PendingApprovalConflict,
    TaskNotFound,
    TaskStateConflict,
    TaskVersionConflict,
    claim_task,
    complete_task,
    create_task,
    decide_approval,
    fail_task,
    get_approval,
    get_task,
    get_task_approvals,
    get_task_events,
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


def create_app(database_path: str | Path | None = None) -> FastAPI:
    path = database_path or os.environ.get("AGENTBUS_DATABASE", "agentbus.sqlite3")
    database = Database(path)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        database.migrate()
        yield

    app = FastAPI(title="AgentBus", version="0.1.0", lifespan=lifespan)
    app.state.database = database

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

    @app.post("/tasks", response_model=TaskRead, status_code=status.HTTP_201_CREATED)
    def create_task_endpoint(
        task: TaskCreate,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
    ) -> JSONResponse:
        result = create_task(database, task, idempotency_key)
        return _task_response(
            result.task,
            status_code=status.HTTP_201_CREATED,
            replayed=result.replayed,
        )

    @app.get("/tasks/{task_id}", response_model=TaskRead)
    def get_task_endpoint(task_id: UUID) -> JSONResponse:
        task = get_task(database, task_id)
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        return _task_response(task, status_code=status.HTTP_200_OK)

    @app.post("/tasks/{task_id}/claim", response_model=TaskRead)
    def claim_task_endpoint(
        task_id: UUID,
        command: TaskClaim,
        idempotency_key: str = Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=200,
        ),
    ) -> JSONResponse:
        result = claim_task(database, task_id, command, idempotency_key)
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
    ) -> JSONResponse:
        result = complete_task(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
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
    ) -> JSONResponse:
        result = fail_task(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
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
    ) -> JSONResponse:
        result = request_task_approval(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
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
    ) -> JSONResponse:
        result = decide_approval(
            database,
            approval_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            ApprovalStatus.APPROVED,
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
    ) -> JSONResponse:
        result = decide_approval(
            database,
            approval_id,
            command,
            idempotency_key,
            _expected_version(if_match),
            ApprovalStatus.REJECTED,
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
    ) -> JSONResponse:
        result = release_task(
            database,
            task_id,
            command,
            idempotency_key,
            _expected_version(if_match),
        )
        return _task_response(
            result.task,
            status_code=status.HTTP_200_OK,
            replayed=result.replayed,
        )

    @app.get("/approvals/{approval_id}", response_model=ApprovalRead)
    def get_approval_endpoint(approval_id: UUID) -> JSONResponse:
        approval = get_approval(database, approval_id)
        if approval is None:
            raise ApprovalNotFound
        return _approval_response(approval)

    @app.get("/tasks/{task_id}/approvals", response_model=list[ApprovalRead])
    def get_task_approvals_endpoint(task_id: UUID) -> list[ApprovalRead]:
        task = get_task(database, task_id)
        if task is None:
            raise TaskNotFound
        return get_task_approvals(database, task_id)

    @app.get("/tasks/{task_id}/events", response_model=list[EventRead])
    def get_task_events_endpoint(task_id: UUID) -> list[EventRead]:
        task = get_task(database, task_id)
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        return get_task_events(database, task_id)

    return app


app = create_app()
