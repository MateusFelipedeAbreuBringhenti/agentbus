from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import os
from pathlib import Path
import sqlite3
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from agentbus.database import Database
from agentbus.models import EventRead, TaskCreate, TaskRead
from agentbus.service import (
    IdempotencyConflict,
    create_task,
    get_task,
    get_task_events,
)


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
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=result.task.model_dump(mode="json"),
            headers={"Idempotency-Replayed": str(result.replayed).lower()},
        )

    @app.get("/tasks/{task_id}", response_model=TaskRead)
    def get_task_endpoint(task_id: UUID) -> TaskRead:
        task = get_task(database, task_id)
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        return task

    @app.get("/tasks/{task_id}/events", response_model=list[EventRead])
    def get_task_events_endpoint(task_id: UUID) -> list[EventRead]:
        task = get_task(database, task_id)
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found.")
        return get_task_events(database, task_id)

    return app


app = create_app()
