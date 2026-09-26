from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class TaskStatus(StrEnum):
    READY = "ready"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1, max_length=100)
    title: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=10_000)
    input: dict[str, Any] = Field(default_factory=dict)
    requested_by: str = Field(min_length=1, max_length=200)
    assigned_to: str | None = Field(default=None, min_length=1, max_length=200)
    correlation_id: UUID
    causation_event_id: UUID | None = None


class TaskClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(min_length=1, max_length=200)
    causation_event_id: UUID | None = None


class TaskComplete(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output: dict[str, Any]
    causation_event_id: UUID | None = None


class TaskFail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_code: str = Field(min_length=1, max_length=100)
    failure_message: str = Field(min_length=1, max_length=10_000)
    causation_event_id: UUID | None = None


class RequestApproval(BaseModel):
    model_config = ConfigDict(extra="forbid")

    gate: str = Field(min_length=1, max_length=200)
    request_reason: str = Field(min_length=1, max_length=10_000)
    requested_by: str = Field(min_length=1, max_length=200)
    assigned_to: str | None = Field(default=None, min_length=1, max_length=200)
    context: dict[str, Any] = Field(default_factory=dict)
    causation_event_id: UUID | None = None


class ApprovalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decided_by: str = Field(min_length=1, max_length=200)
    decision_reason: str | None = Field(default=None, max_length=10_000)
    causation_event_id: UUID | None = None


class TaskRelease(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approval_id: UUID
    actor_id: str = Field(min_length=1, max_length=200)
    causation_event_id: UUID | None = None


class TaskRead(BaseModel):
    id: UUID
    type: str
    title: str
    description: str | None
    input: dict[str, Any]
    output: dict[str, Any] | None
    status: TaskStatus
    requested_by: str
    assigned_to: str | None
    failure_code: str | None
    failure_message: str | None
    retry_of: UUID | None
    waiting_on_approval_id: UUID | None
    correlation_id: UUID
    version: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class ApprovalRead(BaseModel):
    id: UUID
    task_id: UUID
    gate: str
    status: ApprovalStatus
    request_reason: str
    context: dict[str, Any]
    requested_by: str
    assigned_to: str | None
    decided_by: str | None
    decision_reason: str | None
    correlation_id: UUID
    version: int
    created_at: datetime
    updated_at: datetime
    decided_at: datetime | None


class RequestApprovalResult(BaseModel):
    task: TaskRead
    approval: ApprovalRead


class EventRead(BaseModel):
    id: UUID
    task_id: UUID
    sequence: int
    type: str
    data: dict[str, Any]
    actor_id: str
    correlation_id: UUID
    causation_event_id: UUID | None
    command_id: UUID
    event_index: int
    occurred_at: datetime
