from __future__ import annotations

import base64
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Literal
from urllib.parse import quote, urlparse

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from agentbus.auth import TOKEN_PATTERN, read_token_file


SourceKind = Literal["approval_intent", "release", "runner", "outbox"]
SOURCE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str = Field(min_length=1, max_length=64)
    kind: SourceKind
    path: str
    logical_owner: str = Field(min_length=1, max_length=200)


class ProjectionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sources: tuple[SourceConfig, ...]

    @classmethod
    def from_file(cls, path: str | Path) -> ProjectionManifest:
        manifest_path = Path(path)
        if not manifest_path.is_absolute():
            raise ValueError("Operational manifest path must be absolute.")
        if manifest_path.stat().st_mode & 0o077:
            raise PermissionError("Operational manifest must have mode 0600.")
        if manifest_path.is_symlink():
            raise ValueError("Operational manifest cannot be a symbolic link.")
        manifest = cls.model_validate_json(manifest_path.read_text(encoding="utf-8"))
        ids: set[str] = set()
        paths: set[str] = set()
        for source in manifest.sources:
            source_path = Path(source.path)
            if not SOURCE_ID.fullmatch(source.source_id):
                raise ValueError("Source ids must use lowercase safe characters.")
            if not source_path.is_absolute():
                raise ValueError("Journal paths must be absolute.")
            if source_path.is_symlink():
                raise ValueError("Journal paths cannot be symbolic links.")
            rendered = str(source_path)
            if source.source_id in ids or rendered in paths:
                raise ValueError("Source ids and journal paths must be unique.")
            ids.add(source.source_id); paths.add(rendered)
        return manifest


class SourceStatus(BaseModel):
    source_id: str
    kind: SourceKind
    status: Literal["available", "unavailable", "incompatible"]
    observed_at: str
    public_error_code: str | None = None


class OperationalItem(BaseModel):
    operation_id: str
    source_id: str
    kind: SourceKind
    persisted_state: str
    attention_class: str
    recovery_mode: str
    task_id: str | None = None
    approval_id: str | None = None
    correlation_id: str | None = None
    logical_owner: str
    created_at: str | None = None
    state_observed_at: str | None = None
    timestamp_quality: Literal["exact", "lower_bound", "unknown"]
    last_http_status: int | None = None
    public_error_code: str | None = None
    evidence: tuple[str, ...]
    summary: str


class OperationalPage(BaseModel):
    items: list[OperationalItem]
    next_cursor: str | None
    sources: list[SourceStatus]
    snapshot_scope: Literal["per_request_not_global"] = "per_request_not_global"


SCHEMAS: dict[SourceKind, tuple[str, frozenset[str]]] = {
    "approval_intent": ("approval_intents", frozenset({"intent_id", "approval_id", "task_id", "correlation_id", "state", "created_at", "confirmed_at", "sending_at", "completed_at", "intended_subject_id", "last_http_status", "last_error_code"})),
    "release": ("release_commands", frozenset({"approval_id", "task_id", "correlation_id", "state", "created_at", "prepared_at", "completed_at", "expected_subject_id", "last_http_status", "last_error_code"})),
    "runner": ("executions", frozenset({"task_id", "runner_instance_id", "claimed_etag", "claim_rejected_at", "result_kind", "reported_at", "created_at", "updated_at"})),
    "outbox": ("successor_commands", frozenset({"coordination_id", "predecessor_task_id", "successor_task_id", "created_at", "confirmed_at"})),
}


def _operation_id(source_id: str, native_id: str) -> str:
    raw = json.dumps([source_id, native_id], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_operation_id(value: str) -> tuple[str, str]:
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        decoded = json.loads(raw)
        if not isinstance(decoded, list) or len(decoded) != 2 or not all(isinstance(v, str) and v for v in decoded):
            raise ValueError
        return decoded[0], decoded[1]
    except (ValueError, json.JSONDecodeError):
        raise HTTPException(status_code=404, detail={"code": "operation_not_found"}) from None


def _now() -> str:
    return datetime.now(UTC).isoformat()


class AgentBusReadClient:
    def __init__(self, url: str, token: str, transport: httpx.BaseTransport | None = None):
        parsed = urlparse(url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Operational projection AgentBus target must be loopback HTTP.")
        self.client = httpx.Client(base_url=url, headers={"Authorization": f"Bearer {token}"}, transport=transport)

    def close(self) -> None: self.client.close()

    def verify_read_only(self) -> None:
        response = self.client.get("/auth/me")
        if response.status_code != 200:
            raise RuntimeError("Operational AgentBus identity is unavailable.")
        try:
            body = response.json(); permissions = set(body["permissions"])
        except (ValueError, KeyError, TypeError):
            raise RuntimeError("Operational AgentBus identity is invalid.") from None
        allowed = {"task:read:any", "approval:read:any", "event:read:any", "trace:read:any"}
        if "task:read:any" not in permissions or not permissions.issubset(allowed):
            raise RuntimeError("Operational AgentBus principal must be read-only.")

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        response = self.client.get(f"/tasks/{task_id}")
        if response.status_code != 200: return None
        try:
            body = response.json()
        except ValueError:
            return None
        return body if isinstance(body, dict) and body.get("id") == task_id else None


class OperationalProjection:
    def __init__(self, manifest: ProjectionManifest, agentbus: AgentBusReadClient):
        self.manifest = manifest
        self.agentbus = agentbus

    @staticmethod
    def _connect(path: str) -> sqlite3.Connection:
        uri = f"file:{quote(path, safe='/')}?mode=ro"
        db = sqlite3.connect(uri, uri=True, timeout=1, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        return db

    def _rows(self, source: SourceConfig) -> list[sqlite3.Row]:
        table, required = SCHEMAS[source.kind]
        with closing(self._connect(source.path)) as db:
            columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
            if not required.issubset(columns):
                raise RuntimeError("incompatible")
            rows = db.execute(f"SELECT {','.join(sorted(required))} FROM {table} LIMIT 10001").fetchall()
            if len(rows) > 10000:
                raise RuntimeError("incompatible")
            return rows

    def _item(self, source: SourceConfig, row: sqlite3.Row) -> OperationalItem:
        data = dict(row)
        if source.kind == "approval_intent":
            native, state = data["intent_id"], data["state"]
            mapping = {
                "pending": ("human_required", "human_confirmation", "Aguarda confirmação humana."),
                "confirmed": ("authentication_required" if data["last_error_code"] == "authentication_required" else "recoverable_exact_replay", "exact_command_replay", "Decisão confirmada aguarda envio ou recuperação."),
                "sending": ("active_or_ambiguous", "exact_command_replay", "Envio ativo ou resposta ainda não confirmada."),
                "completed": ("terminal_success", "none", "Decisão confirmada pelo AgentBus."),
                "stopped": ("terminal_stopped", "none", "Decisão interrompida; requer investigação."),
                "expired": ("terminal_stopped", "none", "Intenção expirada."),
            }
            if state not in mapping: raise RuntimeError("incompatible")
            attention, recovery, summary = mapping[state]
            observed = data["completed_at"] or data["sending_at"] or data["confirmed_at"] or data["created_at"]
            task_id, approval_id, correlation_id = data["task_id"], data["approval_id"], data["correlation_id"]
            evidence = ("journal.approval_id", "journal.task_id", "journal.correlation_id")
        elif source.kind == "release":
            native, state = data["approval_id"], data["state"]
            mapping = {
                "observed": ("informational", "explicit_reconcile", "Approval observada; comando ainda não preparado."),
                "prepared": ("recoverable_exact_replay", "exact_command_replay", "Release preparado para envio."),
                "sending": ("active_or_ambiguous", "exact_command_replay", "Release ativo ou resposta ainda não confirmada."),
                "auth_required": ("authentication_required", "exact_command_replay", "Release aguarda credencial válida do mesmo subject."),
                "completed": ("terminal_success", "none", "Release confirmado pelo AgentBus."),
                "stopped": ("terminal_stopped", "none", "Release interrompido; requer investigação."),
            }
            if state not in mapping: raise RuntimeError("incompatible")
            attention, recovery, summary = mapping[state]
            observed = data["completed_at"] or data["prepared_at"] or data["created_at"]
            task_id, approval_id, correlation_id = data["task_id"], data["approval_id"], data["correlation_id"]
            evidence = ("journal.approval_id", "journal.task_id", "journal.correlation_id")
        elif source.kind == "runner":
            native, task_id = data["task_id"], data["task_id"]
            approval_id = None
            task = self.agentbus.get_task(task_id)
            correlation_id = task.get("correlation_id") if task else None
            if data["claim_rejected_at"]:
                state, attention, recovery, summary, observed = "claim_rejected", "terminal_stopped", "none", "Outra instância venceu o claim.", data["claim_rejected_at"]
            elif data["reported_at"]:
                state, attention, recovery, summary, observed = "reported", "terminal_success", "none", "Resultado confirmado pelo AgentBus.", data["reported_at"]
            elif data["result_kind"]:
                state, attention, recovery, summary, observed = "report_pending", "recoverable_exact_replay", "exact_command_replay", "Resultado durável aguarda reporte.", data["updated_at"]
            elif data["claimed_etag"]:
                state, attention, recovery, summary, observed = "execution_unconfirmed", "active_or_ambiguous", "conditional_executor_resume", "Execução ativa ou interrompida; retomada depende do executor.", data["updated_at"]
            else:
                state, attention, recovery, summary, observed = "claim_prepared", "recoverable_exact_replay", "exact_command_replay", "Claim preparado e ainda não confirmado.", data["updated_at"]
            evidence = ("journal.task_id", "agentbus.task" if task else "agentbus.task_unavailable")
        else:
            native, task_id, approval_id = data["coordination_id"], data["predecessor_task_id"], None
            task = self.agentbus.get_task(task_id)
            correlation_id = task.get("correlation_id") if task else None
            if data["confirmed_at"]:
                state, attention, recovery, summary, observed = "confirmed", "terminal_success", "none", "Task sucessora confirmada.", data["confirmed_at"]
            else:
                state, attention, recovery, summary, observed = "unconfirmed", "recoverable_exact_replay", "startup_replay", "Comando sucessor ainda não confirmado.", data["created_at"]
            evidence = ("journal.predecessor_task_id", "journal.successor_task_id" if data["successor_task_id"] else "journal.successor_unconfirmed", "agentbus.task" if task else "agentbus.task_unavailable")
        quality = "exact" if observed else "unknown"
        if not correlation_id and source.kind in {"runner", "outbox"}:
            attention, summary = "insufficient_evidence", "Task canônica indisponível; correlação não comprovada."
        error_code = data.get("last_error_code")
        known_errors = {None, "transport_error", "authentication_required", "invalid_success_response", "gate_not_allowed", "invalid_authenticated_provenance", "task_not_waiting_on_approval", "credential_subject_changed", "credential_identity_or_permission_changed"}
        public_error = error_code if error_code in known_errors else "operation_error"
        return OperationalItem(
            operation_id=_operation_id(source.source_id, str(native)), source_id=source.source_id,
            kind=source.kind, persisted_state=state, attention_class=attention,
            recovery_mode=recovery, task_id=task_id, approval_id=approval_id,
            correlation_id=correlation_id, logical_owner=source.logical_owner,
            created_at=data.get("created_at"), state_observed_at=observed,
            timestamp_quality=quality, last_http_status=data.get("last_http_status"),
            public_error_code=public_error, evidence=evidence, summary=summary,
        )

    def snapshot(self) -> tuple[list[OperationalItem], list[SourceStatus]]:
        items: list[OperationalItem] = []; statuses: list[SourceStatus] = []
        for source in self.manifest.sources:
            observed = _now()
            try:
                rows = self._rows(source)
                source_items = [self._item(source, row) for row in rows]
            except (OSError, sqlite3.Error):
                statuses.append(SourceStatus(source_id=source.source_id, kind=source.kind, status="unavailable", observed_at=observed, public_error_code="source_unavailable"))
                continue
            except (RuntimeError, KeyError, TypeError, ValueError):
                statuses.append(SourceStatus(source_id=source.source_id, kind=source.kind, status="incompatible", observed_at=observed, public_error_code="source_incompatible"))
                continue
            items.extend(source_items)
            statuses.append(SourceStatus(source_id=source.source_id, kind=source.kind, status="available", observed_at=observed))
        return items, statuses


def _fingerprint(filters: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(filters, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _cursor(value: tuple[str, str], fingerprint: str) -> str:
    raw = json.dumps([*value, fingerprint], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _read_cursor(value: str, fingerprint: str) -> tuple[str, str]:
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        created, operation_id, stored = json.loads(raw)
        if stored != fingerprint or not isinstance(created, str) or not isinstance(operation_id, str): raise ValueError
        return created, operation_id
    except (ValueError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail={"code": "invalid_operations_cursor"}) from None


def create_projection_app(*, projection: OperationalProjection, reader_token: str) -> FastAPI:
    if not TOKEN_PATTERN.fullmatch(reader_token):
        raise ValueError("Operational reader token is invalid.")
    token_hash = hashlib.sha256(reader_token.encode()).digest()
    def authenticated(authorization: str | None = Header(default=None)) -> None:
        scheme, _, token = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not TOKEN_PATTERN.fullmatch(token) or not hmac.compare_digest(hashlib.sha256(token.encode()).digest(), token_hash):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"code": "invalid_operational_reader"})
    app = FastAPI(
        title="AgentBus Operational Projection",
        docs_url=None, redoc_url=None, openapi_url=None,
    )

    @app.get("/operations", response_model=OperationalPage)
    def operations(
        kind: SourceKind | None = None, attention_class: str | None = Query(default=None, max_length=64),
        task_id: str | None = Query(default=None, max_length=64), approval_id: str | None = Query(default=None, max_length=64),
        correlation_id: str | None = Query(default=None, max_length=64), logical_owner: str | None = Query(default=None, max_length=200),
        cursor: str | None = Query(default=None, max_length=2000), limit: int = Query(default=25, ge=1, le=100),
        _: None = Depends(authenticated),
    ) -> OperationalPage:
        filters = {"kind": kind, "attention_class": attention_class, "task_id": task_id, "approval_id": approval_id, "correlation_id": correlation_id, "logical_owner": logical_owner}
        fingerprint = _fingerprint(filters)
        after = _read_cursor(cursor, fingerprint) if cursor else None
        items, sources = projection.snapshot()
        filtered = [item for item in items if all(value is None or getattr(item, key) == value for key, value in filters.items())]
        filtered.sort(key=lambda item: (item.created_at or "", item.operation_id), reverse=True)
        if after: filtered = [item for item in filtered if (item.created_at or "", item.operation_id) < after]
        page = filtered[:limit]
        next_cursor = _cursor((page[-1].created_at or "", page[-1].operation_id), fingerprint) if len(filtered) > limit and page else None
        return OperationalPage(items=page, next_cursor=next_cursor, sources=sources)

    @app.get("/operations/{operation_id}", response_model=OperationalItem)
    def operation(operation_id: str, _: None = Depends(authenticated)) -> OperationalItem:
        source_id, native_id = _decode_operation_id(operation_id)
        items, _ = projection.snapshot()
        for item in items:
            decoded_source, decoded_native = _decode_operation_id(item.operation_id)
            if decoded_source == source_id and decoded_native == native_id: return item
        raise HTTPException(status_code=404, detail={"code": "operation_not_found"})

    @app.get("/sources", response_model=list[SourceStatus])
    def sources(_: None = Depends(authenticated)) -> list[SourceStatus]:
        return projection.snapshot()[1]
    return app


def from_environment() -> FastAPI:
    manifest = ProjectionManifest.from_file(os.environ["AGENTBUS_OPERATIONAL_MANIFEST"])
    agentbus = AgentBusReadClient(os.environ.get("AGENTBUS_URL", "http://127.0.0.1:8000"), read_token_file(os.environ["AGENTBUS_OPERATIONAL_AGENTBUS_TOKEN_FILE"]))
    agentbus.verify_read_only()
    app = create_projection_app(projection=OperationalProjection(manifest, agentbus), reader_token=read_token_file(os.environ["AGENTBUS_OPERATIONAL_READER_TOKEN_FILE"]))
    return app


app = from_environment() if os.environ.get("AGENTBUS_OPERATIONAL_MANIFEST") else FastAPI()
