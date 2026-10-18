from __future__ import annotations

from contextlib import asynccontextmanager, closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
from typing import Any, Callable, Literal
from urllib.parse import urlparse
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel, ConfigDict, Field

from agentbus.auth import read_token_file
from agentbus.models import ApprovalRead, ApprovalStatus


COOKIE_NAME = "agentbus_intent_session"
PENDING_TTL = timedelta(minutes=30)
TERMINAL_RETENTION = timedelta(days=30)
TERMINAL_STATES = {"completed", "stopped", "expired"}


class IntentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    approval_id: UUID
    decision: Literal["approve", "reject"]
    decision_reason: str | None = Field(default=None, max_length=10_000)
    if_match: str = Field(pattern=r'^"v[1-9][0-9]*"$')


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()


class IntentConflict(Exception): pass
class IntentLimit(Exception): pass
class IntentChanged(Exception): pass
class IntentNotFound(Exception): pass


class ApprovalIntentJournal:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("Approval intent journal path must be absolute.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600,
            )
        except FileExistsError:
            if self.path.stat().st_mode & 0o077:
                raise PermissionError(
                    "Approval intent journal must not be accessible by group or others."
                ) from None
        else:
            os.close(descriptor)
        with self.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS approval_intents (
                intent_id TEXT PRIMARY KEY,
                intent_key TEXT NOT NULL UNIQUE,
                request_hash TEXT NOT NULL,
                browser_session_id TEXT NOT NULL,
                approval_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                decision TEXT NOT NULL CHECK(decision IN ('approve','reject')),
                decision_reason TEXT,
                approval_etag TEXT NOT NULL,
                approval_snapshot_json TEXT NOT NULL,
                intended_subject_id TEXT NOT NULL,
                confirmed_principal_id TEXT,
                confirmed_subject_id TEXT,
                command_body_json TEXT,
                command_sha256 TEXT,
                idempotency_key TEXT UNIQUE,
                state TEXT NOT NULL CHECK(state IN
                    ('pending','confirmed','sending','completed','stopped','expired')),
                revision INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                confirmed_at TEXT,
                sending_at TEXT,
                completed_at TEXT,
                attempt_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_http_status INTEGER,
                last_error_code TEXT,
                response_json TEXT,
                response_etag TEXT,
                replayed INTEGER
            );
            CREATE INDEX IF NOT EXISTS approval_intents_state
                ON approval_intents(state, created_at);
            """)

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        return db

    @staticmethod
    def _public(row: sqlite3.Row) -> dict[str, Any]:
        approval = json.loads(row["approval_snapshot_json"])
        return {
            "intent_id": row["intent_id"],
            "approval": {key: approval[key] for key in (
                "id", "task_id", "gate", "status", "request_reason",
                "assigned_to", "version",
            )},
            "decision": row["decision"],
            "decision_reason": row["decision_reason"],
            "state": row["state"],
            "created_at": row["created_at"],
            "confirmed_at": row["confirmed_at"],
            "completed_at": row["completed_at"],
            "result": json.loads(row["response_json"]) if row["response_json"] else None,
            "public_error": row["last_error_code"],
            "replayed": bool(row["replayed"]) if row["replayed"] is not None else None,
        }

    def expire_and_gc(self, now: datetime | None = None) -> None:
        current = now or datetime.now(UTC)
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE approval_intents SET state='expired', revision=revision+1 "
                "WHERE state='pending' AND created_at < ?",
                ((current - PENDING_TTL).isoformat(),),
            )
            db.execute(
                "DELETE FROM approval_intents WHERE state IN ('completed','stopped','expired') "
                "AND COALESCE(completed_at, created_at) < ?",
                ((current - TERMINAL_RETENTION).isoformat(),),
            )
            db.commit()

    def create(
        self, *, session_id: str, intent_key: str, command: IntentCreate,
        approval: ApprovalRead, etag: str,
    ) -> tuple[dict[str, Any], bool]:
        request_body = command.model_dump(mode="json")
        request_hash = _hash(_canonical(request_body))
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE approval_intents SET state='expired', revision=revision+1 "
                "WHERE state='pending' AND created_at < ?",
                ((datetime.now(UTC) - PENDING_TTL).isoformat(),),
            )
            existing = db.execute(
                "SELECT * FROM approval_intents WHERE intent_key=?", (intent_key,)
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash or existing["browser_session_id"] != session_id:
                    db.rollback()
                    raise IntentConflict
                db.commit()
                return self._public(existing), True
            session_count = db.execute(
                "SELECT count(*) FROM approval_intents "
                "WHERE state='pending' AND browser_session_id=?", (session_id,),
            ).fetchone()[0]
            approval_count = db.execute(
                "SELECT count(*) FROM approval_intents WHERE state='pending' AND approval_id=?",
                (str(command.approval_id),),
            ).fetchone()[0]
            global_count = db.execute(
                "SELECT count(*) FROM approval_intents WHERE state='pending'"
            ).fetchone()[0]
            if session_count >= 50 or approval_count >= 3 or global_count >= 500:
                db.rollback()
                raise IntentLimit
            intent_id = str(uuid4())
            db.execute("""INSERT INTO approval_intents(
                intent_id,intent_key,request_hash,browser_session_id,approval_id,task_id,
                correlation_id,decision,decision_reason,approval_etag,approval_snapshot_json,
                intended_subject_id,state,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'pending',?)""", (
                intent_id, intent_key, request_hash, session_id, str(approval.id),
                str(approval.task_id), str(approval.correlation_id), command.decision,
                command.decision_reason, etag, approval.model_dump_json(),
                approval.assigned_to, _now(),
            ))
            row = db.execute(
                "SELECT * FROM approval_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            db.commit()
            return self._public(row), False

    def public(self, intent_id: str, session_id: str) -> dict[str, Any]:
        with closing(self.connect()) as db:
            row = db.execute(
                "SELECT * FROM approval_intents WHERE intent_id=? AND browser_session_id=?",
                (intent_id, session_id),
            ).fetchone()
        if row is None:
            raise IntentNotFound
        return self._public(row)

    def read(self, intent_id: str) -> dict[str, Any]:
        with closing(self.connect()) as db:
            row = db.execute(
                "SELECT * FROM approval_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
        if row is None:
            raise IntentNotFound
        return dict(row)

    def confirm(
        self, intent_id: str, *, expected_revision: int, principal_id: str,
        subject_id: str, command_body: dict[str, Any], idempotency_key: str,
    ) -> None:
        rendered = _canonical(command_body)
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM approval_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if row is None:
                db.rollback(); raise IntentNotFound
            if row["state"] != "pending" or row["revision"] != expected_revision:
                db.rollback(); raise IntentChanged
            if datetime.fromisoformat(row["created_at"]) < datetime.now(UTC) - PENDING_TTL:
                db.execute(
                    "UPDATE approval_intents SET state='expired',revision=revision+1 WHERE intent_id=?",
                    (intent_id,),
                )
                db.commit(); raise IntentChanged
            if subject_id != row["intended_subject_id"]:
                db.rollback(); raise IntentChanged
            db.execute("""UPDATE approval_intents SET
                confirmed_principal_id=?,confirmed_subject_id=?,command_body_json=?,
                command_sha256=?,idempotency_key=?,confirmed_at=?,state='confirmed',
                revision=revision+1 WHERE intent_id=?""", (
                principal_id, subject_id, rendered, _hash(rendered), idempotency_key,
                _now(), intent_id,
            ))
            db.commit()

    def begin_send(self, intent_id: str, subject_id: str) -> dict[str, Any]:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM approval_intents WHERE intent_id=?", (intent_id,)
            ).fetchone()
            if row is None:
                db.rollback(); raise IntentNotFound
            if row["state"] not in {"confirmed", "sending"}:
                db.rollback(); raise IntentChanged
            if row["confirmed_subject_id"] != subject_id:
                db.rollback(); raise IntentChanged
            if _hash(row["command_body_json"]) != row["command_sha256"]:
                db.rollback(); raise IntentChanged
            attempt_id = str(uuid4())
            db.execute("""UPDATE approval_intents SET state='sending',sending_at=?,
                attempt_id=?,attempt_count=attempt_count+1,revision=revision+1
                WHERE intent_id=?""", (_now(), attempt_id, intent_id))
            db.commit()
        result = self.read(intent_id)
        result["attempt_id"] = attempt_id
        return result

    def record_completed(
        self, intent_id: str, attempt_id: str, *, response: dict[str, Any],
        etag: str, replayed: bool,
    ) -> None:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE approval_intents SET state='completed',
                completed_at=?,response_json=?,response_etag=?,replayed=?,
                last_http_status=200,last_error_code=NULL,revision=revision+1
                WHERE intent_id=? AND state='sending' AND attempt_id=?""", (
                _now(), _canonical(response), etag, int(replayed), intent_id, attempt_id,
            )).rowcount
            if changed != 1:
                db.rollback(); raise IntentChanged
            db.commit()

    def record_error(
        self, intent_id: str, attempt_id: str, *, status: int | None,
        code: str, state: Literal["confirmed", "sending", "stopped"],
    ) -> None:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE approval_intents SET state=?,last_http_status=?,
                last_error_code=?,revision=revision+1
                WHERE intent_id=? AND state='sending' AND attempt_id=?""",
                (state, status, code, intent_id, attempt_id),
            ).rowcount
            if changed != 1:
                db.rollback(); raise IntentChanged
            db.commit()

    def recoverable(self) -> list[str]:
        with closing(self.connect()) as db:
            return [row[0] for row in db.execute(
                "SELECT intent_id FROM approval_intents WHERE state IN ('confirmed','sending') "
                "ORDER BY confirmed_at,intent_id"
            )]


class BrowserSessions:
    def __init__(self) -> None:
        self.sessions: dict[str, tuple[str, datetime]] = {}

    def issue(self) -> tuple[str, str, datetime]:
        session_id, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        expires = datetime.now(UTC) + timedelta(minutes=30)
        self.sessions[session_id] = (_hash(csrf), expires)
        return session_id, csrf, expires

    def validate(self, session_id: str | None, csrf: str | None = None) -> str:
        stored = self.sessions.get(session_id or "")
        if stored is None or stored[1] <= datetime.now(UTC):
            raise HTTPException(403, detail={"code": "invalid_intent_session"})
        if csrf is not None and not secrets.compare_digest(stored[0], _hash(csrf)):
            raise HTTPException(403, detail={"code": "invalid_csrf"})
        return session_id or ""


def create_intent_app(
    *, journal: ApprovalIntentJournal, agentbus_url: str, token: str,
    allowed_host: str = "127.0.0.1:5173",
    transport: httpx.BaseTransport | None = None,
) -> FastAPI:
    parsed = urlparse(agentbus_url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("AgentBus intent target must be loopback HTTP.")
    host_name = allowed_host.rsplit(":", 1)[0].strip("[]")
    if host_name not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Approval intent Host must be loopback.")
    sessions = BrowserSessions()
    client = httpx.Client(
        base_url=agentbus_url, headers={"Authorization": f"Bearer {token}"},
        transport=transport,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        journal.expire_and_gc()
        yield
        client.close()

    app = FastAPI(title="AgentBus Approval Intent Service", lifespan=lifespan)

    @app.middleware("http")
    async def boundary(request: Request, call_next):
        if request.headers.get("host") != allowed_host:
            return JSONResponse({"detail": {"code": "invalid_host"}}, status_code=400)
        origin = request.headers.get("origin")
        if origin is not None and origin != f"http://{allowed_host}":
            return JSONResponse({"detail": {"code": "invalid_origin"}}, status_code=403)
        if request.headers.get("sec-fetch-site") not in {None, "same-origin", "none"}:
            return JSONResponse({"detail": {"code": "cross_site_request"}}, status_code=403)
        length = request.headers.get("content-length")
        if length:
            try:
                too_large = int(length) > 16 * 1024
            except ValueError:
                return JSONResponse({"detail": {"code": "invalid_content_length"}}, status_code=400)
            if too_large:
                return JSONResponse({"detail": {"code": "intent_payload_too_large"}}, status_code=413)
        response = await call_next(request)
        response.headers.update({
            "Cache-Control": "no-store",
            "Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'",
            "X-Content-Type-Options": "nosniff",
        })
        return response

    @app.get("/intent-api/session")
    def session() -> JSONResponse:
        session_id, csrf, expires = sessions.issue()
        response = JSONResponse({"csrf_token": csrf, "expires_at": expires.isoformat()})
        response.set_cookie(
            COOKIE_NAME, session_id, httponly=True, samesite="strict",
            path="/intent-api", max_age=1800,
        )
        return response

    @app.post("/intent-api/approval-intents")
    def create_intent(
        command: IntentCreate, request: Request,
        intent_key: str = Header(alias="Intent-Key", min_length=1, max_length=200),
        csrf: str = Header(alias="X-AgentBus-CSRF", min_length=32, max_length=200),
    ) -> JSONResponse:
        if request.headers.get("content-type", "").split(";", 1)[0] != "application/json":
            raise HTTPException(415, detail={"code": "json_required"})
        session_id = sessions.validate(request.cookies.get(COOKIE_NAME), csrf)
        # Idempotency is checked before the upstream read and all capacity limits.
        with closing(journal.connect()) as db:
            existing = db.execute(
                "SELECT * FROM approval_intents WHERE intent_key=?", (intent_key,)
            ).fetchone()
        if existing is not None:
            request_hash = _hash(_canonical(command.model_dump(mode="json")))
            if existing["request_hash"] != request_hash or existing["browser_session_id"] != session_id:
                raise HTTPException(409, detail={"code": "intent_key_reused"})
            return JSONResponse(journal._public(existing), headers={"Intent-Replayed": "true"})
        upstream = client.get(f"/approvals/{command.approval_id}")
        if upstream.status_code != 200:
            return JSONResponse(upstream.json(), status_code=upstream.status_code)
        if upstream.headers.get("etag") != command.if_match:
            raise HTTPException(409, detail={"code": "approval_snapshot_changed"})
        approval = ApprovalRead.model_validate(upstream.json())
        if approval.status is not ApprovalStatus.PENDING or approval.assigned_to is None:
            raise HTTPException(409, detail={"code": "approval_not_actionable"})
        try:
            result, replayed = journal.create(
                session_id=session_id, intent_key=intent_key, command=command,
                approval=approval, etag=command.if_match,
            )
        except IntentConflict:
            raise HTTPException(409, detail={"code": "intent_key_reused"}) from None
        except IntentLimit:
            raise HTTPException(429, detail={"code": "intent_rate_limited"}) from None
        return JSONResponse(result, status_code=200 if replayed else 201,
                            headers={"Intent-Replayed": str(replayed).lower()})

    @app.get("/intent-api/approval-intents/{intent_id}")
    def get_intent(intent_id: UUID, request: Request) -> dict[str, Any]:
        session_id = sessions.validate(request.cookies.get(COOKIE_NAME))
        try:
            return journal.public(str(intent_id), session_id)
        except IntentNotFound:
            raise HTTPException(404, detail={"code": "intent_not_found"}) from None

    return app


def from_environment() -> FastAPI:
    path = Path(os.environ["AGENTBUS_APPROVAL_INTENT_JOURNAL"])
    if not path.is_absolute():
        raise ValueError("AGENTBUS_APPROVAL_INTENT_JOURNAL must be absolute.")
    return create_intent_app(
        journal=ApprovalIntentJournal(path),
        agentbus_url=os.environ.get("AGENTBUS_URL", "http://127.0.0.1:8000"),
        token=read_token_file(os.environ["AGENTBUS_CONTROL_CENTER_TOKEN_FILE"]),
        allowed_host=os.environ.get("AGENTBUS_CONTROL_CENTER_HOST", "127.0.0.1:5173"),
    )


app = from_environment() if os.environ.get("AGENTBUS_APPROVAL_INTENT_JOURNAL") else FastAPI()
