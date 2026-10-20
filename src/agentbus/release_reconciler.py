from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any
from urllib.parse import urlparse
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

import httpx
from pydantic import TypeAdapter, ValidationError

from agentbus.auth import read_token_file
from agentbus.models import ApprovalRead, ApprovalStatus, AuthIdentityRead, EventRead, TaskRead, TaskStatus


class ReconciliationError(RuntimeError):
    pass


class ReconciliationStopped(ReconciliationError):
    pass


class AmbiguousRelease(ReconciliationError):
    pass


class ReleaseBusy(ReconciliationError):
    pass


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class ReleasePolicy:
    allowed_gates: frozenset[str]

    @classmethod
    def from_file(cls, path: str | Path) -> ReleasePolicy:
        policy_path = Path(path)
        if not policy_path.is_absolute():
            raise ValueError("Release policy path must be absolute.")
        if policy_path.stat().st_mode & 0o077:
            raise PermissionError("Release policy must have mode 0600.")
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
        if set(payload) != {"allowed_gates"} or not isinstance(payload["allowed_gates"], list):
            raise ValueError("Release policy must contain only allowed_gates.")
        gates = payload["allowed_gates"]
        if not gates or not all(isinstance(gate, str) and 0 < len(gate) <= 200 for gate in gates):
            raise ValueError("Release policy gates must be non-empty strings.")
        if len(set(gates)) != len(gates):
            raise ValueError("Release policy gates must be unique.")
        return cls(frozenset(gates))


class ReleaseJournal:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("Release journal path must be absolute.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            if self.path.stat().st_mode & 0o077:
                raise PermissionError("Release journal must have mode 0600.") from None
        else:
            os.close(descriptor)
        with self.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS release_commands (
                approval_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                gate TEXT NOT NULL,
                expected_subject_id TEXT NOT NULL,
                approval_snapshot_json TEXT NOT NULL,
                task_snapshot_json TEXT,
                causation_event_id TEXT,
                task_etag TEXT,
                command_body_json TEXT,
                command_sha256 TEXT,
                idempotency_key TEXT UNIQUE,
                state TEXT NOT NULL CHECK(state IN (
                    'observed','prepared','sending','auth_required','completed','stopped'
                )),
                revision INTEGER NOT NULL DEFAULT 1,
                attempt_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                prepared_at TEXT,
                completed_at TEXT,
                last_http_status INTEGER,
                last_error_code TEXT,
                response_json TEXT,
                response_etag TEXT,
                replayed INTEGER
            );
            CREATE INDEX IF NOT EXISTS release_commands_state
                ON release_commands(state, created_at);
            """)

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def exclusive_send(self):
        """Own the network send and its journal result across local processes."""
        lock_path = self.path.with_name(f"{self.path.name}.send.lock")
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ReleaseBusy("Another release send is already in progress.") from None
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def read(self, approval_id: UUID | str) -> dict[str, Any]:
        with closing(self.connect()) as db:
            row = db.execute(
                "SELECT * FROM release_commands WHERE approval_id=?", (str(approval_id),)
            ).fetchone()
        if row is None:
            raise ReconciliationError("Release command does not exist.")
        return dict(row)

    def observe(self, approval: ApprovalRead, subject_id: str) -> dict[str, Any]:
        snapshot = approval.model_dump_json()
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                "SELECT * FROM release_commands WHERE approval_id=?", (str(approval.id),)
            ).fetchone()
            if existing is None:
                db.execute("""INSERT INTO release_commands(
                    approval_id,task_id,correlation_id,gate,expected_subject_id,
                    approval_snapshot_json,state,created_at
                ) VALUES(?,?,?,?,?,?,'observed',?)""", (
                    str(approval.id), str(approval.task_id), str(approval.correlation_id),
                    approval.gate, subject_id, snapshot, _now(),
                ))
            elif (
                existing["task_id"] != str(approval.task_id)
                or existing["correlation_id"] != str(approval.correlation_id)
                or existing["gate"] != approval.gate
                or existing["expected_subject_id"] != subject_id
            ):
                db.rollback()
                raise ReconciliationStopped("Observed Approval identity changed.")
            db.commit()
        return self.read(approval.id)

    def prepare(
        self, approval_id: UUID, *, expected_revision: int, task: TaskRead,
        causation_event_id: UUID, body: dict[str, Any], idempotency_key: str,
    ) -> dict[str, Any]:
        rendered = _canonical(body)
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE release_commands SET
                task_snapshot_json=?,causation_event_id=?,task_etag=?,command_body_json=?,
                command_sha256=?,idempotency_key=?,state='prepared',prepared_at=?,revision=revision+1
                WHERE approval_id=? AND state='observed' AND revision=?""", (
                task.model_dump_json(), str(causation_event_id), f'"v{task.version}"', rendered,
                _digest(rendered), idempotency_key, _now(), str(approval_id), expected_revision,
            )).rowcount
            if changed != 1:
                db.rollback()
                raise ReconciliationStopped("Release observation changed before preparation.")
            db.commit()
        return self.read(approval_id)

    def stop(self, approval_id: UUID | str, code: str, status: int | None = None) -> None:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""UPDATE release_commands SET state='stopped',last_error_code=?,
                last_http_status=?,revision=revision+1
                WHERE approval_id=? AND state!='completed'""", (code, status, str(approval_id)))
            db.commit()

    def begin_send(self, approval_id: UUID | str, *, expected_revision: int) -> dict[str, Any]:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM release_commands WHERE approval_id=?",
                             (str(approval_id),)).fetchone()
            if row is None or row["revision"] != expected_revision or row["state"] not in {
                "prepared", "sending", "auth_required",
            }:
                db.rollback()
                raise ReconciliationStopped("Release changed concurrently.")
            if not row["command_body_json"] or _digest(row["command_body_json"]) != row["command_sha256"]:
                db.rollback()
                raise ReconciliationStopped("Persisted release command failed integrity validation.")
            attempt_id = str(uuid4())
            changed = db.execute("""UPDATE release_commands SET state='sending',attempt_id=?,
                attempt_count=attempt_count+1,revision=revision+1
                WHERE approval_id=? AND revision=?""",
                (attempt_id, str(approval_id), expected_revision),
            ).rowcount
            if changed != 1:
                db.rollback()
                raise ReconciliationStopped("Release changed concurrently.")
            db.commit()
        result = self.read(approval_id)
        result["attempt_id"] = attempt_id
        return result

    def record_failure(
        self, approval_id: UUID | str, attempt_id: str, *, state: str,
        code: str, status: int | None,
    ) -> None:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE release_commands SET state=?,last_error_code=?,
                last_http_status=?,revision=revision+1
                WHERE approval_id=? AND state='sending' AND attempt_id=?""",
                (state, code, status, str(approval_id), attempt_id),
            ).rowcount
            if changed != 1:
                db.rollback()
                raise ReconciliationStopped("Release attempt changed concurrently.")
            db.commit()

    def complete(
        self, approval_id: UUID | str, attempt_id: str, *, task: TaskRead,
        etag: str, replayed: bool,
    ) -> None:
        with closing(self.connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE release_commands SET state='completed',completed_at=?,
                response_json=?,response_etag=?,replayed=?,last_http_status=200,
                last_error_code=NULL,revision=revision+1
                WHERE approval_id=? AND state='sending' AND attempt_id=?""", (
                    _now(), task.model_dump_json(), etag, int(replayed),
                    str(approval_id), attempt_id,
                )).rowcount
            if changed != 1:
                db.rollback()
                raise ReconciliationStopped("Release completion changed concurrently.")
            db.commit()

    def recoverable(self) -> list[str]:
        with closing(self.connect()) as db:
            return [row[0] for row in db.execute(
                """SELECT approval_id FROM release_commands
                WHERE state IN ('prepared','sending','auth_required') ORDER BY created_at,approval_id"""
            )]


class AgentBusReleaseClient:
    def __init__(
        self, base_url: str, token: str, *, transport: httpx.BaseTransport | None = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("AgentBus release target must be loopback HTTP.")
        self.client = httpx.Client(
            base_url=base_url, headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()

    def identity(self) -> tuple[int, AuthIdentityRead | None]:
        response = self.client.get("/auth/me")
        if response.status_code != 200:
            return response.status_code, None
        try:
            return 200, AuthIdentityRead.model_validate(response.json())
        except (ValueError, ValidationError):
            raise AmbiguousRelease("AgentBus returned an invalid identity response.") from None

    def approval(self, approval_id: UUID) -> ApprovalRead:
        response = self.client.get(f"/approvals/{approval_id}")
        if response.status_code != 200:
            raise ReconciliationStopped(f"Approval lookup failed with HTTP {response.status_code}.")
        try:
            approval = ApprovalRead.model_validate(response.json())
        except (ValueError, ValidationError):
            raise ReconciliationStopped("AgentBus returned an invalid Approval.") from None
        if approval.id != approval_id or response.headers.get("etag") != f'"v{approval.version}"':
            raise ReconciliationStopped("Approval response identity or ETag is invalid.")
        return approval

    def task(self, task_id: UUID) -> TaskRead:
        response = self.client.get(f"/tasks/{task_id}")
        if response.status_code != 200:
            raise ReconciliationStopped(f"Task lookup failed with HTTP {response.status_code}.")
        try:
            task = TaskRead.model_validate(response.json())
        except (ValueError, ValidationError):
            raise ReconciliationStopped("AgentBus returned an invalid Task.") from None
        if task.id != task_id or response.headers.get("etag") != f'"v{task.version}"':
            raise ReconciliationStopped("Task response identity or ETag is invalid.")
        return task

    def events(self, task_id: UUID) -> list[EventRead]:
        response = self.client.get(f"/tasks/{task_id}/events")
        if response.status_code != 200:
            raise ReconciliationStopped(f"Event lookup failed with HTTP {response.status_code}.")
        try:
            events = TypeAdapter(list[EventRead]).validate_python(response.json())
        except (ValueError, ValidationError):
            raise ReconciliationStopped("AgentBus returned invalid Events.") from None
        if any(event.task_id != task_id for event in events):
            raise ReconciliationStopped("AgentBus returned Events for another Task.")
        return events

    def release(self, row: dict[str, Any]) -> httpx.Response:
        return self.client.post(
            f"/tasks/{row['task_id']}/release",
            content=row["command_body_json"],
            headers={
                "Content-Type": "application/json",
                "If-Match": row["task_etag"],
                "Idempotency-Key": row["idempotency_key"],
            },
        )


class ReleaseReconciler:
    def __init__(
        self, *, journal: ReleaseJournal, client: AgentBusReleaseClient,
        policy: ReleasePolicy, expected_subject_id: str,
    ) -> None:
        self.journal = journal
        self.client = client
        self.policy = policy
        self.expected_subject_id = expected_subject_id

    def _identity(self) -> AuthIdentityRead:
        status, identity = self.client.identity()
        if status == 401:
            raise ReconciliationError("Authentication is required.")
        if status != 200 or identity is None:
            raise ReconciliationStopped(f"Identity lookup failed with HTTP {status}.")
        if identity.subject_id != self.expected_subject_id:
            raise ReconciliationStopped("Credential belongs to another coordinator subject.")
        if "task:release" not in identity.permissions:
            raise ReconciliationStopped("Coordinator credential lacks task:release.")
        return identity

    @staticmethod
    def _matching_events(
        approval: ApprovalRead, events: list[EventRead], event_type: str,
    ) -> list[EventRead]:
        return [event for event in events if (
            event.type == event_type
            and event.task_id == approval.task_id
            and event.correlation_id == approval.correlation_id
            and event.authenticated_principal_id is not None
            and event.data.get("approval_id") == str(approval.id)
            and event.data.get("task_id") == str(approval.task_id)
        )]

    def reconcile(self, approval_id: UUID | str) -> dict[str, Any]:
        approval_id = UUID(str(approval_id))
        identity = self._identity()
        approval = self.client.approval(approval_id)
        observed = self.journal.observe(approval, identity.subject_id)
        if observed["state"] == "completed":
            return observed
        if observed["state"] in {"prepared", "sending", "auth_required"}:
            return self._send(approval_id, identity)
        if observed["state"] == "stopped":
            raise ReconciliationStopped(observed["last_error_code"] or "Release stopped.")
        if approval.requested_by != identity.subject_id:
            self.journal.stop(approval_id, "requester_subject_mismatch")
            raise ReconciliationStopped("Approval belongs to another requester subject.")
        if approval.status is not ApprovalStatus.APPROVED:
            self.journal.stop(approval_id, f"approval_{approval.status.value}")
            raise ReconciliationStopped("Approval is not approved.")
        if approval.gate not in self.policy.allowed_gates:
            self.journal.stop(approval_id, "gate_not_allowed")
            raise ReconciliationStopped("Approval gate is not allowlisted.")
        task = self.client.task(approval.task_id)
        events = self.client.events(approval.task_id)
        requested = self._matching_events(approval, events, "approval.requested")
        approved = self._matching_events(approval, events, "approval.approved")
        if (
            len(requested) != 1
            or requested[0].actor_id != approval.requested_by
            or len(approved) != 1
            or approved[0].data.get("status") != ApprovalStatus.APPROVED.value
        ):
            self.journal.stop(approval_id, "invalid_authenticated_provenance")
            raise ReconciliationStopped("Approval provenance or decision Event is invalid.")
        if (
            task.status is not TaskStatus.WAITING_APPROVAL
            or task.waiting_on_approval_id != approval.id
            or task.correlation_id != approval.correlation_id
        ):
            self.journal.stop(approval_id, "task_not_waiting_on_approval")
            raise ReconciliationStopped("Task is not waiting on this Approval.")
        body = {
            "approval_id": str(approval.id),
            "actor_id": identity.subject_id,
            "causation_event_id": str(approved[0].id),
        }
        key = str(uuid5(
            NAMESPACE_URL, f"agentbus:release:{identity.subject_id}:{approval.id}",
        ))
        prepared = self.journal.prepare(
            approval.id, expected_revision=observed["revision"], task=task,
            causation_event_id=approved[0].id, body=body, idempotency_key=key,
        )
        return self._send(prepared["approval_id"], identity)

    def _send(self, approval_id: UUID | str, identity: AuthIdentityRead) -> dict[str, Any]:
        with self.journal.exclusive_send():
            return self._send_exclusive(approval_id, identity)

    def _send_exclusive(
        self, approval_id: UUID | str, identity: AuthIdentityRead,
    ) -> dict[str, Any]:
        current = self.journal.read(approval_id)
        if current["state"] == "completed":
            return current
        if current["expected_subject_id"] != identity.subject_id:
            self.journal.stop(approval_id, "credential_subject_changed")
            raise ReconciliationStopped("Credential subject changed during recovery.")
        attempt = self.journal.begin_send(
            approval_id, expected_revision=current["revision"],
        )
        try:
            response = self.client.release(attempt)
        except httpx.TransportError:
            self.journal.record_failure(
                approval_id, attempt["attempt_id"], state="sending",
                code="transport_error", status=None,
            )
            raise AmbiguousRelease("Release response is ambiguous.") from None
        if response.status_code == 401:
            self.journal.record_failure(
                approval_id, attempt["attempt_id"], state="auth_required",
                code="authentication_required", status=401,
            )
            raise ReconciliationError("Release suspended until reauthentication.")
        if response.status_code == 200:
            try:
                task = TaskRead.model_validate(response.json())
                original = TaskRead.model_validate_json(attempt["task_snapshot_json"])
                expected_etag = f'"v{task.version}"'
                if not (
                    task.id == original.id
                    and task.correlation_id == original.correlation_id
                    and task.status is TaskStatus.READY
                    and task.waiting_on_approval_id is None
                    and task.version == original.version + 1
                    and response.headers.get("etag") == expected_etag
                ):
                    raise ValueError
            except (ValueError, ValidationError):
                self.journal.record_failure(
                    approval_id, attempt["attempt_id"], state="sending",
                    code="invalid_success_response", status=200,
                )
                raise AmbiguousRelease("Release response is invalid and ambiguous.") from None
            self.journal.complete(
                approval_id, attempt["attempt_id"], task=task, etag=expected_etag,
                replayed=response.headers.get("Idempotency-Replayed") == "true",
            )
            return self.journal.read(approval_id)
        try:
            detail = response.json().get("detail", {})
            code = detail.get("code", f"http_{response.status_code}")
        except ValueError:
            code = f"http_{response.status_code}"
        recoverable = response.status_code >= 500
        self.journal.record_failure(
            approval_id, attempt["attempt_id"],
            state="sending" if recoverable else "stopped",
            code=code, status=response.status_code,
        )
        if recoverable:
            raise AmbiguousRelease(f"AgentBus returned recoverable HTTP {response.status_code}.")
        raise ReconciliationStopped(f"AgentBus rejected release: {code}.")

    def recover(self) -> list[dict[str, Any]]:
        status, identity = self.client.identity()
        if status == 401:
            return []
        if status != 200 or identity is None:
            raise ReconciliationStopped(f"Identity lookup failed with HTTP {status}.")
        if identity.subject_id != self.expected_subject_id or "task:release" not in identity.permissions:
            for approval_id in self.journal.recoverable():
                self.journal.stop(approval_id, "credential_identity_or_permission_changed")
            raise ReconciliationStopped("Current credential cannot recover releases.")
        return [self._send(approval_id, identity) for approval_id in self.journal.recoverable()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile approved AgentBus releases")
    parser.add_argument("command", choices=["reconcile", "recover"])
    parser.add_argument("approval_id", nargs="?")
    parser.add_argument("--journal", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--agentbus-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    paths = [Path(args.journal), Path(args.policy), Path(args.token_file)]
    if not all(path.is_absolute() for path in paths):
        parser.error("journal, policy and token paths must be absolute")
    client = AgentBusReleaseClient(args.agentbus_url, read_token_file(paths[2]))
    reconciler = ReleaseReconciler(
        journal=ReleaseJournal(paths[0]), client=client,
        policy=ReleasePolicy.from_file(paths[1]), expected_subject_id=args.subject,
    )
    try:
        if args.command == "reconcile":
            if not args.approval_id:
                parser.error("reconcile requires approval_id")
            reconciler.reconcile(UUID(args.approval_id))
        else:
            reconciler.recover()
    except ReconciliationError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        client.close()


if __name__ == "__main__":
    main()
