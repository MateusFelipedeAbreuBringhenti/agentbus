from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from uuid import UUID, uuid4


class CoordinationDecisionConflict(Exception):
    """A coordination id was reused for a different successor decision."""


@dataclass(frozen=True)
class SuccessorCommand:
    coordination_id: UUID
    predecessor_task_id: UUID
    request_json: str
    request_hash: str
    idempotency_key: str
    successor_task_id: UUID | None
    confirmed_at: str | None


class CoordinatorOutbox:
    """Durable facts needed to replay one causal successor command."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS successor_commands (
                    coordination_id TEXT PRIMARY KEY,
                    predecessor_task_id TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    successor_task_id TEXT,
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    confirmed_at TEXT
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _record(row: sqlite3.Row) -> SuccessorCommand:
        return SuccessorCommand(
            coordination_id=UUID(row["coordination_id"]),
            predecessor_task_id=UUID(row["predecessor_task_id"]),
            request_json=row["request_json"],
            request_hash=row["request_hash"],
            idempotency_key=row["idempotency_key"],
            successor_task_id=(
                UUID(row["successor_task_id"]) if row["successor_task_id"] else None
            ),
            confirmed_at=row["confirmed_at"],
        )

    def prepare(
        self,
        coordination_id: UUID,
        predecessor_task_id: UUID,
        request: dict,
    ) -> SuccessorCommand:
        request_json = json.dumps(
            request, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        request_hash = hashlib.sha256(request_json.encode()).hexdigest()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM successor_commands WHERE coordination_id = ?",
                (str(coordination_id),),
            ).fetchone()
            if row is not None:
                if (
                    row["predecessor_task_id"] != str(predecessor_task_id)
                    or row["request_hash"] != request_hash
                    or row["request_json"] != request_json
                ):
                    connection.rollback()
                    raise CoordinationDecisionConflict
                connection.commit()
                return self._record(row)
            connection.execute(
                """
                INSERT INTO successor_commands(
                    coordination_id, predecessor_task_id, request_json,
                    request_hash, idempotency_key, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    str(coordination_id), str(predecessor_task_id), request_json,
                    request_hash, str(uuid4()), datetime.now(UTC).isoformat(),
                ),
            )
            row = connection.execute(
                "SELECT * FROM successor_commands WHERE coordination_id = ?",
                (str(coordination_id),),
            ).fetchone()
            connection.commit()
            return self._record(row)

    def confirm(self, coordination_id: UUID, successor_task_id: UUID, response: dict) -> None:
        response_json = json.dumps(
            response, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT successor_task_id FROM successor_commands WHERE coordination_id = ?",
                (str(coordination_id),),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise RuntimeError("Cannot confirm an unprepared successor command.")
            if row["successor_task_id"] not in (None, str(successor_task_id)):
                connection.rollback()
                raise CoordinationDecisionConflict
            connection.execute(
                """UPDATE successor_commands
                SET successor_task_id = ?, response_json = ?,
                    confirmed_at = COALESCE(confirmed_at, ?)
                WHERE coordination_id = ?""",
                (
                    str(successor_task_id), response_json,
                    datetime.now(UTC).isoformat(), str(coordination_id),
                ),
            )
            connection.commit()
