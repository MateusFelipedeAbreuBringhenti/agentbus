import sqlite3
from uuid import uuid4

import pytest

from agentbus import database as storage
from agentbus.database import Database, MIGRATIONS_DIRECTORY


def test_existing_version_one_database_receives_approval_migration(tmp_path):
    database_path = tmp_path / "existing-agentbus.sqlite3"
    migration_one = (MIGRATIONS_DIRECTORY / "0001_initial.sql").read_text(encoding="utf-8")
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        connection.executescript(migration_one)
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (1, 'existing')"
        )

    Database(database_path).migrate()

    with sqlite3.connect(database_path) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        approvals_table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'approvals'"
        ).fetchone()
    assert versions == [(1,), (2,), (3,)]
    assert approvals_table == ("approvals",)


def _migrate_to_v2(path, monkeypatch):
    migrations = storage.MIGRATIONS
    monkeypatch.setattr(storage, "MIGRATIONS", migrations[:2])
    Database(path).migrate()
    monkeypatch.setattr(storage, "MIGRATIONS", migrations)


def _insert_v2_task(connection, *, status="ready", correlation_id=None):
    task_id = str(uuid4())
    correlation_id = correlation_id or str(uuid4())
    connection.execute(
        """INSERT INTO tasks (
            id, type, title, description, input_json, output_json, status,
            requested_by, assigned_to, failure_code, failure_message, retry_of,
            correlation_id, version, created_at, updated_at, started_at, finished_at
        ) VALUES (?, 'deploy', 'Legacy task', NULL, '{}', NULL, ?, 'orion',
                  NULL, NULL, NULL, NULL, ?, 2, 'created', 'updated', NULL, NULL)""",
        (task_id, status, correlation_id),
    )
    return task_id, correlation_id


def _insert_v2_approval(
    connection,
    task_id,
    correlation_id,
    *,
    gate="deployment.production",
    status="pending",
):
    approval_id = str(uuid4())
    connection.execute(
        """INSERT INTO approvals (
            id, task_id, gate, status, request_reason, context_json,
            requested_by, assigned_to, decided_by, decision_reason,
            correlation_id, version, created_at, updated_at, decided_at
        ) VALUES (?, ?, ?, ?, 'Legacy request', '{}', 'orion',
                  NULL, NULL, NULL, ?, 1, 'created', 'updated', NULL)""",
        (approval_id, task_id, gate, status, correlation_id),
    )
    return approval_id


def test_v2_to_v3_preserves_tasks_and_backfills_unambiguous_wait(tmp_path, monkeypatch):
    path = tmp_path / "v2-unambiguous.sqlite3"
    _migrate_to_v2(path, monkeypatch)
    with sqlite3.connect(path) as connection:
        ready_id, _ = _insert_v2_task(connection)
        waiting_id, correlation_id = _insert_v2_task(
            connection, status="waiting_approval"
        )
        approval_id = _insert_v2_approval(connection, waiting_id, correlation_id)

    Database(path).migrate()

    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """SELECT id, status, waiting_on_approval_id, title, version
            FROM tasks ORDER BY id"""
        ).fetchall()
        by_id = {row[0]: row[1:] for row in rows}
        assert by_id[ready_id] == ("ready", None, "Legacy task", 2)
        assert by_id[waiting_id] == (
            "waiting_approval",
            approval_id,
            "Legacy task",
            2,
        )
        assert connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]


def test_v2_to_v3_uses_only_open_pending_candidate_from_valid_history(
    tmp_path, monkeypatch
):
    path = tmp_path / "v2-history.sqlite3"
    _migrate_to_v2(path, monkeypatch)
    with sqlite3.connect(path) as connection:
        task_id, correlation_id = _insert_v2_task(
            connection, status="waiting_approval"
        )
        _insert_v2_approval(
            connection,
            task_id,
            correlation_id,
            gate="old.gate",
            status="approved",
        )
        current_id = _insert_v2_approval(
            connection,
            task_id,
            correlation_id,
            gate="current.gate",
        )

    Database(path).migrate()

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT waiting_on_approval_id FROM tasks WHERE id=?", (task_id,)
        ).fetchone() == (current_id,)


@pytest.mark.parametrize(
    ("approval_count", "approval_status"),
    [(0, "pending"), (2, "approved"), (2, "pending")],
)
def test_v2_to_v3_ambiguous_wait_rolls_back_entire_migration(
    tmp_path, monkeypatch, approval_count, approval_status
):
    path = tmp_path / f"v2-ambiguous-{approval_count}-{approval_status}.sqlite3"
    _migrate_to_v2(path, monkeypatch)
    with sqlite3.connect(path) as connection:
        task_id, correlation_id = _insert_v2_task(
            connection, status="waiting_approval"
        )
        for index in range(approval_count):
            _insert_v2_approval(
                connection,
                task_id,
                correlation_id,
                gate=f"gate.{index}",
                status=approval_status,
            )

    with pytest.raises(sqlite3.IntegrityError):
        Database(path).migrate()

    with sqlite3.connect(path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(tasks)")
        }
        assert "waiting_on_approval_id" not in columns
        assert connection.execute(
            "SELECT status, version FROM tasks WHERE id = ?", (task_id,)
        ).fetchone() == ("waiting_approval", 2)
        assert connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,)]
