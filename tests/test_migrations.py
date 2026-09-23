import sqlite3

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
    assert versions == [(1,), (2,)]
    assert approvals_table == ("approvals",)
