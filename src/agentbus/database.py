from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import sqlite3


MIGRATIONS_DIRECTORY = Path(__file__).resolve().parents[2] / "migrations"
MIGRATIONS = (
    (1, MIGRATIONS_DIRECTORY / "0001_initial.sql"),
    (2, MIGRATIONS_DIRECTORY / "0002_approvals.sql"),
)


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def migrate(self) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            for version, migration_path in MIGRATIONS:
                applied = connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?",
                    (version,),
                ).fetchone()
                if applied is not None:
                    continue

                migration = migration_path.read_text(encoding="utf-8")
                connection.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + migration
                    + f"""
                    INSERT INTO schema_migrations(version, applied_at)
                    VALUES ({version}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
                    COMMIT;
                    """
                )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
