from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
from threading import Barrier
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus import database as storage
from agentbus.app import create_app
from agentbus.database import Database
from agentbus.models import TaskCreate, TaskClaim, TaskComplete, TaskFail
from agentbus.service import create_task, claim_task, complete_task, fail_task


@pytest.mark.parametrize('terminal', ['complete', 'fail'])
def test_v1_replays_survive_v2_upgrade(tmp_path, monkeypatch, terminal):
    path = tmp_path / 'v1.sqlite3'
    db = Database(path)
    migrations = storage.MIGRATIONS
    monkeypatch.setattr(storage, 'MIGRATIONS', migrations[:1])
    db.migrate()
    task = create_task(db, TaskCreate(type='work', title='Legacy', requested_by='orion',
                                    correlation_id=uuid4()), 'create').task
    claim_body = TaskClaim(agent_id='dex')
    claimed = claim_task(db, task.id, claim_body, 'claim').task
    body = TaskComplete(output={'done': True}) if terminal == 'complete' else TaskFail(failure_code='error', failure_message='Failed')
    done = (complete_task if terminal == 'complete' else fail_task)(db, task.id, body, 'terminal', 2).task
    # Encode the actual Slice #002 persisted wire format, independent of current helper.
    with sqlite3.connect(path) as connection:
        for key, command, version in [('claim', claim_body, None), ('terminal', body, 2)]:
            payload = json.dumps({'body': command.model_dump(mode='json'), 'expected_version': version,
                                  'task_id': str(task.id)}, ensure_ascii=False, separators=(',', ':'), sort_keys=True)
            connection.execute('UPDATE idempotency_records SET request_hash=? WHERE key=?',
                               (hashlib.sha256(payload.encode()).hexdigest(), key))
    monkeypatch.setattr(storage, 'MIGRATIONS', migrations)
    with TestClient(create_app(path)) as api:
        for action, command, key, version, expected in [
            ('claim', claim_body, 'claim', None, claimed), (terminal, body, 'terminal', 2, done),
        ]:
            headers = {'Idempotency-Key': key}
            if version:
                headers['If-Match'] = f'"v{version}"'
            response = api.post(f'/tasks/{task.id}/{action}', json=command.model_dump(mode='json'), headers=headers)
            assert response.status_code == 200
            assert response.json() == expected.model_dump(mode='json')
            assert response.headers['Idempotency-Replayed'] == 'true'
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('SELECT count(*) FROM events').fetchone()[0] == 3
        assert connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall() == [(1,), (2,), (3,)]


def test_two_migrators_recheck_version_after_acquiring_write_lock(tmp_path, monkeypatch):
    path = tmp_path / 'race.sqlite3'
    migrations = storage.MIGRATIONS
    monkeypatch.setattr(storage, 'MIGRATIONS', migrations[:1])
    Database(path).migrate()
    monkeypatch.setattr(storage, 'MIGRATIONS', migrations)
    barrier = Barrier(2)
    original_read = Path.read_text

    def simultaneous_read(file, *args, **kwargs):
        result = original_read(file, *args, **kwargs)
        if file.name == '0002_approvals.sql':
            barrier.wait(timeout=5)
        return result

    monkeypatch.setattr(Path, 'read_text', simultaneous_read)
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(Database(path).migrate) for _ in range(2)]
        for future in futures:
            future.result(timeout=10)
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall() == [(1,), (2,), (3,)]


def test_failed_v2_migration_rolls_back_schema_and_can_retry(tmp_path, monkeypatch):
    path = tmp_path / 'failure.sqlite3'
    migrations = storage.MIGRATIONS
    monkeypatch.setattr(storage, 'MIGRATIONS', migrations[:1])
    Database(path).migrate()
    monkeypatch.setattr(storage, 'MIGRATIONS', migrations)
    original_read = Path.read_text

    def broken_migration(file, *args, **kwargs):
        script = original_read(file, *args, **kwargs)
        return script + '\nINSERT INTO missing_table VALUES (1);' if file.name == '0002_approvals.sql' else script

    monkeypatch.setattr(Path, 'read_text', broken_migration)
    with pytest.raises(sqlite3.OperationalError):
        Database(path).migrate()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='approvals'").fetchall() == []
        assert connection.execute('SELECT version FROM schema_migrations').fetchall() == [(1,)]
    monkeypatch.setattr(Path, 'read_text', original_read)
    Database(path).migrate()
    Database(path).migrate()
