from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from agentbus.app import create_app
from test_approval_flow import (
    client, database_path, create_task, request_approval, decide, release,
)


def snapshot(path):
    with closing(sqlite3.connect(path)) as db:
        return {table: db.execute(f'SELECT * FROM {table} ORDER BY id' if table !=
                'idempotency_records' else 'SELECT * FROM idempotency_records ORDER BY scope, key').fetchall()
                for table in ('tasks', 'approvals', 'events', 'idempotency_records')}


def test_partial_index_failure_rolls_back_preceding_task_update(client, database_path):
    task = create_task(client)
    approval = request_approval(client, task['id']).json()['approval']
    # Deliberately seed the otherwise unreachable ready + pending storage boundary.
    with sqlite3.connect(database_path) as db:
        db.execute("UPDATE tasks SET status='ready', version=1 WHERE id=?", (task['id'],))
    before = snapshot(database_path)
    response = request_approval(client, task['id'], key='index-rejection')
    assert response.status_code == 409
    assert response.json()['detail']['code'] == 'pending_approval_conflict'
    assert snapshot(database_path) == before
    assert before['approvals'][0][0] == approval['id']


@pytest.mark.parametrize('operation,table,event_type', [
    ('request', 'tasks', None), ('request', 'approvals', None),
    ('request', 'events', 'task.approval_requested'),
    ('request', 'events', 'approval.requested'),
    ('request', 'idempotency_records', None),
    ('approve', 'approvals', None), ('approve', 'events', 'approval.approved'),
    ('approve', 'idempotency_records', None),
    ('reject', 'approvals', None), ('reject', 'events', 'approval.rejected'),
    ('reject', 'idempotency_records', None),
    ('release', 'tasks', None), ('release', 'events', 'task.released'),
    ('release', 'idempotency_records', None),
])
def test_failure_at_each_write_restores_entire_database(client, database_path, operation, table, event_type):
    task = create_task(client)
    approval = None
    if operation != 'request':
        approval = request_approval(client, task['id']).json()['approval']
    if operation == 'release':
        assert decide(client, approval['id'], 'approve', key='setup').status_code == 200
    before = snapshot(database_path)
    verb = 'UPDATE' if table == 'tasks' or (table == 'approvals' and operation != 'request') else 'INSERT'
    condition = f"WHEN NEW.type = '{event_type}'" if event_type else ''
    with sqlite3.connect(database_path) as db:
        db.execute(f"CREATE TRIGGER injected_failure BEFORE {verb} ON {table} {condition} "
                   "BEGIN SELECT RAISE(ABORT, 'injected failure'); END")

    def invoke():
        if operation == 'request':
            return request_approval(client, task['id'], key='retryable')
        if operation == 'release':
            return release(client, task['id'], approval['id'], key='retryable')
        return decide(client, approval['id'], operation, key='retryable')

    assert invoke().status_code == 500
    assert snapshot(database_path) == before
    with sqlite3.connect(database_path) as db:
        db.execute('DROP TRIGGER injected_failure')
    assert invoke().status_code == 200  # Failed command did not consume the key.


@pytest.mark.parametrize('race', ['request', 'same-request', 'decision', 'release'])
def test_independent_apps_serialize_real_sqlite_writers(client, database_path, race):
    task = create_task(client)
    approval = None
    if race in ('decision', 'release'):
        approval = request_approval(client, task['id']).json()['approval']
    if race == 'release':
        decide(client, approval['id'], 'approve', key='setup')
    barrier = Barrier(2)
    with TestClient(create_app(database_path)) as other:
        def compete(index):
            api = [client, other][index]
            barrier.wait(timeout=5)
            if race in ('request', 'same-request'):
                return request_approval(api, task['id'], key='same' if race == 'same-request' else f'race-{index}')
            if race == 'decision':
                return decide(api, approval['id'], ['approve', 'reject'][index], key=f'race-{index}')
            return release(api, task['id'], approval['id'], key=f'race-{index}')
        with ThreadPoolExecutor(2) as pool:
            responses = list(pool.map(compete, [0, 1]))
    assert sorted(r.status_code for r in responses) == ([200, 200] if race == 'same-request' else [200, 409])
    state = snapshot(database_path)
    assert len(state['approvals']) == 1
    expected = {'request': 3, 'same-request': 3, 'decision': 4, 'release': 5}[race]
    assert len(state['events']) == expected
    with closing(sqlite3.connect(database_path)) as db:
        assert db.execute('SELECT sequence FROM events ORDER BY sequence').fetchall() == [(n,) for n in range(1, expected + 1)]
        assert db.execute('SELECT version FROM approvals').fetchone()[0] == (1 if 'request' in race else 2)
        assert db.execute('SELECT version FROM tasks').fetchone()[0] == (3 if race == 'release' else 2)


def test_historical_replays_after_release_and_execution_are_read_only(client, database_path):
    task = create_task(client)
    requested = request_approval(client, task['id'])
    approval = requested.json()['approval']
    approved = decide(client, approval['id'], 'approve', key='approve')
    released = release(client, task['id'], approval['id'])
    assert client.post(f"/tasks/{task['id']}/claim", json={'agent_id': 'dex'},
                       headers={'Idempotency-Key': 'claim'}).status_code == 200
    before = snapshot(database_path)
    for original, replay in [
        (requested, request_approval(client, task['id'])),
        (approved, decide(client, approval['id'], 'approve', key='approve')),
        (released, release(client, task['id'], approval['id'])),
    ]:
        assert original.status_code == replay.status_code == 200
        assert replay.content == original.content
        for header in ('etag', 'task-etag', 'approval-etag'):
            assert replay.headers.get(header) == original.headers.get(header)
    assert snapshot(database_path) == before


@pytest.mark.parametrize('operation', ['approve', 'reject', 'release'])
@pytest.mark.parametrize('cause_kind', ['missing', 'foreign', 'same-correlation'])
def test_causal_validation_for_every_decision_and_reaction(client, database_path, operation, cause_kind):
    task = create_task(client)
    approval = request_approval(client, task['id']).json()['approval']
    if operation == 'release':
        decide(client, approval['id'], 'approve', key='setup')
    cause = str(uuid4())
    if cause_kind != 'missing':
        other = create_task(client, correlation_id=task['correlation_id'] if cause_kind == 'same-correlation' else None)
        cause = client.get(f"/tasks/{other['id']}/events").json()[0]['id']
    before = snapshot(database_path)
    response = (release(client, task['id'], approval['id'], cause=cause) if operation == 'release'
                else decide(client, approval['id'], operation, key='causal', cause=cause))
    if cause_kind == 'same-correlation':
        assert response.status_code == 200
        with closing(sqlite3.connect(database_path)) as db:
            assert db.execute('SELECT causation_event_id FROM events WHERE task_id=? ORDER BY sequence DESC',
                              (task['id'],)).fetchone()[0] == cause
    else:
        assert response.status_code == (422 if cause_kind == 'missing' else 409)
        assert snapshot(database_path) == before


def test_sqlite_command_event_index_and_pending_index_boundaries(client, database_path):
    task = create_task(client)
    approval = request_approval(client, task['id']).json()['approval']
    with sqlite3.connect(database_path) as db:
        # Different sequence/id cannot bypass command/index uniqueness.
        with pytest.raises(sqlite3.IntegrityError, match='events.command_id, events.event_index'):
            db.execute("INSERT INTO events SELECT ?, task_id, 99, type, data_json, actor_id, "
                       "correlation_id, causation_event_id, command_id, event_index, occurred_at "
                       "FROM events WHERE type='approval.requested'", (str(uuid4()),))
        # Same gate is allowed for terminal history and a different pending gate is allowed.
        for gate, state in [('deployment.production', 'rejected'), ('another.gate', 'pending')]:
            db.execute("INSERT INTO approvals SELECT ?, task_id, ?, ?, request_reason, context_json, "
                       "requested_by, assigned_to, decided_by, decision_reason, correlation_id, version, "
                       "created_at, updated_at, decided_at FROM approvals WHERE id=?",
                       (str(uuid4()), gate, state, approval['id']))


def test_decision_scope_belongs_to_decider_not_requester(client, database_path):
    first = create_task(client)
    second = create_task(client)
    a = request_approval(client, first['id'], key='a').json()['approval']
    b = request_approval(client, second['id'], key='b').json()['approval']
    assert decide(client, a['id'], 'approve', key='shared', decided_by='alice').status_code == 200
    assert decide(client, b['id'], 'approve', key='shared', decided_by='bob').status_code == 200
    before = snapshot(database_path)
    # Same decider/key on another resource is a collision; target is in the hash.
    response = decide(client, b['id'], 'approve', key='shared', decided_by='alice')
    assert response.status_code == 409
    assert response.json()['detail']['code'] == 'idempotency_key_reused'
    assert snapshot(database_path) == before


def test_wrong_task_same_correlation_and_rejected_approval_cannot_release(client, database_path):
    task = create_task(client)
    another = create_task(client, correlation_id=task['correlation_id'])
    a = request_approval(client, task['id'], key='a').json()['approval']
    b = request_approval(client, another['id'], key='b').json()['approval']
    decide(client, a['id'], 'reject', key='reject')
    decide(client, b['id'], 'approve', key='approve')
    before = snapshot(database_path)
    for approval_id, code in [(a['id'], 'approval_state_conflict'), (b['id'], 'approval_task_mismatch')]:
        response = release(client, task['id'], approval_id, key=code)
        assert response.status_code == 409
        assert response.json()['detail']['code'] == code
    assert snapshot(database_path) == before


@pytest.mark.parametrize('operation', ['request', 'approve', 'reject', 'release'])
def test_commit_failure_restores_every_record(client, database_path, monkeypatch, operation):
    task = create_task(client)
    approval = None
    if operation != 'request':
        approval = request_approval(client, task['id']).json()['approval']
    if operation == 'release':
        decide(client, approval['id'], 'approve', key='setup')
    before = snapshot(database_path)

    class FailedCommit(sqlite3.Connection):
        def commit(self):
            raise sqlite3.OperationalError('injected commit failure')

    def connect():
        db = sqlite3.connect(database_path, factory=FailedCommit)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        return db

    monkeypatch.setattr(client.app.state.database, 'connect', connect)
    if operation == 'request':
        response = request_approval(client, task['id'], key='commit-failure')
    elif operation == 'release':
        response = release(client, task['id'], approval['id'], key='commit-failure')
    else:
        response = decide(client, approval['id'], operation, key='commit-failure')
    assert response.status_code == 500
    assert snapshot(database_path) == before


@pytest.mark.xfail(strict=True, reason='Contract decision required: no binding between current wait and its Approval')
def test_old_approval_cannot_release_new_wait(client):
    task = create_task(client)
    old = request_approval(client, task['id']).json()['approval']
    decide(client, old['id'], 'approve', key='approve-old')
    release(client, task['id'], old['id'])
    new = client.post(f"/tasks/{task['id']}/request-approval",
                      json={'gate': 'different.gate', 'request_reason': 'New decision', 'requested_by': 'orion'},
                      headers={'Idempotency-Key': 'new-wait', 'If-Match': '"v3"'})
    assert new.status_code == 200
    bypass = client.post(f"/tasks/{task['id']}/release",
                        json={'approval_id': old['id'], 'actor_id': 'orion'},
                        headers={'Idempotency-Key': 'old-approval-again', 'If-Match': '"v4"'})
    assert bypass.status_code == 409
