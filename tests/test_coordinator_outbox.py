from concurrent.futures import ThreadPoolExecutor
import sqlite3
from uuid import uuid4

import pytest

from agentbus.coordinator import CoordinationDecisionConflict, CoordinatorOutbox


def test_outbox_persists_key_and_exact_payload_across_restart(tmp_path):
    path = tmp_path / "outbox.sqlite3"
    coordination, parent = uuid4(), uuid4()
    body = {"type": "consumer", "title": "Consume", "requested_by": "coordinator"}
    first = CoordinatorOutbox(path).prepare(coordination, parent, body)
    replay = CoordinatorOutbox(path).prepare(coordination, parent, body)
    assert replay.idempotency_key == first.idempotency_key
    assert replay.request_json == first.request_json
    assert CoordinatorOutbox(path).pending() == [replay]


def test_outbox_rejects_divergent_decision(tmp_path):
    outbox = CoordinatorOutbox(tmp_path / "outbox.sqlite3")
    coordination, parent = uuid4(), uuid4()
    outbox.prepare(coordination, parent, {"title": "A"})
    with pytest.raises(CoordinationDecisionConflict):
        outbox.prepare(coordination, parent, {"title": "B"})


def test_two_instances_share_one_durable_command(tmp_path):
    path = tmp_path / "outbox.sqlite3"
    coordination, parent = uuid4(), uuid4()
    body = {"title": "same"}
    first, second = CoordinatorOutbox(path), CoordinatorOutbox(path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(pool.map(lambda box: box.prepare(coordination, parent, body), [first, second]))
    assert records[0].idempotency_key == records[1].idempotency_key
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM successor_commands").fetchone()[0] == 1


def test_confirmation_is_retained_and_cannot_switch_successor(tmp_path):
    outbox = CoordinatorOutbox(tmp_path / "outbox.sqlite3")
    coordination, parent, successor = uuid4(), uuid4(), uuid4()
    outbox.prepare(coordination, parent, {"title": "same"})
    outbox.confirm(coordination, successor, {"id": str(successor)})
    record = outbox.prepare(coordination, parent, {"title": "same"})
    assert record.successor_task_id == successor
    assert record.confirmed_at is not None
    assert outbox.pending() == []
    with pytest.raises(CoordinationDecisionConflict):
        outbox.confirm(coordination, uuid4(), {"id": "different"})
