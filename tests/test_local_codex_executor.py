from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_app
from agentbus.local_codex import (
    CodexThreadSnapshot,
    CodexTurnSnapshot,
    LocalCodexExecutionConflict,
    LocalCodexExecutionStore,
    LocalCodexExecutor,
    LocalCodexSecurityError,
    LocalCodexStateAmbiguous,
)
from agentbus.runner import (
    AgentRunner,
    BusResponse,
    ExecutionRequest,
    ExecutionSucceeded,
    RunnerJournal,
)


class FakeHandle:
    def __init__(self, provider, turn_id):
        self.provider = provider
        self.turn_id = turn_id

    def run(self):
        turn = self.provider.turns[self.turn_id]
        self.provider.turns[self.turn_id] = CodexTurnSnapshot(
            self.turn_id, "completed", self.provider.result
        )
        return self.provider.turns[self.turn_id]


class FakeProvider:
    def __init__(self, result=None):
        self.result = result or json.dumps(
            {
                "status": "succeeded",
                "output": {"result": "alpha-beta"},
                "failure_code": None,
                "failure_message": None,
            }
        )
        self.thread_count = 0
        self.turn_count = 0
        self.threads = {}
        self.turns = {}
        self.workspaces = []
        self.auth_checked = False

    def assert_chatgpt_authenticated(self):
        self.auth_checked = True

    def start_thread(self, workspace):
        self.thread_count += 1
        thread_id = f"thread-{self.thread_count}"
        self.threads[thread_id] = []
        self.workspaces.append(Path(workspace))
        return thread_id

    def read_thread(self, thread_id, workspace):
        return CodexThreadSnapshot(
            thread_id, tuple(self.turns[item] for item in self.threads[thread_id])
        )

    def start_turn(self, thread_id, workspace, prompt, output_schema):
        self.turn_count += 1
        turn_id = f"turn-{self.turn_count}"
        self.threads[thread_id].append(turn_id)
        self.turns[turn_id] = CodexTurnSnapshot(turn_id, "inProgress")
        return FakeHandle(self, turn_id)


class TestBusClient:
    __test__ = False

    def __init__(self, api):
        self.api = api

    @staticmethod
    def _response(response):
        return BusResponse(
            response.status_code,
            response.json(),
            {key.lower(): value for key, value in response.headers.items()},
        )

    def get_inbox(self, agent_id):
        return self._response(self.api.get(f"/agents/{agent_id}/inbox"))

    def get_task(self, task_id):
        return self._response(self.api.get(f"/tasks/{task_id}"))

    def claim_task(self, task_id, agent_id, idempotency_key):
        return self._response(
            self.api.post(
                f"/tasks/{task_id}/claim",
                json={"agent_id": agent_id},
                headers={"Idempotency-Key": idempotency_key},
            )
        )

    def complete_task(self, task_id, output, idempotency_key, if_match):
        return self._response(
            self.api.post(
                f"/tasks/{task_id}/complete",
                json={"output": output},
                headers={"Idempotency-Key": idempotency_key, "If-Match": if_match},
            )
        )

    def fail_task(
        self, task_id, failure_code, failure_message, idempotency_key, if_match
    ):
        return self._response(
            self.api.post(
                f"/tasks/{task_id}/fail",
                json={
                    "failure_code": failure_code,
                    "failure_message": failure_message,
                },
                headers={"Idempotency-Key": idempotency_key, "If-Match": if_match},
            )
        )


def request(*, execution_id=None, task_id=None, input_data=None):
    return ExecutionRequest(
        execution_id=execution_id or uuid4(),
        task_id=task_id or uuid4(),
        task_type="local_test",
        title="Combine the two input files",
        description=None,
        input=input_data or {"files": {"a.txt": "alpha", "b.txt": "beta"}},
        correlation_id=uuid4(),
        task_etag='"v2"',
        agent_id="dex",
    )


@pytest.fixture(autouse=True)
def no_api_keys(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("CODEX_API_KEY", raising=False)


def build(tmp_path, provider=None):
    provider = provider or FakeProvider()
    store = LocalCodexExecutionStore(tmp_path / "adapter.sqlite3")
    executor = LocalCodexExecutor(
        provider=provider, store=store, workspace_root=tmp_path / "workspaces"
    )
    return executor, provider, store


def test_same_execution_replays_completed_result_without_new_remote_work(tmp_path):
    executor, provider, store = build(tmp_path)
    work = request()
    first = executor.execute(work)
    second = executor.execute(work)
    assert first == second == ExecutionSucceeded(output={"result": "alpha-beta"})
    assert provider.thread_count == provider.turn_count == 1
    record = store.get(str(work.execution_id))
    assert (record.thread_id, record.turn_id) == ("thread-1", "turn-1")
    store.close()


def test_completed_remote_turn_is_recovered_and_persisted(tmp_path):
    executor, provider, store = build(tmp_path)
    work = request()
    workspace = executor.workspace_root / str(work.execution_id)
    record = store.prepare(str(work.execution_id), executor._request_hash(work), workspace)
    workspace.mkdir(parents=True)
    provider.threads["thread-existing"] = ["turn-existing"]
    provider.turns["turn-existing"] = CodexTurnSnapshot(
        "turn-existing", "completed", provider.result
    )
    store.set_thread(record.execution_id, "thread-existing")

    result = executor.execute(work)
    assert result == ExecutionSucceeded(output={"result": "alpha-beta"})
    persisted = store.get(record.execution_id)
    assert persisted.turn_id == "turn-existing"
    assert persisted.result_json is not None
    assert provider.turn_count == 0
    store.close()


def test_ambiguous_state_never_starts_duplicate_thread_or_turn(tmp_path):
    executor, provider, store = build(tmp_path)
    work = request()
    workspace = executor.workspace_root / str(work.execution_id)
    store.prepare(str(work.execution_id), executor._request_hash(work), workspace)
    workspace.mkdir(parents=True)
    with pytest.raises(LocalCodexStateAmbiguous, match="Thread creation"):
        executor.execute(work)
    assert provider.thread_count == provider.turn_count == 0

    store.set_thread(str(work.execution_id), "thread-existing")
    provider.threads["thread-existing"] = ["one", "two"]
    provider.turns["one"] = CodexTurnSnapshot("one", "completed", provider.result)
    provider.turns["two"] = CodexTurnSnapshot("two", "completed", provider.result)
    with pytest.raises(LocalCodexStateAmbiguous, match="Multiple"):
        executor.execute(work)
    assert provider.turn_count == 0
    store.close()


def test_running_recorded_turn_stops_instead_of_reexecuting(tmp_path):
    executor, provider, store = build(tmp_path)
    work = request()
    workspace = executor.workspace_root / str(work.execution_id)
    store.prepare(str(work.execution_id), executor._request_hash(work), workspace)
    workspace.mkdir(parents=True)
    store.set_thread(str(work.execution_id), "thread")
    store.set_turn(str(work.execution_id), "turn")
    provider.threads["thread"] = ["turn"]
    provider.turns["turn"] = CodexTurnSnapshot("turn", "inProgress")
    with pytest.raises(LocalCodexStateAmbiguous, match="refusing duplication"):
        executor.execute(work)
    assert provider.turn_count == 0
    store.close()


def test_invalid_remote_result_is_not_persisted(tmp_path):
    executor, _, store = build(tmp_path, FakeProvider('{"status":"succeeded"}'))
    work = request()
    with pytest.raises(ValueError):
        executor.execute(work)
    assert store.get(str(work.execution_id)).result_json is None
    store.close()


def test_execution_id_cannot_be_reused_for_different_request(tmp_path):
    executor, _, store = build(tmp_path)
    work = request()
    executor.execute(work)
    changed = request(execution_id=work.execution_id, input_data={"files": {}})
    with pytest.raises(LocalCodexExecutionConflict):
        executor.execute(changed)
    store.close()


def test_each_task_has_isolated_workspace(tmp_path):
    executor, provider, store = build(tmp_path)
    one, two = request(), request()
    executor.execute(one)
    executor.execute(two)
    assert len(set(provider.workspaces)) == 2
    assert all(path.parent == executor.workspace_root for path in provider.workspaces)
    assert (provider.workspaces[0] / "a.txt").read_text() == "alpha"
    store.close()


def test_api_keys_and_home_workspace_are_rejected_before_provider_use(
    tmp_path, monkeypatch
):
    provider = FakeProvider()
    store = LocalCodexExecutionStore(tmp_path / "store.sqlite3")
    monkeypatch.setenv("OPENAI_API_KEY", "never-forward-this")
    with pytest.raises(LocalCodexSecurityError, match="API-key"):
        LocalCodexExecutor(provider=provider, store=store, workspace_root=tmp_path / "w")
    assert provider.auth_checked is False
    monkeypatch.delenv("OPENAI_API_KEY")
    with pytest.raises(LocalCodexSecurityError, match="outside"):
        LocalCodexExecutor(provider=provider, store=store, workspace_root=Path.home() / "w")
    store.close()


def test_workspace_symlink_escape_is_rejected(tmp_path):
    executor, provider, store = build(tmp_path)
    work = request()
    outside = tmp_path / "outside"
    outside.mkdir()
    (executor.workspace_root / str(work.execution_id)).symlink_to(
        outside, target_is_directory=True
    )
    with pytest.raises(LocalCodexSecurityError, match="escapes"):
        executor.execute(work)
    assert provider.thread_count == 0
    store.close()


def _create_task(api, *, assigned_to="dex"):
    response = api.post(
        "/tasks",
        json={
            "type": "local_codex_test",
            "title": "Combine files",
            "description": (
                "Read a.txt and b.txt, create combined.txt with their contents joined "
                "by a hyphen, and report what you did."
            ),
            "input": {"files": {"a.txt": "alpha", "b.txt": "beta"}},
            "requested_by": "orion",
            "assigned_to": assigned_to,
            "correlation_id": str(uuid4()),
        },
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 201
    return response.json()


def test_full_heartbeat_and_approval_is_structurally_ignored(tmp_path):
    with TestClient(create_app(tmp_path / "bus.sqlite3")) as api:
        task = _create_task(api)
        approval_task = _create_task(api, assigned_to=None)
        requested = api.post(
            f"/tasks/{approval_task['id']}/request-approval",
            json={
                "gate": "local.review",
                "request_reason": "Human only",
                "requested_by": "orion",
                "assigned_to": "dex",
            },
            headers={
                "Idempotency-Key": str(uuid4()),
                "If-Match": '"v1"',
            },
        )
        assert requested.status_code == 200

        executor, provider, store = build(tmp_path)
        with RunnerJournal(tmp_path / "runner.sqlite3") as journal:
            runner = AgentRunner(
                agent_id="dex",
                client=TestBusClient(api),
                journal=journal,
                executor=executor,
            )
            runner.poll_once()

        terminal = api.get(f"/tasks/{task['id']}").json()
        assert terminal["status"] == "succeeded"
        assert terminal["output"] == {"result": "alpha-beta"}
        approval_id = requested.json()["approval"]["id"]
        assert api.get(f"/approvals/{approval_id}").json()["status"] == "pending"
        assert provider.thread_count == 1
        store.close()


@pytest.mark.live
def test_real_local_codex_full_heartbeat(tmp_path):
    if os.environ.get("AGENTBUS_RUN_LOCAL_CODEX_SMOKE") != "1":
        pytest.skip("set AGENTBUS_RUN_LOCAL_CODEX_SMOKE=1 for the real Codex heartbeat")

    from agentbus.local_codex import CodexSDKProvider

    with TestClient(create_app(tmp_path / "live-bus.sqlite3")) as api:
        task = _create_task(api)
        with CodexSDKProvider() as provider:
            with LocalCodexExecutionStore(tmp_path / "live-adapter.sqlite3") as store:
                executor = LocalCodexExecutor(
                    provider=provider,
                    store=store,
                    workspace_root=tmp_path / "live-workspaces",
                )
                with RunnerJournal(tmp_path / "live-runner.sqlite3") as journal:
                    AgentRunner(
                        agent_id="dex",
                        client=TestBusClient(api),
                        journal=journal,
                        executor=executor,
                    ).poll_once()

        terminal = api.get(f"/tasks/{task['id']}").json()
        assert terminal["status"] == "succeeded"
        assert isinstance(terminal["output"]["result"], str)
        assert terminal["output"]["result"]
        artifacts = list((tmp_path / "live-workspaces").glob("*/combined.txt"))
        assert len(artifacts) == 1
        assert artifacts[0].read_text(encoding="utf-8").strip() == "alpha-beta"
