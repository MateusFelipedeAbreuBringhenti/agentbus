from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
from threading import Barrier, Lock
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import pytest

from agentbus.app import create_test_app as create_app
from agentbus.coordinator import CoordinatorOutbox
from agentbus.orion_ops import (
    ControlResponse,
    CreateTaskInput,
    CreateSuccessorInput,
    OrionOpsControlPlane,
)
from agentbus.runner import (
    AgentBusTransportError,
    AgentBusAuthorizationError,
    AgentRunner,
    BusResponse,
    DeterministicWorkExecutor,
    ExecutionRequest,
    ExecutionSucceeded,
    JournalLockedError,
    HttpAgentBusClient,
    RunnerJournal,
)


class UnauthorizedBusClient:
    authenticated_subject_id = "dex"

    def get_inbox(self, agent_id):
        return BusResponse(401, {"detail": {"code": "invalid_credential"}}, {})


class TestAgentBusClient:
    __test__ = False

    def __init__(self, api: TestClient) -> None:
        self.api = api

    @staticmethod
    def _response(response) -> BusResponse:
        return BusResponse(
            status_code=response.status_code,
            body=response.json(),
            headers={key.lower(): value for key, value in response.headers.items()},
        )

    def get_inbox(self, agent_id: str) -> BusResponse:
        return self._response(self.api.get(f"/agents/{agent_id}/inbox"))

    def get_task(self, task_id: str) -> BusResponse:
        return self._response(self.api.get(f"/tasks/{task_id}"))

    def claim_task(
        self, task_id, agent_id, idempotency_key, execution_id, runner_instance_id
    ) -> BusResponse:
        return self._response(
            self.api.post(
                f"/tasks/{task_id}/claim",
                json={"agent_id": agent_id, "execution_id": execution_id,
                      "runner_instance_id": runner_instance_id},
                headers={"Idempotency-Key": idempotency_key},
            )
        )

    def complete_task(
        self, task_id, output, idempotency_key, if_match, execution_id, runner_instance_id
    ) -> BusResponse:
        return self._response(
            self.api.post(
                f"/tasks/{task_id}/complete",
                json={"output": output, "execution_id": execution_id,
                      "runner_instance_id": runner_instance_id},
                headers={
                    "Idempotency-Key": idempotency_key,
                    "If-Match": if_match,
                },
            )
        )

    def fail_task(
        self,
        task_id,
        failure_code,
        failure_message,
        idempotency_key,
        if_match,
        execution_id,
        runner_instance_id,
    ) -> BusResponse:
        return self._response(
            self.api.post(
                f"/tasks/{task_id}/fail",
                json={
                    "failure_code": failure_code,
                    "failure_message": failure_message,
                    "execution_id": execution_id,
                    "runner_instance_id": runner_instance_id,
                },
                headers={
                    "Idempotency-Key": idempotency_key,
                    "If-Match": if_match,
                },
            )
        )


class IntegrationControlTransport:
    def __init__(self, api: TestClient) -> None:
        self.api = api

    def request(self, method, path, *, json=None, headers=None):
        response = self.api.request(method, path, json=json, headers=headers)
        return ControlResponse(
            response.status_code,
            response.json() if response.content else None,
            {key.lower(): value for key, value in response.headers.items()},
        )


class LoseConfirmedResponseClient:
    def __init__(self, wrapped: TestAgentBusClient, operation: str) -> None:
        self.wrapped = wrapped
        self.operation = operation
        self.lost = False

    def _lose_once(self, operation, invoke):
        response = invoke()
        if self.operation == operation and not self.lost:
            self.lost = True
            raise AgentBusTransportError("response lost after server commit")
        return response

    def get_inbox(self, agent_id):
        return self.wrapped.get_inbox(agent_id)

    def get_task(self, task_id):
        return self.wrapped.get_task(task_id)

    def claim_task(self, task_id, agent_id, idempotency_key, execution_id, runner_instance_id):
        return self._lose_once(
            "claim",
            lambda: self.wrapped.claim_task(
                task_id, agent_id, idempotency_key, execution_id, runner_instance_id
            ),
        )

    def complete_task(
        self, task_id, output, idempotency_key, if_match, execution_id, runner_instance_id
    ):
        return self._lose_once(
            "complete",
            lambda: self.wrapped.complete_task(
                task_id, output, idempotency_key, if_match, execution_id, runner_instance_id
            ),
        )

    def fail_task(
        self, task_id, failure_code, failure_message, idempotency_key, if_match,
        execution_id, runner_instance_id
    ):
        return self._lose_once(
            "fail",
            lambda: self.wrapped.fail_task(
                task_id,
                failure_code,
                failure_message,
                idempotency_key,
                if_match,
                execution_id,
                runner_instance_id,
            ),
        )


def test_runner_stops_safely_when_credential_is_rejected(tmp_path):
    with RunnerJournal(tmp_path / "unauthorized.sqlite3") as journal:
        runner = AgentRunner(
            agent_id="dex", client=UnauthorizedBusClient(), journal=journal,
            executor=DeterministicWorkExecutor(), allowed_task_types=frozenset({"runner_test"}),
        )
        with pytest.raises(AgentBusAuthorizationError):
            runner.poll_once()
        assert journal.pending() == []


def test_http_runner_client_rejects_non_loopback_agentbus():
    with pytest.raises(ValueError, match="loopback HTTP"):
        HttpAgentBusClient(
            "https://example.com", token="x" * 32, subject_id="dex"
        )


class SynchronizedInboxClient:
    def __init__(self, wrapped: TestAgentBusClient, barrier: Barrier) -> None:
        self.wrapped = wrapped
        self.barrier = barrier

    def get_inbox(self, agent_id):
        response = self.wrapped.get_inbox(agent_id)
        self.barrier.wait(timeout=5)
        return response

    def __getattr__(self, name):
        return getattr(self.wrapped, name)


class UnavailableReportClient:
    def __init__(self, wrapped: TestAgentBusClient) -> None:
        self.wrapped = wrapped

    def __getattr__(self, name):
        return getattr(self.wrapped, name)

    def complete_task(self, *args, **kwargs):
        raise AgentBusTransportError("report unavailable before request")

    def fail_task(self, *args, **kwargs):
        raise AgentBusTransportError("report unavailable before request")


class RecordingExecutor:
    def __init__(self, *, interrupt_once=False) -> None:
        self.execution_ids: list[UUID] = []
        self.interrupt_once = interrupt_once

    def execute(self, request: ExecutionRequest):
        self.execution_ids.append(request.execution_id)
        if self.interrupt_once:
            self.interrupt_once = False
            raise RuntimeError("simulated executor interruption")
        return ExecutionSucceeded(output={"execution_id": str(request.execution_id)})


class ArtifactProducer:
    def execute(self, request: ExecutionRequest):
        content = {"combined": "A+B"}
        digest = hashlib.sha256(
            json.dumps(content, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        return ExecutionSucceeded(output={"artifact": {"content": content, "sha256": digest}})


class ArtifactConsumer:
    def execute(self, request: ExecutionRequest):
        artifact = request.input["artifact"]
        digest = hashlib.sha256(
            json.dumps(
                artifact["content"], separators=(",", ":"), sort_keys=True
            ).encode()
        ).hexdigest()
        assert digest == artifact["sha256"]
        return ExecutionSucceeded(output={"validated_sha256": digest, "derived": "A+B:ok"})


class CountingExecutor:
    def __init__(self, shared: list[str], lock: Lock) -> None:
        self.shared = shared
        self.lock = lock

    def execute(self, request: ExecutionRequest):
        with self.lock:
            self.shared.append(str(request.execution_id))
        return ExecutionSucceeded(output={"winner": str(request.execution_id)})


@pytest.fixture
def database_path(tmp_path):
    return tmp_path / "agentbus-runner.sqlite3"


@pytest.fixture
def api(database_path):
    with TestClient(create_app(database_path)) as client:
        yield client


def create_task(api, *, assigned_to="dex", input_data=None):
    response = api.post(
        "/tasks",
        json={
            "type": "runner_test",
            "title": "Run through adapter",
            "input": input_data or {},
            "requested_by": "orion",
            "assigned_to": assigned_to,
            "correlation_id": str(uuid4()),
        },
        headers={"Idempotency-Key": str(uuid4())},
    )
    assert response.status_code == 201
    return response.json()


def build_runner(api, journal, executor, *, client=None):
    return AgentRunner(
        agent_id="dex",
        client=client or TestAgentBusClient(api),
        journal=journal,
        executor=executor,
        allowed_task_types=frozenset({"runner_test"}),
    )


@pytest.mark.parametrize(
    ("input_data", "expected_status"),
    [({}, "succeeded"), ({"should_fail": True}, "failed")],
)
def test_runner_executes_success_and_failure_end_to_end(
    api, tmp_path, input_data, expected_status
):
    task = create_task(api, input_data=input_data)
    with RunnerJournal(tmp_path / f"{expected_status}.sqlite3") as journal:
        build_runner(api, journal, DeterministicWorkExecutor()).poll_once()
        record = journal.get(task["id"])
        assert record is not None
        assert record.claimed_etag == '"v2"'
        assert record.result_kind == expected_status
        assert record.report_key is not None
        assert record.reported_at is not None

    persisted = api.get(f"/tasks/{task['id']}").json()
    assert persisted["status"] == expected_status
    assert persisted["version"] == 3


def test_restart_before_claim_uses_prepared_durable_keys(api, tmp_path):
    task = create_task(api)
    path = tmp_path / "restart-before-claim.sqlite3"
    with RunnerJournal(path) as journal:
        prepared = journal.prepare(task["id"])
        identity = journal.runner_instance_id
    with RunnerJournal(path, runner_instance_id=identity) as journal:
        assert journal.get(task["id"]).claim_key == prepared.claim_key
        build_runner(api, journal, DeterministicWorkExecutor()).poll_once()
        assert journal.get(task["id"]).reported_at is not None
    assert api.get(f"/tasks/{task['id']}").json()["status"] == "succeeded"


def test_lost_claim_response_is_proven_by_replay_of_this_instances_key(
    api, tmp_path
):
    task = create_task(api)
    path = tmp_path / "lost-claim.sqlite3"
    base_client = TestAgentBusClient(api)
    with RunnerJournal(path) as journal:
        identity = journal.runner_instance_id
        lossy = LoseConfirmedResponseClient(base_client, "claim")
        build_runner(api, journal, DeterministicWorkExecutor(), client=lossy).poll_once()
        record = journal.get(task["id"])
        assert record.claimed_etag is None
        assert api.get(f"/tasks/{task['id']}").json()["status"] == "running"

    with RunnerJournal(path, runner_instance_id=identity) as journal:
        build_runner(api, journal, DeterministicWorkExecutor()).poll_once()
        record = journal.get(task["id"])
        assert record.claimed_etag == '"v2"'
        assert record.reported_at is not None
    events = api.get(f"/tasks/{task['id']}/events").json()
    assert [event["type"] for event in events] == [
        "task.created",
        "task.started",
        "task.completed",
    ]


def test_restart_during_executor_reuses_same_execution_id(api, tmp_path):
    task = create_task(api)
    path = tmp_path / "executor-restart.sqlite3"
    executor = RecordingExecutor(interrupt_once=True)
    with RunnerJournal(path) as journal:
        identity = journal.runner_instance_id
        with pytest.raises(RuntimeError, match="interruption"):
            build_runner(api, journal, executor).poll_once()
        record = journal.get(task["id"])
        assert record.claimed_etag == '"v2"'
        assert record.result_kind is None

    with RunnerJournal(path, runner_instance_id=identity) as journal:
        build_runner(api, journal, executor).poll_once()
        assert journal.get(task["id"]).reported_at is not None
    assert len(executor.execution_ids) == 2
    assert executor.execution_ids[0] == executor.execution_ids[1]


def test_restart_before_executor_uses_confirmed_ownership(api, tmp_path):
    task = create_task(api)
    path = tmp_path / "before-executor.sqlite3"
    client = TestAgentBusClient(api)
    with RunnerJournal(path) as journal:
        identity = journal.runner_instance_id
        record = journal.prepare(task["id"])
        claimed = client.claim_task(
            task["id"], "dex", record.claim_key,
            record.execution_id, record.runner_instance_id,
        )
        journal.confirm_claim(task["id"], claimed.headers["etag"])

    executor = RecordingExecutor()
    with RunnerJournal(path, runner_instance_id=identity) as journal:
        build_runner(api, journal, executor).poll_once()
        assert journal.get(task["id"]).reported_at is not None
    assert executor.execution_ids == [UUID(record.execution_id)]


def test_persisted_result_is_reported_after_restart_without_reexecution(
    api, tmp_path
):
    task = create_task(api)
    path = tmp_path / "result-before-report.sqlite3"
    executor = RecordingExecutor()
    base_client = TestAgentBusClient(api)
    with RunnerJournal(path) as journal:
        identity = journal.runner_instance_id
        build_runner(
            api,
            journal,
            executor,
            client=UnavailableReportClient(base_client),
        ).poll_once()
        record = journal.get(task["id"])
        assert record.result_kind == "succeeded"
        assert record.report_key is not None
        assert record.reported_at is None
        assert api.get(f"/tasks/{task['id']}").json()["status"] == "running"

    with RunnerJournal(path, runner_instance_id=identity) as journal:
        build_runner(api, journal, executor).poll_once()
        assert journal.get(task["id"]).reported_at is not None
    assert len(executor.execution_ids) == 1


@pytest.mark.parametrize("terminal", ["complete", "fail"])
def test_lost_terminal_response_replays_persisted_result_without_reexecution(
    api, tmp_path, terminal
):
    task = create_task(
        api,
        input_data={"should_fail": True} if terminal == "fail" else {},
    )
    path = tmp_path / f"lost-{terminal}.sqlite3"
    executor = RecordingExecutor() if terminal == "complete" else DeterministicWorkExecutor()
    base_client = TestAgentBusClient(api)
    with RunnerJournal(path) as journal:
        identity = journal.runner_instance_id
        lossy = LoseConfirmedResponseClient(base_client, terminal)
        build_runner(api, journal, executor, client=lossy).poll_once()
        record = journal.get(task["id"])
        assert record.result_kind == ("failed" if terminal == "fail" else "succeeded")
        assert record.reported_at is None
        assert api.get(f"/tasks/{task['id']}").json()["status"] == (
            "failed" if terminal == "fail" else "succeeded"
        )

    with RunnerJournal(path, runner_instance_id=identity) as journal:
        build_runner(api, journal, executor).poll_once()
        assert journal.get(task["id"]).reported_at is not None
    events = api.get(f"/tasks/{task['id']}/events").json()
    assert len(events) == 3
    if terminal == "complete":
        assert len(executor.execution_ids) == 1


def test_running_task_without_local_ownership_is_never_adopted(api, tmp_path):
    task = create_task(api)
    claimed = api.post(
        f"/tasks/{task['id']}/claim",
        json={"agent_id": "dex"},
        headers={"Idempotency-Key": "external-claim"},
    )
    assert claimed.status_code == 200
    executor = RecordingExecutor()
    with RunnerJournal(tmp_path / "no-ownership.sqlite3") as journal:
        build_runner(api, journal, executor).poll_once()
        assert journal.get(task["id"]) is None
    assert executor.execution_ids == []
    assert api.get(f"/tasks/{task['id']}").json()["status"] == "running"


def test_current_task_must_match_claimed_version_before_executor(api, tmp_path):
    task = create_task(api)
    client = TestAgentBusClient(api)
    executor = RecordingExecutor()
    with RunnerJournal(tmp_path / "validate-current.sqlite3") as journal:
        record = journal.prepare(task["id"])
        claim_response = client.claim_task(
            task["id"], "dex", record.claim_key,
            record.execution_id, record.runner_instance_id,
        )
        journal.confirm_claim(task["id"], claim_response.headers["etag"])
        completed = client.complete_task(
            task["id"], {"external": True}, "external-complete", '"v2"',
            record.execution_id, record.runner_instance_id,
        )
        assert completed.status_code == 200
        build_runner(api, journal, executor).poll_once()
        assert journal.get(task["id"]).result_kind is None
    assert executor.execution_ids == []


def test_two_runner_instances_compete_but_only_winner_executes(database_path, tmp_path):
    with TestClient(create_app(database_path)) as setup:
        task = create_task(setup)
    executed: list[str] = []
    executed_lock = Lock()
    inbox_read = Barrier(2)

    def run(path: Path):
        with TestClient(create_app(database_path)) as api:
            with RunnerJournal(path) as journal:
                runner = build_runner(
                    api,
                    journal,
                    CountingExecutor(executed, executed_lock),
                    client=SynchronizedInboxClient(
                        TestAgentBusClient(api),
                        inbox_read,
                    ),
                )
                runner.poll_once()
                return journal.get(task["id"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(
            pool.map(run, [tmp_path / "runner-a.sqlite3", tmp_path / "runner-b.sqlite3"])
        )

    assert len(executed) == 1
    assert sum(record is not None and record.reported_at is not None for record in records) == 1
    loser = next(record for record in records if record.reported_at is None)
    assert loser.claimed_etag is None
    assert loser.claim_rejected_at is not None
    with TestClient(create_app(database_path)) as verify:
        assert verify.get(f"/tasks/{task['id']}").json()["status"] == "succeeded"


def test_same_journal_cannot_be_opened_by_two_runner_copies(tmp_path):
    path = tmp_path / "locked.sqlite3"
    with RunnerJournal(path) as first:
        with pytest.raises(JournalLockedError):
            RunnerJournal(path, runner_instance_id=first.runner_instance_id)


def test_approval_item_is_structurally_ignored(api, tmp_path):
    task = create_task(api, assigned_to="orion")
    requested = api.post(
        f"/tasks/{task['id']}/request-approval",
        json={
            "gate": "deployment.production",
            "request_reason": "Human only.",
            "requested_by": "orion",
            "assigned_to": "dex",
        },
        headers={"Idempotency-Key": "human-only", "If-Match": '"v1"'},
    )
    approval_id = requested.json()["approval"]["id"]
    executor = RecordingExecutor()
    with RunnerJournal(tmp_path / "ignore-approval.sqlite3") as journal:
        build_runner(api, journal, executor).poll_once()
        assert journal.pending() == []
    assert executor.execution_ids == []
    assert api.get(f"/approvals/{approval_id}").json()["status"] == "pending"


def test_two_worker_causal_heartbeat_recovers_lost_completion(api, tmp_path):
    coordinator = OrionOpsControlPlane(
        IntegrationControlTransport(api), instance_id=uuid4(),
        outbox=CoordinatorOutbox(tmp_path / "coordinator.sqlite3"),
    )
    producer_task = coordinator.create_task(
        CreateTaskInput(
            type="artifact-producer", title="Produce artifact",
            input={"a": "A", "b": "B"}, assigned_to="worker-a",
            correlation_id=uuid4(), idempotency_key="heartbeat-producer",
        )
    ).data
    base_client = TestAgentBusClient(api)
    lossy_client = LoseConfirmedResponseClient(base_client, "complete")
    with RunnerJournal(tmp_path / "producer.sqlite3") as producer_journal:
        producer = AgentRunner(
            agent_id="worker-a", client=lossy_client,
            journal=producer_journal, executor=ArtifactProducer(),
            allowed_task_types=frozenset({"artifact-producer"}),
        )
        producer.poll_once()
        assert api.get(f"/tasks/{producer_task['id']}").json()["status"] == "succeeded"
        assert producer_journal.get(producer_task["id"]).reported_at is None
        producer.poll_once()
        assert producer_journal.get(producer_task["id"]).reported_at is not None

    completed_parent = api.get(f"/tasks/{producer_task['id']}").json()
    successor = coordinator.create_successor_task(
        CreateSuccessorInput(
            coordination_id=uuid4(), predecessor_task_id=producer_task["id"],
            type="artifact-consumer", title="Validate artifact",
            input={"artifact": completed_parent["output"]["artifact"]},
            assigned_to="worker-b",
        )
    )
    with RunnerJournal(tmp_path / "consumer.sqlite3") as consumer_journal:
        consumer = AgentRunner(
            agent_id="worker-b", client=base_client,
            journal=consumer_journal, executor=ArtifactConsumer(),
            allowed_task_types=frozenset({"artifact-consumer"}),
        )
        consumer.poll_once()

    child = api.get(f"/tasks/{successor.data['id']}").json()
    assert child["status"] == "succeeded"
    assert child["output"]["validated_sha256"] == completed_parent["output"]["artifact"]["sha256"]
    trace = api.get(f"/correlations/{producer_task['correlation_id']}/trace").json()
    assert [task["id"] for task in trace["tasks"]] == [producer_task["id"], child["id"]]
    child_created = next(
        event for event in trace["events"]
        if event["task_id"] == child["id"] and event["type"] == "task.created"
    )
    parent_completed = next(
        event for event in trace["events"]
        if event["task_id"] == producer_task["id"] and event["type"] == "task.completed"
    )
    assert child_created["causation_event_id"] == parent_completed["id"]
