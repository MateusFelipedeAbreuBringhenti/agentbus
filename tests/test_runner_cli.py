from pathlib import Path

import pytest

from agentbus.runner import BusResponse
from agentbus import runner_cli


TOKEN = "runner-cli-token-000000000000000000000000"


class FakeClient:
    instances = []

    def __init__(self, base_url, *, token, subject_id):
        self.base_url = base_url
        self.token = token
        self.authenticated_subject_id = subject_id
        self.closed = False
        self.__class__.instances.append(self)

    def get_identity(self):
        return BusResponse(200, {
            "principal_id": "runner-cli-1",
            "subject_id": self.authenticated_subject_id,
            "permissions": sorted(runner_cli.REQUIRED_PERMISSIONS),
        }, {})

    def get_inbox(self, agent_id):
        return BusResponse(200, {"items": []}, {})

    def close(self):
        self.closed = True


def arguments(tmp_path: Path) -> list[str]:
    token = tmp_path / "runner.token"
    token.write_text(TOKEN + "\n", encoding="utf-8")
    token.chmod(0o600)
    return [
        "run", "--agent-id", "dex-e2e",
        "--journal", str(tmp_path / "runner.sqlite3"),
        "--token-file", str(token),
        "--executor", "deterministic-noop",
        "--allow-task-type", "palace.e2e.noop",
        "--once",
    ]


def test_runner_cli_once_validates_identity_and_uses_closed_executor(tmp_path, monkeypatch):
    FakeClient.instances.clear()
    monkeypatch.setattr(runner_cli, "HttpAgentBusClient", FakeClient)
    runner_cli.main(arguments(tmp_path))
    assert FakeClient.instances[-1].closed is True
    assert (tmp_path / "runner.sqlite3").exists()
    assert (tmp_path / "runner.sqlite3").stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "runner.sqlite3.lock").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("replacement", ["*", "other.type"])
def test_runner_cli_rejects_open_or_wrong_task_allowlist(tmp_path, replacement):
    args = arguments(tmp_path)
    args[args.index("palace.e2e.noop")] = replacement
    with pytest.raises(SystemExit) as stopped:
        runner_cli.main(args)
    assert stopped.value.code == 2
    assert not (tmp_path / "runner.sqlite3").exists()


def test_runner_cli_rejects_relative_private_paths(tmp_path):
    args = arguments(tmp_path)
    args[args.index(str(tmp_path / "runner.sqlite3"))] = "runner.sqlite3"
    with pytest.raises(SystemExit) as stopped:
        runner_cli.main(args)
    assert stopped.value.code == 2


def test_runner_cli_fails_before_polling_when_subject_differs(tmp_path, monkeypatch):
    class WrongSubjectClient(FakeClient):
        def get_identity(self):
            response = super().get_identity()
            return BusResponse(200, {**response.body, "subject_id": "quack"}, {})

    monkeypatch.setattr(runner_cli, "HttpAgentBusClient", WrongSubjectClient)
    with pytest.raises(SystemExit) as stopped:
        runner_cli.main(arguments(tmp_path))
    assert stopped.value.code == 1
    assert not (tmp_path / "runner.sqlite3").exists()
