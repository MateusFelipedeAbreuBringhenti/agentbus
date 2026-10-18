import json
import sys

from agentbus.auth import AuthRegistry, read_token_file
from agentbus.auth_cli import main


def _invoke(monkeypatch, *arguments):
    monkeypatch.setattr(sys, "argv", ["agentbus-auth", *arguments])
    main()


def test_issue_rotate_inspect_and_revoke_are_atomic_and_secret_safe(tmp_path, monkeypatch, capsys):
    config = tmp_path / "principals.json"
    first = tmp_path / "first.token"
    second = tmp_path / "second.token"
    common = ["--config", str(config), "--subject", "dex", "--permission", "task:claim:self"]
    _invoke(monkeypatch, "issue", *common, "--principal", "runner-1", "--token-file", str(first))
    _invoke(monkeypatch, "rotate", *common, "--principal", "runner-2", "--token-file", str(second))
    assert config.stat().st_mode & 0o777 == 0o600
    assert first.stat().st_mode & 0o777 == 0o600
    assert second.stat().st_mode & 0o777 == 0o600
    assert read_token_file(first) != read_token_file(second)
    AuthRegistry.from_file(config)

    _invoke(monkeypatch, "inspect", "--config", str(config))
    inspected = capsys.readouterr().out
    assert "runner-1" in inspected and "token_sha256" not in inspected
    assert read_token_file(first) not in inspected

    _invoke(monkeypatch, "revoke", "--config", str(config), "--principal", "runner-1")
    payload = json.loads(config.read_text())
    assert [item["principal_id"] for item in payload["principals"]] == ["runner-2"]
    AuthRegistry.from_file(config)
