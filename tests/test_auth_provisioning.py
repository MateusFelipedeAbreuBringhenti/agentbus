import json
import sys

import pytest

from agentbus.auth import AuthRegistry, read_token_file
from agentbus import auth_cli
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


def test_interrupted_issue_is_recovered_without_orphaning_the_credential(
    tmp_path, monkeypatch
):
    config = tmp_path / "principals.json"
    token_file = tmp_path / "runner.token"
    original_write = auth_cli._write_validated
    failed = False

    def fail_before_registry_write(path, payload):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("simulated interruption")
        original_write(path, payload)

    monkeypatch.setattr(auth_cli, "_write_validated", fail_before_registry_write)
    with pytest.raises(OSError, match="simulated interruption"):
        _invoke(
            monkeypatch,
            "issue",
            "--config", str(config),
            "--subject", "dex",
            "--permission", "task:claim:self",
            "--principal", "runner-1",
            "--token-file", str(token_file),
        )

    pending = config.with_suffix(".json.pending")
    assert pending.exists()
    assert pending.stat().st_mode & 0o777 == 0o600
    assert token_file.exists()
    assert not config.exists()

    monkeypatch.setattr(auth_cli, "_write_validated", original_write)
    _invoke(monkeypatch, "recover", "--config", str(config))

    registry = AuthRegistry.from_file(config)
    token = read_token_file(token_file)
    assert registry.authenticate(f"Bearer {token}").principal_id == "runner-1"
    assert not pending.exists()
