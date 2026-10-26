from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "tests/support/isolation_probe.py"


def _require_gate() -> None:
    if os.environ.get("AGENTBUS_RUN_ISOLATION_FEASIBILITY") != "1":
        pytest.skip("set AGENTBUS_RUN_ISOLATION_FEASIBILITY=1 for the host probe")


@pytest.mark.isolation
def test_bubblewrap_hides_host_secrets_blocks_network_and_writes_only_workspace(
    tmp_path: Path,
) -> None:
    _require_gate()
    bwrap = shutil.which("bwrap")
    if bwrap is None:
        pytest.fail("bubblewrap is required for the isolation feasibility gate")

    workspace = tmp_path / "workspace"
    workspace.mkdir(mode=0o700)
    host_sentinel = tmp_path / "host-secret-sentinel"
    host_sentinel.write_text("synthetic-secret\n", encoding="utf-8")
    result = subprocess.run(
        [
            bwrap,
            "--die-with-parent",
            "--new-session",
            "--unshare-user",
            "--unshare-pid",
            "--unshare-net",
            "--clearenv",
            "--setenv", "HOME", "/home/sandbox",
            "--setenv", "PATH", "/usr/bin",
            "--ro-bind", "/usr", "/usr",
            "--ro-bind", "/lib", "/lib",
            "--ro-bind", "/lib64", "/lib64",
            "--proc", "/proc",
            "--dev", "/dev",
            "--tmpfs", "/tmp",
            "--tmpfs", "/home",
            "--dir", "/home/sandbox",
            "--bind", str(workspace), "/workspace",
            "--ro-bind", str(PROBE), "/isolation_probe.py",
            "/usr/bin/python3", "/isolation_probe.py",
            str(host_sentinel), str(ROOT / "pyproject.toml"),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
        env={
            **os.environ,
            "AGENTBUS_TOKEN": "must-not-cross-boundary",
            "PALACE_SECRET": "must-not-cross-boundary",
        },
    )
    evidence = json.loads(result.stdout)
    assert evidence == {
        "artifact_written": True,
        "host_paths_hidden": True,
        "network_blocked": True,
        "secrets_absent": True,
    }
    assert (workspace / "probe-artifact.txt").read_text(encoding="utf-8") == (
        "isolated-artifact\n"
    )


@pytest.mark.isolation
def test_user_systemd_accepts_contract_cgroup_limits() -> None:
    _require_gate()
    systemd_run = shutil.which("systemd-run")
    if systemd_run is None:
        pytest.fail("systemd-run is required for the isolation feasibility gate")
    result = subprocess.run(
        [
            systemd_run,
            "--user",
            "--scope",
            "--quiet",
            "--property=CPUQuota=200%",
            "--property=MemoryMax=2G",
            "--property=TasksMax=64",
            "/usr/bin/true",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.isolation
def test_user_systemd_enforces_memory_limit_on_child_process() -> None:
    _require_gate()
    systemd_run = shutil.which("systemd-run")
    if systemd_run is None:
        pytest.fail("systemd-run is required for the isolation feasibility gate")
    unit = f"agentbus-c0-{uuid4().hex}.scope"
    try:
        result = subprocess.run(
            [
                systemd_run,
                "--user",
                "--scope",
                f"--unit={unit.removesuffix('.scope')}",
                "--quiet",
                "--property=MemoryMax=64M",
                "--property=TasksMax=16",
                "/usr/bin/sh",
                "-c",
                "/usr/bin/python3 -c 'bytearray(256 * 1024 * 1024)'",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        properties = subprocess.run(
            [
                "systemctl", "--user", "show", unit,
                "--property=Result", "--property=MemoryPeak",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        observed = dict(
            line.split("=", 1)
            for line in properties.stdout.splitlines()
            if "=" in line
        )
        assert result.returncode != 0
        assert observed["Result"] == "oom-kill"
        assert 0 < int(observed["MemoryPeak"]) <= 64 * 1024 * 1024
    finally:
        subprocess.run(
            ["systemctl", "--user", "reset-failed", unit],
            capture_output=True,
            timeout=10,
        )


def test_codex_sdk_0158_public_client_exposes_no_observed_tool_executor_boundary() -> None:
    from openai_codex.client import CodexClient, CodexConfig

    parameters = inspect.signature(CodexClient).parameters
    config_fields = set(CodexConfig.__dataclass_fields__)
    assert "approval_handler" in parameters
    assert "tool_executor" not in parameters
    assert "command_handler" not in parameters
    assert "tool_executor" not in config_fields
    assert "command_handler" not in config_fields
