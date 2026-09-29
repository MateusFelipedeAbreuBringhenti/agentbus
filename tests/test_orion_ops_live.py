from __future__ import annotations

import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import time
from uuid import uuid4

import httpx
import pytest

from agentbus.local_codex import CodexSDKProvider, LocalCodexExecutionStore, LocalCodexExecutor
from agentbus.runner import AgentRunner, HttpAgentBusClient, RunnerJournal


pytestmark = pytest.mark.live


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_server(base_url: str, process: subprocess.Popen) -> None:
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("AgentBus stopped before becoming ready.")
        try:
            if httpx.get(f"{base_url}/openapi.json", timeout=0.2).status_code == 200:
                return
        except httpx.RequestError:
            pass
        time.sleep(0.05)
    raise RuntimeError("AgentBus did not become ready.")


@pytest.mark.skipif(
    os.environ.get("RUN_ORION_OPS_LIVE") != "1",
    reason="set RUN_ORION_OPS_LIVE=1 for the real ChatGPT-authenticated heartbeat",
)
def test_real_orion_ops_to_local_codex_heartbeat(tmp_path):
    """One human intent crosses MCP, AgentBus, Runner, and real local Codex."""
    codex = shutil.which("codex") or "/home/kali/.npm-global/bin/codex"
    if not Path(codex).is_file():
        pytest.skip("Codex CLI is unavailable")
    if os.environ.get("OPENAI_API_KEY") or os.environ.get("CODEX_API_KEY"):
        pytest.fail("Live local heartbeat forbids API-key authentication.")

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    server_env = os.environ.copy()
    server_env["AGENTBUS_DATABASE"] = str(tmp_path / "agentbus.sqlite3")
    server = subprocess.Popen(
        [
            str(Path(__file__).parents[1] / ".venv/bin/python"),
            "-m",
            "uvicorn",
            "agentbus.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=Path(__file__).parents[1],
        env=server_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_server(base_url, server)
        with (
            HttpAgentBusClient(base_url) as bus,
            RunnerJournal(tmp_path / "runner.sqlite3") as journal,
            CodexSDKProvider() as provider,
            LocalCodexExecutionStore(tmp_path / "local-codex.sqlite3") as store,
        ):
            executor = LocalCodexExecutor(
                provider=provider,
                store=store,
                workspace_root=tmp_path / "workspaces",
            )
            runner = AgentRunner(agent_id="dex", client=bus, journal=journal, executor=executor)
            instance = uuid4()
            correlation = uuid4()
            bridge = str(
                Path(__file__).parents[1] / ".venv/bin/agentbus-orion-ops-mcp"
            )
            workspace = tmp_path / "orion-ops-workspace"
            workspace.mkdir()
            result_path = tmp_path / "orion-result.txt"
            command = [
                codex,
                "exec",
                "--ignore-user-config",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                "-C",
                str(workspace),
                "-c",
                f'mcp_servers.orion_ops.command="{bridge}"',
                "-c",
                "mcp_servers.orion_ops.env="
                + "{"
                + f'AGENTBUS_URL="{base_url}",'
                + f'AGENTBUS_ORION_OPS_INSTANCE_ID="{instance}"'
                + "}",
                "-c",
                'mcp_servers.orion_ops.enabled_tools=["create_task","get_task",'
                '"get_task_events","get_approval","list_task_approvals",'
                '"request_approval"]',
                "-c",
                "mcp_servers.orion_ops.required=true",
                "-c",
                'mcp_servers.orion_ops.default_tools_approval_mode="approve"',
                "-o",
                str(result_path),
                (
                    "Peça ao Dex para combinar A e B. Use somente Orion-ops MCP. Crie uma "
                    "Task assigned_to dex, type combine, com instrução inequívoca para devolver "
                    "exatamente A+B em output.result; use correlation_id "
                    f"{correlation} e idempotency_key heartbeat-create. Observe a mesma Task "
                    "até succeeded ou failed usando get_task, leia seus eventos e apresente o "
                    "resultado final. Não use shell ou filesystem."
                ),
            ]
            child_env = os.environ.copy()
            child_env.pop("OPENAI_API_KEY", None)
            child_env.pop("CODEX_API_KEY", None)
            coordinator = subprocess.Popen(
                command,
                env=child_env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + 240
            while coordinator.poll() is None and time.monotonic() < deadline:
                runner.poll_once()
                time.sleep(0.1)
            if coordinator.poll() is None:
                coordinator.kill()
                stdout, stderr = coordinator.communicate()
                pytest.fail(f"Orion-ops timed out. stdout={stdout!r} stderr={stderr!r}")
            stdout, stderr = coordinator.communicate()
            completed = subprocess.CompletedProcess(
                command, coordinator.returncode, stdout, stderr
            )
            assert completed.returncode == 0, completed.stderr
            presentation = result_path.read_text(encoding="utf-8")

        connection = sqlite3.connect(tmp_path / "agentbus.sqlite3")
        connection.row_factory = sqlite3.Row
        task = connection.execute(
            "SELECT * FROM tasks WHERE correlation_id = ?", (str(correlation),)
        ).fetchone()
        events = connection.execute(
            "SELECT type FROM events WHERE task_id = ? ORDER BY sequence", (task["id"],)
        ).fetchall()
        connection.close()
        assert task["status"] == "succeeded"
        assert task["requested_by"] == f"local-coordinator:{instance}"
        assert task["assigned_to"] == "dex"
        assert "A+B" in task["output_json"]
        assert [row["type"] for row in events] == [
            "task.created",
            "task.started",
            "task.completed",
        ]
        assert "A+B" in presentation
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
