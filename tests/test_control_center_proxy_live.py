import json
import os
from pathlib import Path
import socket
import subprocess
import time
from uuid import uuid4

import httpx

from agentbus.auth import token_digest


ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _wait_for(url: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            httpx.get(url, timeout=0.25)
            return
        except httpx.TransportError:
            time.sleep(0.05)
    raise AssertionError(f"server did not start: {url}")


def _start(command: list[str], environment: dict[str, str], cwd: Path) -> subprocess.Popen:
    return subprocess.Popen(
        command,
        cwd=cwd,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def test_browser_vite_gateway_agentbus_path_preserves_api_prefix(tmp_path):
    bus_port, gateway_port, intent_port, vite_port = (_free_port() for _ in range(4))
    coordinator_token = "coordinator-token-000000000000000000000"
    viewer_token = "viewer-token-000000000000000000000000000"
    registry = tmp_path / "principals.json"
    registry.write_text(json.dumps({"principals": [
        {
            "principal_id": "coordinator-1",
            "subject_id": "orion",
            "token_sha256": token_digest(coordinator_token),
            "permissions": ["task:create", "task:read:any"],
        },
        {
            "principal_id": "viewer-1",
            "subject_id": "control-center",
            "token_sha256": token_digest(viewer_token),
            "permissions": ["task:read:any", "event:read:any", "trace:read:any", "approval:read:any"],
        },
    ]}), encoding="utf-8")
    registry.chmod(0o600)
    token_file = tmp_path / "viewer.token"
    token_file.write_text(viewer_token + "\n", encoding="utf-8")
    token_file.chmod(0o600)
    execution_policy = tmp_path / "execution-policy.json"
    execution_policy.write_text(json.dumps({
        "policy_id": "proxy-live-v1", "subjects": {"dex": ["proxy-test"]},
    }), encoding="utf-8")
    execution_policy.chmod(0o600)

    base_env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    bus = _start([
        "python", "-m", "uvicorn", "agentbus.app:create_app", "--factory",
        "--host", "127.0.0.1", "--port", str(bus_port),
    ], {
        **base_env,
        "AGENTBUS_DATABASE": str(tmp_path / "agentbus.sqlite3"),
        "AGENTBUS_AUTH_CONFIG": str(registry),
        "AGENTBUS_EXECUTION_POLICY": str(execution_policy),
    }, ROOT)
    gateway = intent_service = vite = None
    try:
        _wait_for(f"http://127.0.0.1:{bus_port}/tasks")
        created = httpx.post(
            f"http://127.0.0.1:{bus_port}/tasks",
            headers={
                "Authorization": f"Bearer {coordinator_token}",
                "Idempotency-Key": "proxy-e2e-create",
            },
            json={
                "type": "proxy-test", "title": "Visible through Vite", "input": {},
                "requested_by": "orion", "assigned_to": "dex",
                "correlation_id": str(uuid4()),
            },
        )
        assert created.status_code == 201

        gateway = _start([
            "python", "-m", "uvicorn", "agentbus.control_center_gateway:app",
            "--host", "127.0.0.1", "--port", str(gateway_port),
        ], {
            **base_env,
            "AGENTBUS_URL": f"http://127.0.0.1:{bus_port}",
            "AGENTBUS_CONTROL_CENTER_TOKEN_FILE": str(token_file),
            "AGENTBUS_CONTROL_CENTER_HOST": f"127.0.0.1:{vite_port}",
        }, ROOT)
        _wait_for(f"http://127.0.0.1:{gateway_port}/api/tasks")

        intent_service = _start([
            "python", "-m", "uvicorn", "agentbus.approval_intents:app",
            "--host", "127.0.0.1", "--port", str(intent_port),
        ], {
            **base_env,
            "AGENTBUS_URL": f"http://127.0.0.1:{bus_port}",
            "AGENTBUS_CONTROL_CENTER_TOKEN_FILE": str(token_file),
            "AGENTBUS_CONTROL_CENTER_HOST": f"127.0.0.1:{vite_port}",
            "AGENTBUS_APPROVAL_INTENT_JOURNAL": str(tmp_path / "intents.sqlite3"),
        }, ROOT)
        _wait_for(f"http://127.0.0.1:{intent_port}/intent-api/session")

        vite = _start([
            "npm", "run", "dev", "--", "--port", str(vite_port), "--strictPort",
        ], {
            **os.environ,
            "AGENTBUS_CONTROL_CENTER_GATEWAY": f"http://127.0.0.1:{gateway_port}",
            "AGENTBUS_APPROVAL_INTENT_SERVICE": f"http://127.0.0.1:{intent_port}",
        }, ROOT / "frontend")
        _wait_for(f"http://127.0.0.1:{vite_port}/")
        response = httpx.get(
            f"http://127.0.0.1:{vite_port}/api/tasks",
            headers={
                "Origin": f"http://127.0.0.1:{vite_port}",
                "Sec-Fetch-Site": "same-origin",
            },
        )
        assert response.status_code == 200
        assert [item["id"] for item in response.json()["items"]] == [created.json()["id"]]
        intent_session = httpx.get(
            f"http://127.0.0.1:{vite_port}/intent-api/session",
            headers={
                "Origin": f"http://127.0.0.1:{vite_port}",
                "Sec-Fetch-Site": "same-origin",
            },
        )
        assert intent_session.status_code == 200
        assert len(intent_session.json()["csrf_token"]) >= 32
    finally:
        for process in (vite, intent_service, gateway, bus):
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
