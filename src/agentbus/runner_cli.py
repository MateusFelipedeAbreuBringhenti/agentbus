from __future__ import annotations

import argparse
from pathlib import Path
import signal
from threading import Event
from typing import Sequence

from .auth import read_token_file
from .runner import (
    AgentBusAuthorizationError,
    AgentBusTransportError,
    AgentRunner,
    DeterministicNoopExecutor,
    HttpAgentBusClient,
    JournalIdentityMismatch,
    JournalLockedError,
    RunnerJournal,
)


NOOP_TASK_TYPE = DeterministicNoopExecutor.task_type
REQUIRED_PERMISSIONS = frozenset({
    "inbox:read:self",
    "task:read:self",
    "task:claim:self",
    "task:complete:self",
    "task:fail:self",
})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a closed local AgentBus worker")
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--executor", required=True, choices=["deterministic-noop"])
    parser.add_argument("--allow-task-type", action="append", required=True)
    parser.add_argument("--agentbus-url", default="http://127.0.0.1:8000")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    return parser


def _absolute_private_path(parser: argparse.ArgumentParser, value: str, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        parser.error(f"{label} must be an absolute path")
    return path


def main(argv: Sequence[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    journal_path = _absolute_private_path(parser, args.journal, "journal")
    token_path = _absolute_private_path(parser, args.token_file, "token-file")
    allowed = frozenset(args.allow_task_type)
    if allowed != {NOOP_TASK_TYPE}:
        parser.error(f"deterministic-noop requires exactly --allow-task-type {NOOP_TASK_TYPE}")
    if args.poll_interval <= 0:
        parser.error("poll-interval must be positive")

    client = HttpAgentBusClient(
        args.agentbus_url,
        token=read_token_file(token_path),
        subject_id=args.agent_id,
    )
    try:
        identity = client.get_identity()
        if identity.status_code != 200:
            raise AgentBusAuthorizationError("Runner credential was not accepted.")
        if identity.body.get("subject_id") != args.agent_id:
            raise AgentBusAuthorizationError("Runner credential subject differs from agent-id.")
        permissions = frozenset(identity.body.get("permissions", []))
        if not REQUIRED_PERMISSIONS.issubset(permissions):
            raise AgentBusAuthorizationError("Runner credential lacks required permissions.")

        stop = Event()

        def request_stop(*_: object) -> None:
            stop.set()

        if not args.once:
            signal.signal(signal.SIGINT, request_stop)
            signal.signal(signal.SIGTERM, request_stop)
        with RunnerJournal(journal_path) as journal:
            runner = AgentRunner(
                agent_id=args.agent_id,
                client=client,
                journal=journal,
                executor=DeterministicNoopExecutor(),
                allowed_task_types=allowed,
            )
            if args.once:
                runner.poll_once()
            else:
                runner.run_forever(
                    poll_interval_seconds=args.poll_interval,
                    stop_event=stop,
                )
    except (AgentBusAuthorizationError, AgentBusTransportError,
            JournalIdentityMismatch, JournalLockedError) as error:
        parser.exit(1, f"agentbus-runner: {error}\n")
    finally:
        client.close()


if __name__ == "__main__":
    main()
