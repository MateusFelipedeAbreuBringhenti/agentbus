from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import secrets
import tempfile

from agentbus.auth import AuthRegistry, KNOWN_PERMISSIONS, token_digest


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _locked_config(path: Path):
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = open(lock_path, "a+", encoding="utf-8")
    os.chmod(lock_path, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX)
    return lock


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"principals": []}


def _write_validated(path: Path, payload: dict) -> None:
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    fd, candidate_name = tempfile.mkstemp(prefix=".auth-candidate.", dir=path.parent)
    os.close(fd)
    candidate = Path(candidate_name)
    try:
        candidate.chmod(0o600)
        candidate.write_text(rendered, encoding="utf-8")
        AuthRegistry.from_file(candidate)
    finally:
        candidate.unlink(missing_ok=True)
    _atomic_write(path, rendered)


def _issue(args: argparse.Namespace) -> None:
    config, token_file = Path(args.config), Path(args.token_file)
    if not config.is_absolute() or not token_file.is_absolute():
        raise ValueError("Config and token paths must be absolute.")
    unknown = set(args.permission) - KNOWN_PERMISSIONS
    if unknown:
        raise ValueError(f"Unknown permissions: {sorted(unknown)}")
    token = secrets.token_urlsafe(32)
    with _locked_config(config):
        payload = _read(config)
        if any(item["principal_id"] == args.principal for item in payload["principals"]):
            raise ValueError("Principal id already exists.")
        payload["principals"].append({
            "principal_id": args.principal,
            "subject_id": args.subject,
            "token_sha256": token_digest(token),
            "permissions": sorted(set(args.permission)),
        })
        _write_validated(config, payload)
        _atomic_write(token_file, token + "\n")


def _revoke(args: argparse.Namespace) -> None:
    config = Path(args.config)
    with _locked_config(config):
        payload = _read(config)
        kept = [p for p in payload["principals"] if p["principal_id"] != args.principal]
        if len(kept) == len(payload["principals"]):
            raise ValueError("Principal id does not exist.")
        payload["principals"] = kept
        _write_validated(config, payload)


def _inspect(args: argparse.Namespace) -> None:
    payload = _read(Path(args.config))
    safe = [{key: value for key, value in item.items() if key != "token_sha256"}
            for item in payload["principals"]]
    print(json.dumps(safe, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline AgentBus credential provisioning")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("issue", "rotate"):
        command = commands.add_parser(name)
        command.add_argument("--config", required=True)
        command.add_argument("--principal", required=True)
        command.add_argument("--subject", required=True)
        command.add_argument("--token-file", required=True)
        command.add_argument("--permission", action="append", default=[], required=True)
        command.set_defaults(handler=_issue)
    revoke = commands.add_parser("revoke")
    revoke.add_argument("--config", required=True)
    revoke.add_argument("--principal", required=True)
    revoke.set_defaults(handler=_revoke)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--config", required=True)
    inspect.set_defaults(handler=_inspect)
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
