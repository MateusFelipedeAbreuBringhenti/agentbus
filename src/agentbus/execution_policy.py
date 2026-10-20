from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re


IDENTIFIER = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")


class TaskTypeNotAllowed(Exception):
    pass


@dataclass(frozen=True)
class ExecutionPolicy:
    policy_id: str
    policy_sha256: str
    subjects: dict[str, frozenset[str]]

    @classmethod
    def from_file(cls, path: str | Path) -> ExecutionPolicy:
        policy_path = Path(path)
        if not policy_path.is_absolute():
            raise ValueError("Execution policy path must be absolute.")
        if policy_path.stat().st_mode & 0o077:
            raise PermissionError("Execution policy must have mode 0600.")
        raw = policy_path.read_bytes()
        payload = json.loads(raw)
        if set(payload) != {"policy_id", "subjects"}:
            raise ValueError("Execution policy has an invalid shape.")
        policy_id = payload["policy_id"]
        entries = payload["subjects"]
        if not isinstance(policy_id, str) or not IDENTIFIER.fullmatch(policy_id):
            raise ValueError("Execution policy id is invalid.")
        if not isinstance(entries, dict) or not entries:
            raise ValueError("Execution policy requires at least one subject.")
        subjects: dict[str, frozenset[str]] = {}
        for subject, task_types in entries.items():
            if not isinstance(subject, str) or not IDENTIFIER.fullmatch(subject):
                raise ValueError("Execution policy subject is invalid.")
            if not isinstance(task_types, list) or not task_types:
                raise ValueError("Each execution subject requires allowed task types.")
            if not all(isinstance(item, str) and IDENTIFIER.fullmatch(item) for item in task_types):
                raise ValueError("Execution policy task type is invalid.")
            if len(task_types) != len(set(task_types)):
                raise ValueError("Execution policy task types must be unique.")
            subjects[subject] = frozenset(task_types)
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return cls(policy_id, hashlib.sha256(canonical).hexdigest(), subjects)

    def require(self, subject_id: str, task_type: str) -> None:
        if task_type not in self.subjects.get(subject_id, frozenset()):
            raise TaskTypeNotAllowed


class TestExecutionPolicy(ExecutionPolicy):
    def __init__(self) -> None:
        super().__init__("test-allow-all", "0" * 64, {"*": frozenset({"*"})})

    def require(self, subject_id: str, task_type: str) -> None:
        return
