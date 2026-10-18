from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
from pathlib import Path
import re
from typing import Protocol
import os


TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{32,}$")
KNOWN_PERMISSIONS = frozenset({
    "task:read:any", "task:read:self", "event:read:any", "trace:read:any",
    "inbox:read:self", "approval:read:any", "approval:read:self",
    "task:create", "task:create:delegated", "task:create:successor",
    "task:claim:self", "task:complete:self", "task:fail:self",
    "approval:request", "approval:decide:self", "task:release",
})


class AuthenticationRequired(Exception):
    pass


class InvalidCredential(Exception):
    pass


class PermissionDenied(Exception):
    pass


class PrincipalMismatch(Exception):
    pass


class ResourceAccessDenied(Exception):
    pass


@dataclass(frozen=True)
class AuthContext:
    principal_id: str
    subject_id: str
    permissions: frozenset[str]

    def require(self, permission: str) -> None:
        if permission not in self.permissions:
            raise PermissionDenied


class Authenticator(Protocol):
    def authenticate(self, authorization: str | None) -> AuthContext: ...


@dataclass(frozen=True)
class _Credential:
    context: AuthContext
    token_sha256: str


class AuthRegistry:
    def __init__(self, credentials: tuple[_Credential, ...]) -> None:
        if not credentials:
            raise ValueError("At least one principal credential is required.")
        self._credentials = credentials

    @classmethod
    def from_file(cls, path: str | Path) -> AuthRegistry:
        config_path = Path(path)
        if not config_path.is_absolute():
            raise ValueError("AGENTBUS_AUTH_CONFIG must be an absolute path.")
        if config_path.stat().st_mode & 0o077:
            raise PermissionError("Authentication config must have mode 0600.")
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        if set(payload) != {"principals"} or not isinstance(payload["principals"], list):
            raise ValueError("Authentication config must contain only a principals list.")
        credentials: list[_Credential] = []
        principal_ids: set[str] = set()
        digests: set[str] = set()
        for entry in payload["principals"]:
            if not isinstance(entry, dict) or set(entry) != {
                "principal_id", "subject_id", "token_sha256", "permissions"
            }:
                raise ValueError("Each principal entry has an invalid shape.")
            principal_id = entry["principal_id"]
            subject_id = entry["subject_id"]
            digest = entry["token_sha256"]
            permissions = entry["permissions"]
            if not all(isinstance(value, str) and value for value in (principal_id, subject_id)):
                raise ValueError("Principal and subject ids must be non-empty strings.")
            if principal_id in principal_ids or digest in digests:
                raise ValueError("Principal ids and token digests must be unique.")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("token_sha256 must be a lowercase SHA-256 digest.")
            if not isinstance(permissions, list) or not all(
                isinstance(item, str) and item in KNOWN_PERMISSIONS for item in permissions
            ):
                raise ValueError("Authentication config contains an unknown permission.")
            principal_ids.add(principal_id)
            digests.add(digest)
            credentials.append(_Credential(
                AuthContext(principal_id, subject_id, frozenset(permissions)), digest
            ))
        return cls(tuple(credentials))

    def authenticate(self, authorization: str | None) -> AuthContext:
        if authorization is None:
            raise AuthenticationRequired
        scheme, separator, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not separator or not TOKEN_PATTERN.fullmatch(token):
            raise InvalidCredential
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        matched: AuthContext | None = None
        for credential in self._credentials:
            if hmac.compare_digest(digest, credential.token_sha256):
                matched = credential.context
        if matched is None:
            raise InvalidCredential
        return matched


class TestAuthenticator:
    """Explicit test-only authenticator for pre-security domain regression tests."""

    context = AuthContext("test-suite", "*", KNOWN_PERMISSIONS)

    def authenticate(self, authorization: str | None) -> AuthContext:
        return self.context


class EnvironmentAuthenticator:
    def __init__(self) -> None:
        self._registry: AuthRegistry | None = None

    def load(self) -> None:
        path = os.environ.get("AGENTBUS_AUTH_CONFIG")
        if not path:
            raise RuntimeError("AGENTBUS_AUTH_CONFIG is required; anonymous mode is disabled.")
        self._registry = AuthRegistry.from_file(path)

    def authenticate(self, authorization: str | None) -> AuthContext:
        if self._registry is None:
            raise RuntimeError("Authentication registry was not loaded.")
        return self._registry.authenticate(authorization)


def token_digest(token: str) -> str:
    if not TOKEN_PATTERN.fullmatch(token):
        raise ValueError("Token must contain at least 32 URL-safe characters.")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def read_token_file(path: str | Path) -> str:
    token_path = Path(path)
    if not token_path.is_absolute():
        raise ValueError("Credential file path must be absolute.")
    if token_path.stat().st_mode & 0o077:
        raise PermissionError("Credential file must not be accessible by group or others.")
    token = token_path.read_text(encoding="utf-8").strip()
    if not TOKEN_PATTERN.fullmatch(token):
        raise ValueError("Credential file contains an invalid token.")
    return token
