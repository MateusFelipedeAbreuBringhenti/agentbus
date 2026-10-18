from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import sys
from typing import Callable
from urllib.parse import urlparse
from uuid import uuid4

import httpx

from agentbus.approval_intents import (
    ApprovalIntentJournal, IntentChanged, IntentNotFound,
)
from agentbus.auth import read_token_file
from agentbus.models import ApprovalRead, ApprovalStatus, AuthIdentityRead


NOTICE = "Esta ação decide a Approval. Ela não libera nem executa automaticamente a Task."


class ConfirmationError(Exception): pass


@contextmanager
def _exclusive_lock(journal_path: Path):
    path = journal_path.with_suffix(journal_path.suffix + ".confirm.lock")
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    if os.fstat(descriptor).st_mode & 0o077:
        os.close(descriptor)
        raise ConfirmationError("Confirmer lock file has unsafe permissions.")
    lock = open(descriptor, "a+", encoding="utf-8", closefd=True)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close()
        raise ConfirmationError("Another confirmer is already using this journal.") from None
    try:
        yield
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


class ApprovalConfirmer:
    def __init__(
        self, *, journal: ApprovalIntentJournal, journal_path: Path,
        agentbus_url: str, token: str,
        transport: httpx.BaseTransport | None = None,
        prompt: Callable[[str], str] = input,
        output: Callable[[str], None] = print,
    ) -> None:
        parsed = urlparse(agentbus_url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("AgentBus confirmer target must be loopback HTTP.")
        self.journal = journal
        self.journal_path = journal_path
        self.client = httpx.Client(
            base_url=agentbus_url, headers={"Authorization": f"Bearer {token}"},
            transport=transport,
        )
        self.prompt = prompt
        self.output = output

    def close(self) -> None:
        self.client.close()

    def _identity(self) -> AuthIdentityRead:
        response = self.client.get("/auth/me")
        if response.status_code != 200:
            raise ConfirmationError(f"Authentication failed ({response.status_code}).")
        try:
            identity = AuthIdentityRead.model_validate(response.json())
        except (ValueError, json.JSONDecodeError):
            raise ConfirmationError("Authentication response is invalid.") from None
        if "approval:decide:self" not in identity.permissions:
            raise ConfirmationError("Authenticated principal cannot decide Approvals.")
        return identity

    def confirm(self, intent_id: str) -> dict:
        with _exclusive_lock(self.journal_path):
            intent = self.journal.read(intent_id)
            if intent["state"] != "pending":
                raise ConfirmationError("Intent is not pending confirmation.")
            identity = self._identity()
            if identity.subject_id != intent["intended_subject_id"]:
                raise ConfirmationError("Approval is assigned to another subject.")
            approval_response = self.client.get(f"/approvals/{intent['approval_id']}")
            if approval_response.status_code != 200:
                raise ConfirmationError("Approval is no longer readable by this subject.")
            try:
                approval = ApprovalRead.model_validate(approval_response.json())
            except (ValueError, json.JSONDecodeError):
                raise ConfirmationError("Approval response is invalid.") from None
            if approval.status is not ApprovalStatus.PENDING:
                raise ConfirmationError("Approval is no longer pending.")
            if approval_response.headers.get("etag") != intent["approval_etag"]:
                raise ConfirmationError("Approval changed after the intent was created.")

            word = "APROVAR" if intent["decision"] == "approve" else "REJEITAR"
            self.output(
                f"Approval: {approval.id}\nTask: {approval.task_id}\nGate: {approval.gate}\n"
                f"Responsável: {approval.assigned_to}\nDecisão: {intent['decision']}\n"
                f"Motivo: {intent['decision_reason'] or '—'}\n\n{NOTICE}"
            )
            if self.prompt(f"Digite {word} para confirmar: ").strip() != word:
                raise ConfirmationError("Confirmation cancelled.")
            body = {
                "decided_by": identity.subject_id,
                "decision_reason": intent["decision_reason"],
            }
            self.journal.confirm(
                intent_id, expected_revision=intent["revision"],
                principal_id=identity.principal_id, subject_id=identity.subject_id,
                command_body=body, idempotency_key=str(uuid4()),
            )
            return self._send_locked(intent_id, identity.subject_id)

    def _send_locked(self, intent_id: str, subject_id: str) -> dict:
        attempt = self.journal.begin_send(intent_id, subject_id)
        endpoint = f"/approvals/{attempt['approval_id']}/{attempt['decision']}"
        try:
            response = self.client.post(
                endpoint,
                content=attempt["command_body_json"],
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": attempt["idempotency_key"],
                    "If-Match": attempt["approval_etag"],
                },
            )
        except httpx.TransportError:
            self.journal.record_error(
                intent_id, attempt["attempt_id"], status=None,
                code="transport_error", state="sending",
            )
            raise ConfirmationError("Response is ambiguous; recover with the same intent.") from None
        if response.status_code == 200:
            try:
                approval = ApprovalRead.model_validate(response.json())
                expected_status = (
                    ApprovalStatus.APPROVED if attempt["decision"] == "approve"
                    else ApprovalStatus.REJECTED
                )
                expected_etag = f'"v{approval.version}"'
                if (
                    str(approval.id) != attempt["approval_id"]
                    or str(approval.task_id) != attempt["task_id"]
                    or str(approval.correlation_id) != attempt["correlation_id"]
                    or approval.status is not expected_status
                    or approval.decided_by != subject_id
                    or approval.assigned_to != attempt["intended_subject_id"]
                    or approval.version != json.loads(attempt["approval_snapshot_json"])["version"] + 1
                    or response.headers.get("etag") != expected_etag
                ):
                    raise ValueError
            except (ValueError, json.JSONDecodeError):
                self.journal.record_error(
                    intent_id, attempt["attempt_id"], status=200,
                    code="invalid_success_response", state="sending",
                )
                raise ConfirmationError("Response is ambiguous; recover with the same intent.") from None
            replayed = response.headers.get("Idempotency-Replayed") == "true"
            self.journal.record_completed(
                intent_id, attempt["attempt_id"], response=approval.model_dump(mode="json"),
                etag=expected_etag, replayed=replayed,
            )
            return self.journal.read(intent_id)

        try:
            code = response.json().get("detail", {}).get("code", f"http_{response.status_code}")
        except ValueError:
            code = f"http_{response.status_code}"
        if response.status_code == 401:
            next_state = "confirmed"
        elif response.status_code >= 500:
            next_state = "sending"
        else:
            next_state = "stopped"
        self.journal.record_error(
            intent_id, attempt["attempt_id"], status=response.status_code,
            code=code, state=next_state,
        )
        raise ConfirmationError(f"AgentBus rejected the decision: {code}.")

    def recover(self) -> list[dict]:
        results = []
        with _exclusive_lock(self.journal_path):
            identity = self._identity()
            for intent_id in self.journal.recoverable():
                intent = self.journal.read(intent_id)
                if intent["confirmed_subject_id"] != identity.subject_id:
                    continue
                results.append(self._send_locked(intent_id, identity.subject_id))
        return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Confirm AgentBus Approval intents")
    parser.add_argument("command", choices=["confirm", "recover"])
    parser.add_argument("intent_id", nargs="?")
    parser.add_argument("--journal", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--agentbus-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    journal_path = Path(args.journal)
    token_path = Path(args.token_file)
    if not journal_path.is_absolute() or not token_path.is_absolute():
        raise ValueError("Journal and token paths must be absolute.")
    confirmer = ApprovalConfirmer(
        journal=ApprovalIntentJournal(journal_path), journal_path=journal_path,
        agentbus_url=args.agentbus_url, token=read_token_file(token_path),
    )
    try:
        if args.command == "confirm":
            if not args.intent_id:
                parser.error("confirm requires intent_id")
            confirmer.confirm(args.intent_id)
        else:
            confirmer.recover()
    except (ConfirmationError, IntentChanged, IntentNotFound) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error
    finally:
        confirmer.close()


if __name__ == "__main__":
    main()
