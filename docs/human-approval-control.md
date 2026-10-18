# Human Approval control — Slice #009C

The browser never receives an approver credential. The read gateway remains a
GET-only allowlist. A separate loopback intent service accepts only closed
Approval intent payloads, protects them with Host, Origin, Fetch Metadata, a
non-privileged SameSite session and CSRF token, and stores them in its own
mode-`0600` SQLite journal.

Start the intent service on loopback:

```text
AGENTBUS_APPROVAL_INTENT_JOURNAL=/absolute/private/approval-intents.sqlite3
AGENTBUS_CONTROL_CENTER_TOKEN_FILE=/absolute/private/control-center.token
AGENTBUS_CONTROL_CENTER_HOST=127.0.0.1:5173
AGENTBUS_URL=http://127.0.0.1:8000
.venv/bin/uvicorn agentbus.approval_intents:app --app-dir src --host 127.0.0.1 --port 8002
```

After the panel registers an intent, confirm it from a trusted terminal:

```text
agentbus-approval-confirm confirm INTENT_ID \
  --journal /absolute/private/approval-intents.sqlite3 \
  --token-file /absolute/private/mateus.token
```

Recover confirmed or ambiguous sends explicitly:

```text
agentbus-approval-confirm recover \
  --journal /absolute/private/approval-intents.sqlite3 \
  --token-file /absolute/private/mateus.token
```

`PENDING` is never sent during recovery. `CONFIRMED` and `SENDING` reuse the
persisted endpoint, body, ETag, subject and idempotency key. A lost successful
response is recovered through AgentBus replay and recorded with `replayed=true`.
Approve/reject changes only Approval; it never releases or executes the Task.

Pending intents expire after 30 minutes. Completed, stopped and expired records
are retained for 30 days; confirmed and ambiguous records are never removed by
automatic cleanup. Limits are 16 KiB per request, three pending intents per
Approval, 50 per browser session, and 500 globally.

The CSRF session is not approval authority. This local design does not protect
against malicious processes running as the same Unix user that can read the
human token or rewrite the journal and its hash.
