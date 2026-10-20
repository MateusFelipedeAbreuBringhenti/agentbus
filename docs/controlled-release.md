# Controlled Task release — Slice #009D

An approved Approval does not release a Task by itself. A separate local
coordinator reconciler may release it only when all of these facts agree:

- the current credential authenticates the configured coordinator subject and
  has `task:release`;
- the Approval gate is present in a local mode-`0600` allowlist;
- `approval.requested_by` equals that authenticated subject;
- exactly one authenticated `approval.requested` Event identifies the same
  Approval, Task and correlation and was emitted by that subject;
- exactly one authenticated `approval.approved` Event identifies the same
  Approval, Task and correlation;
- the Task is still waiting on that exact Approval.

The API repeats the resource and provenance checks transactionally before
consulting idempotency replay. Historical records without authenticated
provenance therefore fail closed. The approved Event is the exact
`causation_event_id` of `task.released`.

## Private local configuration

Create a policy file outside the repository with mode `0600`:

```json
{"allowed_gates":["deployment.production"]}
```

Run one explicit reconciliation:

```text
agentbus-release-reconcile reconcile APPROVAL_ID \
  --journal /absolute/private/releases.sqlite3 \
  --policy /absolute/private/release-policy.json \
  --token-file /absolute/private/coordinator.token \
  --subject local-coordinator:INSTANCE_ID
```

Recover prepared, ambiguous, or authentication-suspended commands explicitly:

```text
agentbus-release-reconcile recover \
  --journal /absolute/private/releases.sqlite3 \
  --policy /absolute/private/release-policy.json \
  --token-file /absolute/private/coordinator.token \
  --subject local-coordinator:INSTANCE_ID
```

The journal uses `OBSERVED`, `PREPARED`, `SENDING`, `AUTH_REQUIRED`,
`COMPLETED`, and `STOPPED`. Before sending, it durably freezes the request body,
Task ETag, exact causal Event, subject, and deterministic idempotency key. A
lost response remains `SENDING`; recovery reuses those bytes and identifiers.
A `401` becomes `AUTH_REQUIRED` and may resume after credential rotation only
when the new credential represents the same subject and still has permission.
Authorization or state conflicts stop instead of constructing a new command.
A compare-and-swap revision prevents two coordinator attempts from claiming
the same persisted decision concurrently.

There is no scheduler, generic release endpoint in Orion-ops MCP, model tool,
or human credential in this process. Selecting an Approval remains an explicit
operator action. This is logical local control, not isolation from malicious
processes running as the same Unix user.
