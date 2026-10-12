# Causal multiworker coordination

Slice #008C demonstrates two logical workers cooperating through AgentBus without direct
communication. Both workers may use the same Codex provider: this proves independent
assignment, runner recovery, and causal coordination, not model or vendor independence.

## Late materialization

A downstream Task does not exist until its predecessor succeeds. The coordinator calls
`POST /tasks/{predecessor_id}/successors`; AgentBus verifies the predecessor is `succeeded`
and derives both `correlation_id` and `causation_event_id` from its unique matching
`task.completed` Event. The request cannot supply either field. The successor is created
`ready`, with its `task.created` Event and idempotency record in one transaction.

The command does not prohibit multiple legitimate successors. Deduplication of one
coordination decision is provided by the coordinator outbox, not by a global Task constraint.

## Durable coordinator outbox

Orion-ops stores the exact canonical successor request and one generated idempotency key under
a caller-provided stable `coordination_id` before sending it. SQLite `BEGIN IMMEDIATE` makes
two coordinator processes sharing the journal converge on the same row and key. A restart or
lost response resends the stored body and key; a divergent decision using the same id is
rejected locally.

Set `AGENTBUS_ORION_OPS_JOURNAL` to a durable local SQLite path. Sharing that file is required
for coordinator instances that must represent the same decision. Different coordination ids
represent independent decisions and may legitimately create different successors.

The MCP process requires this setting to be an absolute path. On startup it enumerates every
unconfirmed row and resends the stored body and key before accepting MCP traffic. Recovery does
not ask the model or human to repeat the operation. If AgentBus is unavailable or a pending row
belongs to another configured principal, startup fails closed; the process supervisor may
restart it after the condition is corrected. This is startup recovery, not a scheduler.

AgentBus currently retains idempotency records indefinitely: there is no TTL or cleanup path.
This is current storage policy, not a promise that replay may be garbage-collected without a
future retention contract. Any future cleanup must preserve at least the maximum outbox
recovery window and must not silently invalidate unconfirmed commands.

## Artifact transport

The producer puts a small JSON artifact and its SHA-256 in `Task.output`. The coordinator
copies that envelope into the successor's `Task.input`; the consumer independently validates
the hash. Shared host files, arbitrary URLs, blob storage, DAGs, blocked Tasks, and automatic
failure propagation are outside this slice.

## Trace

`GET /correlations/{correlation_id}/trace` returns deterministically ordered Tasks and Events.
It deliberately has no `complete`, success, percentage, or aggregate workflow state. It is a
factual read model; the coordinator remains responsible for interpreting its own intent.

## Security boundary

The route enforces structural causal consistency, not authentication or authorization.
AgentBus identities remain self-declared. Orion-ops removes identity, correlation, causation,
and idempotency-key control from its MCP tool and injects its configured local principal, but
an independent HTTP client is not thereby authenticated. Tasks must remain controlled and
trusted under the LocalCodexExecutor limitation documented separately.
