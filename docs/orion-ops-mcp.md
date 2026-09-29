# Orion-ops local MCP control plane

This bridge is a separate STDIO MCP server that exposes exactly six AgentBus
operations: `create_task`, `get_task`, `get_task_events`, `get_approval`,
`list_task_approvals`, and `request_approval`.

It deliberately has no generic HTTP, shell, SQL, filesystem, worker execution, or
Approval-decision tool. Tool schemas do not accept a principal or `requested_by`.
The bridge injects the fixed local identity and AgentBus uses it for authority,
audit events, and idempotency scope:

```ini
principal = local-coordinator:<instance-id>
actor_label = orion-ops
persona_version = orion-ops/v1
```

The last two values describe the coordinator persona only. They grant no authority.

## Local configuration

Install the project in its virtual environment, choose and retain one random UUID
for this coordinator instance, and keep AgentBus bound to loopback. A local Codex
configuration can launch the bridge with:

```bash
codex mcp add orion-ops \
  --env AGENTBUS_URL=http://127.0.0.1:8000 \
  --env AGENTBUS_ORION_OPS_INSTANCE_ID=<persistent-instance-uuid> \
  -- /absolute/path/to/.venv/bin/agentbus-orion-ops-mcp
```

Do not commit the chosen instance UUID or a machine-specific absolute path. The
server rejects a non-loopback AgentBus URL, credentials in the URL, queries, and
fragments. No API key is used.

For a dedicated Orion-ops session, enable only these six tools in the Codex MCP
configuration and set `default_tools_approval_mode = "approve"` for this trusted,
closed local server. Without that setting a non-interactive `approval = "never"`
session correctly refuses mutating MCP calls instead of silently approving them.
Run it in a disposable working directory with no repository data.
The MCP allowlist prevents the model from acquiring worker/decision capabilities;
OS-level process isolation remains the boundary for denying access to unrelated
host files.

```toml
[mcp_servers.orion-ops]
enabled_tools = [
  "create_task",
  "get_task",
  "get_task_events",
  "get_approval",
  "list_task_approvals",
  "request_approval",
]
required = true
default_tools_approval_mode = "approve"
```

## Protocol details

Mutation tools require a caller-supplied stable `idempotency_key`. Reads and writes
return relevant ETags. `create_task` preserves the supplied `correlation_id`, and
both mutations preserve optional `causation_event_id`. AgentBus domain failures are
returned with their stable error code rather than converted to a generic success.

If an Approval appears, Orion-ops may read and present it, then must stop. There is
no `approve` or `reject` tool.
