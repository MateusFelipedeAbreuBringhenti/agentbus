# Local authentication and authorization

AgentBus has no anonymous compatibility mode. `AGENTBUS_AUTH_CONFIG` must name an
absolute, mode-`0600` JSON file. The server validates the complete immutable snapshot
at startup and fails closed for missing files, malformed hashes, duplicate principals,
or unknown permissions.

Create credentials offline with `agentbus-auth issue` (or `rotate`) and remove an old
credential with `agentbus-auth revoke`. Config and token files are written by same-directory
atomic replacement with mode `0600`; raw tokens are never written to the repository or
printed by `inspect`. Restart AgentBus and its client after a change. Rotation keeps the
same logical subject so durable idempotent recovery remains valid.

Issuance first records a mode-`0600` recovery journal beside the registry. If the process is
interrupted between writing the client token and activating its registry entry, run
`agentbus-auth recover --config /absolute/path/principals.json`. Recovery rewrites the same
token, validates or creates the matching principal entry, and removes the journal only after
both durable writes succeed. Other provisioning operations fail closed while recovery is
pending.

Each client sends its own bearer credential. Runner and Orion-ops read token files outside
their SQLite journals. `actor_id` is the authenticated logical subject;
`authenticated_principal_id` is the concrete credential. `requested_by` may be a delegated,
auditable statement of intent, but is never authorization or proof that the named human
participated. Historical Events and idempotency rows deliberately retain a null verified
principal.

The Control Center browser never receives an AgentBus credential. Run the enumerated local
gateway on `127.0.0.1:8001` with a read-only token and point Vite at it. The gateway checks
the exact Host, Origin when present, and Fetch Metadata, and exposes only the three read
routes used by Slice #009A. It is not a generic proxy. This protects the browser boundary
from ordinary cross-site requests and DNS rebinding, but does not isolate AgentBus from a
malicious process running as the same OS user.

Example startup variables:

```text
AGENTBUS_DATABASE=/absolute/path/agentbus.sqlite3
AGENTBUS_AUTH_CONFIG=/absolute/private/path/principals.json
AGENTBUS_CONTROL_CENTER_TOKEN_FILE=/absolute/private/path/control-center.token
AGENTBUS_ORION_OPS_TOKEN_FILE=/absolute/private/path/orion-ops.token
```

Keep FastAPI, the gateway, and Vite bound to loopback. Do not expose them through a tunnel.
