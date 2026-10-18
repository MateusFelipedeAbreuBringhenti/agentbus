# AgentBus Control Center — Slice #009A

The Control Center is a local, read-only React application. It reads the
AgentBus database exclusively through the public FastAPI contracts; it does
not open SQLite directly and exposes no mutation controls.

## Local operation

Start the API on loopback from the repository root:

```bash
AGENTBUS_DATABASE=/absolute/path/to/agentbus.sqlite3 \
  .venv/bin/uvicorn agentbus.app:app --app-dir src --host 127.0.0.1 --port 8000
```

Start the enumerated read-only gateway on `127.0.0.1:8001` using the dedicated
Control Center token described in `docs/local-security.md`. In another terminal, start Vite:

```bash
cd frontend
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. Vite listens only on loopback and preserves `/api`
while proxying it to the enumerated gateway on `http://127.0.0.1:8001`; the
gateway maps those routes to AgentBus on `http://127.0.0.1:8000`. It separately
preserves `/intent-api` while proxying to the non-authoritative Approval intent
service on `http://127.0.0.1:8002`. No CORS configuration or public endpoint is
needed.

## Implemented surface

- paginated Task catalog with status, assignee and type filters;
- Task details including version/ETag, correlation, timestamps and failures;
- causal trace projection containing persisted Tasks and append-only Events;
- distinct loading, empty, malformed-response, HTTP-error and unavailable states.

The Approval screen reads the real paginated catalog and can register a local
intent. Registration is not an AgentBus mutation. A separate terminal confirmer
with a human credential is required to decide an Approval. Other remaining
navigation entries are visibly deferred and display no mock operational data.

## Task pagination contract

`GET /tasks` accepts:

- `limit` (1–100, default 25);
- `cursor` (opaque);
- `status`;
- `assigned_to`;
- `requested_by`;
- `correlation_id`;
- `type`.

Tasks are ordered by `(created_at DESC, id DESC)`. The cursor records the last
tuple and a fingerprint of the active filters. Reusing it with different
filters returns `400 invalid_task_cursor`; clients must not parse or construct
cursors.

The cursor stabilizes traversal over the chosen ordering and prevents repeats
caused by timestamp ties. It is not a database snapshot: newly inserted Tasks
that sort before the cursor may appear only after the consumer restarts from
the first page.

## Security boundary

This is a controlled local interface, not a remote or multi-user console.
The browser has no AgentBus credential. Its same-origin `/api` calls go through the
enumerated gateway, which injects a read-only principal and validates Host, Origin, and
Fetch Metadata. This remains a controlled single-installation interface, not multi-user
isolation. Keep every process on loopback and do not expose it through a tunnel.

## Verification

Backend:

```bash
.venv/bin/pytest
```

Frontend:

```bash
cd frontend
npm test
npm run build
```

The backend heartbeat test proves the required path using persisted data:

`Task create → GET /tasks → GET /tasks/{id} → GET /correlations/{id}/trace`.
