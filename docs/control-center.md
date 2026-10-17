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

In another terminal, start the Vite development server:

```bash
cd frontend
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. Vite listens only on loopback and proxies
`/api/*` to `http://127.0.0.1:8000`, removing the `/api` prefix. No CORS
configuration and no public endpoint are needed.

## Implemented surface

- paginated Task catalog with status, assignee and type filters;
- Task details including version/ETag, correlation, timestamps and failures;
- causal trace projection containing persisted Tasks and append-only Events;
- distinct loading, empty, malformed-response, HTTP-error and unavailable states.

The remaining navigation entries are visibly deferred. They do not display
mock operational data and do not invoke mutations.

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
AgentBus still has no general HTTP authentication. Keep both processes bound
to loopback and do not expose the Vite or FastAPI ports through a tunnel or
public reverse proxy.

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
