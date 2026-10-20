# Operational Recovery & Visibility — Slice #009E-B

The operational projection is a separate, read-only local service. It reads
only explicitly configured Approval Intent, Release, Runner, and Coordinator
Outbox SQLite journals. It never discovers files, executes recovery, or writes
to a journal.

For each source present at startup, the service records its device and inode.
Every read opens the path with `O_NOFOLLOW`, requires a regular file, compares
that descriptor identity, and keeps the descriptor open while SQLite reads it
through `/proc/self/fd` in `mode=ro`. A missing or replaced source is reported
as unavailable and is not adopted until restart with a validated manifest.
This implementation therefore targets Linux, like the existing `flock`-based
Runner and release controls.

When a live `-wal` file is present, the service copies the database and WAL
through protected descriptors into a private ephemeral snapshot. It verifies
that their identity, size, and modification time remained stable during the
copy, then opens only the snapshot in `mode=ro`. This lets SQLite consume
committed WAL frames without writing lock coordination into the source `-shm`.
If either source changes during capture, that read reports the source as
unavailable instead of presenting a potentially inconsistent snapshot.

## Manifest

The manifest must be an absolute mode-`0600` JSON file. Every source id and
path must be unique; journal paths are absolute and statically allowlisted.

```json
{"sources":[
  {"source_id":"approval-main","kind":"approval_intent","path":"/absolute/private/approval.sqlite3","logical_owner":"mateus"},
  {"source_id":"release-main","kind":"release","path":"/absolute/private/release.sqlite3","logical_owner":"orion-ops"},
  {"source_id":"runner-dex","kind":"runner","path":"/absolute/private/runner-dex.sqlite3","logical_owner":"dex"},
  {"source_id":"orion-outbox","kind":"outbox","path":"/absolute/private/orion-outbox.sqlite3","logical_owner":"orion-ops"}
]}
```

Start on loopback with separate mode-`0600` token files: one read-only AgentBus
principal (`task:read:any` is sufficient for current enrichment) and one
projection reader credential shared only with the Control Center gateway.

```text
AGENTBUS_OPERATIONAL_MANIFEST=/absolute/private/operations.json
AGENTBUS_OPERATIONAL_AGENTBUS_TOKEN_FILE=/absolute/private/operations-agentbus.token
AGENTBUS_OPERATIONAL_READER_TOKEN_FILE=/absolute/private/operations-reader.token
AGENTBUS_URL=http://127.0.0.1:8000
.venv/bin/uvicorn agentbus.operational_projection:app --app-dir src --host 127.0.0.1 --port 8003
```

Configure the existing gateway with `AGENTBUS_OPERATIONAL_URL` and the same
reader token file. It exposes only `GET /api/operations`,
`GET /api/operations/{id}`, and `GET /api/operation-sources`.

## Semantics and limits

Composite operation ids bind source and native record identity. Responses omit
command bodies, payloads, hashes, idempotency keys, attempt ids, tokens, and
filesystem paths. Missing sources are `unavailable`; known tables with an
unexpected schema or state are `incompatible`. One failed source does not hide
the others.

Temporary AgentBus connection failures degrade only Runner/outbox canonical
enrichment to `insufficient_evidence`; they do not hide healthy journal facts.
The projection has no row-count cutoff that reclassifies a valid schema as
incompatible. HTTP pagination limits returned data, but each request is still
a fresh aggregation rather than a durable global snapshot.

Pagination is deterministic for one query and its filter-bound cursor. Each
request reads the configured journals independently; there is no global
cross-journal snapshot. `sending` means active or ambiguous, never “stuck”.
Age and timestamps are facts, not alerts or recovery authority.

The service trusts the local Unix account and cannot provide confidentiality
against another process with the same filesystem access. Keep all services on
loopback and do not expose them through a tunnel.
