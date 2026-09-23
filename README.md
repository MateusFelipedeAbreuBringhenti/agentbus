# AgentBus

Primeiro slice vertical do AgentBus: criação e consulta de `Task`, auditoria por
`Event` append-only e replay idempotente em SQLite.

## Executar

```bash
python -m pip install -e '.[test]'
uvicorn agentbus.app:app --reload
```

A criação exige `Idempotency-Key` e `correlation_id`:

```bash
curl -X POST http://localhost:8000/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-001' \
  -d '{
    "type": "prepare_release",
    "title": "Prepare release",
    "input": {},
    "requested_by": "orion",
    "correlation_id": "11111111-1111-4111-8111-111111111111"
  }'
```

## Testes

```bash
pytest
```
