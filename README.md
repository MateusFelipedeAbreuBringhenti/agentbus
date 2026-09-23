# AgentBus

`localhost://revolution` — multi-agent orchestration with a human in the loop.

Slices verticais iniciais do AgentBus: criação, consulta e execução básica de
`Task`, auditoria por `Event` append-only e replay idempotente em SQLite.

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

No MVP, o escopo da chave idempotente combina a operação com `requested_by`.
Como ainda não existe autenticação, essa identidade é declarada pelo próprio
cliente e não deve ser tratada como identidade verificada ou autorização.

## Execução de Task

O ciclo básico usa comandos explícitos:

```text
POST /tasks/{id}/claim      ready   → running
POST /tasks/{id}/complete   running → succeeded
POST /tasks/{id}/fail       running → failed
```

Todos exigem `Idempotency-Key`. `claim` recebe `agent_id` e adquire a Task
atomicamente, sem `If-Match`. `complete` e `fail` exigem a versão observada no
cabeçalho `If-Match`.

Tasks usam um ETag forte no formato `"vN"`, devolvido por criação, consulta e
comandos. Por exemplo, uma Task na versão 2 retorna:

```http
ETag: "v2"
```

O cliente copia esse valor sem modificações:

```http
If-Match: "v2"
```

Ausência de `If-Match` em `complete` ou `fail` retorna `428`; formato inválido
retorna `400`; conflito de estado ou versão retorna `409` com código de domínio
estável.

Comandos bem-sucedidos retornam `200` e a representação completa da Task com o
novo ETag. Um replay conserva status HTTP, corpo e ETag da resposta original,
mesmo que a Task já tenha avançado. O cabeçalho `Idempotency-Replayed` apenas
indica se a resposta veio do registro idempotente.

## Approval e reação explícita

O primeiro fluxo humano no loop mantém decisão e reação em comandos separados:

```text
POST /tasks/{id}/request-approval   Task ready → waiting_approval
POST /approvals/{id}/approve       Approval pending → approved
POST /approvals/{id}/reject        Approval pending → rejected
POST /tasks/{id}/release           Task waiting_approval → ready
```

Decidir uma Approval nunca altera a Task. `release` precisa indicar uma
`approval_id` específica, aprovada, pertencente à mesma Task e correlação. Uma
Approval rejeitada deixa a Task em `waiting_approval`; nenhuma política de
workflow é inferida.

`request-approval` retorna as representações de Task e Approval juntas, com
`Task-ETag` e `Approval-ETag`. Decisões retornam a Approval e seu ETag; `release`
retorna a Task e seu ETag. Todos os quatro comandos exigem `If-Match` e
`Idempotency-Key`.

As identidades `requested_by`, `decided_by` e `actor_id` são autodeclaradas no
MVP. Elas definem autoria e escopo idempotente, mas não representam autenticação
ou autorização verificadas.

## Testes

```bash
pytest
```
