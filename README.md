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

Decidir uma Approval nunca altera a Task. Enquanto espera, a Task expõe
`waiting_on_approval_id`, que identifica explicitamente a Approval que abriu a
espera atual. `release` precisa indicar exatamente essa Approval, já aprovada e
pertencente à mesma Task e correlação. Uma Approval antiga não pode liberar uma
espera posterior. Uma Approval rejeitada deixa a Task em `waiting_approval` e
mantém o vínculo; nenhuma política de workflow é inferida.

`request-approval` retorna as representações de Task e Approval juntas, com
`Task-ETag` e `Approval-ETag`. Decisões retornam a Approval e seu ETag; `release`
retorna a Task e seu ETag. Todos os quatro comandos exigem `If-Match` e
`Idempotency-Key`.

As identidades `requested_by`, `decided_by` e `actor_id` são autodeclaradas no
MVP. Elas definem autoria e escopo idempotente, mas não representam autenticação
ou autorização verificadas.

## Inbox de agente

`GET /agents/{agent_id}/inbox` deriva a atenção ativa diretamente de Task e
Approval. A resposta reúne Tasks `ready` destinadas ao agente, Tasks `running`
atualmente atribuídas a ele e Approvals `pending` destinadas a ele. Cada item
inclui a representação atual, correlação, versão, ETag e apenas as ações fixas
que os comandos existentes aceitam naquele estado.

Os itens são ordenados do mais antigo para o mais novo pelo instante em que
passaram a exigir atenção, com tipo e ID como desempate determinístico. Tasks e
Approvals terminais desaparecem da inbox.

Uma Task `ready` com `assigned_to` está reservada para essa identidade e não
pode ser reivindicada por outra. Trabalho sem `assigned_to` não pertence a uma
inbox pessoal; ele continua reivindicável quando seu ID é conhecido, mas este
slice não introduz um pool global de descoberta.

A consulta usa polling HTTP simples e não mantém uma tabela de inbox. Como não
há autenticação no MVP, consultar `/agents/dex/inbox` não comprova que o cliente
é Dex; `agent_id` continua sendo identidade autodeclarada.

## Agent Runner

O runner é um processo separado que consulta a inbox, reivindica Tasks e entrega
um `ExecutionRequest` a qualquer implementação do contrato `WorkExecutor`. O
AgentBus continua sendo a autoridade do lifecycle; o journal SQLite do runner
guarda apenas ownership comprovado, `execution_id`, resultado ainda não reportado
e confirmação terminal.

O executor deve ser idempotente ou retomável por `execution_id`. Se o processo
cair durante um efeito externo antes de persistir o resultado, o runner chamará
o executor novamente com o mesmo ID. Isso não promete exatamente-uma-vez
universal para efeitos externos.

O runner processa estruturalmente apenas itens `kind=task`; seu cliente não
oferece comandos de decisão de Approval. Consulte
[`docs/agent-runner.md`](docs/agent-runner.md) para contrato, recuperação e
operação.

`LocalCodexExecutor` é o primeiro adapter real. Ele usa o SDK oficial do Codex
com o login ChatGPT já existente, sem API key, em workspace descartável com
rede desabilitada e approvals negadas. Esse caminho consome os limites do plano
Codex e não representa capacidade ilimitada. `workspace-write` limita escrita,
mas não oferece confidencialidade contra leitura de outros arquivos acessíveis
ao processo; o adapter deste slice aceita somente Tasks confiáveis/controladas.
Contrato, fronteira de segurança, recuperação e smoke opt-in estão em
[`docs/local-codex-executor.md`](docs/local-codex-executor.md).

## Testes

```bash
pytest
```
