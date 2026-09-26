# PR #3 — revisão adversarial

Base revisada: `e1db24c8d1aafcf0021b932b2a2df6966b9adc42`.
Escopo: invariantes do Slice #003 e compatibilidade com dados do Slice #002.

## Achados confirmados

### Resolvido por decisão de Orion: identidade explícita da espera

Reprodução usando somente endpoints públicos, sem adulterar o banco:

1. Criar Task: `ready`, v1.
2. Pedir Approval A para `deployment.production`: Task `waiting_approval`, v2.
3. Aprovar A; liberar Task com A: Task `ready`, v3.
4. Pedir Approval B para `different.gate`, usando `If-Match: "v3"`:
   retorna 200, Task `waiting_approval`, v4, B `pending`.
5. Executar release novamente com A, outra chave e `If-Match: "v4"`:
   retorna 200, Task `ready`, v5, enquanto B continua `pending`.

A validação atual comprova Task, correlação e status de A, mas não que A
autorize a espera atual. O argumento de que sair de `waiting_approval` impede
reutilização só vale até o próximo pedido. Esse caminho já está acessível por
`request-approval`, embora re-request tenha sido excluído do slice.

Orion escolheu vincular explicitamente cada espera à Approval que a originou.
A Task agora expõe `waiting_on_approval_id`. `request-approval` insere a
Approval e estabelece o vínculo na mesma transação; decisões não o alteram;
`release` aceita somente o ID corrente e limpa o vínculo ao retornar a Task
para `ready`. A tentativa com Approval antiga retorna
`409 approval_not_current_for_task` sem alterar Task ou Approval atual.

O antigo `xfail(strict=True)` virou teste normal e também prova o ciclo seguinte:
A aprovada/liberada, B solicitada, A recusada como stale, B ainda pending e
vinculada, seguida de aprovação/liberação válida de B.

### Migração v2 → v3

A migration incremental adiciona o vínculo e preserva todos os demais dados.
Uma Task v2 em espera é preenchida quando existe exatamente uma Approval
`pending` da mesma Task e correlação — inclusive se houver histórico terminal —
ou, sem pending, quando existe uma única Approval total. Zero candidatas,
múltiplas pending ou múltiplas candidatas terminais tornam a relação ambígua;
nesses casos a migration aborta e reverte por inteiro, sem escolher por gate,
versão, último evento ou ordem de criação.

### Corrigido: upgrade invalida hashes idempotentes anteriores

O PR renomeou a chave interna serializada do hash de `task_id` para
`resource_id`. Requests de claim/complete/fail previamente confirmados em v1
passavam a retornar `409 idempotency_key_reused` após upgrade para v2.

Correção: preservar `task_id` no hash dos três comandos do Slice #002.
Os novos comandos mantêm seu formato. Nenhum registro persistido foi reescrito.
Testes montam registros com o formato efetivamente usado na main, migram v1
para v2 e verificam replay original mesmo após transição terminal.

### Corrigido: corrida entre migradores

Dois migradores podiam observar v2 ausente antes de obter o lock; o segundo
falhava com `sqlite3.OperationalError: table approvals already exists`.

Correção: checar a versão depois de `BEGIN IMMEDIATE` e executar DDL e registro
da versão na mesma transação. Statements completos são executados individualmente
porque `executescript` encerra implicitamente uma transação anterior. A conexão
de migração agora também é fechada explicitamente.

Teste com barreira reproduziu a corrida antes da correção. Falha no final da
migration comprova rollback do schema, ausência do marcador v2 e retry seguro.

## Invariantes procuradas e verificadas

- Pedido concorrente com chaves distintas: uma aquisição; outra retorna 409.
- Mesmo pedido/chave concorrente: uma escrita e um replay.
- Approve × reject em duas instâncias da API/conexões SQLite independentes:
  uma decisão; a outra retorna conflito; Task.version fica intacta.
- Release concorrente: uma reação, incremento único, sequência sem duplicação.
- Replay de request/approve/release após release e claim: bytes da resposta e
  ETags originais; snapshot de todas as tabelas de domínio inalterado.
- Decisor, não solicitante, determina scope idempotente de approve/reject.
- Causas inexistentes e de correlação diferente são rejeitadas antes de mutação;
  causa de outra Task na mesma correlação é aceita conforme contrato.
- Approval rejeitada e Approval de outra Task mesmo na mesma correlação não
  liberam a Task. Testes existentes cobrem correlação divergente e inexistência.
- Partial unique index rejeita dois pending do mesmo gate; permite histórico
  terminal do mesmo gate e pending em outro gate.
- SQLite rejeita duplicação de `(command_id, event_index)` mesmo com id e
  sequence distintos. Os dois eventos do pedido usam índices 0 e 1.
- Rollback por falha em UPDATE de Task, INSERT/UPDATE de Approval, primeiro e
  segundo eventos, INSERT de idempotência e commit; retry usa a mesma chave.

### Caso solicitado por Orion: UPDATE antes do INSERT rejeitado pelo índice

Teste semeia deliberadamente a condição ready + Approval pending existente
para alcançar o ramo normalmente bloqueado pela máquina de estados. O comando
atualiza a Task, tenta inserir a Approval e recebe a violação do índice parcial.
A resposta é `409 pending_approval_conflict`. Snapshot completo antes/depois de
Task, Approval, Event e idempotência é idêntico, incluindo versões e timestamps.
Não houve estado parcial: a ordem atual das operações está protegida pelo rollback.

## Resultado

O vínculo explícito tem cobertura de fluxo, rejeição, replay, rollback,
persistência direta e upgrade v2→v3, inclusive rollback de migration ambígua.
Resultado final e SHA publicados no relatório de entrega. Nenhum Slice #004
foi iniciado.
