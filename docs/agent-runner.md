# Agent Runner

O Agent Runner conecta três fronteiras independentes:

```text
AgentBus ←HTTP→ AgentRunner ←ExecutionRequest/Result→ WorkExecutor
```

`HttpAgentBusClient` conhece o protocolo HTTP. `AgentRunner` conhece polling,
recuperação e idempotência. `WorkExecutor` conhece apenas como executar um tipo
de trabalho; não recebe banco, credenciais do AgentBus ou comandos de Approval.

## Identidade e journal

Cada journal cria uma vez e preserva um `runner_instance_id`. Um lock exclusivo
no arquivo impede duas cópias de abrirem o mesmo journal simultaneamente.
Instâncias diferentes usam journals e chaves de claim diferentes.

O lock é implementado com `fcntl.flock`. Portanto, o Runner deste slice requer
um ambiente POSIX compatível, como Linux, e não oferece suporte a Windows. Uma
abstração de locking multiplataforma fica deliberadamente fora deste slice.

Uma linha de execução preserva somente fatos locais:

- `claim_key` e `execution_id`, gravados antes do claim;
- `claimed_etag`, preenchido somente após sucesso ou replay da própria chave;
- `claim_rejected_at`, prova de que outra instância venceu;
- resultado e `report_key`, gravados juntos antes de complete/fail;
- `reported_at`, confirmação durável sem apagar o histórico.

Não existe enum de fase. O estado da Task continua vindo do AgentBus.

## Recuperação

| Interrupção | Recuperação |
|---|---|
| Antes do claim | Uma linha preparada reenvia a mesma `claim_key`. Sem linha, uma Task `running` nunca é adotada. |
| Resposta do claim perdida | Reenvia a própria chave. Replay 200 prova ownership; 409 grava rejeição e encerra a tentativa. |
| Antes ou durante o executor | Confirma que a Task ainda está `running` no `claimed_etag` e chama novamente com o mesmo `execution_id`. |
| Depois do resultado | O resultado persistido é reportado sem chamar novamente o executor. |
| Resposta de complete/fail perdida | Reenvia exatamente payload, ETag e `report_key`; o AgentBus devolve replay. |
| Depois da confirmação | Preserva a linha com `reported_at`. Retenção e limpeza ficam para outro slice. |

Antes de chamar o executor, o runner consulta a Task atual. Status ou ETag
divergente interrompe a execução local; `assigned_to` isoladamente nunca prova
ownership.

## Executor

O contrato recebe `ExecutionRequest` com `execution_id`, Task, input, correlação,
ETag e identidade lógica do agente. Ele devolve `ExecutionSucceeded(output)` ou
`ExecutionFailed(failure_code, failure_message)`.

O executor é responsável por tornar efeitos externos idempotentes ou retomáveis
usando `execution_id`. Uma exception não vira automaticamente `Task.failed`:
sem resultado explícito, o journal permanece recuperável e o executor poderá ser
chamado novamente.

`DeterministicWorkExecutor` existe apenas para demonstrar o protocolo. Ele não
usa modelo ou fornecedor externo.

## Limites

O runner não decide Approvals, não adota Tasks órfãs, não implementa lease,
heartbeat, scheduler, retry de Task, autenticação, push ou coordenação distribuída.
Identidades permanecem autodeclaradas; impedir impersonação na API exige uma
fronteira de autenticação futura.
