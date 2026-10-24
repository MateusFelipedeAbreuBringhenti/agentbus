# End-to-End Operational Readiness — Slice #009G-C

O ensaio comprova a integração local já existente; não adiciona workflow,
scheduler, autoridade ou recuperação automática. Todos os bancos, tokens,
políticas e journals pertencem a um diretório temporário isolado.

## Fluxo comprovado

```text
Orion-ops cria Task e solicita Approval
→ navegador registra intenção sem autoridade
→ aprovador confirma em processo separado
→ reconciliador libera com causalidade autenticada
→ Runner reivindica sob política central e allowlist local
→ deterministic-noop devolve resultado sem efeito externo
→ Events e quatro journals aparecem na projeção e no gateway
```

O teste automatizado usa uma identidade fictícia exclusiva e fornece a palavra
de confirmação pelo harness. Isso comprova o protocolo, não participação humana.
A demonstração operacional manual continua exigindo que uma pessoa leia o aviso
e digite `APROVAR` no `agentbus-approval-confirm`.

## Executor E2E

`DeterministicNoopExecutor` aceita somente `palace.e2e.noop`. O conteúdo de
`Task.input` é tratado como dados canônicos: o executor calcula seu SHA-256 e
devolve `status`, `input_sha256` e `execution_id`. Ele não interpreta comandos,
não abre arquivos e não usa rede ou shell.

O protocolo continua `at-least-once`. O executor é retomável pelo
`execution_id`, mas isso não promete exactly-once para executores futuros com
efeitos externos.

## Evidências separadas

- `tests/test_e2e_operational_readiness.py` atravessa os contratos reais em
  processo, sem editar SQLite ou chamar serviços de domínio diretamente.
- `tests/test_operational_smoke_live.py` sobe AgentBus, gateway, intent service,
  projeção e Vite em portas dinâmicas. Confirmação, release e Runner são processos
  separados; o Runner usa o CLI publicado. Chromium é obrigatório e observa a
  mesma Task concluída, seu trace e os quatro journals.
- `tests/test_control_center_proxy_live.py` preserva a prova menor do caminho
  Vite → gateway → AgentBus.
- os testes unitários existentes de UI continuam sendo testes de renderização
  com respostas simuladas e não são apresentados como E2E.

## Limites

O ensaio não comprova isolamento contra processos do mesmo usuário Unix,
operação remota, disponibilidade contínua, payload arbitrário ou exatamente uma
vez para efeitos externos. Nenhum serviço deve ser exposto por túnel.
