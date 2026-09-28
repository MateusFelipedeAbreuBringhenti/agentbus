# Local Codex Executor

`LocalCodexExecutor` conecta o contrato neutro `WorkExecutor` ao Codex local já
autenticado por uma conta ChatGPT. O AgentBus e o Runner não importam nem
conhecem Codex.

## Interface escolhida

O adapter usa o SDK Python oficial `openai-codex` sobre o app-server local. O
SDK é a superfície oficial indicada para automação Python e inclui uma versão
compatível do runtime. A dependência está limitada à série `0.158` porque a
ponte de recuperação usa os modelos gerados do SDK para conservar o estado de
turn retornado pelo app-server; a API pública dessa versão descarta esse estado
e `thread/turns/list` ainda não é suportado pelo runtime observado.

Não usamos subprocesso `codex exec`, não fixamos modelo e não criamos
credenciais. O provider exige que `account/read` indique autenticação ChatGPT.
Se `OPENAI_API_KEY` ou `CODEX_API_KEY` existir no processo, o adapter falha antes
de iniciar uma execução para impedir que este caminho use billing de API.

## Isolamento

Cada `execution_id` recebe um diretório próprio abaixo de um `workspace_root`
fora de `$HOME`. O adapter inicia Codex com:

- workspace `cwd` apontando para esse diretório descartável;
- sandbox `workspace-write`;
- rede do sandbox desabilitada;
- approvals negadas;
- modelo escolhido normalmente pelo Codex;
- no máximo oito arquivos inline, com nome simples e 64 KiB cada.

O repositório AgentBus nunca é copiado para o workspace e nenhuma integração de
GitHub é oferecida. O sandbox restringe escrita ao workspace; para tarefas não
confiáveis que exijam também uma barreira de leitura no nível do sistema
operacional, uma camada externa de isolamento continua necessária.

## Persistência e recuperação

O SQLite do adapter mantém `execution_id`, hash imutável do pedido, workspace,
`thread_id`, `turn_id` e resultado validado. IDs são gravados assim que o SDK os
devolve. Resultado confirmado é preservado e reproduzido sem chamar o Codex.

| Estado após restart | Conduta |
|---|---|
| Resultado persistido | Devolver exatamente o resultado local. |
| Thread e turn conhecidos, turn concluído | Ler, validar e persistir o resultado. |
| Thread conhecida, exatamente um turn remoto não registrado | Adotar esse turn e continuar a recuperação. |
| Turn ainda ativo | Parar; não iniciar outro turn. |
| Mais de um turn, ID divergente ou ausência inesperada | Parar como estado ambíguo. |
| Linha preparada sem `thread_id` | Parar: a resposta de criação pode ter sido perdida. |

Uma thread vazia ainda não possui rollout recuperável no runtime observado. Por
isso o handle fica vivo até o primeiro turn no mesmo processo; crash entre a
criação da thread e a persistência do ID resulta em parada segura, não em nova
execução. O slice não promete exactly-once universal.

## Resultado

O modelo só pode devolver o schema fechado:

```json
{"status":"succeeded","output":{"result":"..."},"failure_code":null,"failure_message":null}
```

ou a variante `failed`, com `output: null` e os dois campos de falha. Pydantic
valida o conteúdo antes de convertê-lo em `ExecutionSucceeded` ou
`ExecutionFailed`.

## Uso e custo

O login ChatGPT usa os limites e a franquia aplicáveis ao plano Codex da conta.
Isso evita cobrança adicional obrigatória de API, mas não significa uso
ilimitado nem garante disponibilidade quando a franquia estiver esgotada.

O smoke real é deliberadamente opt-in e executa o heartbeat completo:

```bash
env -u OPENAI_API_KEY -u CODEX_API_KEY \
  AGENTBUS_RUN_LOCAL_CODEX_SMOKE=1 \
  pytest tests/test_local_codex_executor.py::test_real_local_codex_full_heartbeat
```

Ele cria uma Task destinada a `dex`, passa por inbox, claim, Codex real em
workspace temporário, valida `combined.txt`, reporta complete e exige Task
terminal `succeeded`. A suíte normal usa provider determinístico e não consome
franquia.

Referências oficiais: [Codex SDK](https://learn.chatgpt.com/docs/codex-sdk),
[app-server](https://learn.chatgpt.com/docs/app-server),
[autenticação](https://learn.chatgpt.com/docs/auth),
[sandbox](https://learn.chatgpt.com/docs/sandboxing) e
[preços/limites](https://learn.chatgpt.com/docs/pricing).
