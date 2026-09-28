# OpenAI Agents Executor

`OpenAIAgentsExecutor` é um adapter opcional do contrato vendor-neutral
`WorkExecutor`. AgentBus e AgentRunner não importam nem conhecem OpenAI.

O adapter usa a Agents API com Codex harness e ambiente:

```json
{
  "type": "openai_hosted",
  "network": {"access": "disabled"}
}
```

A configuração não inclui variáveis de ambiente, vaults, plugins, MCPs ou
arquivos do notebook. A Task pode fornecer apenas até oito pequenos arquivos de
texto, copiados para `/workspace/inputs` com nomes estritamente saneados. Ela
não controla modelo, instruções administrativas, ferramentas ou sandbox.

Documentação oficial relevante:

- [Agents API quickstart](https://developers.openai.com/api/docs/guides/agents-api/quickstart)
- [OpenAI-hosted sandboxes](https://developers.openai.com/api/docs/guides/agents-api/environments/openai-hosted)
- [Function tools](https://developers.openai.com/api/docs/guides/agents-api/tools/functions)

## Credencial

O único segredo deste caminho hosted é `OPENAI_API_KEY`, mantido no ambiente do
processo e fora do sandbox, repositório, journal e logs. A chave precisa das
permissões `api.agents.read`, `api.agents.write` e `api.responses.write`.

`CODEX_API_KEY` e environment keys pertencem exclusivamente ao executor
self-hosted e não são usados neste slice.

## Recuperação

O store SQLite do adapter preserva:

```text
execution_id → provider_session_id → structured result → tool acknowledgement
```

A sessão recebe `execution_id` também em metadata. Se a resposta de criação se
perder, a próxima invocação procura uma sessão com essa metadata e só continua
quando encontra exatamente uma. Zero ou múltiplas correspondências interrompem
a execução como ambígua; o adapter nunca cria outra sessão silenciosamente.

O agente deve finalizar chamando `submit_result`. Os argumentos são validados
antes de virarem `ExecutionSucceeded` ou `ExecutionFailed`. O resultado é salvo
antes do acknowledgement da function tool; uma resposta perdida é reenviada
com a mesma chave idempotente.

## Testes

Os testes normais usam um provider determinístico e não consomem API.

O smoke real é opt-in:

```bash
AGENTBUS_RUN_OPENAI_SMOKE=1 pytest -m live tests/test_openai_executor.py
```

Não coloque a chave no código, no chat, em `.env` versionado ou na linha de
comando registrada pelo shell. Configure-a por um mecanismo seguro do ambiente.
Sem a credencial e o opt-in explícito, o smoke é marcado como skipped.
