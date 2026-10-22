# Controlled execution handoff

O handoff entre uma Task `ready` e um executor usa duas políticas independentes.
O AgentBus aplica uma política central fechada por `subject_id` e tipo de Task;
o Runner aplica uma allowlist local que escolhe quais tipos seu `WorkExecutor`
pode receber. Uma chamada HTTP direta não contorna a política central.

## Política central

`AGENTBUS_EXECUTION_POLICY` aponta para um arquivo absoluto `0600`, carregado e
validado uma vez no startup:

```json
{
  "policy_id": "palace-execution-v1",
  "subjects": {
    "dex": ["code.review", "artifact.validate"]
  }
}
```

Subjects ou tipos ausentes são negados. O AgentBus persiste no claim o
`execution_id`, o `runner_instance_id` declarado, o subject e principal
autenticados e o id/hash da política. Os mesmos fatos aparecem no evento
`task.started`; os eventos terminais registram a política do claim e a política
vigente no reporte.

## Replay e rotação

Complete/fail validam subject, Task e vínculo da execução antes de procurar o
replay. Um replay exato já confirmado é devolvido mesmo se a política vigente
tiver revogado o tipo ou se a credencial tiver sido rotacionada para outro
principal do mesmo subject. Uma mutação nova sempre exige a política vigente.
Chaves continuam isoladas por subject; outro subject não pode recuperar a
resposta.

Tasks históricas `running` sem proveniência estruturada não são preenchidas
retroativamente. Novos reportes para elas falham com
`execution_provenance_required`; replays históricos já persistidos permanecem
recuperáveis.

## Garantias e limites

- `execution_id` é único entre Tasks e identifica a tentativa recuperável.
- `runner_instance_id` deve coincidir com o claim, mas é um identificador
  declarado, não uma identidade criptográfica.
- O protocolo oferece execução `at-least-once`; efeitos externos precisam usar
  `execution_id` para idempotência ou retomada.
- Conflitos terminais ficam persistidos no journal do Runner e aparecem na
  projeção operacional. Falhas de transporte permanecem recuperáveis.
- `401` persiste `AUTH_REQUIRED` sem alterar o comando. Uma autenticação posterior
  do mesmo subject reabilita a tentativa com os mesmos IDs, chave, corpo e ETag;
  `403` e conflitos permanentes ficam em `STOPPED`.
- Não há scheduler, broker, executor arbitrário nem adoção administrativa de
  Tasks históricas neste slice.
