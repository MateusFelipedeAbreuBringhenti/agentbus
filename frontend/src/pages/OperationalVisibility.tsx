import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getOperation, listOperations } from "../api";
import { EmptyState, ErrorState, Loading } from "../components/AsyncState";
import { useLoad } from "../hooks/useLoad";
import type { OperationKind } from "../types";

export function OperationalVisibility() {
  const [kind, setKind] = useState<OperationKind | "">("");
  const [cursor, setCursor] = useState<string | undefined>();
  const load = useCallback((signal: AbortSignal) => listOperations({ kind: kind || undefined, cursor, limit: 20 }, signal), [kind, cursor]);
  const { data, error, loading, retry } = useLoad(load, [load]);
  return <>
    <section className="page-heading"><div><small>OPERAÇÕES · EVIDÊNCIA PERSISTIDA</small><h1>Recuperação e visibilidade</h1><p>Estados factuais dos journals. Idade não representa diagnóstico de travamento.</p></div><span className="readonly-pill">SOMENTE LEITURA</span></section>
    <div className="warning-banner"><strong>Visibilidade ≠ autorização</strong> Esta tela não recupera, reenvia ou altera operações.</div>
    <div className="source-strip">{data?.sources.map((source) => <span className={`source-${source.status}`} key={source.source_id}>{source.source_id}: {source.status}</span>)}</div>
    <form className="filters" onSubmit={(event) => { event.preventDefault(); setCursor(undefined); }}><select aria-label="Tipo operacional" value={kind} onChange={(event) => { setKind(event.target.value as OperationKind | ""); setCursor(undefined); }}><option value="">Todas as fontes</option><option value="approval_intent">Approval intents</option><option value="release">Releases</option><option value="runner">Runners</option><option value="outbox">Outbox</option></select><button type="submit">Aplicar</button></form>
    {loading && <Loading />}{error && <ErrorState message={error} retry={retry} />}
    {!loading && !error && data?.items.length === 0 && <EmptyState title="Nenhuma operação encontrada" message="As fontes disponíveis não contêm registros para este filtro." />}
    {!loading && !error && data && data.items.length > 0 && <section className="operation-grid">{data.items.map((item) => <Link className="operation-card" to={`/operations/${item.operation_id}`} key={item.operation_id}><small>{item.kind} · {item.source_id}</small><h2>{item.persisted_state}</h2><p>{item.summary}</p><dl><div><dt>Atenção</dt><dd>{item.attention_class}</dd></div><div><dt>Dono lógico</dt><dd>{item.logical_owner}</dd></div><div><dt>Tempo</dt><dd>{item.timestamp_quality}</dd></div></dl></Link>)}</section>}
    {data && <div className="pagination"><button disabled={!cursor} onClick={() => setCursor(undefined)}>← Início</button><span>Consulta por fonte; sem snapshot global</span><button disabled={!data.next_cursor} onClick={() => setCursor(data.next_cursor ?? undefined)}>Próxima →</button></div>}
  </>;
}

export function OperationalDetail() {
  const { operationId = "" } = useParams();
  const load = useCallback((signal: AbortSignal) => getOperation(operationId, signal), [operationId]);
  const { data, error, loading, retry } = useLoad(load, [load]);
  if (loading) return <Loading />; if (error) return <ErrorState message={error} retry={retry} />; if (!data) return null;
  return <><Link className="back" to="/operations">← Operações</Link><section className="page-heading"><div><small>{data.kind} · {data.source_id}</small><h1>{data.persisted_state}</h1><p>{data.summary}</p></div><span className="readonly-pill">{data.attention_class}</span></section><section className="panel"><h2>Evidência pública</h2><dl><div><dt>Task</dt><dd>{data.task_id ?? "não comprovada"}</dd></div><div><dt>Approval</dt><dd>{data.approval_id ?? "não aplicável"}</dd></div><div><dt>Correlação</dt><dd>{data.correlation_id ?? "evidência insuficiente"}</dd></div><div><dt>Recuperação</dt><dd>{data.recovery_mode}</dd></div><div><dt>Qualidade temporal</dt><dd>{data.timestamp_quality}</dd></div><div><dt>Evidências</dt><dd>{data.evidence.join(", ")}</dd></div></dl></section></>;
}
