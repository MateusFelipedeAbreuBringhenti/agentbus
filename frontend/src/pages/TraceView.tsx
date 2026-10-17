import { FormEvent, useCallback, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { getTrace } from "../api";
import { EmptyState, ErrorState, Loading } from "../components/AsyncState";
import { StatusBadge } from "../components/StatusBadge";
import { useLoad } from "../hooks/useLoad";

function LoadedTrace({ correlationId }: { correlationId: string }) {
  const load = useCallback((signal: AbortSignal) => getTrace(correlationId, signal), [correlationId]);
  const { data, error, loading, retry } = useLoad(load, [load]);
  if (loading) return <Loading label="Reconstruindo a projeção causal…" />;
  if (error) return <ErrorState message={error} retry={retry} />;
  if (!data || (data.tasks.length === 0 && data.events.length === 0)) return <EmptyState title="Correlação sem fatos" message="A consulta foi bem-sucedida, mas nenhuma Task ou Event pertence a esta correlação." />;

  return <div className="trace-layout">
    <section className="panel"><h2>Tasks ({data.tasks.length})</h2><div className="trace-tasks">{data.tasks.map((task) => <Link to={`/tasks/${task.id}`} className="trace-task" key={task.id}><small>v{task.version} · {task.type}</small><strong>{task.title}</strong><StatusBadge status={task.status} /></Link>)}</div></section>
    <section className="panel"><h2>Eventos append-only ({data.events.length})</h2><ol className="timeline">{data.events.map((event) => <li key={event.id}><span className="event-sequence">{event.sequence}</span><div><strong>{event.type}</strong><p>{event.actor_id} · {new Date(event.occurred_at).toLocaleString("pt-BR")}</p><small>Task {event.task_id.slice(0, 8)} · causa {event.causation_event_id?.slice(0, 8) ?? "raiz"}</small></div></li>)}</ol></section>
  </div>;
}

export function TraceView() {
  const { correlationId } = useParams();
  const navigate = useNavigate();
  const [value, setValue] = useState(correlationId ?? "");
  function submit(event: FormEvent) { event.preventDefault(); if (value.trim()) navigate(`/correlations/${value.trim()}`); }
  return <>
    <section className="page-heading"><div><small>OBSERVABILIDADE · FATOS PERSISTIDOS</small><h1>Rastreamento Causal</h1><p>Tasks e Events da mesma correlação, sem inferir política de workflow.</p></div></section>
    <form className="trace-search" onSubmit={submit}><input aria-label="Correlation ID" placeholder="Cole um correlation_id" value={value} onChange={(event) => setValue(event.target.value)} /><button>Consultar trace</button></form>
    {correlationId ? <LoadedTrace correlationId={correlationId} /> : <EmptyState title="Informe uma correlação" message="Abra uma Task e siga o vínculo de correlação, ou informe o UUID acima." />}
  </>;
}
