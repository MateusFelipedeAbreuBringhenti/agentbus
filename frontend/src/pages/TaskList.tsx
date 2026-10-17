import { FormEvent, useCallback, useState } from "react";
import { Link } from "react-router-dom";
import { listTasks, type TaskFilters } from "../api";
import { EmptyState, ErrorState, Loading } from "../components/AsyncState";
import { StatusBadge } from "../components/StatusBadge";
import { useLoad } from "../hooks/useLoad";
import type { TaskStatus } from "../types";

const statuses: TaskStatus[] = ["ready", "running", "waiting_approval", "succeeded", "failed", "cancelled"];

export function TaskList() {
  const [draft, setDraft] = useState<TaskFilters>({});
  const [filters, setFilters] = useState<TaskFilters>({});
  const [cursor, setCursor] = useState<string | undefined>();
  const [history, setHistory] = useState<string[]>([]);
  const load = useCallback((signal: AbortSignal) => listTasks({ ...filters, cursor, limit: 15 }, signal), [filters, cursor]);
  const { data, error, loading, retry } = useLoad(load, [load]);

  function apply(event: FormEvent) {
    event.preventDefault();
    setFilters(draft);
    setCursor(undefined);
    setHistory([]);
  }

  function next() {
    if (!data?.next_cursor) return;
    setHistory((items) => [...items, cursor ?? ""]);
    setCursor(data.next_cursor);
  }

  function previous() {
    const prior = history.at(-1);
    setHistory((items) => items.slice(0, -1));
    setCursor(prior || undefined);
  }

  return <>
    <section className="page-heading"><div><small>ORQUESTRAÇÃO · CATÁLOGO REAL</small><h1>Gerenciamento de Tarefas</h1><p>Inspecione o estado persistido sem executar comandos.</p></div><span className="readonly-pill">SOMENTE LEITURA</span></section>
    <form className="filters" onSubmit={apply} aria-label="Filtros de tarefas">
      <input aria-label="Agente atribuído" placeholder="Agente atribuído" value={draft.assignedTo ?? ""} onChange={(e) => setDraft({ ...draft, assignedTo: e.target.value || undefined })} />
      <input aria-label="Tipo" placeholder="Tipo da Task" value={draft.type ?? ""} onChange={(e) => setDraft({ ...draft, type: e.target.value || undefined })} />
      <select aria-label="Status" value={draft.status ?? ""} onChange={(e) => setDraft({ ...draft, status: (e.target.value || undefined) as TaskStatus | undefined })}>
        <option value="">Todos os estados</option>{statuses.map((status) => <option key={status} value={status}>{status}</option>)}
      </select>
      <button type="submit">Aplicar filtros</button>
    </form>
    {loading && <Loading />}
    {error && <ErrorState message={error} retry={retry} />}
    {!loading && !error && data?.items.length === 0 && <EmptyState title="Nenhuma Task encontrada" message="O AgentBus respondeu normalmente, mas não há registros para estes filtros." />}
    {!loading && !error && data && data.items.length > 0 && <section className="table-card">
      <div className="table-title"><span>EXECUÇÕES PERSISTIDAS</span><b>{data.items.length} nesta página</b></div>
      <div className="task-table" role="table">
        <div className="task-row task-header" role="row"><span>Tarefa</span><span>Agente</span><span>Status</span><span>Versão</span><span>Atualização</span></div>
        {data.items.map((task) => <Link className="task-row" role="row" to={`/tasks/${task.id}`} key={task.id}>
          <span><strong>{task.title}</strong><small>{task.type} · {task.id.slice(0, 8)}</small></span>
          <span className="mono">{task.assigned_to ?? "não atribuída"}</span>
          <span><StatusBadge status={task.status} /></span>
          <span className="mono">v{task.version}</span>
          <span>{new Date(task.updated_at).toLocaleString("pt-BR")}</span>
        </Link>)}
      </div>
      <div className="pagination"><button disabled={history.length === 0} onClick={previous}>← Anterior</button><span>Página {history.length + 1}</span><button disabled={!data.next_cursor} onClick={next}>Próxima →</button></div>
    </section>}
  </>;
}
