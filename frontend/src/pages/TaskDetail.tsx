import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";
import { getTask } from "../api";
import { EmptyState, ErrorState, Loading } from "../components/AsyncState";
import { StatusBadge } from "../components/StatusBadge";
import { useLoad } from "../hooks/useLoad";

function JsonBlock({ value }: { value: unknown }) {
  return <pre>{JSON.stringify(value, null, 2)}</pre>;
}

export function TaskDetail() {
  const { taskId = "" } = useParams();
  const load = useCallback((signal: AbortSignal) => getTask(taskId, signal), [taskId]);
  const { data: task, error, loading, retry } = useLoad(load, [load]);
  if (loading) return <Loading label="Carregando a Task persistida…" />;
  if (error) return <ErrorState message={error} retry={retry} />;
  if (!task) return <EmptyState title="Task ausente" message="Nenhuma representação foi devolvida." />;

  return <>
    <Link className="back" to="/tasks">← Voltar ao catálogo</Link>
    <section className="page-heading"><div><small>TASK · {task.id}</small><h1>{task.title}</h1><p>{task.description ?? "Sem descrição."}</p></div><StatusBadge status={task.status} /></section>
    <section className="detail-grid">
      <article className="panel"><h2>Identidade e estado</h2><dl>
        <div><dt>Tipo</dt><dd>{task.type}</dd></div><div><dt>Versão / ETag</dt><dd className="mono">v{task.version} / "v{task.version}"</dd></div>
        <div><dt>Solicitante</dt><dd>{task.requested_by}</dd></div><div><dt>Atribuída a</dt><dd>{task.assigned_to ?? "—"}</dd></div>
        <div><dt>Correlação</dt><dd><Link to={`/correlations/${task.correlation_id}`}>{task.correlation_id}</Link></dd></div>
        <div><dt>Aprovação atual</dt><dd>{task.waiting_on_approval_id ?? "—"}</dd></div>
      </dl></article>
      <article className="panel"><h2>Proveniência da execução</h2><dl>
        <div><dt>Execução</dt><dd className="mono">{task.execution_id ?? "Sem evidência histórica"}</dd></div>
        <div><dt>Instância declarada</dt><dd className="mono">{task.runner_instance_id ?? "—"}</dd></div>
        <div><dt>Subject autenticado</dt><dd>{task.execution_subject_id ?? "—"}</dd></div>
        <div><dt>Principal do claim</dt><dd>{task.claimed_principal_id ?? "—"}</dd></div>
        <div><dt>Política no claim</dt><dd>{task.claim_policy_id ?? "—"}</dd></div>
      </dl><p className="muted">A instância é proveniência declarada; não é uma identidade criptográfica.</p></article>
      <article className="panel"><h2>Linha do tempo</h2><dl>
        <div><dt>Criada</dt><dd>{new Date(task.created_at).toLocaleString("pt-BR")}</dd></div><div><dt>Atualizada</dt><dd>{new Date(task.updated_at).toLocaleString("pt-BR")}</dd></div>
        <div><dt>Iniciada</dt><dd>{task.started_at ? new Date(task.started_at).toLocaleString("pt-BR") : "—"}</dd></div><div><dt>Finalizada</dt><dd>{task.finished_at ? new Date(task.finished_at).toLocaleString("pt-BR") : "—"}</dd></div>
      </dl></article>
      <article className="panel"><h2>Entrada</h2><JsonBlock value={task.input} /></article>
      <article className="panel"><h2>Resultado</h2>{task.output ? <JsonBlock value={task.output} /> : <p className="muted">Nenhum resultado persistido.</p>}
        {task.failure_code && <div className="failure"><strong>{task.failure_code}</strong><p>{task.failure_message}</p></div>}</article>
    </section>
  </>;
}
